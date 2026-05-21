use serde_json::Value;
use tokio::sync::mpsc;

use medousa::{TuiRuntime, events::TuiEvent};
use stasis::application::orchestration::tool_loop_pipeline::{
    ToolCallMode, ToolLoopExecutionRequest,
};
use stasis::prelude::{ChatMessage, PromptExecutionContext};

use super::{ConversationTurn, TuiState};

pub(crate) fn start_prompt_run(
    state: &mut TuiState,
    tui_rt: &TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
    prompt: String,
    persist_user_turn: bool,
) {
    state.is_processing = true;
    state.auto_scroll = true;
    state.conv_scroll = state.conv_max_scroll;
    state.active_agent_stream_turn = None;
    state.in_thinking_tag = false;
    state.stream_tag_tail.clear();

    if persist_user_turn {
        let user_turn = ConversationTurn {
            role: "user".to_string(),
            content: prompt.clone(),
            timestamp: chrono::Utc::now(),
            tool_names: vec![],
        };
        super::append_turn(&state.session_id, &user_turn);
        state.conversation.push(user_turn);
    }

    let pipeline = tui_rt.tool_loop_pipeline.clone();
    let tx = event_tx.clone();
    let prompt_preview: String = prompt.chars().take(48).collect();
    let tool_call_mode = parse_tool_call_mode(&state.settings.tool_call_mode);
    let max_tool_rounds =
        super::parse_usize_with_bounds(&state.settings.max_tool_rounds, 10, 1, 50);
    let prior_messages = build_prior_messages(&state.conversation, &prompt, persist_user_turn);
    let handle = tokio::spawn(async move {
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let chunk_event_tx = tx.clone();
        tokio::spawn(async move {
            while let Some(delta) = chunk_rx.recv().await {
                if chunk_event_tx
                    .send(TuiEvent::AgentChunk { delta })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let _ = tx
            .send(TuiEvent::ToolInvoked {
                tool_name: "llm.chat".to_string(),
                input_summary: prompt_preview,
            })
            .await;

        let request = ToolLoopExecutionRequest {
            user_prompt: prompt,
            system_prompt: Some(super::SYSTEM_PROMPT.to_string()),
            context: PromptExecutionContext::default(),
            tool_name: String::new(),
            tool_input: Value::Null,
            tool_call_mode,
        };
        match pipeline
            .execute_with_stream_prior_messages_max_rounds(
                request,
                prior_messages,
                Some(&chunk_tx),
                max_tool_rounds,
            )
            .await
        {
            Ok(response) => {
                for invocation in &response.tool_invocations {
                    let _ = tx
                        .send(TuiEvent::ToolPayload {
                            tool_name: invocation.tool_name.clone(),
                            tool_input: invocation.tool_input.clone(),
                            tool_output: invocation.tool_output.clone(),
                        })
                        .await;
                }
                let tool_names = response
                    .tool_invocations
                    .iter()
                    .map(|t| t.tool_name.clone())
                    .collect::<Vec<_>>();
                let _ = tx
                    .send(TuiEvent::ToolInvoked {
                        tool_name: "llm.chat".to_string(),
                        input_summary: format!(
                            "done  {} token(s)",
                            response.text.split_whitespace().count()
                        ),
                    })
                    .await;
                let _ = tx
                    .send(TuiEvent::AgentResponse {
                        text: response.text,
                        tool_names,
                    })
                    .await;
            }
            Err(err) => {
                let _ = tx.send(TuiEvent::AgentError(err.to_string())).await;
            }
        }
    });

    state.active_request_task = Some(handle);
}

fn build_prior_messages(
    turns: &[ConversationTurn],
    current_prompt: &str,
    current_user_persisted: bool,
) -> Vec<ChatMessage> {
    const MAX_TURNS: usize = 16;

    let mut selected: Vec<&ConversationTurn> = turns.iter().collect();

    if current_user_persisted {
        if let Some(last) = selected.last() {
            if last.role == "user" && last.content.trim() == current_prompt.trim() {
                selected.pop();
            }
        }
    }

    selected
        .into_iter()
        .rev()
        .take(MAX_TURNS)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .filter_map(|turn| match turn.role.as_str() {
            "user" => Some(ChatMessage::user(turn.content.clone())),
            "assistant" => Some(ChatMessage::assistant(turn.content.clone())),
            _ => None,
        })
        .collect()
}

pub(crate) fn stop_active_generation(state: &mut TuiState) {
    if let Some(task) = state.active_request_task.take() {
        task.abort();
        state.is_processing = false;
        state.active_agent_stream_turn = None;
        super::push_obs(state, "■ generation stopped".to_string());
    }
}

fn parse_tool_call_mode(value: &str) -> ToolCallMode {
    if value.trim().eq_ignore_ascii_case("strict") {
        ToolCallMode::Strict
    } else {
        ToolCallMode::Auto
    }
}
