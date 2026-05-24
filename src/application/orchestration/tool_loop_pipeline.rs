use std::sync::Arc;

use genai::chat::{ChatMessage, ChatRequest, ToolResponse};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::application::orchestration::tool_registry::ToolRegistry;
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::ai_chat_client::StreamDelta;

const DEFAULT_MAX_TOOL_ROUNDS: usize = 10;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub enum ToolCallMode {
    #[default]
    Auto,
    Strict,
}

#[derive(Clone, Debug, Serialize)]
pub struct ToolInvocation {
    pub tool_name: String,
    pub tool_input: Value,
    pub tool_output: Value,
}

#[derive(Clone, Debug)]
pub struct ToolLoopExecutionRequest {
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub context: PromptExecutionContext,
    pub tool_name: String,
    pub tool_input: Value,
    pub tool_call_mode: ToolCallMode,
}

#[derive(Clone, Debug)]
pub struct ToolLoopExecutionResponse {
    pub text: String,
    pub metadata: PromptExecutionContext,
    pub tool_name: String,
    pub tool_output: Value,
    pub tool_invocations: Vec<ToolInvocation>,
    pub rounds_executed: usize,
    pub termination_reason: String,
}

#[derive(Clone)]
pub struct ToolLoopPipeline {
    prompt_pipeline: PromptExecutionPipeline,
    tool_registry: Arc<dyn ToolRegistry>,
}

impl ToolLoopPipeline {
    pub fn new(
        prompt_pipeline: PromptExecutionPipeline,
        tool_registry: Arc<dyn ToolRegistry>,
    ) -> Self {
        Self {
            prompt_pipeline,
            tool_registry,
        }
    }

    pub async fn execute(
        &self,
        request: ToolLoopExecutionRequest,
    ) -> Result<ToolLoopExecutionResponse> {
        self.execute_with_defaults(request, Vec::new(), None).await
    }

    pub async fn execute_with_prior_messages(
        &self,
        request: ToolLoopExecutionRequest,
        prior_messages: Vec<ChatMessage>,
    ) -> Result<ToolLoopExecutionResponse> {
        self.execute_with_defaults(request, prior_messages, None).await
    }

    pub async fn execute_with_stream(
        &self,
        request: ToolLoopExecutionRequest,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
    ) -> Result<ToolLoopExecutionResponse> {
        self.execute_with_defaults(request, Vec::new(), chunk_tx).await
    }

    pub async fn execute_with_stream_prior_messages(
        &self,
        request: ToolLoopExecutionRequest,
        prior_messages: Vec<ChatMessage>,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
    ) -> Result<ToolLoopExecutionResponse> {
        self.execute_with_defaults(request, prior_messages, chunk_tx)
            .await
    }

    pub async fn execute_with_stream_prior_messages_max_rounds(
        &self,
        request: ToolLoopExecutionRequest,
        prior_messages: Vec<ChatMessage>,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
        max_tool_rounds: usize,
    ) -> Result<ToolLoopExecutionResponse> {
        self.execute_internal(request, prior_messages, chunk_tx, max_tool_rounds)
            .await
    }

    async fn execute_with_defaults(
        &self,
        request: ToolLoopExecutionRequest,
        prior_messages: Vec<ChatMessage>,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
    ) -> Result<ToolLoopExecutionResponse> {
        self.execute_internal(request, prior_messages, chunk_tx, DEFAULT_MAX_TOOL_ROUNDS)
            .await
    }

    async fn execute_internal(
        &self,
        request: ToolLoopExecutionRequest,
        prior_messages: Vec<ChatMessage>,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
        max_tool_rounds: usize,
    ) -> Result<ToolLoopExecutionResponse> {
        let ToolLoopExecutionRequest {
            user_prompt,
            system_prompt,
            context,
            tool_name,
            tool_input,
            tool_call_mode,
        } = request;

        let max_tool_rounds = max_tool_rounds.max(1);
        let selected_tool_name = tool_name;
        let has_selected_tool = !selected_tool_name.trim().is_empty();

        let mut messages = Vec::with_capacity(2 + prior_messages.len());
        if let Some(system_prompt) = system_prompt.as_ref() {
            messages.push(ChatMessage::system(system_prompt.clone()));
        }
        messages.extend(prior_messages);
        messages.push(ChatMessage::user(user_prompt.clone()));

        let mut tools = self.tool_registry.list_tools().await?;
        if has_selected_tool {
            let selected_sanitized = sanitize_tool_name_for_model(&selected_tool_name);
            let selected_prefix = format!("{selected_sanitized}_");
            tools.retain(|tool| {
                tool.name == selected_tool_name
                    || tool.name == selected_sanitized
                    || tool.name.starts_with(&selected_prefix)
            });
        }

        let mut invocations = Vec::new();
        let mut should_use_legacy_fallback = false;
        let mut fallback_draft_text: Option<String> = None;
        let mut rounds_executed = 0usize;
        if !tools.is_empty() {
            for _ in 0..max_tool_rounds {
                rounds_executed += 1;
                let chat_request = ChatRequest::new(messages.clone()).with_tools(tools.clone());
                let completion = match chunk_tx {
                    Some(tx) => {
                        self.prompt_pipeline
                            .complete_chat_stream(chat_request, context.clone(), Some(tx))
                            .await?
                    }
                    None => {
                        self.prompt_pipeline
                            .complete_chat(chat_request, context.clone())
                            .await?
                    }
                };
                let response = completion.response;
                let maybe_text = response
                    .first_text()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
                let tool_calls = response.clone().into_tool_calls();

                if tool_calls.is_empty() {
                    if invocations.is_empty() && has_selected_tool {
                        if tool_call_mode == ToolCallMode::Strict {
                            return Err(StasisError::PortFailure(
                                "policy violation: strict tool-call mode expected model tool call but none was returned"
                                    .to_string(),
                            ));
                        }

                        should_use_legacy_fallback = true;
                        fallback_draft_text = maybe_text;
                        break;
                    }

                    if let Some(text) = maybe_text {
                        let last = invocations.last().cloned().unwrap_or(ToolInvocation {
                            tool_name: selected_tool_name.clone(),
                            tool_input: tool_input.clone(),
                            tool_output: Value::Null,
                        });

                        return Ok(ToolLoopExecutionResponse {
                            text,
                            metadata: context,
                            tool_name: last.tool_name,
                            tool_output: last.tool_output,
                            tool_invocations: invocations,
                            rounds_executed,
                            termination_reason: "model_completed_no_tool_calls".to_string(),
                        });
                    }

                    return Err(StasisError::PortFailure(
                        "chat response was empty after tool loop".to_string(),
                    ));
                }

                messages.push(ChatMessage::from(tool_calls.clone()));
                for call in tool_calls {
                    let tool_output = self
                        .tool_registry
                        .invoke_tool(&call.fn_name, call.fn_arguments.clone())
                        .await?;

                    let tool_output_text = tool_output.to_string();
                    messages.push(ChatMessage::from(ToolResponse::new(
                        call.call_id,
                        tool_output_text,
                    )));
                    invocations.push(ToolInvocation {
                        tool_name: call.fn_name,
                        tool_input: call.fn_arguments,
                        tool_output,
                    });
                }
            }

            if !should_use_legacy_fallback {
                return Err(StasisError::PortFailure(format!(
                    "tool loop exceeded max rounds ({max_tool_rounds}) without final response"
                )));
            }
        }

        if !should_use_legacy_fallback {
            return Err(StasisError::PortFailure(
                "no matching tools available for tool loop execution".to_string(),
            ));
        }

        let draft_text = if let Some(text) = fallback_draft_text {
            text
        } else {
            let mut first_request =
                PromptExecutionRequest::from_user_prompt(user_prompt.clone()).with_context(context.clone());
            if let Some(system_prompt) = system_prompt.as_ref() {
                first_request = first_request.with_system_prompt(system_prompt);
            }
            self.prompt_pipeline.execute(first_request).await?.text
        };
        let tool_output = self
            .tool_registry
            .invoke_tool(&selected_tool_name, tool_input.clone())
            .await?;

        let synthesis_prompt = build_fallback_synthesis_prompt(
            &user_prompt,
            &draft_text,
            &selected_tool_name,
            &tool_output,
        );

        let mut final_request = PromptExecutionRequest::from_user_prompt(synthesis_prompt)
            .with_context(context.clone());
        if let Some(system_prompt) = system_prompt {
            final_request = final_request.with_system_prompt(system_prompt);
        }

        let final_response = self.prompt_pipeline.execute(final_request).await?;

        let fallback_invocation = ToolInvocation {
            tool_name: selected_tool_name.clone(),
            tool_input,
            tool_output: tool_output.clone(),
        };

        Ok(ToolLoopExecutionResponse {
            text: final_response.text,
            metadata: final_response.metadata,
            tool_name: selected_tool_name,
            tool_output,
            tool_invocations: vec![fallback_invocation],
            rounds_executed,
            termination_reason: "legacy_fallback_no_model_tool_call".to_string(),
        })
    }
}

fn build_fallback_synthesis_prompt(
    user_prompt: &str,
    draft_text: &str,
    tool_name: &str,
    tool_output: &Value,
) -> String {
    let tool_output_text = tool_output.to_string();
    let mut prompt = String::with_capacity(
        user_prompt.len() + draft_text.len() + tool_name.len() + tool_output_text.len() + 128,
    );
    prompt.push_str("User request:\n");
    prompt.push_str(user_prompt);
    prompt.push_str("\n\nDraft analysis:\n");
    prompt.push_str(draft_text);
    prompt.push_str("\n\nTool '");
    prompt.push_str(tool_name);
    prompt.push_str("' output JSON:\n");
    prompt.push_str(&tool_output_text);
    prompt.push_str("\n\nProduce final answer grounded in the tool output.");
    prompt
}

fn sanitize_tool_name_for_model(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "tool".to_string()
    } else {
        trimmed.to_string()
    }
}
