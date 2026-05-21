use chrono::Utc;
use serde_json::Value;

use medousa::events::TuiEvent;

use super::{ConversationTurn, JobHistoryEntry, TuiState};

pub(crate) fn handle_tui_event(event: TuiEvent, state: &mut TuiState) {
    if !matches!(event, TuiEvent::AgentChunk { .. }) {
        flush_pending_agent_chunks(state);
    }

    match event {
        TuiEvent::UiNotice(text) => {
            super::push_obs(state, text);
        }
        TuiEvent::AgentChunk { delta } => {
            if !delta.is_empty() {
                state.pending_agent_chunk_delta.push_str(&delta);
                state.pending_agent_chunk_count = state.pending_agent_chunk_count.saturating_add(1);
            }
        }
        TuiEvent::AgentResponse { text, tool_names } => {
            state.is_processing = false;
            state.active_request_task = None;
            let (visible_text, thinking_chunks) = strip_thinking_tags(&text);
            for chunk in thinking_chunks {
                super::push_thinking(state, chunk);
            }

            if !state.stream_tag_tail.is_empty() {
                if state.in_thinking_tag {
                    let tail = std::mem::take(&mut state.stream_tag_tail);
                    super::push_thinking(state, tail);
                } else {
                    let tail = std::mem::take(&mut state.stream_tag_tail);
                    if let Some(idx) = state.active_agent_stream_turn {
                        if let Some(turn) = state.conversation.get_mut(idx) {
                            turn.content.push_str(&tail);
                        }
                    }
                }
            }
            state.in_thinking_tag = false;

            if let Some(idx) = state.active_agent_stream_turn.take() {
                if let Some(turn) = state.conversation.get_mut(idx) {
                    turn.content = visible_text;
                    turn.tool_names = tool_names;
                    turn.timestamp = Utc::now();
                    super::append_turn(&state.session_id, turn);
                }
            } else {
                let turn = ConversationTurn {
                    role: "agent".to_string(),
                    content: visible_text,
                    timestamp: Utc::now(),
                    tool_names,
                };
                super::append_turn(&state.session_id, &turn);
                state.conversation.push(turn);
            }
            if state.auto_scroll {
                state.conv_scroll = state.conv_max_scroll;
            }
            super::invalidate_markdown_cache(state);
        }
        TuiEvent::AgentError(err) => {
            state.is_processing = false;
            state.active_request_task = None;
            state.active_agent_stream_turn = None;
            state.in_thinking_tag = false;
            state.stream_tag_tail.clear();
            super::push_obs(state, format!("⚠ {err}"));
        }
        TuiEvent::JobEnqueued { job_id, job_type } => {
            super::push_obs(state, format!("+ {job_type}"));
            state.job_history.push_front(JobHistoryEntry {
                job_id,
                job_type,
                status: "enqueued".to_string(),
            });
            if state.job_history.len() > 100 {
                state.job_history.pop_back();
            }
            super::invalidate_markdown_cache(state);
        }
        TuiEvent::JobProcessed {
            job_id,
            succeeded,
            execution_id,
        } => {
            let symbol = if succeeded { "✓" } else { "✗" };
            let exec_hint = execution_id.as_deref().unwrap_or("—");
            super::push_obs(state, format!("{symbol} [{exec_hint:.12}]"));
            for entry in state.job_history.iter_mut() {
                if entry.job_id == job_id {
                    entry.status = if succeeded { "succeeded" } else { "failed" }.to_string();
                    break;
                }
            }
            super::invalidate_markdown_cache(state);
        }
        TuiEvent::ToolInvoked {
            tool_name,
            input_summary,
        } => {
            super::push_obs(state, format!("◆ {tool_name}  {input_summary}"));
        }
        TuiEvent::ToolPayload {
            tool_name,
            tool_input,
            tool_output,
        } => {
            let request_id = super::next_worker_request_id(state);
            let queued = super::queue_worker_command(
                state,
                super::WorkerCommand::FormatToolPayload {
                    request_id,
                    tool_name: tool_name.clone(),
                    tool_input,
                    tool_output: tool_output.clone(),
                },
                true,
            );
            if !queued {
                super::push_obs(
                    state,
                    format!("◆ {tool_name}  payload omitted (formatter busy)"),
                );
            }

            if tool_name == "editor.gr.run" {
                let source = tool_output
                    .get("source")
                    .and_then(|v| v.as_str())
                    .unwrap_or("editor:buffer");
                let job_id = tool_output
                    .get("job_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown-job");
                let succeeded = tool_output
                    .get("succeeded")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let diagnostics = tool_output
                    .get("diagnostics")
                    .cloned()
                    .unwrap_or(Value::Null);
                super::push_grapheme_console_entry(state, source, job_id, succeeded, &diagnostics);
            }
        }
    }
}

fn apply_agent_chunk_delta(delta: &str, state: &mut TuiState) {
    if delta.is_empty() {
        return;
    }

    let (visible_delta, thinking_chunks) = extract_thinking_from_stream(
        delta,
        &mut state.in_thinking_tag,
        &mut state.stream_tag_tail,
    );
    for chunk in thinking_chunks {
        super::push_thinking(state, chunk);
    }

    if visible_delta.is_empty() {
        return;
    }

    if let Some(idx) = state.active_agent_stream_turn {
        if let Some(turn) = state.conversation.get_mut(idx) {
            turn.content.push_str(&visible_delta);
        }
    } else {
        state.conversation.push(ConversationTurn {
            role: "agent".to_string(),
            content: visible_delta,
            timestamp: Utc::now(),
            tool_names: vec![],
        });
        state.active_agent_stream_turn = Some(state.conversation.len().saturating_sub(1));
    }

    if state.auto_scroll {
        state.conv_scroll = state.conv_max_scroll;
    }
    super::invalidate_markdown_cache(state);
}

pub(crate) fn flush_pending_agent_chunks(state: &mut TuiState) {
    if state.pending_agent_chunk_delta.is_empty() {
        state.pending_agent_chunk_count = 0;
        return;
    }

    let delta = std::mem::take(&mut state.pending_agent_chunk_delta);
    if state.pending_agent_chunk_count > 1 {
        state.perf.coalesced_agent_chunks = state
            .perf
            .coalesced_agent_chunks
            .saturating_add(state.pending_agent_chunk_count.saturating_sub(1));
    }
    state.pending_agent_chunk_count = 0;
    apply_agent_chunk_delta(&delta, state);
}

fn extract_thinking_from_stream(
    delta: &str,
    in_thinking: &mut bool,
    tail: &mut String,
) -> (String, Vec<String>) {
    let mut buffer = String::with_capacity(tail.len() + delta.len());
    buffer.push_str(tail);
    buffer.push_str(delta);
    tail.clear();

    let mut visible = String::new();
    let mut thinking = Vec::new();

    loop {
        if *in_thinking {
            if let Some((idx, marker_len)) =
                find_earliest_marker(&buffer, &["</think>", "</thinking>"])
            {
                let chunk = &buffer[..idx];
                if !chunk.is_empty() {
                    thinking.push(chunk.to_string());
                }
                buffer = buffer[idx + marker_len..].to_string();
                *in_thinking = false;
                continue;
            }

            let keep = trailing_prefix_len(&buffer, &["</think>", "</thinking>"]);
            if buffer.len() > keep {
                thinking.push(buffer[..buffer.len() - keep].to_string());
            }
            *tail = if keep > 0 {
                buffer[buffer.len() - keep..].to_string()
            } else {
                String::new()
            };
            break;
        }

        if let Some((idx, marker_len)) = find_earliest_marker(&buffer, &["<think>", "<thinking>"]) {
            visible.push_str(&buffer[..idx]);
            buffer = buffer[idx + marker_len..].to_string();
            *in_thinking = true;
            continue;
        }

        let keep = trailing_prefix_len(&buffer, &["<think>", "<thinking>"]);
        if buffer.len() > keep {
            visible.push_str(&buffer[..buffer.len() - keep]);
        }
        *tail = if keep > 0 {
            buffer[buffer.len() - keep..].to_string()
        } else {
            String::new()
        };
        break;
    }

    (visible, thinking)
}

fn strip_thinking_tags(text: &str) -> (String, Vec<String>) {
    let mut remaining = text.to_string();
    let mut visible = String::new();
    let mut thinking = Vec::new();
    let mut in_thinking = false;

    loop {
        if remaining.is_empty() {
            break;
        }

        if in_thinking {
            if let Some((idx, marker_len)) =
                find_earliest_marker(&remaining, &["</think>", "</thinking>"])
            {
                let chunk = &remaining[..idx];
                if !chunk.is_empty() {
                    thinking.push(chunk.to_string());
                }
                remaining = remaining[idx + marker_len..].to_string();
                in_thinking = false;
            } else {
                thinking.push(remaining);
                break;
            }
        } else if let Some((idx, marker_len)) =
            find_earliest_marker(&remaining, &["<think>", "<thinking>"])
        {
            visible.push_str(&remaining[..idx]);
            remaining = remaining[idx + marker_len..].to_string();
            in_thinking = true;
        } else {
            visible.push_str(&remaining);
            break;
        }
    }

    (visible, thinking)
}

fn find_earliest_marker(haystack: &str, markers: &[&str]) -> Option<(usize, usize)> {
    markers
        .iter()
        .filter_map(|m| haystack.find(m).map(|idx| (idx, m.len())))
        .min_by_key(|(idx, _)| *idx)
}

fn trailing_prefix_len(s: &str, markers: &[&str]) -> usize {
    for start in s
        .char_indices()
        .map(|(idx, _)| idx)
        .chain(std::iter::once(s.len()))
        .rev()
    {
        if start == s.len() {
            continue;
        }
        let suffix = &s[start..];
        if markers.iter().any(|m| m.starts_with(suffix)) {
            return s.len() - start;
        }
    }
    0
}
