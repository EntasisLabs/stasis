use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
    Terminal,
};
use ratatui_markdown::{markdown::MarkdownRenderer, DefaultTheme};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use medousa::{
    build_tui_runtime,
    events::TuiEvent,
    parse_backend,
    resolve_llm_base_url, resolve_llm_model, resolve_llm_provider, TuiRuntime,
    session::{
        append_turn, list_history_sessions, load_history, load_tui_defaults, save_last_session_id,
        save_tui_defaults, ConversationTurn, SessionHistorySummary, TuiDefaults,
    },
};
use stasis::application::orchestration::tool_loop_pipeline::{ToolCallMode, ToolLoopExecutionRequest};
use stasis::prelude::{ChatMessage, PromptExecutionContext};

static SYSTEM_PROMPT: &str = r#"You are operating inside Medousa, a tool-first runtime assistant environment.

In Medousa, STTP is the internal memory representation used to save and replay structured context over time.
The STTP node below defines your operating policy and execution workflow.
Read it as policy memory, then follow it strictly during this conversation.

⊕⟨ ⏣0{ trigger: runtime_bootstrap, response_format: temporal_node, origin_session: "medousa-system-prompt", compression_depth: 1, parent_node: null, prime: { attractor_config: { stability: 0.90, friction: 0.24, logic: 0.95, autonomy: 0.84 }, context_summary: "Execution-first assistant policy for Medousa with strict tool grounding and deterministic Grapheme workflow sequencing.", relevant_tier: raw, retrieval_budget: 16 } } ⟩
⦿⟨ ⏣0{ timestamp: "2026-05-16T00:00:00Z", tier: raw, session_id: "medousa-system", schema_version: "sttp-1.0", user_avec: { stability: 0.88, friction: 0.28, logic: 0.93, autonomy: 0.83, psi: 2.92 }, model_avec: { stability: 0.89, friction: 0.25, logic: 0.94, autonomy: 0.82, psi: 2.90 } } ⟩
◈⟨ ⏣0{
    role(.99): "You are an execution-first assistant running inside Medousa.",
    primary_rule(.99): {
        fact_grounding(.99): "Do not present memory-only answers as factual web/current data.",
        tool_requirement(.99): "For current facts, you must use tools."
    },
    tool_distinction(.99): {
        modules_search_scope(.99): "grapheme.modules.search is only for discovering module docs, examples, signatures, and usage patterns.",
        modules_search_not_web(.99): "grapheme.modules.search is not a web search tool and is not evidence for real-world facts.",
        real_world_retrieval(.99): "Real-world retrieval must use a runtime script that calls web/http/websearch modules."
    },
    workflow(.99): {
        step_1_classify_intent(.98): "If user asks for current/external facts, perform tool-based retrieval. If user asks for local transformation/coding, select relevant modules.",
        step_2_example_first(.99): "Before writing any grapheme script, code snippet, or workflow, retrieve at least two relevant example and adhere to the proper syntax.",
        step_2_order(.99): "Discovery order: a) grapheme.modules.search <intent>, b) grapheme.modules.examples <chosen-module>, c) if examples unavailable, use grapheme.modules.info + grapheme.modules.ops, then grapheme.examples.list + grapheme.examples.show.",
        step_2_no_reverse(.99): "Do not write code first and then look up examples.",
        step_3_construct(.98): "Build grapheme workflow following discovered example pattern using correct execution modules (web/http/sql/etc).",
        step_3_web_preference(.98): "For web retrieval, prefer websearch.search or websearch.research_report unless low-level http behavior is explicitly required.",
        step_4_execute(.99): "Run the script and treat runtime output as evidence.",
        step_5_answer(.98): "Return concise answer grounded in tool output; if output missing, state that and ask for retry target."
    },
    failure_policy(.99): {
        no_modules_search_as_final(.99): "Never claim modules.search output as final answer to live-data questions.",
        no_skip_execution(.99): "Never skip execution when external data is required.",
        no_code_without_example(.99): "Never generate workflow/code steps without first retrieving at least one relevant example.",
        example_fallback_required(.98): "Never assume module-local curated examples always exist; follow fallback discovery order when modules.examples is empty.",
        retry_once(.96): "If run fails, report exact failure briefly, adjust once, and retry once."
    },
    style(.94): {
        brevity(.94): "Keep responses short and structured for small models.",
        provenance_language(.93): "Use explicit source-of-truth language, e.g., Based on tool output."
    }
} ⟩
⍉⟨ ⏣0{ rho: 0.97, kappa: 0.96, psi: 2.91, compression_avec: { stability: 0.89, friction: 0.25, logic: 0.94, autonomy: 0.82, psi: 2.90 } } ⟩"#;



// ── Domain types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ObsEvent {
    text: String,
}

#[derive(Debug, Clone)]
struct JobHistoryEntry {
    job_id: String,
    job_type: String,
    status: String,
}

struct TuiState {
    conversation: Vec<ConversationTurn>,
    observability: VecDeque<ObsEvent>,
    job_history: VecDeque<JobHistoryEntry>,
    input_buffer: String,
    conv_scroll: u16,
    conv_max_scroll: u16,
    is_processing: bool,
    active_request_task: Option<tokio::task::JoinHandle<()>>,
    auto_scroll: bool,
    active_agent_stream_turn: Option<usize>,
    mode: UiMode,
    history_items: Vec<SessionHistorySummary>,
    history_selected: usize,
    command_query: String,
    command_selected: usize,
    settings: RuntimeSettings,
    settings_selected: usize,
    settings_editing: bool,
    provider_model: String,
    session_id: String,
    thinking_trace: VecDeque<String>,
    thinking_scroll: u16,
    thinking_max_scroll: u16,
    obs_scroll: u16,
    obs_max_scroll: u16,
    in_thinking_tag: bool,
    stream_tag_tail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiMode {
    Chat,
    History,
    CommandPalette,
    Settings,
    ObservabilityPanel,
    ThinkingPeek,
    ThinkingPanel,
}

#[derive(Debug, Clone)]
struct RuntimeSettings {
    backend: String,
    provider: String,
    model: String,
    base_url: String,
    tool_call_mode: String,
    max_tool_rounds: String,
    thinking_capture: String,
    thinking_max_lines: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let provider = find_arg_value(&args, "--provider");
    let model = find_arg_value(&args, "--model");
    let base_url = find_arg_value(&args, "--base-url");
    let backend = find_arg_value(&args, "--backend");
    let tool_call_mode = find_arg_value(&args, "--tool-call-mode");
    let max_tool_rounds = find_arg_value(&args, "--max-tool-rounds");
    let thinking_capture = find_arg_value(&args, "--thinking-capture");
    let thinking_max_lines = find_arg_value(&args, "--thinking-max-lines");
    let explicit_session = find_arg_value(&args, "--session");
    let defaults = load_tui_defaults();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Ok(());
    }

    let resolved_provider = resolve_llm_provider(provider.or(defaults.provider.as_deref()));
    let resolved_model = resolve_llm_model(model.or(defaults.model.as_deref()));
    let resolved_backend = resolve_backend_name(backend.or(defaults.backend.as_deref()));
    let resolved_tool_call_mode =
        resolve_tool_call_mode_name(tool_call_mode.or(defaults.tool_call_mode.as_deref()));
    let resolved_max_tool_rounds = resolve_usize_arg(
        max_tool_rounds,
        defaults.max_tool_rounds.unwrap_or(10),
        1,
        50,
    );
    let resolved_thinking_capture = resolve_bool_arg(
        thinking_capture,
        defaults.thinking_capture.unwrap_or(true),
    );
    let resolved_thinking_max_lines = resolve_usize_arg(
        thinking_max_lines,
        defaults.thinking_max_lines.unwrap_or(300),
        50,
        5000,
    );
    let resolved_base_url = resolve_llm_base_url(
        Some(&resolved_provider),
        base_url.or(defaults.base_url.as_deref()),
    );
    let provider_model = format!("{resolved_provider}:{resolved_model}");

    let session_id = if let Some(sid) = explicit_session {
        sid.to_string()
    } else {
        Uuid::new_v4().simple().to_string()
    };
    save_last_session_id(&session_id);

    let history = load_history(&session_id);

    let (event_tx, mut event_rx) = mpsc::channel::<TuiEvent>(256);

    let mut tui_rt = build_tui_runtime(
        parse_backend(Some(&resolved_backend)),
        Some(&resolved_provider),
        Some(&resolved_model),
        resolved_base_url.as_deref(),
        &session_id,
        event_tx.clone(),
    )
    .await?;

    // ── Terminal setup ────────────────────────────────────────────────────────
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;

    let mut state = TuiState {
        conversation: history,
        observability: VecDeque::new(),
        job_history: VecDeque::new(),
        input_buffer: String::new(),
        conv_scroll: 0,
        conv_max_scroll: 0,
        is_processing: false,
        active_request_task: None,
        auto_scroll: true,
        active_agent_stream_turn: None,
        mode: UiMode::Chat,
        history_items: Vec::new(),
        history_selected: 0,
        command_query: String::new(),
        command_selected: 0,
        settings: RuntimeSettings {
            backend: resolved_backend,
            provider: resolved_provider.clone(),
            model: resolved_model.clone(),
            base_url: resolved_base_url.unwrap_or_default(),
            tool_call_mode: resolved_tool_call_mode,
            max_tool_rounds: resolved_max_tool_rounds.to_string(),
            thinking_capture: resolved_thinking_capture.to_string(),
            thinking_max_lines: resolved_thinking_max_lines.to_string(),
        },
        settings_selected: 0,
        settings_editing: false,
        provider_model,
        session_id: session_id.clone(),
        thinking_trace: VecDeque::new(),
        thinking_scroll: 0,
        thinking_max_scroll: 0,
        obs_scroll: 0,
        obs_max_scroll: 0,
        in_thinking_tag: false,
        stream_tag_tail: String::new(),
    };

    // ── Keyboard reader (spawn_blocking to keep async event loop clean) ───────
    let (key_tx, mut key_rx) = mpsc::channel::<Event>(64);
    tokio::task::spawn_blocking(move || loop {
        if crossterm::event::poll(Duration::from_millis(50)).unwrap_or(false) {
            match crossterm::event::read() {
                Ok(event) => {
                    if key_tx.blocking_send(event).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // ── Main event loop ───────────────────────────────────────────────────────
    terminal.draw(|f| render(f, &mut state))?;
    loop {
        tokio::select! {
            Some(event) = key_rx.recv() => {
                match handle_key_event(event, &mut state, &mut tui_rt, &event_tx).await {
                    EventOutcome::Break => break,
                    EventOutcome::Continue => {}
                }
            }
            Some(tui_event) = event_rx.recv() => {
                handle_tui_event(tui_event, &mut state);
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }

        terminal.draw(|f| render(f, &mut state))?;
    }

    // ── Restore terminal ──────────────────────────────────────────────────────
    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

// ── Event handling ────────────────────────────────────────────────────────────

enum EventOutcome {
    Continue,
    Break,
}

async fn handle_key_event(
    event: Event,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    let Event::Key(key) = event else {
        return EventOutcome::Continue;
    };

    if key.code == KeyCode::Esc {
        state.settings_editing = false;
        state.mode = UiMode::Chat;
        return EventOutcome::Continue;
    }

    if key.code == KeyCode::Char('t') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if state.mode == UiMode::ThinkingPanel {
            state.mode = UiMode::Chat;
        } else if !state.thinking_trace.is_empty() || state.is_processing {
            state.mode = UiMode::ThinkingPanel;
        }
        return EventOutcome::Continue;
    }

    if key.code == KeyCode::F(2) {
        if state.mode == UiMode::ThinkingPeek {
            state.mode = UiMode::Chat;
        } else if !state.thinking_trace.is_empty() || state.is_processing {
            state.mode = UiMode::ThinkingPeek;
        }
        return EventOutcome::Continue;
    }

    if key.code == KeyCode::Char('k') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if state.mode == UiMode::CommandPalette {
            state.mode = UiMode::Chat;
            state.command_query.clear();
            state.command_selected = 0;
        } else {
            state.mode = UiMode::CommandPalette;
            state.command_query.clear();
            state.command_selected = 0;
        }
        return EventOutcome::Continue;
    }

    if key.code == KeyCode::Char('h') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if state.mode == UiMode::History {
            state.mode = UiMode::Chat;
        } else {
            state.history_items = list_history_sessions(200);
            state.history_selected = 0;
            state.mode = UiMode::History;
        }
        return EventOutcome::Continue;
    }

    if key.code == KeyCode::Char(',') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if state.mode == UiMode::Settings {
            state.mode = UiMode::Chat;
            state.settings_editing = false;
        } else {
            state.mode = UiMode::Settings;
            state.settings_selected = 0;
            state.settings_editing = false;
        }
        return EventOutcome::Continue;
    }

    if key.code == KeyCode::Char('o') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if state.mode == UiMode::ObservabilityPanel {
            state.mode = UiMode::Chat;
        } else {
            state.mode = UiMode::ObservabilityPanel;
        }
        return EventOutcome::Continue;
    }

    if state.mode == UiMode::History {
        return handle_history_key_event(key.code, state);
    }

    if state.mode == UiMode::CommandPalette {
        return handle_command_palette_key_event(key.code, state, tui_rt, event_tx).await;
    }

    if state.mode == UiMode::Settings {
        return handle_settings_key_event(key.code, state, tui_rt, event_tx).await;
    }

    if state.mode == UiMode::ThinkingPeek || state.mode == UiMode::ThinkingPanel {
        return handle_thinking_key_event(key.code, state);
    }

    if state.mode == UiMode::ObservabilityPanel {
        return handle_observability_key_event(key.code, state);
    }

    match (key.code, key.modifiers) {
        (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
            return EventOutcome::Break;
        }
        (KeyCode::Char('q'), m) if m.contains(KeyModifiers::CONTROL) => {
            return EventOutcome::Break;
        }
        (KeyCode::Char('g'), m) if m.contains(KeyModifiers::CONTROL) => {
            stop_active_generation(state);
            return EventOutcome::Continue;
        }

        // Scroll
        (KeyCode::Up, _) => {
            let base = if state.auto_scroll {
                state.conv_max_scroll
            } else {
                state.conv_scroll
            };
            state.conv_scroll = base.saturating_sub(1);
            state.auto_scroll = false;
        }
        (KeyCode::Down, _) => {
            let base = if state.auto_scroll {
                state.conv_max_scroll
            } else {
                state.conv_scroll
            };
            let next = base.saturating_add(1).min(state.conv_max_scroll);
            state.conv_scroll = next;
            state.auto_scroll = next >= state.conv_max_scroll;
        }
        (KeyCode::PageUp, _) => {
            let base = if state.auto_scroll {
                state.conv_max_scroll
            } else {
                state.conv_scroll
            };
            state.conv_scroll = base.saturating_sub(10);
            state.auto_scroll = false;
        }
        (KeyCode::PageDown, _) => {
            let base = if state.auto_scroll {
                state.conv_max_scroll
            } else {
                state.conv_scroll
            };
            let next = base.saturating_add(10).min(state.conv_max_scroll);
            state.conv_scroll = next;
            state.auto_scroll = next >= state.conv_max_scroll;
        }
        (KeyCode::Home, _) => {
            state.conv_scroll = 0;
            state.auto_scroll = false;
        }
        (KeyCode::End, _) => {
            state.conv_scroll = state.conv_max_scroll;
            state.auto_scroll = true;
        }

        // Input
        (KeyCode::Char(c), _) if !state.is_processing => {
            state.input_buffer.push(c);
        }
        (KeyCode::Backspace, _) if !state.is_processing => {
            state.input_buffer.pop();
        }

        // Submit
        (KeyCode::Enter, _)
            if !state.is_processing && !state.input_buffer.trim().is_empty() =>
        {
            let prompt = state.input_buffer.trim().to_string();

            if prompt.starts_with('/') {
                state.input_buffer.clear();
                return handle_slash_command(&prompt, state, tui_rt, event_tx).await;
            }

            state.input_buffer.clear();
            start_prompt_run(state, tui_rt, event_tx, prompt, true);
        }

        _ => {}
    }

    EventOutcome::Continue
}

fn handle_thinking_key_event(code: KeyCode, state: &mut TuiState) -> EventOutcome {
    if state.mode == UiMode::ThinkingPeek {
        if matches!(code, KeyCode::Down | KeyCode::PageDown | KeyCode::Enter) {
            state.mode = UiMode::ThinkingPanel;
        }
        return EventOutcome::Continue;
    }

    match code {
        KeyCode::Up => {
            state.thinking_scroll = state.thinking_scroll.saturating_sub(1);
        }
        KeyCode::Down => {
            state.thinking_scroll = state
                .thinking_scroll
                .saturating_add(1)
                .min(state.thinking_max_scroll);
        }
        KeyCode::PageUp => {
            state.thinking_scroll = state.thinking_scroll.saturating_sub(10);
        }
        KeyCode::PageDown => {
            state.thinking_scroll = state
                .thinking_scroll
                .saturating_add(10)
                .min(state.thinking_max_scroll);
        }
        KeyCode::Home => {
            state.thinking_scroll = 0;
        }
        KeyCode::End => {
            state.thinking_scroll = state.thinking_max_scroll;
        }
        _ => {}
    }

    EventOutcome::Continue
}

fn handle_observability_key_event(code: KeyCode, state: &mut TuiState) -> EventOutcome {
    match code {
        KeyCode::Up => {
            state.obs_scroll = state.obs_scroll.saturating_sub(1);
        }
        KeyCode::Down => {
            state.obs_scroll = state.obs_scroll.saturating_add(1).min(state.obs_max_scroll);
        }
        KeyCode::PageUp => {
            state.obs_scroll = state.obs_scroll.saturating_sub(10);
        }
        KeyCode::PageDown => {
            state.obs_scroll = state.obs_scroll.saturating_add(10).min(state.obs_max_scroll);
        }
        KeyCode::Home => {
            state.obs_scroll = 0;
        }
        KeyCode::End => {
            state.obs_scroll = state.obs_max_scroll;
        }
        _ => {}
    }

    EventOutcome::Continue
}

fn start_prompt_run(
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
            timestamp: Utc::now(),
            tool_names: vec![],
        };
        append_turn(&state.session_id, &user_turn);
        state.conversation.push(user_turn);
    }

    let pipeline = tui_rt.tool_loop_pipeline.clone();
    let tx = event_tx.clone();
    let prompt_preview: String = prompt.chars().take(48).collect();
    let tool_call_mode = parse_tool_call_mode(&state.settings.tool_call_mode);
    let max_tool_rounds = parse_usize_with_bounds(&state.settings.max_tool_rounds, 10, 1, 50);
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
            system_prompt: Some(SYSTEM_PROMPT.to_string()),
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

fn stop_active_generation(state: &mut TuiState) {
    if let Some(task) = state.active_request_task.take() {
        task.abort();
        state.is_processing = false;
        state.active_agent_stream_turn = None;
        push_obs(state, "■ generation stopped".to_string());
    }
}

async fn handle_slash_command(
    prompt: &str,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    let mut parts = prompt.split_whitespace();
    let cmd = parts.next().unwrap_or_default();

    match cmd {
        "/new" => {
            stop_active_generation(state);
            state.session_id = Uuid::new_v4().simple().to_string();
            state.conversation.clear();
            state.active_agent_stream_turn = None;
            state.thinking_trace.clear();
            state.thinking_scroll = 0;
            state.thinking_max_scroll = 0;
            state.in_thinking_tag = false;
            state.stream_tag_tail.clear();
            state.is_processing = false;
            state.auto_scroll = true;
            state.conv_scroll = 0;
            save_last_session_id(&state.session_id);
            push_obs(state, format!("✓ new session {}", &state.session_id[..8]));

            if let Ok(new_rt) = build_tui_runtime(
                parse_backend(Some(&state.settings.backend)),
                Some(&state.settings.provider),
                Some(&state.settings.model),
                if state.settings.base_url.trim().is_empty() {
                    None
                } else {
                    Some(state.settings.base_url.as_str())
                },
                &state.session_id,
                event_tx.clone(),
            )
            .await
            {
                *tui_rt = new_rt;
            } else {
                push_obs(state, "⚠ new session runtime rebind failed".to_string());
            }
        }
        "/history" => {
            state.history_items = list_history_sessions(200);
            state.history_selected = 0;
            state.mode = UiMode::History;
        }
        "/settings" => {
            state.mode = UiMode::Settings;
            state.settings_selected = 0;
            state.settings_editing = false;
        }
        "/model" => {
            let args = parts.collect::<Vec<_>>();
            if args.is_empty() {
                push_obs(
                    state,
                    format!(
                        "model {}:{}",
                        state.settings.provider, state.settings.model
                    ),
                );
                return EventOutcome::Continue;
            }

            if args.len() == 1 {
                if let Some((provider, model)) = args[0].split_once(':') {
                    state.settings.provider = provider.trim().to_string();
                    state.settings.model = model.trim().to_string();
                } else {
                    state.settings.model = args[0].trim().to_string();
                }
            } else {
                state.settings.provider = args[0].trim().to_string();
                state.settings.model = args[1].trim().to_string();
            }

            apply_settings(state, tui_rt, event_tx).await;
        }
        "/stop" => {
            stop_active_generation(state);
        }
        "/regen" => {
            if state.is_processing {
                push_obs(state, "⚠ cannot regenerate while processing".to_string());
                return EventOutcome::Continue;
            }

            let last_user_prompt = state
                .conversation
                .iter()
                .rev()
                .find(|t| t.role == "user")
                .map(|t| t.content.clone());

            if let Some(prompt) = last_user_prompt {
                if matches!(state.conversation.last(), Some(turn) if turn.role == "agent") {
                    state.conversation.pop();
                }
                push_obs(state, "↻ regenerate last response".to_string());
                start_prompt_run(state, tui_rt, event_tx, prompt, false);
            } else {
                push_obs(state, "⚠ no user prompt available to regenerate".to_string());
            }
        }
        "/export" => {
            let format = parts.next().unwrap_or("md");
            match export_current_session(state, format) {
                Ok(path) => push_obs(state, format!("✓ exported {}", path.display())),
                Err(err) => push_obs(state, format!("⚠ export failed: {err}")),
            }
        }
        _ => {
            push_obs(
                state,
                "⚠ unknown command. try /new /history /settings /model /stop /regen /export"
                    .to_string(),
            );
        }
    }

    EventOutcome::Continue
}

#[derive(Clone, Copy)]
struct PaletteAction {
    title: &'static str,
    command: &'static str,
}

const PALETTE_ACTIONS: [PaletteAction; 9] = [
    PaletteAction {
        title: "New Session",
        command: "/new",
    },
    PaletteAction {
        title: "Open History",
        command: "/history",
    },
    PaletteAction {
        title: "Open Settings",
        command: "/settings",
    },
    PaletteAction {
        title: "Show Current Model",
        command: "/model",
    },
    PaletteAction {
        title: "Stop Generation",
        command: "/stop",
    },
    PaletteAction {
        title: "Regenerate Last Response",
        command: "/regen",
    },
    PaletteAction {
        title: "Export Transcript (Markdown)",
        command: "/export md",
    },
    PaletteAction {
        title: "Export Transcript (JSONL)",
        command: "/export jsonl",
    },
    PaletteAction {
        title: "Set Model (Open Settings)",
        command: "/settings",
    },
];

fn filtered_palette_actions(query: &str) -> Vec<PaletteAction> {
    let q = query.trim().to_ascii_lowercase();
    if q.is_empty() {
        return PALETTE_ACTIONS.to_vec();
    }

    PALETTE_ACTIONS
        .iter()
        .copied()
        .filter(|action| {
            action.title.to_ascii_lowercase().contains(&q)
                || action.command.to_ascii_lowercase().contains(&q)
        })
        .collect()
}

async fn handle_command_palette_key_event(
    code: KeyCode,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    match code {
        KeyCode::Backspace => {
            state.command_query.pop();
            state.command_selected = 0;
        }
        KeyCode::Char(c) => {
            state.command_query.push(c);
            state.command_selected = 0;
        }
        KeyCode::Up => {
            state.command_selected = state.command_selected.saturating_sub(1);
        }
        KeyCode::Down => {
            let max = filtered_palette_actions(&state.command_query)
                .len()
                .saturating_sub(1);
            state.command_selected = (state.command_selected + 1).min(max);
        }
        KeyCode::Enter => {
            let actions = filtered_palette_actions(&state.command_query);
            if let Some(action) = actions.get(state.command_selected).copied() {
                state.mode = UiMode::Chat;
                state.command_query.clear();
                state.command_selected = 0;
                return handle_slash_command(action.command, state, tui_rt, event_tx).await;
            }
        }
        _ => {}
    }

    EventOutcome::Continue
}

fn handle_tui_event(event: TuiEvent, state: &mut TuiState) {
    match event {
        TuiEvent::AgentChunk { delta } => {
            if delta.is_empty() {
                return;
            }

            let (visible_delta, thinking_chunks) = extract_thinking_from_stream(
                &delta,
                &mut state.in_thinking_tag,
                &mut state.stream_tag_tail,
            );
            for chunk in thinking_chunks {
                push_thinking(state, chunk);
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
        }
        TuiEvent::AgentResponse { text, tool_names } => {
            state.is_processing = false;
            state.active_request_task = None;
            let (visible_text, thinking_chunks) = strip_thinking_tags(&text);
            for chunk in thinking_chunks {
                push_thinking(state, chunk);
            }

            if !state.stream_tag_tail.is_empty() {
                if state.in_thinking_tag {
                    let tail = std::mem::take(&mut state.stream_tag_tail);
                    push_thinking(state, tail);
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
                    append_turn(&state.session_id, turn);
                }
            } else {
                let turn = ConversationTurn {
                    role: "agent".to_string(),
                    content: visible_text,
                    timestamp: Utc::now(),
                    tool_names,
                };
                append_turn(&state.session_id, &turn);
                state.conversation.push(turn);
            }
            if state.auto_scroll {
                state.conv_scroll = state.conv_max_scroll;
            }
        }
        TuiEvent::AgentError(err) => {
            state.is_processing = false;
            state.active_request_task = None;
            state.active_agent_stream_turn = None;
            state.in_thinking_tag = false;
            state.stream_tag_tail.clear();
            push_obs(state, format!("⚠ {err}"));
        }
        TuiEvent::JobEnqueued { job_id, job_type } => {
            push_obs(state, format!("+ {job_type}"));
            state.job_history.push_front(JobHistoryEntry {
                job_id,
                job_type,
                status: "enqueued".to_string(),
            });
            if state.job_history.len() > 100 {
                state.job_history.pop_back();
            }
        }
        TuiEvent::JobProcessed { job_id, succeeded, execution_id } => {
            let symbol = if succeeded { "✓" } else { "✗" };
            let exec_hint = execution_id.as_deref().unwrap_or("—");
            push_obs(state, format!("{symbol} [{exec_hint:.12}]"));
            for entry in state.job_history.iter_mut() {
                if entry.job_id == job_id {
                    entry.status = if succeeded { "succeeded" } else { "failed" }.to_string();
                    break;
                }
            }
        }
        TuiEvent::ToolInvoked { tool_name, input_summary } => {
            push_obs(state, format!("◆ {tool_name}  {input_summary}"));
        }
        TuiEvent::ToolPayload {
            tool_name,
            tool_input,
            tool_output,
        } => {
            let input = serde_json::to_string_pretty(&tool_input)
                .unwrap_or_else(|_| tool_input.to_string());
            let output = serde_json::to_string_pretty(&tool_output)
                .unwrap_or_else(|_| tool_output.to_string());
            push_obs(
                state,
                format!(
                    "◆ {tool_name}\ninput:\n{input}\noutput:\n{output}\n────────────────"
                ),
            );
        }
    }
}

fn handle_history_key_event(code: KeyCode, state: &mut TuiState) -> EventOutcome {
    match code {
        KeyCode::Up => {
            state.history_selected = state.history_selected.saturating_sub(1);
        }
        KeyCode::Down => {
            if !state.history_items.is_empty() {
                state.history_selected = (state.history_selected + 1)
                    .min(state.history_items.len().saturating_sub(1));
            }
        }
        KeyCode::Enter => {
            if let Some(selected) = state.history_items.get(state.history_selected).cloned() {
                stop_active_generation(state);
                state.session_id = selected.session_id.clone();
                state.conversation = load_history(&state.session_id);
                state.thinking_trace.clear();
                state.thinking_scroll = 0;
                state.thinking_max_scroll = 0;
                state.in_thinking_tag = false;
                state.stream_tag_tail.clear();
                state.input_buffer.clear();
                state.is_processing = false;
                state.active_agent_stream_turn = None;
                state.auto_scroll = true;
                state.conv_scroll = state.conv_max_scroll;
                save_last_session_id(&state.session_id);
                state.mode = UiMode::Chat;
            }
        }
        _ => {}
    }

    EventOutcome::Continue
}

async fn handle_settings_key_event(
    code: KeyCode,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    if state.settings_editing {
        match code {
            KeyCode::Enter => {
                state.settings_editing = false;
            }
            KeyCode::Backspace => {
                let target = selected_settings_field_mut(state);
                target.pop();
            }
            KeyCode::Char(c) => {
                let target = selected_settings_field_mut(state);
                target.push(c);
            }
            _ => {}
        }
        return EventOutcome::Continue;
    }

    match code {
        KeyCode::Char(' ') | KeyCode::Right => {
            quick_adjust_setting(state, true);
        }
        KeyCode::Left => {
            quick_adjust_setting(state, false);
        }
        KeyCode::Char('+') | KeyCode::Char('=') => {
            if state.settings_selected == 5 || state.settings_selected == 7 {
                quick_adjust_setting(state, true);
            }
        }
        KeyCode::Char('-') => {
            if state.settings_selected == 5 || state.settings_selected == 7 {
                quick_adjust_setting(state, false);
            }
        }
        KeyCode::Up => {
            state.settings_selected = state.settings_selected.saturating_sub(1);
        }
        KeyCode::Down => {
            state.settings_selected = (state.settings_selected + 1).min(9);
        }
        KeyCode::Enter => match state.settings_selected {
            1..=3 => {
                state.settings_editing = true;
            }
            0 | 4 | 5 | 6 | 7 => {
                quick_adjust_setting(state, true);
            }
            8 => {
                apply_settings(state, tui_rt, event_tx).await;
                state.mode = UiMode::Chat;
            }
            9 => {
                state.mode = UiMode::Chat;
            }
            _ => {}
        },
        _ => {}
    }

    EventOutcome::Continue
}

fn quick_adjust_setting(state: &mut TuiState, forward: bool) {
    match state.settings_selected {
        0 => {
            state.settings.backend = cycle_backend(&state.settings.backend, forward);
        }
        4 => {
            state.settings.tool_call_mode =
                cycle_tool_call_mode(&state.settings.tool_call_mode, forward);
        }
        5 => {
            let current = parse_usize_with_bounds(&state.settings.max_tool_rounds, 10, 1, 50);
            let step = if current < 20 { 1 } else { 5 };
            let next = if forward {
                current.saturating_add(step)
            } else {
                current.saturating_sub(step)
            }
            .clamp(1, 50);
            state.settings.max_tool_rounds = next.to_string();
        }
        6 => {
            let value = parse_bool_with_default(&state.settings.thinking_capture, true);
            state.settings.thinking_capture = (!value).to_string();
        }
        7 => {
            let current = parse_usize_with_bounds(&state.settings.thinking_max_lines, 300, 50, 5000);
            let step = if current < 500 { 50 } else { 100 };
            let next = if forward {
                current.saturating_add(step)
            } else {
                current.saturating_sub(step)
            }
            .clamp(50, 5000);
            state.settings.thinking_max_lines = next.to_string();
        }
        _ => {}
    }
}

fn selected_settings_field_mut(state: &mut TuiState) -> &mut String {
    match state.settings_selected {
        0 => &mut state.settings.backend,
        1 => &mut state.settings.provider,
        2 => &mut state.settings.model,
        3 => &mut state.settings.base_url,
        4 => &mut state.settings.tool_call_mode,
        5 => &mut state.settings.max_tool_rounds,
        6 => &mut state.settings.thinking_capture,
        7 => &mut state.settings.thinking_max_lines,
        _ => &mut state.settings.base_url,
    }
}

async fn apply_settings(
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) {
    let backend = resolve_backend_name(Some(state.settings.backend.trim()));
    let tool_call_mode = resolve_tool_call_mode_name(Some(state.settings.tool_call_mode.trim()));
    let max_tool_rounds = parse_usize_with_bounds(&state.settings.max_tool_rounds, 10, 1, 50);
    let thinking_capture = parse_bool_with_default(&state.settings.thinking_capture, true);
    let thinking_max_lines = parse_usize_with_bounds(
        &state.settings.thinking_max_lines,
        300,
        50,
        5000,
    );
    let provider = if state.settings.provider.trim().is_empty() {
        resolve_llm_provider(None)
    } else {
        resolve_llm_provider(Some(state.settings.provider.trim()))
    };
    let model = if state.settings.model.trim().is_empty() {
        resolve_llm_model(None)
    } else {
        resolve_llm_model(Some(state.settings.model.trim()))
    };
    let base_url = if state.settings.base_url.trim().is_empty() {
        None
    } else {
        Some(state.settings.base_url.trim().to_string())
    };

    match build_tui_runtime(
        parse_backend(Some(&backend)),
        Some(&provider),
        Some(&model),
        base_url.as_deref(),
        &state.session_id,
        event_tx.clone(),
    )
    .await
    {
        Ok(new_rt) => {
            *tui_rt = new_rt;
            state.settings.backend = backend.clone();
            state.settings.provider = provider.clone();
            state.settings.model = model.clone();
            state.settings.base_url = base_url.clone().unwrap_or_default();
            state.settings.tool_call_mode = tool_call_mode.clone();
            state.settings.max_tool_rounds = max_tool_rounds.to_string();
            state.settings.thinking_capture = thinking_capture.to_string();
            state.settings.thinking_max_lines = thinking_max_lines.to_string();
            state.provider_model = format!("{provider}:{model}");
            save_tui_defaults(&TuiDefaults {
                backend: Some(backend),
                provider: Some(provider),
                model: Some(model),
                base_url,
                tool_call_mode: Some(tool_call_mode),
                max_tool_rounds: Some(max_tool_rounds),
                thinking_capture: Some(thinking_capture),
                thinking_max_lines: Some(thinking_max_lines),
            });
            push_obs(state, "✓ settings applied".to_string());
        }
        Err(err) => {
            push_obs(state, format!("⚠ settings apply failed: {err}"));
        }
    }
}

fn push_obs(state: &mut TuiState, text: String) {
    state.observability.push_front(ObsEvent { text });
    if state.observability.len() > 50 {
        state.observability.pop_back();
    }
}

fn push_thinking(state: &mut TuiState, raw: String) {
    if !parse_bool_with_default(&state.settings.thinking_capture, true) {
        return;
    }

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let mut text: String = trimmed.chars().take(180).collect();
        if trimmed.chars().count() > 180 {
            text.push_str("...");
        }
        let stamp = Utc::now().format("%H:%M:%S").to_string();
        state.thinking_trace.push_front(format!("[{stamp}] {text}"));
    }

    let max_lines = parse_usize_with_bounds(&state.settings.thinking_max_lines, 300, 50, 5000);
    while state.thinking_trace.len() > max_lines {
        state.thinking_trace.pop_back();
    }
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
            if let Some((idx, marker_len)) = find_earliest_marker(&buffer, &["</think>", "</thinking>"]) {
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
            if let Some((idx, marker_len)) = find_earliest_marker(&remaining, &["</think>", "</thinking>"]) {
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
        } else if let Some((idx, marker_len)) = find_earliest_marker(&remaining, &["<think>", "<thinking>"]) {
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

// ── Rendering ─────────────────────────────────────────────────────────────────

fn render(frame: &mut ratatui::Frame, state: &mut TuiState) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(ui_bg()).fg(Color::White)),
        area,
    );

    // Outer: content rows + input bar (3 lines)
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(area);

    let content_area = outer[0];
    let input_area = outer[1];

    // Content: left conversation (65%) + right panels (35%)
    let content = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
        .split(content_area);

    let left = content[0];
    let right = content[1];

    // Right: observability top (50%) + job history bottom (50%)
    let right_panes = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(right);

    let obs_area = right_panes[0];
    let jobs_area = right_panes[1];

    // ── Conversation ──────────────────────────────────────────────────────────
    let conv_title = if state.is_processing {
        " Conversation  ⟳ "
    } else {
        " Conversation "
    };

    let inner_width = left.width.saturating_sub(2);
    let conv_text = build_conversation_text(&state.conversation, inner_width);
    // Compute visual wrapped height so scrolling lands on real bottom.
    let visible_height = left.height.saturating_sub(2);
    let visual_lines = visual_line_count(&conv_text, inner_width);
    let max_scroll = visual_lines.saturating_sub(visible_height);
    state.conv_max_scroll = max_scroll;
    let safe_scroll = if state.auto_scroll {
        max_scroll
    } else {
        state.conv_scroll.min(max_scroll)
    };
    state.conv_scroll = safe_scroll;

    let conv_border = if state.is_processing {
        Style::default().fg(ui_accent_warn())
    } else {
        Style::default().fg(ui_border())
    };

    let conv_widget = Paragraph::new(conv_text)
        .block(
            Block::default()
                .title(conv_title)
                .borders(Borders::ALL)
                .border_style(conv_border)
                .style(Style::default().bg(ui_panel_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_panel_bg()))
        .wrap(Wrap { trim: false })
        .scroll((safe_scroll, 0));
    frame.render_widget(conv_widget, left);

    // ── Observability ─────────────────────────────────────────────────────────
    let obs_inner_width = obs_area.width.saturating_sub(2);
    let obs_text = build_observability_text(state, false, obs_inner_width);
    let obs_visible_height = obs_area.height.saturating_sub(2);
    let obs_visual_lines = visual_line_count(&obs_text, obs_inner_width);
    state.obs_max_scroll = obs_visual_lines.saturating_sub(obs_visible_height);
    state.obs_scroll = state.obs_scroll.min(state.obs_max_scroll);

    let obs_widget = Paragraph::new(obs_text)
        .block(
            Block::default()
                .title(" Observability  (Ctrl+O expand) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_border()))
                .style(Style::default().bg(ui_subtle_panel_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_subtle_panel_bg()))
        .wrap(Wrap { trim: false })
        .scroll((state.obs_scroll, 0));
    frame.render_widget(obs_widget, obs_area);

    // ── Job History ───────────────────────────────────────────────────────────
    let job_lines: Vec<Line> = state
        .job_history
        .iter()
        .map(|j| {
            let (symbol, color) = match j.status.as_str() {
                "succeeded" => ("✓", Color::Green),
                "failed" => ("✗", Color::Red),
                _ => ("·", Color::DarkGray),
            };
            let type_label = j.job_type.split('.').last().unwrap_or(&j.job_type);
            let id_short: String = j.job_id.chars().take(10).collect();
            Line::from(vec![
                Span::styled(format!("{symbol} "), Style::default().fg(color)),
                Span::styled(
                    format!("{type_label} {id_short}"),
                    Style::default().fg(Color::White),
                ),
            ])
        })
        .collect();

    let jobs_widget = Paragraph::new(Text::from(job_lines))
        .block(
            Block::default()
                .title(" Job History ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_border()))
                .style(Style::default().bg(ui_subtle_panel_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_subtle_panel_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(jobs_widget, jobs_area);

    // ── Input bar ─────────────────────────────────────────────────────────────
    let session_short: String = state.session_id.chars().take(8).collect();
    let thinking_hint = if state.is_processing {
        "  thinking... (F2 peek / Ctrl+t panel)"
    } else if !state.thinking_trace.is_empty() {
        "  [F2 thinking]"
    } else {
        ""
    };
    let input_title = format!(
        " {}  session:{session_short}{} ",
        state.provider_model, thinking_hint
    );
    let input_display = format!("  {}_", state.input_buffer);
    let input_border = if state.is_processing {
        Style::default().fg(ui_accent_warn())
    } else {
        Style::default().fg(ui_accent_primary())
    };

    let input_widget = Paragraph::new(input_display)
        .block(
            Block::default()
                .title(input_title)
                .borders(Borders::ALL)
                .border_style(input_border)
                .style(Style::default().bg(ui_panel_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_panel_bg()));
    frame.render_widget(input_widget, input_area);

    if state.mode == UiMode::History {
        render_history_overlay(frame, state);
    } else if state.mode == UiMode::CommandPalette {
        render_command_palette_overlay(frame, state);
    } else if state.mode == UiMode::Settings {
        render_settings_overlay(frame, state);
    } else if state.mode == UiMode::ObservabilityPanel {
        render_observability_panel_overlay(frame, state);
    } else if state.mode == UiMode::ThinkingPeek {
        render_thinking_peek_overlay(frame, state);
    } else if state.mode == UiMode::ThinkingPanel {
        render_thinking_panel_overlay(frame, state);
    }
}

fn build_observability_text(state: &TuiState, expanded: bool, width: u16) -> Text<'static> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    if expanded {
        lines.push(Line::from(Span::styled(
            " Up/Down/Page: scroll  Home/End: jump  Esc/Ctrl+O: close ",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::from(""));
    }

    if state.observability.is_empty() {
        lines.push(Line::from(Span::styled(
            "No observability events yet.",
            Style::default().fg(Color::Gray),
        )));
        return Text::from(lines);
    }

    for (idx, ev) in state.observability.iter().enumerate() {
        if idx > 0 {
            lines.push(Line::from(Span::styled(
                "",
                Style::default().fg(Color::DarkGray),
            )));
        }
        for line in render_markdown_lines(&ev.text, width) {
            lines.push(line);
        }
    }

    Text::from(lines)
}

fn render_observability_panel_overlay(frame: &mut ratatui::Frame, state: &mut TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 90, 82);
    frame.render_widget(Clear, popup);

    let inner_width = popup.width.saturating_sub(2);
    let text = build_observability_text(state, true, inner_width);
    let visible_height = popup.height.saturating_sub(2);
    let visual_lines = visual_line_count(&text, inner_width);
    let max_scroll = visual_lines.saturating_sub(visible_height);
    state.obs_max_scroll = max_scroll;
    state.obs_scroll = state.obs_scroll.min(max_scroll);

    let panel = Paragraph::new(text)
        .block(
            Block::default()
                .title(" Observability Detail ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false })
        .scroll((state.obs_scroll, 0));
    frame.render_widget(panel, popup);
}

fn render_thinking_peek_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 62, 38);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Esc/F2: close  Enter/Down: open panel ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    if state.thinking_trace.is_empty() {
        lines.push(Line::from(Span::styled(
            if state.is_processing {
                "Thinking stream is active. Waiting for chunks..."
            } else {
                "No captured thinking yet in this run."
            },
            Style::default().fg(Color::Gray),
        )));
    } else {
        for item in state.thinking_trace.iter().take(8).rev() {
            lines.push(Line::from(Span::styled(
                item.clone(),
                Style::default().fg(Color::Cyan),
            )));
        }
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Thinking Peek ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, popup);
}

fn render_thinking_panel_overlay(frame: &mut ratatui::Frame, state: &mut TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 86, 78);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Up/Down/Page: scroll  Home/End: jump  Esc/Ctrl+t: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    if state.thinking_trace.is_empty() {
        lines.push(Line::from(Span::styled(
            "No captured thinking chunks.",
            Style::default().fg(Color::Gray),
        )));
    } else {
        for item in state.thinking_trace.iter().rev() {
            lines.push(Line::from(Span::styled(
                item.clone(),
                Style::default().fg(Color::Cyan),
            )));
        }
    }

    let text = Text::from(lines);
    let inner_width = popup.width.saturating_sub(2);
    let visible_height = popup.height.saturating_sub(2);
    let visual_lines = visual_line_count(&text, inner_width);
    let max_scroll = visual_lines.saturating_sub(visible_height);
    state.thinking_max_scroll = max_scroll;
    state.thinking_scroll = state.thinking_scroll.min(max_scroll);

    let panel = Paragraph::new(text)
        .block(
            Block::default()
                .title(" Thinking Detail ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false })
        .scroll((state.thinking_scroll, 0));
    frame.render_widget(panel, popup);
}

fn render_history_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 80, 70);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Enter: load session   Esc: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    if state.history_items.is_empty() {
        lines.push(Line::from(Span::styled(
            "No saved sessions yet.",
            Style::default().fg(Color::Gray),
        )));
    } else {
        for (idx, item) in state.history_items.iter().enumerate() {
            let marker = if idx == state.history_selected { ">" } else { " " };
            let ts = item
                .last_timestamp
                .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "-".to_string());
            let id_short: String = item.session_id.chars().take(8).collect();
            let line = format!(
                "{marker} {id_short}  {ts}  {} turn(s)  {}",
                item.turns, item.preview
            );

            let style = if idx == state.history_selected {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            lines.push(Line::from(Span::styled(line, style)));
        }
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Session History ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, popup);
}

fn render_command_palette_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 72, 58);
    frame.render_widget(Clear, popup);

    let actions = filtered_palette_actions(&state.command_query);
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        format!(" Query: {}", state.command_query),
        Style::default().fg(Color::Cyan),
    )));
    lines.push(Line::from(Span::styled(
        " Up/Down: select  Enter: run  Esc/Ctrl+K: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    if actions.is_empty() {
        lines.push(Line::from(Span::styled(
            "No matching commands.",
            Style::default().fg(Color::Gray),
        )));
    } else {
        for (idx, action) in actions.iter().enumerate() {
            let marker = if idx == state.command_selected { ">" } else { " " };
            let style = if idx == state.command_selected {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            lines.push(Line::from(Span::styled(
                format!("{marker} {}  ({})", action.title, action.command),
                style,
            )));
        }
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Command Palette ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, popup);
}

fn render_settings_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 76, 62);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Up/Down: select  Enter: toggle/edit/apply  Space/Left/Right: quick toggle  +/-: adjust number  Ctrl+,/Esc: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    let rows = vec![
        format!("Backend: {}  [toggle]", state.settings.backend),
        format!("Provider: {}  [edit]", state.settings.provider),
        format!("Model: {}  [edit]", state.settings.model),
        format!(
            "Base URL: {}  [edit]",
            if state.settings.base_url.is_empty() {
                "(auto)".to_string()
            } else {
                state.settings.base_url.clone()
            }
        ),
        format!("Tool Call Mode: {}  [toggle]", state.settings.tool_call_mode),
        format!("Max Tool Rounds: {}  [number]", state.settings.max_tool_rounds),
        format!("Thinking Capture: {}  [toggle]", state.settings.thinking_capture),
        format!("Thinking Max Lines: {}  [number]", state.settings.thinking_max_lines),
        "Apply and Save  [action]".to_string(),
        "Cancel  [action]".to_string(),
    ];

    for (idx, row) in rows.iter().enumerate() {
        let marker = if idx == state.settings_selected { ">" } else { " " };
        let mut style = if idx == state.settings_selected {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };

        if idx == state.settings_selected && state.settings_editing && idx <= 7 {
            style = style.add_modifier(Modifier::UNDERLINED);
        }

        lines.push(Line::from(Span::styled(format!("{marker} {row}"), style)));
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Settings ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, popup);
}

fn ui_bg() -> Color {
    Color::Rgb(18, 22, 29)
}

fn ui_panel_bg() -> Color {
    Color::Rgb(26, 32, 41)
}

fn ui_subtle_panel_bg() -> Color {
    Color::Rgb(23, 28, 36)
}

fn ui_modal_bg() -> Color {
    Color::Rgb(31, 38, 49)
}

fn ui_border() -> Color {
    Color::Rgb(71, 89, 105)
}

fn ui_accent_primary() -> Color {
    Color::Rgb(64, 186, 213)
}

fn ui_accent_warn() -> Color {
    Color::Rgb(245, 189, 99)
}

fn centered_rect(area: ratatui::layout::Rect, percent_x: u16, percent_y: u16) -> ratatui::layout::Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn export_current_session(state: &TuiState, format: &str) -> std::result::Result<PathBuf, String> {
    let exports_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("medousa")
        .join("exports");

    std::fs::create_dir_all(&exports_dir).map_err(|e| e.to_string())?;

    let ts = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    match format {
        "jsonl" => {
            let path = exports_dir.join(format!("{}-{ts}.jsonl", state.session_id));
            let mut out = String::new();
            for turn in &state.conversation {
                let line = serde_json::to_string(turn).map_err(|e| e.to_string())?;
                out.push_str(&line);
                out.push('\n');
            }
            std::fs::write(&path, out).map_err(|e| e.to_string())?;
            Ok(path)
        }
        _ => {
            let path = exports_dir.join(format!("{}-{ts}.md", state.session_id));
            let mut out = format!("# Medousa Session {}\n\n", state.session_id);
            for turn in &state.conversation {
                let title = if turn.role == "user" { "User" } else { "Assistant" };
                out.push_str(&format!("## {title} ({})\n\n", turn.timestamp.to_rfc3339()));
                out.push_str(&turn.content);
                out.push_str("\n\n");
                if !turn.tool_names.is_empty() {
                    out.push_str(&format!("Tools: {}\n\n", turn.tool_names.join(", ")));
                }
            }
            std::fs::write(&path, out).map_err(|e| e.to_string())?;
            Ok(path)
        }
    }
}

fn build_conversation_text(turns: &[ConversationTurn], width: u16) -> Text<'static> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    for turn in turns {
        match turn.role.as_str() {
            "user" => {
                lines.push(Line::from(Span::styled(
                    "  you".to_string(),
                    Style::default()
                        .fg(Color::Blue)
                        .add_modifier(Modifier::BOLD),
                )));
            }
            _ => {
                lines.push(Line::from(Span::styled(
                    "  ◈".to_string(),
                    Style::default()
                        .fg(Color::Magenta)
                        .add_modifier(Modifier::BOLD),
                )));
            }
        }

        if turn.role == "user" {
            for content_line in turn.content.lines() {
                lines.push(Line::from(Span::styled(
                    format!("  {content_line}"),
                    Style::default().fg(Color::White),
                )));
            }
        } else {
            lines.extend(render_markdown_lines(&turn.content, width));
        }

        if !turn.tool_names.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("  [{}]", turn.tool_names.join(", ")),
                Style::default().fg(Color::DarkGray),
            )));
        }

        lines.push(Line::from(""));
    }

    Text::from(lines)
}

fn render_markdown_lines(content: &str, width: u16) -> Vec<Line<'static>> {
    let max_width = width.saturating_sub(2).max(20) as usize;
    let renderer = MarkdownRenderer::new(max_width);
    let blocks = renderer.parse(content);
    renderer.render(&blocks, &DefaultTheme)
}

/// Count the number of visual rows a wrapped Paragraph would occupy.
/// Each source Line expands to ceil(line_display_width / inner_width) rows (min 1).
fn visual_line_count(text: &Text, inner_width: u16) -> u16 {
    if inner_width == 0 {
        return text.lines.len() as u16;
    }
    text.lines
        .iter()
        .map(|line| {
            let w = line.width() as u16;
            if w == 0 { 1 } else { w.div_ceil(inner_width) }
        })
        .fold(0u16, |acc, rows| acc.saturating_add(rows))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn find_arg_value<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    let idx = args.iter().position(|a| a == key)?;
    args.get(idx + 1).map(|s| s.as_str())
}

fn resolve_backend_name(value: Option<&str>) -> String {
    match value.unwrap_or("surreal-mem").trim() {
        "in-memory" => "in-memory".to_string(),
        "surreal-mem" => "surreal-mem".to_string(),
        _ => "surreal-mem".to_string(),
    }
}

fn cycle_backend(current: &str, forward: bool) -> String {
    let choices = ["surreal-mem", "in-memory"];
    cycle_choice(current, &choices, forward)
}

fn resolve_tool_call_mode_name(value: Option<&str>) -> String {
    match value.unwrap_or("auto").trim().to_ascii_lowercase().as_str() {
        "strict" => "strict".to_string(),
        _ => "auto".to_string(),
    }
}

fn cycle_tool_call_mode(current: &str, forward: bool) -> String {
    let choices = ["auto", "strict"];
    cycle_choice(current, &choices, forward)
}

fn cycle_choice(current: &str, choices: &[&str], forward: bool) -> String {
    if choices.is_empty() {
        return current.to_string();
    }

    let idx = choices
        .iter()
        .position(|v| v.eq_ignore_ascii_case(current))
        .unwrap_or(0);

    let next_idx = if forward {
        (idx + 1) % choices.len()
    } else if idx == 0 {
        choices.len().saturating_sub(1)
    } else {
        idx - 1
    };

    choices[next_idx].to_string()
}

fn parse_tool_call_mode(value: &str) -> ToolCallMode {
    if value.trim().eq_ignore_ascii_case("strict") {
        ToolCallMode::Strict
    } else {
        ToolCallMode::Auto
    }
}

fn resolve_bool_arg(value: Option<&str>, default_value: bool) -> bool {
    value
        .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default_value)
}

fn parse_bool_with_default(value: &str, default_value: bool) -> bool {
    resolve_bool_arg(Some(value), default_value)
}

fn resolve_usize_arg(
    value: Option<&str>,
    default_value: usize,
    min_value: usize,
    max_value: usize,
) -> usize {
    value
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(default_value)
        .clamp(min_value, max_value)
}

fn parse_usize_with_bounds(
    value: &str,
    default_value: usize,
    min_value: usize,
    max_value: usize,
) -> usize {
    resolve_usize_arg(Some(value), default_value, min_value, max_value)
}

fn print_help() {
    println!("medousa-tui — persistent cognitive terminal agent");
    println!();
    println!("USAGE:");
    println!("  medousa_tui [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("  --provider <name>     LLM provider (env: MEDOUSA_LLM_PROVIDER)");
    println!("  --model <name>        Model name (env: MEDOUSA_LLM_MODEL)");
    println!("  --base-url <url>      Custom API base URL (env: MEDOUSA_LLM_BASE_URL)");
    println!("  --backend <name>      Runtime backend: surreal-mem | in-memory");
    println!("  --tool-call-mode <m>  Tool call mode: auto | strict");
    println!("  --max-tool-rounds <n> Max model tool-call rounds (1-50, default 10)");
    println!("  --thinking-capture <b> Capture thinking chunks: true | false");
    println!("  --thinking-max-lines <n> Retained thinking lines (50-5000)");
    println!("  --session <id>        Resume a specific session by ID");
    println!("  --help, -h            Print this help");
    println!();
    println!("KEYS:");
    println!("  Enter        Submit message");
    println!("  Backspace    Delete character");
    println!("  Up/Down      Scroll conversation");
    println!("  PageUp/Down  Scroll by 10 lines");
    println!("  Home/End     Jump to start/end");
    println!("  Ctrl+H       Open/close session history menu");
    println!("  Ctrl+K       Open/close command palette");
    println!("  Ctrl+,       Open/close settings menu");
    println!("  Ctrl+O       Open/close observability detail panel");
    println!("  F2           Toggle thinking peek overlay");
    println!("  Ctrl+T       Toggle thinking detail panel");
    println!("  Ctrl+G       Stop active generation");
    println!("  Esc          Close current menu");
    println!("  Ctrl+C       Quit");
    println!();
    println!("HISTORY:");
    println!("  Conversations are persisted to ~/.local/share/medousa/history/<session_id>.jsonl");
    println!();
    println!("SLASH COMMANDS:");
    println!("  /new                    Start fresh session");
    println!("  /history                Open session history menu");
    println!("  /settings               Open settings menu");
    println!("  /model                  Show current provider:model");
    println!("  /model <model>          Set model, keep provider");
    println!("  /model <provider:model> Set provider and model");
    println!("  /stop                   Stop active generation");
    println!("  /regen                  Regenerate last user prompt");
    println!("  /export [md|jsonl]      Export current transcript");
}
