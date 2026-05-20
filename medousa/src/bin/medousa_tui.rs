use std::collections::{HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use ratatui_markdown::{DefaultTheme, markdown::MarkdownRenderer};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use medousa::{
    EnqueueAskRequest, EnqueueResponse, HealthResponse, RegisterRecurringPromptRequest,
    RegisterRecurringResponse, TuiRuntime, build_tui_runtime,
    events::TuiEvent,
    parse_backend, process_once, resolve_daemon_url, resolve_llm_base_url, resolve_llm_model,
    resolve_llm_provider,
    session::{
        ApiKeyStorageBackend, ConversationTurn, SessionHistorySummary, TuiDefaults, append_turn,
        detect_tui_api_key_storage_backend, list_history_sessions, load_history, load_tui_api_key,
        load_tui_defaults, save_last_session_id, save_tui_api_key, save_tui_defaults,
    },
    settings_guard::{invalid_module_ids, parse_allowed_modules, redact_json_value},
    tui::allowlist_preview::analyze_allowlist_preview,
    tui::editor_buffer::TextBuffer,
    tui::settings::{
        RuntimeSettings, cycle_backend, cycle_tool_call_mode, parse_bool_with_default,
        parse_usize_with_bounds, resolve_backend_name, resolve_bool_arg,
        resolve_tool_call_mode_name, resolve_usize_arg, settings_validation_errors,
    },
};
use stasis::application::orchestration::tool_loop_pipeline::{
    ToolCallMode, ToolLoopExecutionRequest,
};
use stasis::prelude::{
    BackoffPolicy, ChatMessage, JobAttemptOutcome, JobAttemptStore, NewJob, PromptExecutionContext,
    RuntimeComposition,
};

#[path = "medousa_tui/command_preview_ui.rs"]
mod command_preview_ui;
#[path = "medousa_tui/settings_ui.rs"]
mod settings_ui;

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
    settings_draft: RuntimeSettings,
    allowlist_preview_source: String,
    editor_buffer: TextBuffer,
    editor_file_path: Option<PathBuf>,
    editor_status: String,
    editor_dirty: bool,
    editor_preferred_col: Option<usize>,
    editor_scroll: u16,
    settings_selected: usize,
    settings_editing: bool,
    provider_model: String,
    session_id: String,
    thinking_trace: VecDeque<String>,
    thinking_scroll: u16,
    thinking_max_scroll: u16,
    obs_scroll: u16,
    obs_max_scroll: u16,
    job_scroll: u16,
    job_max_scroll: u16,
    in_thinking_tag: bool,
    stream_tag_tail: String,
    daemon_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiMode {
    Chat,
    History,
    CommandPalette,
    Settings,
    ObservabilityPanel,
    AllowlistPreview,
    Editor,
    ThinkingPeek,
    ThinkingPanel,
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
    let daemon_url = find_arg_value(&args, "--daemon-url");
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
    let resolved_thinking_capture =
        resolve_bool_arg(thinking_capture, defaults.thinking_capture.unwrap_or(true));
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
    let resolved_api_key = load_tui_api_key().unwrap_or_default();
    let resolved_allowed_modules = defaults
        .allowed_modules
        .clone()
        .unwrap_or_default()
        .join(",");
    let provider_model = format!("{resolved_provider}:{resolved_model}");
    let resolved_daemon_url = resolve_daemon_url(daemon_url);

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
        parse_allowed_modules(&resolved_allowed_modules),
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

    let initial_settings = RuntimeSettings {
        backend: resolved_backend.clone(),
        provider: resolved_provider.clone(),
        model: resolved_model.clone(),
        base_url: resolved_base_url.clone().unwrap_or_default(),
        api_key: resolved_api_key.clone(),
        allowed_modules: resolved_allowed_modules.clone(),
        tool_call_mode: resolved_tool_call_mode.clone(),
        max_tool_rounds: resolved_max_tool_rounds.to_string(),
        thinking_capture: resolved_thinking_capture.to_string(),
        thinking_max_lines: resolved_thinking_max_lines.to_string(),
    };

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
        settings: initial_settings.clone(),
        settings_draft: initial_settings,
        allowlist_preview_source: String::new(),
        editor_buffer: TextBuffer::default(),
        editor_file_path: None,
        editor_status: "No file loaded".to_string(),
        editor_dirty: false,
        editor_preferred_col: None,
        editor_scroll: 0,
        settings_selected: 0,
        settings_editing: false,
        provider_model,
        session_id: session_id.clone(),
        thinking_trace: VecDeque::new(),
        thinking_scroll: 0,
        thinking_max_scroll: 0,
        obs_scroll: 0,
        obs_max_scroll: 0,
        job_scroll: 0,
        job_max_scroll: 0,
        in_thinking_tag: false,
        stream_tag_tail: String::new(),
        daemon_url: resolved_daemon_url,
    };

    // ── Keyboard reader (spawn_blocking to keep async event loop clean) ───────
    let (key_tx, mut key_rx) = mpsc::channel::<Event>(64);
    tokio::task::spawn_blocking(move || {
        loop {
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
        if state.mode == UiMode::Settings {
            state.settings_draft = state.settings.clone();
        }
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
            state.settings_draft = state.settings.clone();
        } else {
            state.mode = UiMode::Settings;
            state.settings_selected = 0;
            state.settings_editing = false;
            state.settings_draft = state.settings.clone();
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

    if state.mode == UiMode::AllowlistPreview {
        return handle_allowlist_preview_key_event(key.code, state);
    }

    if state.mode == UiMode::Editor {
        return handle_editor_key_event(key, state);
    }

    if state.mode == UiMode::ThinkingPeek || state.mode == UiMode::ThinkingPanel {
        return handle_thinking_key_event(key.code, state);
    }

    if state.mode == UiMode::ObservabilityPanel {
        return handle_observability_key_event(key.code, state);
    }

    if state.mode == UiMode::Chat && key.modifiers.contains(KeyModifiers::SHIFT) {
        match key.code {
            KeyCode::Up => {
                state.obs_scroll = state.obs_scroll.saturating_sub(1);
                state.job_scroll = state.job_scroll.saturating_sub(1);
                return EventOutcome::Continue;
            }
            KeyCode::Down => {
                state.obs_scroll = state.obs_scroll.saturating_add(1).min(state.obs_max_scroll);
                state.job_scroll = state.job_scroll.saturating_add(1).min(state.job_max_scroll);
                return EventOutcome::Continue;
            }
            KeyCode::PageUp => {
                state.obs_scroll = state.obs_scroll.saturating_sub(10);
                state.job_scroll = state.job_scroll.saturating_sub(10);
                return EventOutcome::Continue;
            }
            KeyCode::PageDown => {
                state.obs_scroll = state
                    .obs_scroll
                    .saturating_add(10)
                    .min(state.obs_max_scroll);
                state.job_scroll = state
                    .job_scroll
                    .saturating_add(10)
                    .min(state.job_max_scroll);
                return EventOutcome::Continue;
            }
            KeyCode::Home => {
                state.obs_scroll = 0;
                state.job_scroll = 0;
                return EventOutcome::Continue;
            }
            KeyCode::End => {
                state.obs_scroll = state.obs_max_scroll;
                state.job_scroll = state.job_max_scroll;
                return EventOutcome::Continue;
            }
            _ => {}
        }
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
        (KeyCode::Enter, _) if !state.is_processing && !state.input_buffer.trim().is_empty() => {
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
            state.obs_scroll = state
                .obs_scroll
                .saturating_add(10)
                .min(state.obs_max_scroll);
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

fn handle_allowlist_preview_key_event(code: KeyCode, state: &mut TuiState) -> EventOutcome {
    command_preview_ui::handle_allowlist_preview_key_event(code, state)
}

fn handle_editor_key_event(key: KeyEvent, state: &mut TuiState) -> EventOutcome {
    if key.code == KeyCode::Char('s') && key.modifiers.contains(KeyModifiers::CONTROL) {
        save_editor_buffer(state, None);
        return EventOutcome::Continue;
    }

    let mut edited = false;
    let mut vertical_nav = false;

    match key.code {
        KeyCode::Left => {
            state.editor_buffer.move_left();
            state.editor_preferred_col = None;
        }
        KeyCode::Right => {
            state.editor_buffer.move_right();
            state.editor_preferred_col = None;
        }
        KeyCode::Up => {
            let (_, col) = state.editor_buffer.line_col();
            let preferred_col = state.editor_preferred_col.unwrap_or(col);
            state.editor_buffer.move_up(preferred_col);
            state.editor_preferred_col = Some(preferred_col);
            vertical_nav = true;
        }
        KeyCode::Down => {
            let (_, col) = state.editor_buffer.line_col();
            let preferred_col = state.editor_preferred_col.unwrap_or(col);
            state.editor_buffer.move_down(preferred_col);
            state.editor_preferred_col = Some(preferred_col);
            vertical_nav = true;
        }
        KeyCode::Home => {
            state.editor_buffer.move_line_start();
            state.editor_preferred_col = Some(1);
        }
        KeyCode::End => {
            state.editor_buffer.move_line_end();
            let (_, col) = state.editor_buffer.line_col();
            state.editor_preferred_col = Some(col);
        }
        KeyCode::Enter => {
            state.editor_buffer.insert_newline();
            state.editor_preferred_col = None;
            edited = true;
        }
        KeyCode::Backspace => {
            state.editor_buffer.backspace();
            state.editor_preferred_col = None;
            edited = true;
        }
        KeyCode::Tab => {
            state.editor_buffer.insert_str("    ");
            state.editor_preferred_col = None;
            edited = true;
        }
        KeyCode::Char(c) => {
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT {
                state.editor_buffer.insert_char(c);
                state.editor_preferred_col = None;
                edited = true;
            }
        }
        _ => {}
    }

    if edited {
        state.editor_dirty = true;
        state.editor_status = "Unsaved changes".to_string();
    }

    if edited || vertical_nav {
        keep_editor_cursor_visible(state, 12);
    }

    EventOutcome::Continue
}

fn keep_editor_cursor_visible(state: &mut TuiState, viewport_lines: usize) {
    let viewport = viewport_lines.max(3) as u16;
    let cursor_line = state.editor_buffer.line_col().0.saturating_sub(1) as u16;

    if cursor_line < state.editor_scroll {
        state.editor_scroll = cursor_line;
        return;
    }

    let max_visible = state
        .editor_scroll
        .saturating_add(viewport.saturating_sub(1));
    if cursor_line > max_visible {
        state.editor_scroll = cursor_line.saturating_sub(viewport.saturating_sub(1));
    }
}

fn save_editor_buffer(state: &mut TuiState, path_override: Option<&str>) {
    if let Some(path_raw) = path_override {
        if !path_raw.trim().is_empty() {
            state.editor_file_path = Some(PathBuf::from(path_raw.trim()));
        }
    }

    let Some(path) = state.editor_file_path.clone() else {
        state.editor_status = "Save failed: no path. Use /save <path>".to_string();
        push_obs(
            state,
            "⚠ save failed: no target path. use /save <path>".to_string(),
        );
        return;
    };

    match write_editor_file(&path, state.editor_buffer.as_text()) {
        Ok(_) => {
            state.editor_dirty = false;
            state.editor_status = format!("Saved {}", path.display());
            push_obs(state, format!("✓ saved {}", path.display()));
        }
        Err(err) => {
            state.editor_status = format!("Save failed: {err}");
            push_obs(state, format!("⚠ save failed: {err}"));
        }
    }
}

fn load_editor_file(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(Some(content)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_editor_file(path: &Path, content: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)
}

fn resolve_editor_run_source(
    path_override: Option<&str>,
    editor_file_path: Option<&Path>,
    editor_text: &str,
) -> std::result::Result<(String, String), String> {
    let source_target = path_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);

    if let Some(path) = source_target {
        return match load_editor_file(&path) {
            Ok(Some(content)) => Ok((content, format!("file:{}", path.display()))),
            Ok(None) => Err(format!("run failed: file not found {}", path.display())),
            Err(err) => Err(format!("run failed: {err}")),
        };
    }

    let label = editor_file_path
        .map(|path| format!("editor:{}", path.display()))
        .unwrap_or_else(|| "editor:buffer".to_string());
    Ok((editor_text.to_string(), label))
}

fn validate_editor_run_allowlist(
    source: &str,
    allowed_modules_csv: &str,
) -> std::result::Result<Vec<String>, String> {
    let analysis = analyze_allowlist_preview(source, allowed_modules_csv);
    if !analysis.invalid_allowlist.is_empty() {
        return Err(format!(
            "run blocked: invalid allowlist entries: {}",
            analysis.invalid_allowlist.join(", ")
        ));
    }

    if !analysis.blocked_ops.is_empty() {
        return Err(format!(
            "run blocked by allowlist: {}",
            analysis.blocked_ops.join(", ")
        ));
    }

    Ok(analysis.referenced_ops)
}

async fn run_editor_source_via_runtime(
    state: &mut TuiState,
    tui_rt: &TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
    path_override: Option<&str>,
) {
    let (source, source_label) = match resolve_editor_run_source(
        path_override,
        state.editor_file_path.as_deref(),
        state.editor_buffer.as_text(),
    ) {
        Ok(values) => values,
        Err(message) => {
            push_obs(state, format!("⚠ {message}"));
            return;
        }
    };

    if source.trim().is_empty() {
        push_obs(state, "⚠ run failed: source is empty".to_string());
        return;
    }

    let referenced_ops =
        match validate_editor_run_allowlist(&source, &state.settings.allowed_modules) {
            Ok(ops) => ops,
            Err(message) => {
                push_obs(state, format!("⚠ {message}"));
                return;
            }
        };

    let _ = event_tx
        .send(TuiEvent::ToolInvoked {
            tool_name: "editor.gr.run".to_string(),
            input_summary: format!("{source_label}  {} byte(s)", source.len()),
        })
        .await;

    let job_id = format!("editor-gr-run-{}", Uuid::new_v4().simple());
    let now = Utc::now();
    let job = NewJob {
        id: job_id.clone(),
        queue: "default".to_string(),
        job_type: "workflow.grapheme.run".to_string(),
        payload_ref: format!("grapheme:inline:{source}"),
        priority: 100,
        max_attempts: 1,
        idempotency_key: format!("idem-{job_id}"),
        correlation_id: job_id.clone(),
        causation_id: "medousa_tui.editor_run".to_string(),
        trace_id: job_id.clone(),
        sttp_input_node_id: "sttp:in:cognition:grapheme:editor-run".to_string(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy::default(),
    };

    let enqueue_result = match &*tui_rt.runtime {
        RuntimeComposition::InMemory(rt) => rt.enqueue(job).await,
        RuntimeComposition::Surreal(rt) => rt.enqueue(job).await,
    };

    if let Err(err) = enqueue_result {
        push_obs(state, format!("⚠ run enqueue failed: {err}"));
        return;
    }

    let _ = event_tx
        .send(TuiEvent::JobEnqueued {
            job_id: job_id.clone(),
            job_type: "workflow.grapheme.run".to_string(),
        })
        .await;

    if let Err(err) = process_once(&tui_rt.runtime, "medousa_tui.editor_run").await {
        push_obs(state, format!("⚠ run processing failed: {err}"));
        return;
    }

    let attempts_result = match &*tui_rt.runtime {
        RuntimeComposition::InMemory(rt) => rt.job_attempt_store.list_by_job_id(&job_id).await,
        RuntimeComposition::Surreal(rt) => rt.job_attempt_store.list_by_job_id(&job_id).await,
    };

    let attempts = match attempts_result {
        Ok(list) => list,
        Err(err) => {
            push_obs(state, format!("⚠ run diagnostics failed: {err}"));
            return;
        }
    };

    let Some(last_attempt) = attempts.last() else {
        push_obs(state, "⚠ run failed: no attempt recorded".to_string());
        return;
    };

    let succeeded = last_attempt.outcome == JobAttemptOutcome::Succeeded;
    let _ = event_tx
        .send(TuiEvent::JobProcessed {
            job_id: job_id.clone(),
            succeeded,
            execution_id: last_attempt.execution_id.clone(),
        })
        .await;

    let diagnostics_json = last_attempt
        .diagnostics
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or_else(|| {
            Value::String(
                last_attempt
                    .diagnostics
                    .clone()
                    .unwrap_or_else(|| "".to_string()),
            )
        });

    let output = serde_json::json!({
        "source": source_label,
        "job_id": job_id,
        "succeeded": succeeded,
        "attempt_outcome": format!("{:?}", last_attempt.outcome),
        "execution_id": last_attempt.execution_id,
        "diagnostics": diagnostics_json,
    });
    let _ = event_tx
        .send(TuiEvent::ToolPayload {
            tool_name: "editor.gr.run".to_string(),
            tool_input: serde_json::json!({
                "source_bytes": source.len(),
                "referenced_ops": referenced_ops,
                "allowed_modules": parse_allowed_modules(&state.settings.allowed_modules),
            }),
            tool_output: output,
        })
        .await;
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
                parse_allowed_modules(&state.settings.allowed_modules),
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
            state.settings_draft = state.settings.clone();
        }
        "/allowlist-preview" => {
            state.mode = UiMode::AllowlistPreview;
            state.allowlist_preview_source = parts.collect::<Vec<_>>().join(" ");
            if state.allowlist_preview_source.trim().is_empty() {
                state.allowlist_preview_source =
                    "query Run { websearch.search(query: \"\") { ok } }".to_string();
            }
        }
        "/edit" | "/open" => {
            let path_raw = parts.collect::<Vec<_>>().join(" ");
            if path_raw.trim().is_empty() {
                state.mode = UiMode::Editor;
                state.editor_status =
                    "Editor opened. Use /open <path> or /save <path> to persist.".to_string();
                state.editor_preferred_col = None;
                keep_editor_cursor_visible(state, 12);
            } else {
                let path = PathBuf::from(path_raw.trim());
                match load_editor_file(&path) {
                    Ok(Some(content)) => {
                        state.editor_buffer = TextBuffer::from_text(content);
                        state.editor_file_path = Some(path.clone());
                        state.editor_status = format!("Opened {}", path.display());
                        state.editor_dirty = false;
                        state.editor_preferred_col = None;
                        state.editor_scroll = 0;
                        keep_editor_cursor_visible(state, 12);
                        state.mode = UiMode::Editor;
                    }
                    Ok(None) => {
                        state.editor_buffer = TextBuffer::default();
                        state.editor_file_path = Some(path.clone());
                        state.editor_status =
                            format!("New file {} (not saved yet)", path.display());
                        state.editor_dirty = false;
                        state.editor_preferred_col = None;
                        state.editor_scroll = 0;
                        state.mode = UiMode::Editor;
                    }
                    Err(err) => {
                        push_obs(state, format!("⚠ open failed: {err}"));
                    }
                }
            }
        }
        "/save" => {
            let path_raw = parts.collect::<Vec<_>>().join(" ");
            save_editor_buffer(state, Some(path_raw.as_str()));
        }
        "/run" => {
            let path_raw = parts.collect::<Vec<_>>().join(" ");
            let override_path = if path_raw.trim().is_empty() {
                None
            } else {
                Some(path_raw.as_str())
            };
            run_editor_source_via_runtime(state, tui_rt, event_tx, override_path).await;
        }
        "/run-current" => {
            let Some(path) = state.editor_file_path.clone() else {
                push_obs(
                    state,
                    "⚠ run-current failed: no editor file path set. use /open <path> or /run <path>"
                        .to_string(),
                );
                return EventOutcome::Continue;
            };

            let path_value = path.display().to_string();
            run_editor_source_via_runtime(state, tui_rt, event_tx, Some(path_value.as_str())).await;
        }
        "/close" => {
            push_obs(state, "✓ closing medousa_tui".to_string());
            return EventOutcome::Break;
        }
        "/clear-key" => {
            state.settings.api_key.clear();
            state.settings_draft.api_key.clear();
            save_tui_api_key(None);
            push_obs(state, "✓ api key cleared from secure storage".to_string());
        }
        "/rotate-key" => {
            let key = state.settings_draft.api_key.trim().to_string();
            if key.is_empty() {
                push_obs(
                    state,
                    "⚠ key rotation requires a non-empty draft API key".to_string(),
                );
                return EventOutcome::Continue;
            }

            save_tui_api_key(Some(&key));
            state.settings.api_key = key.clone();
            state.settings_draft.api_key = key;
            push_obs(state, "✓ api key rotated in secure storage".to_string());
        }
        "/model" => {
            let args = parts.collect::<Vec<_>>();
            if args.is_empty() {
                push_obs(
                    state,
                    format!("model {}:{}", state.settings.provider, state.settings.model),
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

            state.settings_draft = state.settings.clone();

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
                push_obs(
                    state,
                    "⚠ no user prompt available to regenerate".to_string(),
                );
            }
        }
        "/export" => {
            let format = parts.next().unwrap_or("md");
            match export_current_session(state, format) {
                Ok(path) => push_obs(state, format!("✓ exported {}", path.display())),
                Err(err) => push_obs(state, format!("⚠ export failed: {err}")),
            }
        }
        "/daemon" => {
            let sub = parts.next().unwrap_or("");
            match sub {
                "" => {
                    push_obs(
                        state,
                        format!(
                            "daemon url={} | commands: /daemon health | /daemon ask <prompt> | /daemon url <url>",
                            state.daemon_url
                        ),
                    );
                }
                "url" => {
                    let next = parts.collect::<Vec<_>>().join(" ");
                    if next.trim().is_empty() {
                        push_obs(state, format!("daemon url={}", state.daemon_url));
                    } else {
                        state.daemon_url = next.trim().to_string();
                        push_obs(state, format!("✓ daemon url set to {}", state.daemon_url));
                    }
                }
                "health" => match daemon_health(&state.daemon_url).await {
                    Ok(payload) => push_obs(
                        state,
                        format!(
                            "✓ daemon {} backend={} worker={}",
                            payload.status, payload.backend, payload.worker_id
                        ),
                    ),
                    Err(err) => push_obs(state, format!("⚠ daemon health failed: {err}")),
                },
                "ask" => {
                    let prompt = parts.collect::<Vec<_>>().join(" ");
                    if prompt.trim().is_empty() {
                        push_obs(state, "⚠ usage: /daemon ask <prompt>".to_string());
                    } else {
                        match daemon_enqueue_ask(&state.daemon_url, &prompt).await {
                            Ok(payload) => {
                                push_obs(state, format!("✓ daemon job enqueued {}", payload.job_id))
                            }
                            Err(err) => {
                                push_obs(state, format!("⚠ daemon ask failed: {err}"));
                            }
                        }
                    }
                }
                _ => {
                    push_obs(
                        state,
                        "⚠ unknown /daemon command. try /daemon health | /daemon ask <prompt> | /daemon url <url>"
                            .to_string(),
                    );
                }
            }
        }
        "/watch" => {
            let sub = parts.next().unwrap_or("");
            if sub != "add" {
                push_obs(
                    state,
                    "⚠ usage: /watch add <cron_expr> <prompt...>".to_string(),
                );
                return EventOutcome::Continue;
            }

            let cron_expr = match parts.next() {
                Some(value) => value,
                None => {
                    push_obs(
                        state,
                        "⚠ usage: /watch add <cron_expr> <prompt...>".to_string(),
                    );
                    return EventOutcome::Continue;
                }
            };

            let prompt = parts.collect::<Vec<_>>().join(" ");
            if prompt.trim().is_empty() {
                push_obs(
                    state,
                    "⚠ usage: /watch add <cron_expr> <prompt...>".to_string(),
                );
                return EventOutcome::Continue;
            }

            match daemon_register_recurring_prompt(&state.daemon_url, cron_expr, &prompt).await {
                Ok(payload) => push_obs(
                    state,
                    format!(
                        "✓ watch {} next={}",
                        payload.recurring_id, payload.next_run_at_utc
                    ),
                ),
                Err(err) => push_obs(state, format!("⚠ watch add failed: {err}")),
            }
        }
        _ => {
            push_obs(
                state,
                "⚠ unknown command. try /new /history /settings /edit /open /save /run /run-current /close /allowlist-preview /clear-key /rotate-key /model /stop /regen /export /daemon /watch"
                    .to_string(),
            );
        }
    }

    EventOutcome::Continue
}

async fn daemon_health(daemon_url: &str) -> Result<HealthResponse> {
    let client = Client::new();
    let response = client
        .get(format!("{daemon_url}/health"))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json::<HealthResponse>().await?)
}

async fn daemon_enqueue_ask(daemon_url: &str, prompt: &str) -> Result<EnqueueResponse> {
    let client = Client::new();
    let request = EnqueueAskRequest {
        prompt: prompt.to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        max_turns: Some(1),
    };

    let response = client
        .post(format!("{daemon_url}/v1/jobs/ask"))
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json::<EnqueueResponse>().await?)
}

async fn daemon_register_recurring_prompt(
    daemon_url: &str,
    cron_expr: &str,
    prompt: &str,
) -> Result<RegisterRecurringResponse> {
    let client = Client::new();
    let request = RegisterRecurringPromptRequest {
        id: None,
        queue: Some("default".to_string()),
        prompt: prompt.to_string(),
        system_prompt: Some(
            "You are Medousa, a practical research assistant. Be concise and evidence-driven."
                .to_string(),
        ),
        cron_expr: cron_expr.to_string(),
        timezone: Some("UTC".to_string()),
        jitter_seconds: Some(0),
        enabled: Some(true),
        max_attempts: Some(1),
        policy_profile: Some("default".to_string()),
        model_hint: None,
    };

    let response = client
        .post(format!("{daemon_url}/v1/recurring/prompt"))
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json::<RegisterRecurringResponse>().await?)
}

async fn handle_command_palette_key_event(
    code: KeyCode,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    command_preview_ui::handle_command_palette_key_event(code, state, tui_rt, event_tx).await
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
        TuiEvent::JobProcessed {
            job_id,
            succeeded,
            execution_id,
        } => {
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
        TuiEvent::ToolInvoked {
            tool_name,
            input_summary,
        } => {
            push_obs(state, format!("◆ {tool_name}  {input_summary}"));
        }
        TuiEvent::ToolPayload {
            tool_name,
            tool_input,
            tool_output,
        } => {
            let safe_input = redact_json_value(&tool_input);
            let safe_output = redact_json_value(&tool_output);
            let input = serde_json::to_string_pretty(&safe_input)
                .unwrap_or_else(|_| safe_input.to_string());
            let output = serde_json::to_string_pretty(&safe_output)
                .unwrap_or_else(|_| safe_output.to_string());
            push_obs(
                state,
                format!("◆ {tool_name}\ninput:\n{input}\noutput:\n{output}\n────────────────"),
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
                state.history_selected =
                    (state.history_selected + 1).min(state.history_items.len().saturating_sub(1));
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
    settings_ui::handle_settings_key_event(code, state, tui_rt, event_tx).await
}

fn emit_settings_validation_summary(state: &mut TuiState) -> bool {
    settings_ui::emit_settings_validation_summary(state)
}

async fn apply_settings(
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) {
    if !emit_settings_validation_summary(state) {
        return;
    }

    let allowed_modules = parse_allowed_modules(&state.settings_draft.allowed_modules);
    let invalid_modules = invalid_module_ids(&allowed_modules);
    if !invalid_modules.is_empty() {
        let invalid_list = invalid_modules.join(", ");
        push_obs(
            state,
            format!(
                "⚠ settings rejected: invalid allowed module ids ({invalid_list}). use dotted ids like websearch.search"
            ),
        );
        return;
    }

    let backend = resolve_backend_name(Some(state.settings_draft.backend.trim()));
    let tool_call_mode =
        resolve_tool_call_mode_name(Some(state.settings_draft.tool_call_mode.trim()));
    let max_tool_rounds = parse_usize_with_bounds(&state.settings_draft.max_tool_rounds, 10, 1, 50);
    let thinking_capture = parse_bool_with_default(&state.settings_draft.thinking_capture, true);
    let thinking_max_lines =
        parse_usize_with_bounds(&state.settings_draft.thinking_max_lines, 300, 50, 5000);
    let provider = if state.settings_draft.provider.trim().is_empty() {
        resolve_llm_provider(None)
    } else {
        resolve_llm_provider(Some(state.settings_draft.provider.trim()))
    };
    let model = if state.settings_draft.model.trim().is_empty() {
        resolve_llm_model(None)
    } else {
        resolve_llm_model(Some(state.settings_draft.model.trim()))
    };
    let base_url = if state.settings_draft.base_url.trim().is_empty() {
        None
    } else {
        Some(state.settings_draft.base_url.trim().to_string())
    };

    match build_tui_runtime(
        parse_backend(Some(&backend)),
        Some(&provider),
        Some(&model),
        base_url.as_deref(),
        allowed_modules.clone(),
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
            state.settings.allowed_modules = allowed_modules.join(",");
            state.settings.tool_call_mode = tool_call_mode.clone();
            state.settings.max_tool_rounds = max_tool_rounds.to_string();
            state.settings.thinking_capture = thinking_capture.to_string();
            state.settings.thinking_max_lines = thinking_max_lines.to_string();
            state.provider_model = format!("{provider}:{model}");

            let api_key = state.settings_draft.api_key.trim().to_string();
            state.settings.api_key = api_key.clone();
            if api_key.is_empty() {
                save_tui_api_key(None);
            } else {
                save_tui_api_key(Some(&api_key));
            }

            state.settings_draft = state.settings.clone();

            save_tui_defaults(&TuiDefaults {
                backend: Some(backend),
                provider: Some(provider),
                model: Some(model),
                base_url,
                allowed_modules: if allowed_modules.is_empty() {
                    None
                } else {
                    Some(allowed_modules)
                },
                tool_call_mode: Some(tool_call_mode),
                max_tool_rounds: Some(max_tool_rounds),
                thinking_capture: Some(thinking_capture),
                thinking_max_lines: Some(thinking_max_lines),
            });
            push_obs(
                state,
                "✓ settings applied (sensitive values redacted)".to_string(),
            );
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
                .title(" Observability  (Ctrl+O expand, Shift+Arrows scroll side panes) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_border()))
                .style(Style::default().bg(ui_subtle_panel_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_subtle_panel_bg()))
        .wrap(Wrap { trim: false })
        .scroll((state.obs_scroll, 0));
    frame.render_widget(obs_widget, obs_area);

    // ── Job History ───────────────────────────────────────────────────────────
    let jobs_inner_width = jobs_area.width.saturating_sub(2);
    let jobs_text = build_job_history_text(state, jobs_inner_width);
    let jobs_visible_height = jobs_area.height.saturating_sub(2);
    let jobs_visual_lines = visual_line_count(&jobs_text, jobs_inner_width);
    state.job_max_scroll = jobs_visual_lines.saturating_sub(jobs_visible_height);
    state.job_scroll = state.job_scroll.min(state.job_max_scroll);

    let jobs_widget = Paragraph::new(jobs_text)
        .block(
            Block::default()
                .title(" Job History  (Shift+Arrows scroll side panes) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_border()))
                .style(Style::default().bg(ui_subtle_panel_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_subtle_panel_bg()))
        .wrap(Wrap { trim: false })
        .scroll((state.job_scroll, 0));
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
    } else if state.mode == UiMode::AllowlistPreview {
        render_allowlist_preview_overlay(frame, state);
    } else if state.mode == UiMode::Editor {
        render_editor_overlay(frame, state);
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

    lines.push(Line::from(Span::styled(
        format!(
            " Redaction mode: strict (payload secrets scrubbed) | Secret backend: {} ",
            api_key_storage_backend_label()
        ),
        Style::default().fg(Color::Cyan),
    )));
    lines.push(Line::from(""));

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

fn build_job_history_text(state: &TuiState, width: u16) -> Text<'static> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    if state.job_history.is_empty() {
        lines.push(Line::from(Span::styled(
            "No jobs yet.",
            Style::default().fg(Color::Gray),
        )));
        return Text::from(lines);
    }

    for (idx, j) in state.job_history.iter().enumerate() {
        if idx > 0 {
            lines.push(Line::from(""));
        }

        let symbol = match j.status.as_str() {
            "succeeded" => "✓",
            "failed" => "✗",
            _ => "·",
        };
        let type_label = j.job_type.split('.').last().unwrap_or(&j.job_type);
        let id_short: String = j.job_id.chars().take(12).collect();
        let summary = format!("{symbol} {type_label}  {id_short}  [{}]", j.status);
        lines.extend(render_markdown_lines(&summary, width));
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
            let marker = if idx == state.history_selected {
                ">"
            } else {
                " "
            };
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
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
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
    command_preview_ui::render_command_palette_overlay(frame, state)
}

fn render_allowlist_preview_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    command_preview_ui::render_allowlist_preview_overlay(frame, state)
}

fn render_settings_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    settings_ui::render_settings_overlay(frame, state)
}

fn render_editor_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 90, 80);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    let (line, col) = state.editor_buffer.line_col();
    let dirty_marker = if state.editor_dirty { "*" } else { "" };
    lines.push(Line::from(Span::styled(
        " Type to edit  Enter: newline  Up/Down: keep column  Ctrl+S: save  /save [path]: save  /run [path]: execute  Esc: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(Span::styled(
        format!(
            " File{dirty_marker}: {} | Cursor: {line}:{col} | {} ",
            state
                .editor_file_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(unspecified)".to_string()),
            state.editor_status
        ),
        Style::default().fg(Color::Cyan),
    )));
    lines.push(Line::from(""));

    let content_height = popup.height.saturating_sub(5) as usize;
    let total_lines = state.editor_buffer.line_count();
    let start = state.editor_scroll as usize;
    let end = start.saturating_add(content_height).min(total_lines.max(1));

    for idx in start..end {
        let src_line = state.editor_buffer.line_at(idx).unwrap_or("");
        if idx + 1 == line {
            let cursor_index = col.saturating_sub(1);
            let mut spans: Vec<Span> = Vec::new();
            spans.push(Span::styled(
                format!("{:>4}  ", idx + 1),
                Style::default().fg(Color::DarkGray),
            ));

            let mut chars = src_line.chars().collect::<Vec<_>>();
            if chars.is_empty() {
                spans.push(Span::styled(
                    " ",
                    Style::default().bg(Color::White).fg(Color::Black),
                ));
            } else if cursor_index >= chars.len() {
                let body = chars.drain(..).collect::<String>();
                spans.push(Span::styled(body, Style::default().fg(Color::White)));
                spans.push(Span::styled(
                    " ",
                    Style::default().bg(Color::White).fg(Color::Black),
                ));
            } else {
                let before = chars.iter().take(cursor_index).collect::<String>();
                let current = chars[cursor_index].to_string();
                let after = chars.iter().skip(cursor_index + 1).collect::<String>();
                spans.push(Span::styled(before, Style::default().fg(Color::White)));
                spans.push(Span::styled(
                    current,
                    Style::default().bg(Color::White).fg(Color::Black),
                ));
                spans.push(Span::styled(after, Style::default().fg(Color::White)));
            }

            lines.push(Line::from(spans));
        } else {
            lines.push(Line::from(Span::styled(
                format!("{:>4}  {}", idx + 1, src_line),
                Style::default().fg(Color::White),
            )));
        }
    }

    if lines.len() <= 3 {
        lines.push(Line::from(Span::styled(
            "(empty buffer)",
            Style::default().fg(Color::Gray),
        )));
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Editor ")
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

fn centered_rect(
    area: ratatui::layout::Rect,
    percent_x: u16,
    percent_y: u16,
) -> ratatui::layout::Rect {
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
                let title = if turn.role == "user" {
                    "User"
                } else {
                    "Assistant"
                };
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
    let max_width = width.max(20) as usize;
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

fn mask_secret_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "(not set)".to_string();
    }

    let visible_suffix_len = trimmed.chars().count().min(4);
    let suffix: String = trimmed
        .chars()
        .rev()
        .take(visible_suffix_len)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("********{suffix}")
}

fn api_key_storage_backend_label() -> &'static str {
    match detect_tui_api_key_storage_backend() {
        ApiKeyStorageBackend::KeychainActive => "keychain(active)",
        ApiKeyStorageBackend::KeychainReady => "keychain(ready)",
        ApiKeyStorageBackend::FileFallbackActive => "file-fallback(active)",
        ApiKeyStorageBackend::FileFallbackReady => "file-fallback(ready)",
    }
}

fn parse_tool_call_mode(value: &str) -> ToolCallMode {
    if value.trim().eq_ignore_ascii_case("strict") {
        ToolCallMode::Strict
    } else {
        ToolCallMode::Auto
    }
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
    println!("  --daemon-url <url>    Medousa daemon base URL (env: MEDOUSA_DAEMON_URL)");
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
    println!("  Shift+Arrows Scroll observability + job panes");
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
    println!("  /edit [path]            Open embedded editor (optional file)");
    println!("  /open <path>            Open file in embedded editor");
    println!("  /save [path]            Save editor buffer to file");
    println!("  /run [path]             Execute .gr source via runtime (allowlist-enforced)");
    println!("  /run-current            Execute current editor file path via runtime");
    println!("  /close                  Exit medousa_tui gracefully");
    println!("  /allowlist-preview      Open allowlist preview panel");
    println!("  /clear-key              Clear stored API key");
    println!("  /rotate-key             Rotate stored API key from draft value");
    println!("  /model                  Show current provider:model");
    println!("  /model <model>          Set model, keep provider");
    println!("  /model <provider:model> Set provider and model");
    println!("  /stop                   Stop active generation");
    println!("  /regen                  Regenerate last user prompt");
    println!("  /export [md|jsonl]      Export current transcript");
    println!("  /daemon                 Show daemon URL and command help");
    println!("  /daemon health          Probe central daemon status");
    println!("  /daemon ask <prompt>    Submit prompt to central daemon");
    println!("  /watch add <cron> <p>   Schedule recurring prompt on daemon");
}

#[cfg(test)]
mod tests {
    use super::{
        JobHistoryEntry, RuntimeSettings, TextBuffer, TuiState, UiMode, build_tui_runtime,
        load_editor_file, parse_allowed_modules, parse_backend, resolve_editor_run_source,
        run_editor_source_via_runtime, validate_editor_run_allowlist, write_editor_file,
    };
    use medousa::events::TuiEvent;
    use medousa::session::{ConversationTurn, SessionHistorySummary};
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "medousa_tui_editor_{name}_{}",
            uuid::Uuid::new_v4()
        ));
        path
    }

    #[test]
    fn load_editor_file_returns_none_for_missing_path() {
        let path = temp_path("missing");
        let loaded = load_editor_file(&path).expect("load should not fail for missing path");
        assert!(loaded.is_none());
    }

    #[test]
    fn write_editor_file_creates_parent_dirs_and_roundtrips() {
        let dir = temp_path("roundtrip");
        let path = dir.join("nested").join("script.gr");

        write_editor_file(&path, "run {\n  ok: true\n}\n").expect("write should succeed");

        let loaded = load_editor_file(&path)
            .expect("read should succeed")
            .expect("file should exist");
        assert_eq!(loaded, "run {\n  ok: true\n}\n");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn resolve_editor_run_source_fails_for_missing_override_path() {
        let missing = temp_path("missing_run").join("script.gr");
        let result = resolve_editor_run_source(Some(missing.to_string_lossy().as_ref()), None, "");
        assert!(result.is_err());
        let err = result.err().unwrap_or_default();
        assert!(err.contains("file not found"));
    }

    #[test]
    fn validate_editor_run_allowlist_rejects_blocked_ops() {
        let source = "query Run { websearch.search(query: \"x\") { ok } }";
        let result = validate_editor_run_allowlist(source, "http.fetch");
        assert!(result.is_err());
        let err = result.err().unwrap_or_default();
        assert!(err.contains("run blocked by allowlist"));
        assert!(err.contains("websearch.search"));
    }

    fn test_settings() -> RuntimeSettings {
        RuntimeSettings {
            backend: "in-memory".to_string(),
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            base_url: String::new(),
            api_key: String::new(),
            allowed_modules: "http.fetch".to_string(),
            tool_call_mode: "auto".to_string(),
            max_tool_rounds: "10".to_string(),
            thinking_capture: "true".to_string(),
            thinking_max_lines: "300".to_string(),
        }
    }

    fn test_state(settings: RuntimeSettings) -> TuiState {
        TuiState {
            conversation: Vec::<ConversationTurn>::new(),
            observability: VecDeque::new(),
            job_history: VecDeque::<JobHistoryEntry>::new(),
            input_buffer: String::new(),
            conv_scroll: 0,
            conv_max_scroll: 0,
            is_processing: false,
            active_request_task: None,
            auto_scroll: true,
            active_agent_stream_turn: None,
            mode: UiMode::Chat,
            history_items: Vec::<SessionHistorySummary>::new(),
            history_selected: 0,
            command_query: String::new(),
            command_selected: 0,
            settings: settings.clone(),
            settings_draft: settings,
            allowlist_preview_source: String::new(),
            editor_buffer: TextBuffer::from_text(
                "query Run { websearch.search(query: \"x\") { ok } }".to_string(),
            ),
            editor_file_path: None,
            editor_status: String::new(),
            editor_dirty: false,
            editor_preferred_col: None,
            editor_scroll: 0,
            settings_selected: 0,
            settings_editing: false,
            provider_model: "openai:gpt-4o-mini".to_string(),
            session_id: "test-session".to_string(),
            thinking_trace: VecDeque::new(),
            thinking_scroll: 0,
            thinking_max_scroll: 0,
            obs_scroll: 0,
            obs_max_scroll: 0,
            job_scroll: 0,
            job_max_scroll: 0,
            in_thinking_tag: false,
            stream_tag_tail: String::new(),
            daemon_url: "http://127.0.0.1:8787".to_string(),
        }
    }

    #[tokio::test]
    async fn blocked_run_does_not_emit_runtime_events() {
        let settings = test_settings();
        let (event_tx, mut event_rx) = mpsc::channel::<TuiEvent>(64);

        let tui_rt = build_tui_runtime(
            parse_backend(Some(&settings.backend)),
            Some(&settings.provider),
            Some(&settings.model),
            None,
            parse_allowed_modules(&settings.allowed_modules),
            "test-session",
            event_tx.clone(),
        )
        .await
        .expect("runtime should build");

        while event_rx.try_recv().is_ok() {}

        let mut state = test_state(settings);
        run_editor_source_via_runtime(&mut state, &tui_rt, &event_tx, None).await;

        let obs = state
            .observability
            .front()
            .map(|v| v.text.clone())
            .unwrap_or_default();
        assert!(obs.contains("run blocked by allowlist"));

        match event_rx.try_recv() {
            Err(TryRecvError::Empty) => {}
            Ok(evt) => panic!("unexpected runtime event emitted: {evt:?}"),
            Err(err) => panic!("unexpected channel state: {err}"),
        }
    }
}
