use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use medousa::{
    TuiRuntime, build_tui_runtime,
    events::TuiEvent,
    parse_backend, resolve_daemon_url, resolve_llm_base_url, resolve_llm_model,
    resolve_llm_provider,
    session::{
        ApiKeyStorageBackend, ConversationTurn, SessionHistorySummary, TuiDefaults, append_turn,
        detect_tui_api_key_storage_backend, list_history_sessions, load_history, load_tui_api_key,
        load_tui_defaults, save_last_session_id, save_tui_api_key, save_tui_defaults,
    },
    settings_guard::{invalid_module_ids, parse_allowed_modules},
    tui::allowlist_preview::analyze_allowlist_preview,
    tui::editor_buffer::TextBuffer,
    tui::settings::{
        RuntimeSettings, cycle_backend, cycle_tool_call_mode, env_overrides_validation_errors,
        parse_bool_with_default, parse_env_overrides, parse_usize_with_bounds,
        resolve_backend_name, resolve_bool_arg, resolve_tool_call_mode_name, resolve_usize_arg,
        settings_validation_errors,
    },
};
#[path = "medousa_tui/agent_runtime.rs"]
mod agent_runtime;
#[path = "medousa_tui/cli_helpers.rs"]
mod cli_helpers;
#[path = "medousa_tui/command_preview_ui.rs"]
mod command_preview_ui;
#[path = "medousa_tui/daemon_commands.rs"]
mod daemon_commands;
#[path = "medousa_tui/editor_runtime.rs"]
mod editor_runtime;
#[path = "medousa_tui/event_reducer.rs"]
mod event_reducer;
#[path = "medousa_tui/input_router.rs"]
mod input_router;
#[path = "medousa_tui/markdown_cache.rs"]
mod markdown_cache;
#[path = "medousa_tui/perf.rs"]
mod perf;
#[path = "medousa_tui/settings_runtime.rs"]
mod settings_runtime;
#[path = "medousa_tui/settings_ui.rs"]
mod settings_ui;
#[path = "medousa_tui/slash_commands.rs"]
mod slash_commands;
#[path = "medousa_tui/ui_helpers.rs"]
mod ui_helpers;
#[path = "medousa_tui/ui_render.rs"]
mod ui_render;
#[path = "medousa_tui/workers.rs"]
mod workers;

use agent_runtime::{start_prompt_run, stop_active_generation};
use cli_helpers::{find_arg_value, print_help};
use editor_runtime::{load_editor_file, run_editor_source_via_runtime, save_editor_buffer};
#[cfg(test)]
use editor_runtime::{resolve_editor_run_source, validate_editor_run_allowlist, write_editor_file};
use event_reducer::{flush_pending_agent_chunks, handle_tui_event};
use input_router::{handle_key_event, keep_editor_cursor_visible};
use markdown_cache::invalidate_markdown_cache;
use perf::{
    PerfSnapshot, UiPerfStats, capture_perf_snapshot, format_perf_delta, format_perf_snapshot,
    mark_ui_activity, note_frame_rendered,
};
use settings_runtime::{
    apply_env_overrides, apply_settings, finalize_settings_apply_if_ready,
    handle_runtime_env_key_event, handle_settings_key_event, next_ui_wake_delay,
};
use slash_commands::handle_slash_command;
use ui_helpers::{
    centered_rect, ui_accent_primary, ui_accent_warn, ui_bg, ui_border, ui_modal_bg, ui_panel_bg,
};
use ui_render::render;
use workers::{
    WorkerCommand, WorkerResult, handle_worker_result, next_worker_request_id,
    queue_worker_command, worker_loop,
};

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
    startup_selected: usize,
    history_items: Vec<SessionHistorySummary>,
    history_selected: usize,
    command_query: String,
    command_tab: usize,
    command_selected: usize,
    command_scroll: u16,
    command_max_scroll: u16,
    command_usage_counts: HashMap<String, u64>,
    settings: RuntimeSettings,
    settings_draft: RuntimeSettings,
    allowlist_preview_source: String,
    editor_buffer: TextBuffer,
    editor_file_path: Option<PathBuf>,
    editor_status: String,
    editor_dirty: bool,
    editor_preferred_col: Option<usize>,
    editor_scroll: u16,
    settings_tab: usize,
    settings_selected: usize,
    settings_editing: bool,
    settings_scroll: u16,
    settings_max_scroll: u16,
    runtime_env_editing: bool,
    provider_model: String,
    session_id: String,
    thinking_trace: VecDeque<String>,
    thinking_scroll: u16,
    thinking_max_scroll: u16,
    grapheme_console: VecDeque<String>,
    grapheme_console_scroll: u16,
    grapheme_console_max_scroll: u16,
    obs_scroll: u16,
    obs_max_scroll: u16,
    job_scroll: u16,
    job_max_scroll: u16,
    in_thinking_tag: bool,
    stream_tag_tail: String,
    daemon_url: String,
    next_settings_apply_request_id: u64,
    active_settings_apply_request_id: Option<u64>,
    pending_settings_apply: Option<PendingSettingsApply>,
    ui_dirty: bool,
    pending_agent_chunk_delta: String,
    pending_agent_chunk_count: u64,
    pending_paint_since: Option<Instant>,
    perf: UiPerfStats,
    worker_cmd_tx: mpsc::Sender<WorkerCommand>,
    next_worker_request_id: u64,
    latest_daemon_health_request_id: u64,
    latest_daemon_ask_request_id: u64,
    latest_watch_add_request_id: u64,
    markdown_cache: RefCell<HashMap<MarkdownCacheKey, Vec<Line<'static>>>>,
    markdown_cache_order: RefCell<VecDeque<MarkdownCacheKey>>,
    perf_baseline: Option<PerfSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MarkdownCacheKey {
    width: u16,
    content_hash: u64,
}

struct SettingsApplySnapshot {
    backend: String,
    provider: String,
    model: String,
    base_url: Option<String>,
    env_overrides_raw: String,
    allowed_modules: Vec<String>,
    tool_call_mode: String,
    max_tool_rounds: usize,
    thinking_capture: bool,
    thinking_max_lines: usize,
    api_key: String,
}

struct PendingSettingsApply {
    request_id: u64,
    changed_env_count: usize,
    snapshot: SettingsApplySnapshot,
    handle: tokio::task::JoinHandle<std::result::Result<TuiRuntime, String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiMode {
    Startup,
    Chat,
    History,
    CommandPalette,
    Settings,
    RuntimeEnv,
    ObservabilityPanel,
    AllowlistPreview,
    Editor,
    ThinkingPeek,
    ThinkingPanel,
    GraphemeConsole,
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

    if let Some(raw) = defaults.env_overrides.as_deref() {
        apply_env_overrides(raw);
    }

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
    let (worker_cmd_tx, worker_cmd_rx) = mpsc::channel::<WorkerCommand>(32);
    let (worker_result_tx, mut worker_result_rx) = mpsc::channel::<WorkerResult>(64);

    tokio::spawn(worker_loop(worker_cmd_rx, worker_result_tx));

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
        env_overrides: defaults.env_overrides.clone().unwrap_or_default(),
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
        mode: UiMode::Startup,
        startup_selected: 0,
        history_items: Vec::new(),
        history_selected: 0,
        command_query: String::new(),
        command_tab: 0,
        command_selected: 0,
        command_scroll: 0,
        command_max_scroll: 0,
        command_usage_counts: defaults.command_usage_counts.unwrap_or_default(),
        settings: initial_settings.clone(),
        settings_draft: initial_settings,
        allowlist_preview_source: String::new(),
        editor_buffer: TextBuffer::default(),
        editor_file_path: None,
        editor_status: "No file loaded".to_string(),
        editor_dirty: false,
        editor_preferred_col: None,
        editor_scroll: 0,
        settings_tab: 0,
        settings_selected: 0,
        settings_editing: false,
        settings_scroll: 0,
        settings_max_scroll: 0,
        runtime_env_editing: false,
        provider_model,
        session_id: session_id.clone(),
        thinking_trace: VecDeque::new(),
        thinking_scroll: 0,
        thinking_max_scroll: 0,
        grapheme_console: VecDeque::new(),
        grapheme_console_scroll: 0,
        grapheme_console_max_scroll: 0,
        obs_scroll: 0,
        obs_max_scroll: 0,
        job_scroll: 0,
        job_max_scroll: 0,
        in_thinking_tag: false,
        stream_tag_tail: String::new(),
        daemon_url: resolved_daemon_url,
        next_settings_apply_request_id: 0,
        active_settings_apply_request_id: None,
        pending_settings_apply: None,
        ui_dirty: false,
        pending_agent_chunk_delta: String::new(),
        pending_agent_chunk_count: 0,
        pending_paint_since: None,
        perf: UiPerfStats::default(),
        worker_cmd_tx,
        next_worker_request_id: 0,
        latest_daemon_health_request_id: 0,
        latest_daemon_ask_request_id: 0,
        latest_watch_add_request_id: 0,
        markdown_cache: RefCell::new(HashMap::new()),
        markdown_cache_order: RefCell::new(VecDeque::new()),
        perf_baseline: None,
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
    let initial_render_started = Instant::now();
    terminal.draw(|f| render(f, &mut state))?;
    note_frame_rendered(&mut state, initial_render_started);
    loop {
        let wake_after = next_ui_wake_delay(&state);
        tokio::select! {
            Some(event) = key_rx.recv() => {
                mark_ui_activity(&mut state);
                match handle_key_event(event, &mut state, &mut tui_rt, &event_tx).await {
                    EventOutcome::Break => break,
                    EventOutcome::Continue => {
                        state.ui_dirty = true;
                    }
                }
            }
            Some(tui_event) = event_rx.recv() => {
                mark_ui_activity(&mut state);
                handle_tui_event(tui_event, &mut state);
                state.ui_dirty = true;
            }
            Some(worker_result) = worker_result_rx.recv() => {
                mark_ui_activity(&mut state);
                handle_worker_result(worker_result, &mut state);
                state.ui_dirty = true;
            }
            _ = tokio::time::sleep(wake_after) => {}
        }

        match drain_pending_key_events(&mut key_rx, &mut state, &mut tui_rt, &event_tx).await {
            EventOutcome::Break => break,
            EventOutcome::Continue => {}
        }

        flush_pending_agent_chunks(&mut state);

        if finalize_settings_apply_if_ready(&mut state, &mut tui_rt).await {
            mark_ui_activity(&mut state);
            state.ui_dirty = true;
        }

        if state.ui_dirty {
            let render_started = Instant::now();
            terminal.draw(|f| render(f, &mut state))?;
            note_frame_rendered(&mut state, render_started);
            state.ui_dirty = false;
        }
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

async fn drain_pending_key_events(
    key_rx: &mut mpsc::Receiver<Event>,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    let mut drained = 0usize;
    while drained < 32 {
        match key_rx.try_recv() {
            Ok(event) => {
                mark_ui_activity(state);
                match handle_key_event(event, state, tui_rt, event_tx).await {
                    EventOutcome::Break => return EventOutcome::Break,
                    EventOutcome::Continue => {
                        state.ui_dirty = true;
                    }
                }
                drained = drained.saturating_add(1);
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    if drained > 0 {
        state.perf.coalesced_key_events = state
            .perf
            .coalesced_key_events
            .saturating_add(drained as u64);
    }

    EventOutcome::Continue
}

async fn handle_command_palette_key_event(
    code: KeyCode,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    command_preview_ui::handle_command_palette_key_event(code, state, tui_rt, event_tx).await
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
                invalidate_markdown_cache(state);
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

fn push_obs(state: &mut TuiState, text: String) {
    state.observability.push_front(ObsEvent { text });
    if state.observability.len() > 50 {
        state.observability.pop_back();
    }
    invalidate_markdown_cache(state);
}

fn push_grapheme_console_entry(
    state: &mut TuiState,
    source_label: &str,
    job_id: &str,
    succeeded: bool,
    diagnostics: &Value,
) {
    let status = if succeeded { "succeeded" } else { "failed" };
    let mut entry = format!("[{status}] {source_label} ({job_id})");
    let console_json = diagnostics
        .get("final_state")
        .cloned()
        .unwrap_or_else(|| diagnostics.clone());

    let rendered =
        serde_json::to_string_pretty(&console_json).unwrap_or_else(|_| console_json.to_string());
    if !rendered.trim().is_empty() {
        entry.push('\n');
        entry.push_str(&rendered);
    }

    state.grapheme_console.push_front(entry);
    if state.grapheme_console.len() > 100 {
        state.grapheme_console.pop_back();
    }
    invalidate_markdown_cache(state);
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

// ── Export ────────────────────────────────────────────────────────────────────

fn export_current_session(state: &TuiState, format: &str) -> std::result::Result<PathBuf, String> {
    let exports_dir = std::env::current_dir()
        .map_err(|e| e.to_string())?
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

// ── Helpers ───────────────────────────────────────────────────────────────────

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

#[cfg(test)]
mod tests {
    use super::{
        JobHistoryEntry, RuntimeSettings, TextBuffer, TuiState, UiMode, UiPerfStats, WorkerCommand,
        build_tui_runtime, load_editor_file, parse_allowed_modules, parse_backend,
        resolve_editor_run_source, run_editor_source_via_runtime, validate_editor_run_allowlist,
        write_editor_file,
    };
    use medousa::events::TuiEvent;
    use medousa::session::{ConversationTurn, SessionHistorySummary};
    use std::collections::{HashMap, VecDeque};
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
            env_overrides: String::new(),
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
            worker_cmd_tx: mpsc::channel::<WorkerCommand>(8).0,
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
            startup_selected: 0,
            history_items: Vec::<SessionHistorySummary>::new(),
            history_selected: 0,
            command_query: String::new(),
            command_tab: 0,
            command_selected: 0,
            command_scroll: 0,
            command_max_scroll: 0,
            command_usage_counts: HashMap::new(),
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
            settings_tab: 0,
            settings_selected: 0,
            settings_editing: false,
            settings_scroll: 0,
            settings_max_scroll: 0,
            runtime_env_editing: false,
            provider_model: "openai:gpt-4o-mini".to_string(),
            session_id: "test-session".to_string(),
            thinking_trace: VecDeque::new(),
            thinking_scroll: 0,
            thinking_max_scroll: 0,
            grapheme_console: VecDeque::new(),
            grapheme_console_scroll: 0,
            grapheme_console_max_scroll: 0,
            obs_scroll: 0,
            obs_max_scroll: 0,
            job_scroll: 0,
            job_max_scroll: 0,
            in_thinking_tag: false,
            stream_tag_tail: String::new(),
            daemon_url: "http://127.0.0.1:8787".to_string(),
            next_settings_apply_request_id: 0,
            active_settings_apply_request_id: None,
            pending_settings_apply: None,
            ui_dirty: false,
            pending_agent_chunk_delta: String::new(),
            pending_agent_chunk_count: 0,
            pending_paint_since: None,
            perf: UiPerfStats::default(),
            next_worker_request_id: 0,
            latest_daemon_health_request_id: 0,
            latest_daemon_ask_request_id: 0,
            latest_watch_add_request_id: 0,
            markdown_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            markdown_cache_order: std::cell::RefCell::new(VecDeque::new()),
            perf_baseline: None,
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
