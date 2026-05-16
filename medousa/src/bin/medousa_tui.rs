use std::collections::VecDeque;
use std::io;
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
    widgets::{Block, Borders, Paragraph, Wrap},
    Terminal,
};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use medousa::{
    build_tui_runtime,
    events::TuiEvent,
    parse_backend,
    resolve_llm_model, resolve_llm_provider,
    session::{append_turn, load_history, load_last_session_id, save_last_session_id, ConversationTurn},
};
use stasis::application::orchestration::tool_loop_pipeline::{ToolCallMode, ToolLoopExecutionRequest};
use stasis::prelude::PromptExecutionContext;

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
    auto_scroll: bool,
    active_agent_stream_turn: Option<usize>,
    provider_model: String,
    session_id: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let provider = find_arg_value(&args, "--provider");
    let model = find_arg_value(&args, "--model");
    let base_url = find_arg_value(&args, "--base-url");
    let new_session = args.iter().any(|a| a == "--new-session");
    let explicit_session = find_arg_value(&args, "--session");

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Ok(());
    }

    let resolved_provider = resolve_llm_provider(provider);
    let resolved_model = resolve_llm_model(model);
    let provider_model = format!("{resolved_provider}:{resolved_model}");

    let session_id = if new_session {
        Uuid::new_v4().simple().to_string()
    } else if let Some(sid) = explicit_session {
        sid.to_string()
    } else {
        load_last_session_id().unwrap_or_else(|| Uuid::new_v4().simple().to_string())
    };
    save_last_session_id(&session_id);

    let history = load_history(&session_id);

    let (event_tx, mut event_rx) = mpsc::channel::<TuiEvent>(256);

    let tui_rt = build_tui_runtime(
        parse_backend(Some("surreal-mem")),
        provider,
        model,
        base_url,
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
        auto_scroll: true,
        active_agent_stream_turn: None,
        provider_model,
        session_id: session_id.clone(),
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
                match handle_key_event(event, &mut state, &session_id, &tui_rt.tool_loop_pipeline, &event_tx).await {
                    EventOutcome::Break => break,
                    EventOutcome::Continue => {}
                }
            }
            Some(tui_event) = event_rx.recv() => {
                handle_tui_event(tui_event, &mut state, &session_id);
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
    session_id: &str,
    pipeline: &stasis::application::orchestration::tool_loop_pipeline::ToolLoopPipeline,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    let Event::Key(key) = event else {
        return EventOutcome::Continue;
    };

    match (key.code, key.modifiers) {
        (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
            return EventOutcome::Break;
        }
        (KeyCode::Char('q'), m) if m.contains(KeyModifiers::CONTROL) => {
            return EventOutcome::Break;
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
            state.input_buffer.clear();
            state.is_processing = true;
            state.auto_scroll = true;
            state.conv_scroll = state.conv_max_scroll;
            state.active_agent_stream_turn = None;

            let user_turn = ConversationTurn {
                role: "user".to_string(),
                content: prompt.clone(),
                timestamp: Utc::now(),
                tool_names: vec![],
            };
            append_turn(session_id, &user_turn);
            state.conversation.push(user_turn);

            // Spawn the tool-loop pipeline in a background task
            let pipeline = pipeline.clone();
            let tx = event_tx.clone();
            let prompt_preview: String = prompt.chars().take(48).collect();
            tokio::spawn(async move {
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
                    system_prompt: None,
                    context: PromptExecutionContext::default(),
                    tool_name: String::new(), // expose all registered tools
                    tool_input: Value::Null,
                    tool_call_mode: ToolCallMode::Auto,
                };
                match pipeline.execute_with_stream(request, Some(&chunk_tx)).await {
                    Ok(response) => {
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
        }

        _ => {}
    }

    EventOutcome::Continue
}

fn handle_tui_event(event: TuiEvent, state: &mut TuiState, session_id: &str) {
    match event {
        TuiEvent::AgentChunk { delta } => {
            if delta.is_empty() {
                return;
            }

            if let Some(idx) = state.active_agent_stream_turn {
                if let Some(turn) = state.conversation.get_mut(idx) {
                    turn.content.push_str(&delta);
                }
            } else {
                state.conversation.push(ConversationTurn {
                    role: "agent".to_string(),
                    content: delta,
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
            if let Some(idx) = state.active_agent_stream_turn.take() {
                if let Some(turn) = state.conversation.get_mut(idx) {
                    turn.content = text;
                    turn.tool_names = tool_names;
                    turn.timestamp = Utc::now();
                    append_turn(session_id, turn);
                }
            } else {
                let turn = ConversationTurn {
                    role: "agent".to_string(),
                    content: text,
                    timestamp: Utc::now(),
                    tool_names,
                };
                append_turn(session_id, &turn);
                state.conversation.push(turn);
            }
            if state.auto_scroll {
                state.conv_scroll = state.conv_max_scroll;
            }
        }
        TuiEvent::AgentError(err) => {
            state.is_processing = false;
            state.active_agent_stream_turn = None;
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
    }
}

fn push_obs(state: &mut TuiState, text: String) {
    state.observability.push_front(ObsEvent { text });
    if state.observability.len() > 50 {
        state.observability.pop_back();
    }
}

// ── Rendering ─────────────────────────────────────────────────────────────────

fn render(frame: &mut ratatui::Frame, state: &mut TuiState) {
    let area = frame.area();

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

    let conv_text = build_conversation_text(&state.conversation);
    // Compute visual wrapped height so scrolling lands on real bottom.
    let inner_width = left.width.saturating_sub(2);
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
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let conv_widget = Paragraph::new(conv_text)
        .block(
            Block::default()
                .title(conv_title)
                .borders(Borders::ALL)
                .border_style(conv_border),
        )
        .wrap(Wrap { trim: false })
        .scroll((safe_scroll, 0));
    frame.render_widget(conv_widget, left);

    // ── Observability ─────────────────────────────────────────────────────────
    let obs_lines: Vec<Line> = state
        .observability
        .iter()
        .map(|ev| Line::from(Span::styled(ev.text.clone(), Style::default().fg(Color::Cyan))))
        .collect();

    let obs_widget = Paragraph::new(Text::from(obs_lines))
        .block(
            Block::default()
                .title(" Observability ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .wrap(Wrap { trim: false });
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
                    Style::default().fg(Color::Gray),
                ),
            ])
        })
        .collect();

    let jobs_widget = Paragraph::new(Text::from(job_lines))
        .block(
            Block::default()
                .title(" Job History ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(jobs_widget, jobs_area);

    // ── Input bar ─────────────────────────────────────────────────────────────
    let session_short: String = state.session_id.chars().take(8).collect();
    let input_title = format!(
        " {}  session:{session_short} ",
        state.provider_model
    );
    let input_display = format!("  {}_", state.input_buffer);
    let input_border = if state.is_processing {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Blue)
    };

    let input_widget = Paragraph::new(input_display)
        .block(
            Block::default()
                .title(input_title)
                .borders(Borders::ALL)
                .border_style(input_border),
        )
        .style(Style::default().fg(Color::White));
    frame.render_widget(input_widget, input_area);
}

fn build_conversation_text(turns: &[ConversationTurn]) -> Text<'static> {
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

        for content_line in turn.content.lines() {
            lines.push(Line::from(Span::styled(
                format!("  {content_line}"),
                Style::default().fg(Color::White),
            )));
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
    println!("  --session <id>        Resume a specific session by ID");
    println!("  --new-session         Start a new session (discard last session ID)");
    println!("  --help, -h            Print this help");
    println!();
    println!("KEYS:");
    println!("  Enter        Submit message");
    println!("  Backspace    Delete character");
    println!("  Up/Down      Scroll conversation");
    println!("  PageUp/Down  Scroll by 10 lines");
    println!("  Home/End     Jump to start/end");
    println!("  Ctrl+C       Quit");
    println!();
    println!("HISTORY:");
    println!("  Conversations are persisted to ~/.local/share/medousa/history/<session_id>.jsonl");
}
