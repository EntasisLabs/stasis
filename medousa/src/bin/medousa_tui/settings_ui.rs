use super::*;

pub(crate) async fn handle_settings_key_event(
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
            if state.settings_selected == 7 || state.settings_selected == 9 {
                quick_adjust_setting(state, true);
            }
        }
        KeyCode::Char('-') => {
            if state.settings_selected == 7 || state.settings_selected == 9 {
                quick_adjust_setting(state, false);
            }
        }
        KeyCode::Up => {
            state.settings_selected = state.settings_selected.saturating_sub(1);
        }
        KeyCode::Down => {
            state.settings_selected = (state.settings_selected + 1).min(16);
        }
        KeyCode::Enter => match state.settings_selected {
            1..=5 => {
                state.settings_editing = true;
            }
            0 | 6 | 7 | 8 | 9 => {
                quick_adjust_setting(state, true);
            }
            10 => {
                state.mode = UiMode::RuntimeEnv;
                state.runtime_env_editing = true;
            }
            11 => {
                emit_settings_validation_summary(state);
            }
            12 => {
                state.settings_draft.api_key.clear();
                push_obs(
                    state,
                    "✓ settings draft: api key marked for clear".to_string(),
                );
            }
            13 => {
                let key = state.settings_draft.api_key.trim().to_string();
                if key.is_empty() {
                    push_obs(
                        state,
                        "⚠ key rotation requires a non-empty draft API key".to_string(),
                    );
                } else {
                    save_tui_api_key(Some(&key));
                    state.settings.api_key = key.clone();
                    state.settings_draft.api_key = key;
                    push_obs(state, "✓ api key rotated in secure storage".to_string());
                }
            }
            14 => {
                state.settings_draft = state.settings.clone();
                state.settings_editing = false;
                push_obs(
                    state,
                    "✓ settings draft reverted to last applied".to_string(),
                );
            }
            15 => {
                super::apply_settings(state, tui_rt, event_tx).await;
                state.mode = UiMode::Chat;
            }
            16 => {
                state.settings_draft = state.settings.clone();
                state.mode = UiMode::Chat;
            }
            _ => {}
        },
        _ => {}
    }

    EventOutcome::Continue
}

pub(crate) fn render_settings_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 76, 62);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    let has_pending_changes = state.settings_draft != state.settings;
    let validation_errors = settings_validation_errors(&state.settings_draft);
    let validation_line = if validation_errors.is_empty() {
        " Validation: OK (draft can be applied) ".to_string()
    } else {
        format!(
            " Validation: {} issue(s) - press Validate Draft for details ",
            validation_errors.len()
        )
    };
    lines.push(Line::from(Span::styled(
        if has_pending_changes {
            " Draft has unapplied changes "
        } else {
            " Draft matches applied settings "
        },
        Style::default().fg(if has_pending_changes {
            Color::Yellow
        } else {
            Color::Green
        }),
    )));
    lines.push(Line::from(Span::styled(
        format!(" Secret backend: {} ", api_key_storage_backend_label()),
        Style::default().fg(Color::Cyan),
    )));
    lines.push(Line::from(Span::styled(
        validation_line,
        Style::default().fg(if validation_errors.is_empty() {
            Color::Green
        } else {
            Color::Red
        }),
    )));
    lines.push(Line::from(Span::styled(
        " Up/Down: select  Enter: edit/action  Space/Left/Right: quick toggle  +/-: adjust number  Ctrl+,/Esc: cancel ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    let rows = vec![
        format!("Backend: {}  [toggle]", state.settings_draft.backend),
        format!("Provider: {}  [edit]", state.settings_draft.provider),
        format!("Model: {}  [edit]", state.settings_draft.model),
        format!(
            "Base URL: {}  [edit]",
            if state.settings_draft.base_url.is_empty() {
                "(auto)".to_string()
            } else {
                state.settings_draft.base_url.clone()
            }
        ),
        format!(
            "API Key: {}  [edit, secret]",
            mask_secret_value(&state.settings_draft.api_key)
        ),
        format!(
            "Allowed Grapheme Modules: {}  [edit]",
            if state.settings_draft.allowed_modules.trim().is_empty() {
                "(all)".to_string()
            } else {
                state.settings_draft.allowed_modules.clone()
            }
        ),
        format!(
            "Tool Call Mode: {}  [toggle]",
            state.settings_draft.tool_call_mode
        ),
        format!(
            "Max Tool Rounds: {}  [number]",
            state.settings_draft.max_tool_rounds
        ),
        format!(
            "Thinking Capture: {}  [toggle]",
            state.settings_draft.thinking_capture
        ),
        format!(
            "Thinking Max Lines: {}  [number]",
            state.settings_draft.thinking_max_lines
        ),
        format!(
            "Runtime/Env Variables Submenu: {} line(s)  [open]",
            state
                .settings_draft
                .env_overrides
                .lines()
                .filter(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with('#')
                })
                .count()
        ),
        "Validate Draft  [action]".to_string(),
        "Clear API Key (Draft)  [action]".to_string(),
        "Rotate API Key (Persist Draft)  [action]".to_string(),
        "Revert to Last Applied  [action]".to_string(),
        "Apply and Save  [action]".to_string(),
        "Cancel (Discard Draft)  [action]".to_string(),
    ];

    for (idx, row) in rows.iter().enumerate() {
        let marker = if idx == state.settings_selected {
            ">"
        } else {
            " "
        };
        let mut style = if idx == state.settings_selected {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };

        if idx == state.settings_selected && state.settings_editing && idx <= 9 {
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

pub(crate) fn handle_runtime_env_key_event(code: KeyCode, state: &mut TuiState) -> EventOutcome {
    if !state.runtime_env_editing {
        state.runtime_env_editing = true;
    }

    match code {
        KeyCode::Esc => {
            state.runtime_env_editing = false;
            state.mode = UiMode::Settings;
        }
        KeyCode::Enter => {
            state.settings_draft.env_overrides.push('\n');
        }
        KeyCode::Backspace => {
            state.settings_draft.env_overrides.pop();
        }
        KeyCode::Tab => {
            state.settings_draft.env_overrides.push('=');
        }
        KeyCode::Char(c) => {
            state.settings_draft.env_overrides.push(c);
        }
        _ => {}
    }

    EventOutcome::Continue
}

pub(crate) fn render_runtime_env_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 78, 66);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Runtime/Env Variables (Draft) ",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(Span::styled(
        " Format: KEY=VALUE (one per line). Empty lines and # comments are ignored. ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(Span::styled(
        " Esc: back to Settings  Enter: newline  Tab: '=' shortcut ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    let env_errors = env_overrides_validation_errors(&state.settings_draft.env_overrides);
    if env_errors.is_empty() {
        lines.push(Line::from(Span::styled(
            " Validation: OK ",
            Style::default().fg(Color::Green),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            format!(" Validation: {} issue(s) ", env_errors.len()),
            Style::default().fg(Color::Red),
        )));
        for err in env_errors.iter().take(3) {
            lines.push(Line::from(Span::styled(
                format!(" - {err}"),
                Style::default().fg(Color::Red),
            )));
        }
    }
    lines.push(Line::from(""));

    if state.settings_draft.env_overrides.trim().is_empty() {
        lines.push(Line::from(Span::styled(
            "# Example",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::from(Span::styled(
            "MEDOUSA_LLM_PROVIDER=openai",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::from(Span::styled(
            "MEDOUSA_LLM_MODEL=gpt-4o-mini",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for line in state.settings_draft.env_overrides.lines() {
            lines.push(Line::from(line.to_string()));
        }
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Runtime/Env Submenu ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, popup);
}

pub(crate) fn emit_settings_validation_summary(state: &mut TuiState) -> bool {
    let errors = settings_validation_errors(&state.settings_draft);
    if errors.is_empty() {
        push_obs(
            state,
            "✓ settings validation passed (draft ready to apply)".to_string(),
        );
        true
    } else {
        for error in errors {
            push_obs(state, format!("⚠ settings validation: {error}"));
        }
        false
    }
}

fn quick_adjust_setting(state: &mut TuiState, forward: bool) {
    match state.settings_selected {
        0 => {
            state.settings_draft.backend = cycle_backend(&state.settings_draft.backend, forward);
        }
        6 => {
            state.settings_draft.tool_call_mode =
                cycle_tool_call_mode(&state.settings_draft.tool_call_mode, forward);
        }
        7 => {
            let current = parse_usize_with_bounds(&state.settings_draft.max_tool_rounds, 10, 1, 50);
            let step = if current < 20 { 1 } else { 5 };
            let next = if forward {
                current.saturating_add(step)
            } else {
                current.saturating_sub(step)
            }
            .clamp(1, 50);
            state.settings_draft.max_tool_rounds = next.to_string();
        }
        8 => {
            let value = parse_bool_with_default(&state.settings_draft.thinking_capture, true);
            state.settings_draft.thinking_capture = (!value).to_string();
        }
        9 => {
            let current =
                parse_usize_with_bounds(&state.settings_draft.thinking_max_lines, 300, 50, 5000);
            let step = if current < 500 { 50 } else { 100 };
            let next = if forward {
                current.saturating_add(step)
            } else {
                current.saturating_sub(step)
            }
            .clamp(50, 5000);
            state.settings_draft.thinking_max_lines = next.to_string();
        }
        _ => {}
    }
}

fn selected_settings_field_mut(state: &mut TuiState) -> &mut String {
    match state.settings_selected {
        0 => &mut state.settings_draft.backend,
        1 => &mut state.settings_draft.provider,
        2 => &mut state.settings_draft.model,
        3 => &mut state.settings_draft.base_url,
        4 => &mut state.settings_draft.api_key,
        5 => &mut state.settings_draft.allowed_modules,
        6 => &mut state.settings_draft.tool_call_mode,
        7 => &mut state.settings_draft.max_tool_rounds,
        8 => &mut state.settings_draft.thinking_capture,
        9 => &mut state.settings_draft.thinking_max_lines,
        _ => &mut state.settings_draft.base_url,
    }
}
