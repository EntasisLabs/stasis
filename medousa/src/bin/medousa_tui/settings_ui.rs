use super::*;

const SETTINGS_SECTIONS: [(&str, usize, usize); 4] = [
    ("Model", 0, 5),
    ("Runtime", 6, 10),
    ("Safety", 11, 13),
    ("Session", 14, 16),
];

pub(crate) async fn handle_settings_key_event(
    code: KeyCode,
    state: &mut TuiState,
    tui_rt: &mut TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
) -> EventOutcome {
    clamp_selected_to_active_tab(state);

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
        KeyCode::Tab => {
            switch_settings_tab(state, true);
        }
        KeyCode::BackTab => {
            switch_settings_tab(state, false);
        }
        KeyCode::PageUp => {
            state.settings_scroll = state.settings_scroll.saturating_sub(6);
        }
        KeyCode::PageDown => {
            state.settings_scroll = state
                .settings_scroll
                .saturating_add(6)
                .min(state.settings_max_scroll);
        }
        KeyCode::Home => {
            state.settings_scroll = 0;
        }
        KeyCode::End => {
            state.settings_scroll = state.settings_max_scroll;
        }
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
            let (start, _) = active_tab_bounds(state);
            state.settings_selected = state.settings_selected.saturating_sub(1).max(start);
        }
        KeyCode::Down => {
            let (_, end) = active_tab_bounds(state);
            state.settings_selected = state.settings_selected.saturating_add(1).min(end);
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
                    "✓ API key will be cleared when changes are applied".to_string(),
                );
            }
            13 => {
                let key = state.settings_draft.api_key.trim().to_string();
                if key.is_empty() {
                    push_obs(state, "⚠ enter an API key before updating".to_string());
                } else {
                    save_tui_api_key(Some(&key));
                    state.settings.api_key = key.clone();
                    state.settings_draft.api_key = key;
                    push_obs(state, "✓ API key updated".to_string());
                }
            }
            14 => {
                state.settings_draft = state.settings.clone();
                state.settings_editing = false;
                push_obs(state, "✓ changes reverted".to_string());
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

pub(crate) fn render_settings_overlay(frame: &mut ratatui::Frame, state: &mut TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 76, 62);
    frame.render_widget(Clear, popup);

    let mut lines: Vec<Line> = Vec::new();
    let has_pending_changes = state.settings_draft != state.settings;
    let validation_errors = settings_validation_errors(&state.settings_draft);
    let validation_line = if validation_errors.is_empty() {
        " Status: ready to apply ".to_string()
    } else {
        format!(" Status: {} issue(s) to review ", validation_errors.len())
    };
    lines.push(Line::from(Span::styled(
        if has_pending_changes {
            " Changes not applied "
        } else {
            " All changes applied "
        },
        Style::default().fg(if has_pending_changes {
            Color::Yellow
        } else {
            Color::Green
        }),
    )));
    lines.push(Line::from(Span::styled(
        format!(" Secure storage: {} ", api_key_storage_backend_label()),
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
        " Up/Down: move  Enter: edit/action  Space/Left/Right: adjust  +/-: numbers  Tab/Shift+Tab: tab  PgUp/PgDn/Wheel: scroll  Ctrl+,/Esc: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    let active_section = active_settings_tab_index(state);
    let section_nav = SETTINGS_SECTIONS
        .iter()
        .enumerate()
        .map(|(idx, (name, _, _))| {
            if idx == active_section {
                format!("[{name}]")
            } else {
                name.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("  ");
    lines.push(Line::from(Span::styled(
        format!(" Tabs: {section_nav} "),
        Style::default().fg(Color::LightCyan),
    )));
    lines.push(Line::from(Span::styled(
        section_help_text(active_section),
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    let rows = vec![
        format!(
            "Runtime backend: {}  [toggle]",
            state.settings_draft.backend
        ),
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
            "Allowed modules: {}  [edit]",
            if state.settings_draft.allowed_modules.trim().is_empty() {
                "(all)".to_string()
            } else {
                state.settings_draft.allowed_modules.clone()
            }
        ),
        format!(
            "Tool calls: {}  [toggle]",
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
            "Environment variables: {} line(s)  [open]",
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
        "Review configuration  [action]".to_string(),
        "Clear API key  [action]".to_string(),
        "Update API key  [action]".to_string(),
        "Revert changes  [action]".to_string(),
        "Apply changes  [action]".to_string(),
        "Cancel  [action]".to_string(),
    ];

    let (start, end) = active_tab_bounds(state);
    let (tab_title, _, _) = SETTINGS_SECTIONS[active_section];
    lines.push(Line::from(Span::styled(
        format!(" {tab_title} "),
        Style::default()
            .fg(ui_accent_primary())
            .add_modifier(Modifier::BOLD),
    )));

    let mut selected_line: Option<usize> = None;
    for idx in start..=end {
        if idx == state.settings_selected {
            selected_line = Some(lines.len());
        }
        let row = &rows[idx];
        let marker = if idx == state.settings_selected {
            ">"
        } else {
            " "
        };
        let mut style = row_style_for_settings_index(idx, idx == state.settings_selected);
        if idx == state.settings_selected && state.settings_editing && idx <= 9 {
            style = style.add_modifier(Modifier::UNDERLINED);
        }
        lines.push(Line::from(Span::styled(format!("{marker} {row}"), style)));
    }
    lines.push(Line::from(""));

    let text = Text::from(lines);
    let inner_width = popup.width.saturating_sub(2);
    let visible_height = popup.height.saturating_sub(2);
    let visual_lines = visual_line_count(&text, inner_width);
    state.settings_max_scroll = visual_lines.saturating_sub(visible_height);
    state.settings_scroll = state.settings_scroll.min(state.settings_max_scroll);

    if let Some(line_idx) = selected_line {
        let visible_rows = visible_height as usize;
        if visible_rows > 0 {
            let top = state.settings_scroll as usize;
            let bottom = top.saturating_add(visible_rows.saturating_sub(1));
            if line_idx < top {
                state.settings_scroll = line_idx as u16;
            } else if line_idx > bottom {
                state.settings_scroll =
                    line_idx.saturating_add(1).saturating_sub(visible_rows) as u16;
            }
            state.settings_scroll = state.settings_scroll.min(state.settings_max_scroll);
        }
    }

    let panel = Paragraph::new(text)
        .block(
            Block::default()
                .title(" Settings ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false })
        .scroll((state.settings_scroll, 0));
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
        " Environment Variables ",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(Span::styled(
        " Format: KEY=VALUE. One per line. Empty lines and # comments are ignored. ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(Span::styled(
        " Esc: back  Enter: new line  Tab: insert '=' ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    let env_errors = env_overrides_validation_errors(&state.settings_draft.env_overrides);
    if env_errors.is_empty() {
        lines.push(Line::from(Span::styled(
            " Status: ready ",
            Style::default().fg(Color::Green),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            format!(" Status: {} issue(s) ", env_errors.len()),
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
                .title(" Environment ")
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
        push_obs(state, "✓ configuration ready".to_string());
        true
    } else {
        for error in errors {
            push_obs(state, format!("⚠ configuration: {error}"));
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

fn switch_settings_tab(state: &mut TuiState, forward: bool) {
    let current = active_settings_tab_index(state);
    let next = if forward {
        (current + 1) % SETTINGS_SECTIONS.len()
    } else if current == 0 {
        SETTINGS_SECTIONS.len() - 1
    } else {
        current - 1
    };

    state.settings_tab = next;
    let (_, start, _) = SETTINGS_SECTIONS[next];
    state.settings_selected = start;
    state.settings_editing = false;
    state.settings_scroll = 0;
    state.settings_max_scroll = 0;
}

fn active_settings_tab_index(state: &TuiState) -> usize {
    state
        .settings_tab
        .min(SETTINGS_SECTIONS.len().saturating_sub(1))
}

fn active_tab_bounds(state: &TuiState) -> (usize, usize) {
    let tab = active_settings_tab_index(state);
    let (_, start, end) = SETTINGS_SECTIONS[tab];
    (start, end)
}

fn clamp_selected_to_active_tab(state: &mut TuiState) {
    let (start, end) = active_tab_bounds(state);
    state.settings_selected = state.settings_selected.clamp(start, end);
}

fn row_style_for_settings_index(idx: usize, selected: bool) -> Style {
    let base = if selected {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        match idx {
            15 => Style::default().fg(Color::Green),
            16 => Style::default().fg(Color::LightRed),
            12 => Style::default().fg(Color::LightYellow),
            13 => Style::default().fg(Color::LightMagenta),
            11 => Style::default().fg(Color::Cyan),
            _ => Style::default().fg(Color::White),
        }
    };

    if idx >= 14 {
        base.add_modifier(Modifier::BOLD)
    } else {
        base
    }
}

fn section_help_text(active_section: usize) -> &'static str {
    match active_section {
        0 => " Provider, model, connection, API key, and module access.",
        1 => " Runtime behavior, tool limits, and thinking capture.",
        2 => " Review configuration and key-related safety actions.",
        _ => " Revert, apply, or close without applying.",
    }
}

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
