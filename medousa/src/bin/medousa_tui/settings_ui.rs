use super::*;

const SETTINGS_SECTIONS: [(&str, usize, usize); 6] = [
    ("Model", 0, 5),
    ("Runtime", 6, 9),
    ("Verifier", 10, 13),
    ("Safety", 14, 17),
    ("Routing", 18, 25),
    ("Session", 26, 28),
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
                if let Some(target) = selected_route_field_mut(state) {
                    target.pop();
                } else {
                    let target = selected_settings_field_mut(state);
                    target.pop();
                }
            }
            KeyCode::Char(c) => {
                if let Some(target) = selected_route_field_mut(state) {
                    target.push(c);
                } else {
                    let target = selected_settings_field_mut(state);
                    target.push(c);
                }
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
            if matches!(state.settings_selected, 7 | 9 | 10 | 11 | 12 | 13) {
                quick_adjust_setting(state, true);
            }
        }
        KeyCode::Char('-') => {
            if matches!(state.settings_selected, 7 | 9 | 10 | 11 | 12 | 13) {
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
            1..=5 | 19 | 20 => {
                state.settings_editing = true;
            }
            0 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 18 | 22 | 23 | 24 | 25 => {
                quick_adjust_setting(state, true);
            }
            14 => {
                state.mode = UiMode::RuntimeEnv;
                state.runtime_env_editing = true;
            }
            15 => {
                emit_settings_validation_summary(state);
            }
            16 => {
                state.settings_draft.api_key.clear();
                push_obs(
                    state,
                    "✓ API key will be cleared when changes are applied".to_string(),
                );
            }
            17 => {
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
            21 => {
                sync_all_route_targets_to_global(state);
            }
            26 => {
                state.settings_draft = state.settings.clone();
                state.stage_routing_draft = state.stage_routing.clone();
                state.routing_editor_role_idx = 0;
                state.settings_editing = false;
                push_obs(state, "✓ changes reverted".to_string());
            }
            27 => {
                super::apply_settings(state, tui_rt, event_tx).await;
                state.mode = UiMode::Chat;
            }
            28 => {
                state.settings_draft = state.settings.clone();
                state.stage_routing_draft = state.stage_routing.clone();
                state.routing_editor_role_idx = 0;
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
    let has_pending_changes =
        state.settings_draft != state.settings || state.stage_routing_draft != state.stage_routing;
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

    let selected_role = routing_editor_role(state);
    let selected_route = state
        .stage_routing_draft
        .get(selected_role)
        .expect("selected routing role should exist");

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
            "Verifier Min Citation Coverage: {}  [number]",
            state.settings_draft.verifier_min_citation_coverage
        ),
        format!(
            "Verifier Min Avg Support Strength: {}  [number]",
            state.settings_draft.verifier_min_avg_support_strength
        ),
        format!(
            "Verifier Min Supported Claim Ratio: {}  [number]",
            state.settings_draft.verifier_min_supported_claim_ratio
        ),
        format!(
            "Verifier Min Claim Support Strength: {}  [number]",
            state.settings_draft.verifier_min_claim_support_strength
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
        format!("Route Role: {}  [cycle]", selected_role),
        format!("Route Provider: {}  [edit]", selected_route.provider),
        format!("Route Model: {}  [edit]", selected_route.model),
        format!(
            "Set All Route Targets: {}:{}  [action]",
            state.settings_draft.provider.trim(),
            state.settings_draft.model.trim()
        ),
        format!(
            "Route Target Preset: {}:{}  [cycle presets]",
            selected_route.provider, selected_route.model
        ),
        format!(
            "Route Policy Profile: {}  [cycle]",
            selected_route.policy_profile
        ),
        format!(
            "Route Fallback Chain: {}  [cycle]",
            selected_route.fallback_chain.join(",")
        ),
        "Reset Selected Route to Defaults  [action]".to_string(),
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
        if idx == state.settings_selected
            && state.settings_editing
            && matches!(idx, 1..=5 | 19 | 20)
        {
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
        10 => {
            let current = parse_f32_with_bounds(
                &state.settings_draft.verifier_min_citation_coverage,
                0.60,
                0.0,
                1.0,
            );
            let step = 0.05;
            let next = if forward {
                current + step
            } else {
                current - step
            }
            .clamp(0.0, 1.0);
            state.settings_draft.verifier_min_citation_coverage = format!("{next:.2}");
        }
        11 => {
            let current = parse_f32_with_bounds(
                &state.settings_draft.verifier_min_avg_support_strength,
                0.70,
                0.0,
                1.0,
            );
            let step = 0.05;
            let next = if forward {
                current + step
            } else {
                current - step
            }
            .clamp(0.0, 1.0);
            state.settings_draft.verifier_min_avg_support_strength = format!("{next:.2}");
        }
        12 => {
            let current = parse_f32_with_bounds(
                &state.settings_draft.verifier_min_supported_claim_ratio,
                0.60,
                0.0,
                1.0,
            );
            let step = 0.05;
            let next = if forward {
                current + step
            } else {
                current - step
            }
            .clamp(0.0, 1.0);
            state.settings_draft.verifier_min_supported_claim_ratio = format!("{next:.2}");
        }
        13 => {
            let current = parse_f32_with_bounds(
                &state.settings_draft.verifier_min_claim_support_strength,
                0.65,
                0.0,
                1.0,
            );
            let step = 0.05;
            let next = if forward {
                current + step
            } else {
                current - step
            }
            .clamp(0.0, 1.0);
            state.settings_draft.verifier_min_claim_support_strength = format!("{next:.2}");
        }
        18 => {
            let roles = medousa::stage_routing::StageRoutingMatrix::roles();
            if roles.is_empty() {
                return;
            }
            state.routing_editor_role_idx = if forward {
                (state.routing_editor_role_idx + 1) % roles.len()
            } else if state.routing_editor_role_idx == 0 {
                roles.len() - 1
            } else {
                state.routing_editor_role_idx - 1
            };
        }
        22 => {
            let role = routing_editor_role(state).to_string();
            if let Some(route) = state.stage_routing_draft.get_mut(&role) {
                let presets = route_target_presets();
                let current = format!("{}:{}", route.provider, route.model);
                let idx = presets.iter().position(|v| *v == current).unwrap_or(0);
                let next = if forward {
                    (idx + 1) % presets.len()
                } else if idx == 0 {
                    presets.len() - 1
                } else {
                    idx - 1
                };
                if let Some((provider, model)) = presets[next].split_once(':') {
                    route.provider = provider.to_string();
                    route.model = model.to_string();
                }
            }
        }
        23 => {
            let role = routing_editor_role(state).to_string();
            if let Some(route) = state.stage_routing_draft.get_mut(&role) {
                let options = ["balanced", "strict", "analytical", "fast"];
                let idx = options
                    .iter()
                    .position(|v| v.eq_ignore_ascii_case(route.policy_profile.as_str()))
                    .unwrap_or(0);
                let next = if forward {
                    (idx + 1) % options.len()
                } else if idx == 0 {
                    options.len() - 1
                } else {
                    idx - 1
                };
                route.policy_profile = options[next].to_string();
            }
        }
        24 => {
            let role = routing_editor_role(state).to_string();
            if let Some(route) = state.stage_routing_draft.get_mut(&role) {
                let options = vec![
                    vec![role.clone(), "safe-default".to_string()],
                    vec!["safe-default".to_string()],
                    vec![role, "balanced".to_string(), "safe-default".to_string()],
                ];
                let idx = options
                    .iter()
                    .position(|v| *v == route.fallback_chain)
                    .unwrap_or(0);
                let next = if forward {
                    (idx + 1) % options.len()
                } else if idx == 0 {
                    options.len() - 1
                } else {
                    idx - 1
                };
                route.fallback_chain = options[next].clone();
            }
        }
        25 => {
            let role = routing_editor_role(state).to_string();
            let defaults = medousa::stage_routing::StageRoutingMatrix::default_for(
                &state.settings_draft.provider,
                &state.settings_draft.model,
            );
            if let (Some(current), Some(default_route)) = (
                state.stage_routing_draft.get_mut(&role),
                defaults.get(&role),
            ) {
                *current = default_route.clone();
            }
        }
        _ => {}
    }
}

fn sync_all_route_targets_to_global(state: &mut TuiState) {
    let provider = state.settings_draft.provider.trim();
    let model = state.settings_draft.model.trim();
    if provider.is_empty() || model.is_empty() {
        push_obs(
            state,
            "⚠ cannot sync route targets: Provider and Model must be set".to_string(),
        );
        return;
    }

    for role in medousa::stage_routing::StageRoutingMatrix::roles() {
        if let Some(route) = state.stage_routing_draft.get_mut(role) {
            route.provider = provider.to_string();
            route.model = model.to_string();
        }
    }

    push_obs(
        state,
        format!(
            "✓ route targets synced to {}:{} for all stages",
            provider, model
        ),
    );
}

fn routing_editor_role(state: &TuiState) -> &'static str {
    let roles = medousa::stage_routing::StageRoutingMatrix::roles();
    roles
        .get(state.routing_editor_role_idx % roles.len())
        .copied()
        .unwrap_or("final_response")
}

fn route_target_presets() -> [&'static str; 5] {
    [
        "openai:gpt-4o-mini",
        "anthropic:claude-3-7-sonnet-latest",
        "google:gemini-2.5-pro",
        "xai:grok-3-mini",
        "ollama:llama3.2",
    ]
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
        10 => &mut state.settings_draft.verifier_min_citation_coverage,
        11 => &mut state.settings_draft.verifier_min_avg_support_strength,
        12 => &mut state.settings_draft.verifier_min_supported_claim_ratio,
        13 => &mut state.settings_draft.verifier_min_claim_support_strength,
        _ => &mut state.settings_draft.base_url,
    }
}

fn selected_route_field_mut(state: &mut TuiState) -> Option<&mut String> {
    let role = routing_editor_role(state).to_string();
    let route = state.stage_routing_draft.get_mut(&role)?;
    match state.settings_selected {
        19 => Some(&mut route.provider),
        20 => Some(&mut route.model),
        _ => None,
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
            15 => Style::default().fg(Color::Cyan),
            27 => Style::default().fg(Color::Green),
            28 => Style::default().fg(Color::LightRed),
            16 => Style::default().fg(Color::LightYellow),
            17 => Style::default().fg(Color::LightMagenta),
            25 => Style::default().fg(Color::LightCyan),
            _ => Style::default().fg(Color::White),
        }
    };

    if idx >= 26 {
        base.add_modifier(Modifier::BOLD)
    } else {
        base
    }
}

fn section_help_text(active_section: usize) -> &'static str {
    match active_section {
        0 => " Provider, model, connection, API key, and module access.",
        1 => " Runtime behavior, tool limits, and thinking capture.",
        2 => " Verification thresholds for confidence and evidence gating.",
        3 => " Review configuration and key-related safety actions.",
        4 => " Stage routing role, manual provider/model, presets, policy, fallback, and reset.",
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
