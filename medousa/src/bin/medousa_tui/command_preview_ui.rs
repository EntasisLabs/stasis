use super::*;

#[derive(Clone, Copy)]
struct PaletteAction {
    title: &'static str,
    command: &'static str,
}

const PALETTE_ACTIONS: [PaletteAction; 14] = [
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
        title: "Allowlist Preview",
        command: "/allowlist-preview",
    },
    PaletteAction {
        title: "Clear API Key",
        command: "/clear-key",
    },
    PaletteAction {
        title: "Rotate API Key",
        command: "/rotate-key",
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
    PaletteAction {
        title: "Daemon Health",
        command: "/daemon health",
    },
    PaletteAction {
        title: "Daemon Command Help",
        command: "/daemon",
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

pub(crate) async fn handle_command_palette_key_event(
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
                return super::handle_slash_command(action.command, state, tui_rt, event_tx).await;
            }
        }
        _ => {}
    }

    EventOutcome::Continue
}

pub(crate) fn handle_allowlist_preview_key_event(
    code: KeyCode,
    state: &mut TuiState,
) -> EventOutcome {
    let analysis = analyze_allowlist_preview(
        &state.allowlist_preview_source,
        &state.settings_draft.allowed_modules,
    );

    match code {
        KeyCode::Backspace => {
            state.allowlist_preview_source.pop();
        }
        KeyCode::Char(c) => {
            state.allowlist_preview_source.push(c);
        }
        KeyCode::Enter => {
            state.allowlist_preview_source.push('\n');
        }
        KeyCode::Tab => {
            if !analysis.invalid_allowlist.is_empty() {
                push_obs(
                    state,
                    format!(
                        "⚠ allowlist preview invalid allowlist ids: {}",
                        analysis.invalid_allowlist.join(", ")
                    ),
                );
                return EventOutcome::Continue;
            }
            if analysis.referenced_ops.is_empty() {
                push_obs(
                    state,
                    "allowlist preview: no module operation calls found in source".to_string(),
                );
                return EventOutcome::Continue;
            }
            if analysis.blocked_ops.is_empty() {
                push_obs(
                    state,
                    format!(
                        "✓ allowlist preview: all referenced ops allowed ({})",
                        analysis.referenced_ops.join(", ")
                    ),
                );
            } else {
                push_obs(
                    state,
                    format!(
                        "⚠ allowlist preview: blocked ops {}",
                        analysis.blocked_ops.join(", ")
                    ),
                );
            }
        }
        KeyCode::F(5) => {
            if analysis.referenced_ops.is_empty() {
                push_obs(
                    state,
                    "⚠ allowlist preview replace skipped: no referenced ops detected"
                        .to_string(),
                );
            } else {
                state.settings_draft.allowed_modules = analysis.referenced_ops.join(",");
                push_obs(
                    state,
                    format!(
                        "✓ allowlist preview replaced draft allowlist with {} op(s)",
                        analysis.referenced_ops.len()
                    ),
                );
            }
        }
        KeyCode::F(6) => {
            if analysis.referenced_ops.is_empty() {
                push_obs(
                    state,
                    "⚠ allowlist preview append skipped: no referenced ops detected".to_string(),
                );
            } else {
                let mut merged = parse_allowed_modules(&state.settings_draft.allowed_modules);
                let mut seen = merged.iter().cloned().collect::<HashSet<String>>();

                let mut appended = 0usize;
                for op in &analysis.referenced_ops {
                    if seen.insert(op.clone()) {
                        merged.push(op.clone());
                        appended += 1;
                    }
                }

                state.settings_draft.allowed_modules = merged.join(",");
                push_obs(
                    state,
                    format!("✓ allowlist preview appended {appended} op(s) to draft allowlist"),
                );
            }
        }
        _ => {}
    }
    EventOutcome::Continue
}

pub(crate) fn render_command_palette_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
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

pub(crate) fn render_allowlist_preview_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    let area = frame.area();
    let popup = centered_rect(area, 86, 70);
    frame.render_widget(Clear, popup);

    let analysis = analyze_allowlist_preview(
        &state.allowlist_preview_source,
        &state.settings_draft.allowed_modules,
    );

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Type source, Enter: newline, Tab: emit verdict, F5: replace allowlist, F6: append allowlist, Esc: close ",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(Span::styled(
        " Uses same parser/policy shape as runtime module enforcement ",
        Style::default().fg(Color::Cyan),
    )));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        format!(
            "Draft allowlist: {}",
            if state.settings_draft.allowed_modules.trim().is_empty() {
                "(all operations allowed)".to_string()
            } else {
                state.settings_draft.allowed_modules.clone()
            }
        ),
        Style::default().fg(Color::White),
    )));
    lines.push(Line::from(Span::styled(
        "Source (editable):",
        Style::default().fg(Color::Yellow),
    )));
    if state.allowlist_preview_source.is_empty() {
        lines.push(Line::from(Span::styled(
            "  (empty)",
            Style::default().fg(Color::Gray),
        )));
    } else {
        for (idx, source_line) in state.allowlist_preview_source.lines().enumerate() {
            lines.push(Line::from(Span::styled(
                format!("  {:>2}: {}", idx + 1, source_line),
                Style::default().fg(Color::Yellow),
            )));
        }
    }
    lines.push(Line::from(""));

    if !analysis.invalid_allowlist.is_empty() {
        lines.push(Line::from(Span::styled(
            format!(
                "Invalid allowlist IDs: {}",
                analysis.invalid_allowlist.join(", ")
            ),
            Style::default().fg(Color::Red),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            if analysis.blocked_ops.is_empty() {
                "Verdict: ALLOW".to_string()
            } else {
                "Verdict: BLOCK".to_string()
            },
            Style::default().fg(if analysis.blocked_ops.is_empty() {
                Color::Green
            } else {
                Color::Red
            }),
        )));
    }
    lines.push(Line::from(""));

    lines.push(Line::from(Span::styled(
        "Referenced Ops:",
        Style::default().fg(Color::Cyan),
    )));
    if analysis.referenced_ops.is_empty() {
        lines.push(Line::from(Span::styled(
            "  (none detected)",
            Style::default().fg(Color::Gray),
        )));
    } else {
        for op in &analysis.referenced_ops {
            lines.push(Line::from(Span::styled(
                format!("  - {op}"),
                Style::default().fg(Color::White),
            )));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Blocked Ops:",
        Style::default().fg(Color::Cyan),
    )));
    if analysis.blocked_ops.is_empty() {
        lines.push(Line::from(Span::styled(
            "  (none)",
            Style::default().fg(Color::Green),
        )));
    } else {
        for op in &analysis.blocked_ops {
            lines.push(Line::from(Span::styled(
                format!("  - {op}"),
                Style::default().fg(Color::Red),
            )));
        }
    }

    let panel = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(" Allowlist Preview ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ui_accent_primary()))
                .style(Style::default().bg(ui_modal_bg())),
        )
        .style(Style::default().fg(Color::White).bg(ui_modal_bg()))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, popup);
}
