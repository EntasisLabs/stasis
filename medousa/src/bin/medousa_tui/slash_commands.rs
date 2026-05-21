use super::daemon_commands::{handle_daemon_command, handle_watch_command};
use super::*;

pub(crate) async fn handle_slash_command(
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
            invalidate_markdown_cache(state);
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

            let mut draft = state.settings_draft.clone();
            if args.len() == 1 {
                if let Some((provider, model)) = args[0].split_once(':') {
                    draft.provider = provider.trim().to_string();
                    draft.model = model.trim().to_string();
                } else {
                    draft.model = args[0].trim().to_string();
                }
            } else {
                draft.provider = args[0].trim().to_string();
                draft.model = args[1].trim().to_string();
            }

            state.settings_draft = draft;

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
        "/perf" => {
            let sub = parts.next().unwrap_or("report");
            match sub {
                "baseline" => {
                    let label = parts.collect::<Vec<_>>().join(" ");
                    let label = if label.trim().is_empty() {
                        "baseline".to_string()
                    } else {
                        label.trim().to_string()
                    };
                    let snapshot = capture_perf_snapshot(state, label.clone());
                    state.perf_baseline = Some(snapshot.clone());
                    push_obs(
                        state,
                        format!("✓ perf baseline set: {}", format_perf_snapshot(&snapshot)),
                    );
                }
                "reset" => {
                    state.perf = UiPerfStats::default();
                    state.perf_baseline = None;
                    push_obs(state, "✓ perf counters and baseline reset".to_string());
                }
                _ => {
                    let label = if sub == "report" {
                        "report".to_string()
                    } else {
                        sub.to_string()
                    };
                    let current = capture_perf_snapshot(state, label);
                    let mut line = format!("perf {}", format_perf_snapshot(&current));
                    if let Some(baseline) = &state.perf_baseline {
                        line.push_str(" | ");
                        line.push_str(&format_perf_delta(&current, baseline));
                    }
                    push_obs(state, line);
                }
            }
        }
        "/daemon" => {
            return handle_daemon_command(&mut parts, state);
        }
        "/watch" => {
            return handle_watch_command(&mut parts, state);
        }
        _ => {
            push_obs(
                state,
                "⚠ unknown command. try /new /history /settings /edit /open /save /run /run-current /close /allowlist-preview /clear-key /rotate-key /model /stop /regen /export /perf /daemon /watch"
                    .to_string(),
            );
        }
    }

    EventOutcome::Continue
}
