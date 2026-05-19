# Medousa TUI UX/DX Roadmap

Status: Active
Owner: Medousa
Last updated: 2026-05-16

## Goals

1. Make `medousa_tui` feel like a production-ready terminal chat.
2. Preserve fast keyboard-first workflows.
3. Keep architecture simple enough to evolve without rewrites.

## Product Principles

1. Default to immediate chat flow.
2. Make session state explicit and reversible.
3. Keep overlays lightweight and modal.
4. Prefer additive, backward-compatible runtime changes.

## Phased Delivery

## Phase 1: Session Foundations

Status: Completed

Scope:
1. Start a fresh session by default on each app launch.
2. Add a history menu overlay to browse and load older sessions.
3. Keep user in chat mode by default with quick history access.

Acceptance criteria:
1. Launching `medousa_tui` without flags creates a new session ID.
2. User can open history menu via keyboard and navigate with arrow keys.
3. Pressing Enter on a history item loads that session conversation into the main chat pane.
4. UI returns to chat mode after loading a session.

Out of scope for Phase 1:
1. Session rename/pin/delete.
2. Fuzzy search in history.
3. Full runtime hot-rebuild on session swap.

## Phase 2: Settings Overlay

Status: In progress

Scope:
1. Add a settings menu overlay with editable runtime options.
2. Support provider/model/base URL updates from TUI.
3. Persist user defaults between launches.

Acceptance criteria:
1. User can open settings without leaving chat.
2. Updating settings is reflected in the UI immediately.
3. Persisted defaults are loaded at startup.

## Phase 3: Power Chat UX

Status: Planned

Scope:
1. Add command palette and slash commands (`/new`, `/history`, `/settings`, `/model`).
2. Add stop/regenerate flows for assistant responses.
3. Add export controls for session transcript.

Acceptance criteria:
1. User can operate common actions without reaching for CLI flags.
2. Streaming can be canceled safely.
3. Transcript export is available in at least one text format.

## Architecture Notes

1. UI should evolve as explicit mode state (`Chat`, `HistoryMenu`, `SettingsMenu`).
2. Session persistence should stay in `medousa::session` APIs.
3. Pipeline streaming remains independent from overlay state.

## Risks

1. Session switching after runtime build may mismatch memory session context for some tools.
2. Growing keybind surface can create conflicts without a centralized key map.
3. Overlay complexity can leak into chat rendering if mode boundaries are not strict.

## Mitigations

1. Keep Phase 1 session switching focused on transcript/history behavior.
2. Centralize mode-specific key handling early.
3. Add small integration tests for mode transitions in follow-up PRs.

## Execution Log

- 2026-05-16: Roadmap created.
- 2026-05-16: Phase 1 implementation started.
- 2026-05-16: Phase 1 completed (fresh startup session + history overlay).
- 2026-05-16: Phase 2 started (settings overlay + persisted defaults + runtime apply).
- 2026-05-16: Phase 3 started with slash commands (/new, /history, /settings, /model).
- 2026-05-16: Phase 3 expanded with stop/regenerate controls and transcript export (/stop, /regen, /export).
- 2026-05-16: Phase 3 expanded with Ctrl+K command palette (search + Enter-to-run actions).
- 2026-05-19: Settings safety design plan created at docs/design/medousa-tui-settings-safety-plan.md.
- 2026-05-19: Settings safety slice 1 started (masked API key field, module allowlist validation, payload redaction, atomic persistence).
- 2026-05-19: Settings safety slice 2 implemented (runtime allowlist enforcement for Grapheme execution/promote/enqueue tool paths).
- 2026-05-19: Added explicit clear-key action in settings overlay and command palette (/clear-key).
- 2026-05-19: Implemented transactional settings UX in TUI (draft vs applied, validation summary, revert-to-last-applied).
- 2026-05-19: Implemented secret backend hardening (OS keychain adapter with file fallback, key rotation action, observability redaction-mode indicator).
- 2026-05-19: Expanded allowlist preview panel with multiline source editing and one-key actions to replace/append detected ops into draft module allowlist.
