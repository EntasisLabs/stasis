# Medousa TUI Theme System + Theme Menu Plan

## Why
The current TUI colors are hardcoded in `ui_helpers.rs`, which makes visual customization difficult and prevents users from choosing a look that matches their terminal and accessibility needs.

This plan introduces a tokenized theme system, persisted user selection, and a dedicated theme menu integrated with existing settings and command palette workflows.

## Goals
- Add first-class theme support with minimal runtime overhead.
- Keep the existing visual language as the default theme to avoid regressions.
- Provide a fast in-app theme picker with preview and cancel/apply semantics.
- Persist selected theme in session defaults and load it at startup.
- Keep the system extensible so future custom themes are low-friction.

## Non-Goals (Phase 1)
- User-authored theme files on disk.
- Per-component custom color editing UI.
- Dynamic gradients or truecolor effects beyond current ratatui color support.

## Current State
- Color helpers are globally hardcoded in `medousa/src/bin/medousa_tui/ui_helpers.rs`:
  - `ui_bg`
  - `ui_panel_bg`
  - `ui_modal_bg`
  - `ui_border`
  - `ui_accent_primary`
  - `ui_accent_warn`
- Rendering surfaces consume these helpers across:
  - `ui_render.rs`
  - `settings_ui.rs`
  - `command_preview_ui.rs`
  - additional overlays in `medousa_tui`.

## Target Architecture

### 1) Theme Tokens
Create a `TuiTheme` struct containing all palette tokens currently hardcoded.

Suggested fields:
- `name: &'static str`
- `bg`
- `panel_bg`
- `modal_bg`
- `border`
- `accent_primary`
- `accent_warn`
- Optional future tokens:
  - `text_primary`
  - `text_muted`
  - `success`
  - `error`

### 2) Theme Registry
Define a static registry of built-in themes.

Proposed initial presets:
- `medousa-default` (exact current values)
- `arctic-ink` (cool high-contrast)
- `paper-terminal` (light mode, neutral accents)
- `amber-noir` (dark with warm accent)

### 3) Runtime State Integration
Store selected theme id in runtime and settings draft.

Touch points:
- `TuiSettings` and `TuiSettingsDraft` (or equivalent runtime settings object)
- session defaults model in `medousa/src/session.rs` (`TuiDefaults`)
- settings parse/apply paths in `settings_runtime.rs`
- startup load path where defaults become active state

### 4) Color Helper Refactor
Refactor `ui_helpers.rs` from hardcoded functions to theme-aware lookups.

Approach:
- Keep function names (`ui_bg()`, etc.) to minimize call-site changes.
- Back these functions by the active theme in `TuiState`.
- If global access is needed, pass `&TuiState` into render helpers where required.

Preferred approach for safety:
- Introduce `theme_color(state, token)` or `state.theme.<token>` access in render paths.
- Migrate helper calls incrementally; avoid global mutable singleton.

### 5) Theme Menu UX
Add a dedicated theme picker overlay with preview.

Entry points:
- Command palette action: `Open Theme Menu`.
- Settings row under Model or Session tab: `Theme: <name> [open]`.

Behavior:
- Up/Down selects theme.
- Enter applies preview to draft immediately.
- `A` (or Enter in confirm row) applies and saves.
- Esc reverts to prior theme if not applied.

UI content:
- Left: list of themes.
- Right: quick sample swatches and contrast hint text.

### 6) Persistence
Persist selected theme id via `TuiDefaults` and load it on startup.

Rules:
- Unknown id falls back to `medousa-default`.
- Missing value uses existing behavior (`medousa-default`).

## Implementation Plan

### Phase A: Foundations
- Add theme model + built-in registry.
- Add setting field (`theme_id`) to runtime/settings/defaults.
- Wire parse/apply/save paths.
- No UI changes yet beyond data plumbing.

### Phase B: Render Integration
- Update color helpers to resolve from active theme.
- Migrate all render surfaces to theme tokens.
- Validate no visual regressions under default theme.

### Phase C: Theme Menu
- Add new `UiMode::ThemeMenu`.
- Add key handling + renderer for theme picker overlay.
- Add settings row and command palette action to open it.
- Implement preview/apply/revert behavior.

### Phase D: Fit and Finish
- Add contrast checks in theme definitions.
- Add docs with screenshots/gifs.
- Add tests for persistence + fallback behavior.

## Data Model Changes

### TuiDefaults (session persistence)
Add:
- `theme_id: Option<String>`

### Runtime Settings
Add:
- `theme_id: String`

Default value:
- `"medousa-default"`

## Validation + Testing
- Unit tests:
  - theme id fallback to default when invalid
  - persistence round-trip through defaults
  - command palette open-theme action routing
- Snapshot/manual tests:
  - each built-in theme renders readable borders/text
  - settings and overlays remain legible
- Smoke:
  - `cargo check -p medousa`
  - `cargo test -p medousa --bin medousa_tui`

## Risks and Mitigations
- Risk: Hardcoded color literals outside helper functions.
  - Mitigation: grep sweep for `Color::Rgb` and migrate to tokens where UI-facing.
- Risk: Light theme unreadability from inherited white text assumptions.
  - Mitigation: introduce text tokens in phase B if needed.
- Risk: Preview flicker while browsing themes.
  - Mitigation: update only on selection change, avoid expensive recomputation.

## Rollout Strategy
- Land in small PR-sized commits by phase.
- Keep default theme identical to current palette for safe merge.
- Expose theme menu only after render integration is complete.

## Open Decisions
- Should theme picker live under Settings > Session or a dedicated top-level tab?
- Should preview apply to whole UI immediately or only to overlay until confirmed?
- Do we want a per-session temporary theme override separate from saved defaults?

## Proposed Next Step
Implement Phase A now: introduce `theme_id` in settings/defaults/runtime with default fallback, without changing visual rendering yet.
