# Medousa Grapheme Editor Sprint Plan

Status: Active
Owner: Medousa
Last updated: 2026-05-19

## Goal

Build an in-TUI Grapheme coding experience incrementally, with clear sprint boundaries and low-risk integration points.

## Delivery Strategy

1. Keep each sprint scoped to one core capability.
2. Preserve existing TUI behavior while re-organizing internals.
3. End each sprint with explicit keyboard UX notes and validation checks.

## Sprint 1: Source Reorganization (Per-Concern Modules)

Status: In progress

Scope:
1. Reorganize `medousa_tui` by concern (state, event handling, overlays, runtime actions, editor/preview logic).
2. Move logic into focused modules without behavior changes.
3. Keep `medousa_tui` entrypoint as orchestration-oriented.

Definition of done:
1. Functional behavior remains unchanged.
2. `cargo check -p medousa` passes.
3. Existing focused tests remain green (`tools::tests`, `settings_guard`).
4. First extraction slice merged and documented.

## Sprint 2: Minimal Embedded Text Editor

Status: Planned

Scope:
1. Add a simple editor window/mode in TUI.
2. Support open/create, edit, save, cancel for plain text files.
3. Support generic file types (`.txt`, `.gr`, others) with no language features yet.

Definition of done:
1. Operator can open/edit/save file from within TUI.
2. Save/open failure paths are surfaced with clear errors.
3. Keyboard workflow is documented in help text.

## Sprint 3: Run `.gr` Files Through Runtime

Status: Planned

Scope:
1. Add command(s) to execute selected/current `.gr` file through runtime.
2. Route run output/errors into observability and job history.
3. Preserve allowlist enforcement and pre-run policy visibility.

Definition of done:
1. `.gr` file execution is usable from TUI command flow.
2. Policy-violating ops are blocked with explicit diagnostics.
3. Execution output is inspectable in observability.

## Sprint 4: Syntax + LSP Enablement

Status: Planned

Scope:
1. Add syntax highlighting for Grapheme editing surfaces.
2. Integrate LSP incrementally (diagnostics first, then hover/completions).
3. Keep fallback behavior functional when LSP is unavailable.

Definition of done:
1. Syntax coloring is visible and stable in editor view.
2. Diagnostics can be surfaced and navigated.
3. Basic completion path is available when LSP is active.

## Cross-Sprint Guardrails

1. Add at least one integration regression test per sprint.
2. Keep keybind and command docs updated each sprint.
3. Failures must be explicit, operator-facing, and non-destructive.
4. Do not broaden sprint scope once started.

## Execution Log

- 2026-05-19: Plan created.
- 2026-05-19: Sprint 1 started.
- 2026-05-19: Sprint 1 slice: extracted allowlist preview analysis into medousa/src/tui/allowlist_preview.rs and rewired TUI consumers.
- 2026-05-19: Sprint 1 slice: extracted runtime settings model and normalization/validation helpers into medousa/src/tui/settings.rs with unit tests.
- 2026-05-19: Sprint 1 slice: extracted settings menu input/render concern into bin-local module medousa/src/bin/medousa_tui/settings_ui.rs.
- 2026-05-19: Sprint 1 slice: extracted command palette and allowlist preview input/render concern into bin-local module medousa/src/bin/medousa_tui/command_preview_ui.rs.
