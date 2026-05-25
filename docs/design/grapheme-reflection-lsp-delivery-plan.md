# Grapheme Reflection and LSP Delivery Plan

## Purpose

Define a phased delivery plan for production-grade Grapheme authoring in Stasis dashboard and runtime flows using the new Grapheme SDK reflection APIs and optional Grapheme LSP integration.

This plan is internal and execution-oriented.

## Context Snapshot (2026-05-24)

Current state:

- `grapheme-sdk` upgraded to `0.3.0`.
- `grapheme-lsp` added at `0.4.0`.
- Runtime execution is implemented through `WorkflowEngine::execute_grapheme_source`.
- Dashboard workflow save action is still confirmation-level and does not persist workflow definition/revision artifacts.

Important technical update:

- Grapheme SDK now exposes module and executable reflection payload contracts (`modules_*`, `executables_reflection_*`), which can serve as the canonical backend reflection source.
- Grapheme LSP now exposes an embeddable library entrypoint (`run_server`, `run_stdio`) for editor-like authoring experiences.

## Delivery Principles

- Reflection is advisory UX metadata; runtime guardrails remain the enforcement boundary.
- Runtime and dashboard must consume the same reflection contract to avoid drift.
- LSP integration is optional and feature-gated; core runtime users should not pay extra dependencies.
- Preserve backward compatibility while shifting naming and architecture to backend-agnostic behavior.

## Phase Plan

## Phase 0 - Contract and Scope Lock

Outcome:

- Freeze reflection contract boundaries and success criteria before endpoint/UI implementation.

Deliverables:

1. Reflection contract scope decision:
   - executable reflection from source (required)
   - module catalog/info/types/search (required)
   - editor diagnostics/completion via LSP (optional)
2. Feature-gate boundary definition for LSP-assisted authoring.
3. Error model alignment (`PortFailure` mapping policy and diagnostics fields).

Exit criteria:

- Team-approved contract shape and phased ownership.

## Phase 1 - Reflection Port and SDK Adapter (Kickoff)

Outcome:

- Introduce stable Stasis reflection primitives and wire them to Grapheme SDK reflection APIs.

Deliverables:

1. Add outbound runtime port for Grapheme executable reflection.
2. Add infrastructure adapter backed by Grapheme SDK reflection functions.
3. Add unit tests for:
   - valid source reflection
   - invalid source deterministic error mapping

Exit criteria:

- Reflection port compiles cleanly and tests pass.

## Phase 2 - Workflow Definition Persistence and Preflight

Outcome:

- Replace dashboard workflow placeholders with persisted definitions and revisioned validation receipts.

Deliverables:

1. Workflow definition + revision persistence model.
2. Save path executes reflection preflight and stores receipt.
3. Execute path references persisted revision and records execution receipt.

Exit criteria:

- Dashboard workflow save/execute endpoints produce durable IDs and validation status.

## Phase 3 - Dashboard Reflection Surfaces

Outcome:

- Expose module and executable reflection in dashboard for real authoring and inspection.

Deliverables:

1. Reflection query APIs in dashboard service.
2. UI sections for:
   - available modules
   - executable signatures from source
   - guardrail/policy feedback
3. Integration tests for backend parity (in-memory + SurrealMem minimum).

Exit criteria:

- Dashboard reflects runtime-backed Grapheme capabilities without mock-only behavior.

## Phase 4 - Optional LSP-Assisted Authoring

Outcome:

- Add optional editor-grade diagnostics/completion path using Grapheme LSP.

Deliverables:

1. Feature-gated LSP host abstraction in Stasis.
2. Session-scoped authoring diagnostics and completion endpoints for dashboard.
3. Resource and timeout safety limits for LSP process/session lifecycle.

Exit criteria:

- LSP-assisted mode is optional, stable, and isolated from core runtime flows.

## Tracking Table

| Phase | Priority | Status | Owner | Notes |
| --- | --- | --- | --- | --- |
| Phase 0: Contract and scope lock | P0 | Planned | Runtime + Dashboard | Freeze reflection contract and feature-gate boundaries |
| Phase 1: Reflection port and SDK adapter | P0 | Complete | Runtime | Added workflow reflection port + Grapheme SDK adapter + validation tests |
| Phase 2: Workflow definition persistence and preflight | P0 | Complete | Dashboard + Runtime | Workflow save/execute now use durable definition/revision stores and are covered by focused persistence + execute-metadata tests |
| Phase 3: Dashboard reflection surfaces | P1 | In Progress | Dashboard | Added reflection query contracts plus dashboard reflection stream route/UI wiring (`/stream/workflow-reflection`) with embedded route coverage |
| Phase 4: Optional LSP-assisted authoring | P1 | In Progress | Dashboard + Tooling | Added feature-gated diagnostics contract, authoring diagnostics preview, and parse-vs-reflection diagnostic classification with parity coverage |

## Immediate Next Actions

1. Add dashboard UX tests for filter-state retention across module drill-down interactions.
2. Add true grapheme-lsp diagnostics spans/codes (line/column) once upstream exposes richer diagnostic ranges than full-document parse errors.
3. Add dashboard UX tests for diagnostics/filter/module interactions under source override cycles.
