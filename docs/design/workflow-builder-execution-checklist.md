# Workflow Builder Execution Checklist

## Document Metadata

- Document Type: Internal Execution Checklist
- Parent Plan: [docs/design/workflow-builder-product-phase-plan.md](docs/design/workflow-builder-product-phase-plan.md)
- Audience: Product, Design, Dashboard Engineering, Runtime Engineering
- Last Updated: 2026-05-25

## Purpose

Translate the product phase plan into sprint-ready implementation tasks mapped to current code touchpoints.

## Mid-Slice Recovery Delta (2026-05-25)

Completed in this slice:
- [x] Schema-guided parameter controls in Node Inspector seeded by Grapheme input schema refs and function metadata.
- [x] Pre-save validation guardrails for malformed `function_inputs` payloads (client and server).
- [x] Execute provenance now includes the saved `function_inputs` snapshot from the immutable workflow revision.
- [x] Added `Run Draft` action in Workflow Builder to execute in-editor Grapheme flow immediately (without persisting a workflow revision).
- [x] Save and Run Draft payloads now include canonical `graph_state` from the linker model.
- [x] Save/Run Draft now return HTTP 400 responses for graph-state contract failures with user-actionable rejection details.
- [x] Revision compiler metadata `compile_mode` now aligns to actual selected compile path (`graph_compiled`, `legacy_function_steps`, `source_passthrough`).
- [x] Guided Loop Block controls (`max`, `each`, `merge`) are now present in Workflow Builder and are serialized into canonical `graph_state` as `guided_loop`.
- [x] Added save-route integration coverage proving source-only saves mark revision compile mode as `source_passthrough`.

Follow-up already in motion:
- [ ] Replace schema-ref heuristics with full field-level schema expansion once Grapheme reflection exposes concrete input field definitions.

## Contract Lock: Agentic Workflow Semantics

Canonical definitions:
1. Node = Grapheme function step.
2. Edge = piped function output -> next function input.
3. Workflow = AI skill graph compiled into Grapheme script.
4. Job = trigger binding that runs a workflow revision and forwards output into model context.

Loop semantics lock:
1. Guided loops compile to Grapheme `iterator ... @loop(...)` blocks.
2. No guided `while` loop authoring path is supported.
3. Loop safety is explicit and bounded (`max` required; iterable source required for `each`).

Definition of done for contract lock:
- Guided UI labels use action language while preserving Grapheme function identity in metadata.
- Save path compiles graph to Grapheme source artifact deterministically.
- Execute path uses immutable workflow revision IDs generated from graph-backed saves.

## First Slice Plan (Implementation Kickoff)

Objective:
- Deliver one end-to-end vertical slice proving graph->Grapheme compilation and trigger-bound execution provenance.

Scope (v0):
1. One canonical skill template chain:
  - websearch -> extract html elements -> transform to markdown -> output
2. One trigger binding type:
  - queue trigger
3. One execution path:
  - workflow revision selected by ID

Implementation tasks:
- [x] Add workflow graph serialization field in revision domain model and stores.
- [x] Add deterministic graph -> Grapheme compiler for canonical chain.
- [x] Update save action to persist graph + compiled source + reflection receipt.
- [ ] Add queue trigger binding payload referencing workflow revision ID.
- [ ] Update execute path to resolve workflow by revision ID from trigger-bound job metadata.

Validation tasks:
- [ ] Service test: same graph input always compiles to same Grapheme source text.
- [ ] Service test: save persists graph and compiled source in same revision.
- [ ] Integration test: queue-triggered job records workflow revision provenance.
- [ ] Integration test: execution output contains workflow revision ID and trigger source.

Definition of done:
- The canonical chain can be created visually, saved, and executed from a queue trigger with full workflow revision traceability.

## Current Baseline Touchpoints

Primary workflow route and composition:
- [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L77)
- [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L229)
- [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L669)

Current workflow builder view and authoring surface:
- [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L5)
- [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L31)
- [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L142)

Reflection/diagnostics stream panel:
- [templates/dashboard/streams/workflow_reflection.html](templates/dashboard/streams/workflow_reflection.html)

Client behavior for source/query propagation:
- [templates/dashboard/index.html](templates/dashboard/index.html#L383)
- [templates/dashboard/index.html](templates/dashboard/index.html#L419)
- [templates/dashboard/index.html](templates/dashboard/index.html#L499)

Runtime/service contracts already in place:
- [src/dashboard/service.rs](src/dashboard/service.rs#L209)
- [src/dashboard/service.rs](src/dashboard/service.rs#L217)
- [src/ports/outbound/runtime/workflow_reflection.rs](src/ports/outbound/runtime/workflow_reflection.rs#L75)
- [src/ports/outbound/runtime/workflow_definition_store.rs](src/ports/outbound/runtime/workflow_definition_store.rs#L7)
- [src/domain/runtime/workflow_definition.rs](src/domain/runtime/workflow_definition.rs#L4)

Reflection adapter baseline:
- [src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs](src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs#L48)
- [src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs](src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs#L78)
- [src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs](src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs#L106)
- [src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs](src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs#L127)

Existing integration guardrails:
- [src/dashboard/integration.rs](src/dashboard/integration.rs#L277)
- [src/dashboard/integration.rs](src/dashboard/integration.rs#L410)

## Phase 0 Checklist: Product Contract + Language Lock

### P0.1 Rename UI vocabulary to product language
- [ ] Replace engineering-first labels in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L5) and [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L31):
  - Grapheme Workflow Builder -> Workflow Builder
  - Node Types -> Modules
  - Authoring Studio -> Builder Workspace
- [ ] Replace terms in [templates/dashboard/streams/workflow_reflection.html](templates/dashboard/streams/workflow_reflection.html) to user-facing language:
  - Runtime Reflection -> Flow Insights
  - Authoring Diagnostics -> Readiness Guidance
- [ ] Ensure action messages in [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L1072) and [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L1121) use user outcomes, not implementation terms.

Definition of done:
- No default workflow UI text uses raw engineering vocabulary except in advanced mode.

### P0.2 Introduce explicit advanced mode boundary
- [ ] Add query/state flag for advanced mode in [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L958).
- [ ] Gate source editor and raw diagnostics rendering in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L142) and [templates/dashboard/streams/workflow_reflection.html](templates/dashboard/streams/workflow_reflection.html).
- [ ] Keep Save/Execute path intact for non-advanced users.

Definition of done:
- Default mode can complete core actions without exposing source or raw diagnostics.

## Phase 1 Checklist: Canvas Foundation

### P1.1 Replace static stage mock with interactive graph model
- [ ] Introduce a workflow graph DTO in handler/template view model near [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L669).
- [ ] Render graph nodes/edges in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html#L91) replacing hardcoded stage cards.
- [ ] Support node selection state and inspector-target wiring in template and client script.

Definition of done:
- User can add/select/remove nodes visually in default mode.

### P1.2 Add starter templates and one-click populate
- [ ] Add starter skill template payloads (function chains) in handler composition logic (workflows branch) at [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L669).
- [ ] Add template picker surface in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html).
- [ ] Persist selected template intent in query or post body.
- [ ] Add at least one canonical chain template: websearch -> extract html elements -> transform to markdown -> output.

Definition of done:
- User can start from a curated skill template with pre-wired function steps.

### P1.3 Dual light/dark theming parity
- [ ] Introduce workflow-builder light/dark token sets in [dashboard_assets/static/dashboard.css](dashboard_assets/static/dashboard.css).
- [ ] Add theme switch and persisted preference behavior in [templates/dashboard/index.html](templates/dashboard/index.html).
- [ ] Ensure canvas, node cards, edges, inspector, and timeline preserve semantic states in both themes.

Definition of done:
- Guided mode is fully usable and visually coherent in both light and dark themes.

## Phase 2 Checklist: Functional Node System

### P2.1 Add typed port contracts for visual nodes
- [ ] Define visual node/port contract types in domain/ports adjacent to [src/ports/outbound/runtime/workflow_reflection.rs](src/ports/outbound/runtime/workflow_reflection.rs#L75).
- [ ] Map Grapheme function/module info/types to node-port metadata using [src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs](src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs#L106) and [src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs](src/infrastructure/runtime/grapheme_sdk_workflow_reflection.rs#L127).
- [ ] Surface compatibility hints in node inspector panel.

Definition of done:
- Connections validate against typed inputs/outputs in UI before publish.

### P2.2 Graph-to-runtime mapping (no-code parity)
- [x] Add graph serialization field(s) to workflow revision model in [src/domain/runtime/workflow_definition.rs](src/domain/runtime/workflow_definition.rs#L13).
- [x] Extend save path in [src/dashboard/service.rs](src/dashboard/service.rs#L632) to compile graph into Grapheme source artifact.
- [ ] Ensure execute path in [src/dashboard/service.rs](src/dashboard/service.rs#L650) consumes latest graph-backed revision.
- [x] Add deterministic graph -> Grapheme script compiler tests for canonical skill templates.
- [x] Add deterministic compiler support for iterator loop blocks that emit `iterator ... @loop(max: ..., each: ..., merge: ...)`.
- [x] Add save-time validation that guided loop blocks always include bounded `max`.

Definition of done:
- Visual-only workflows compile and run with parity to source-authored Grapheme flows.
- Guided loop workflows compile to bounded Grapheme iterator blocks with no generic loop semantics.

### P2.4 Controlled Loop Blocks (Grapheme-native)
- [x] Add guided Loop Block type backed by Grapheme iterator semantics in workflow DTO mapping near [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L669).
- [x] Add Loop Block form fields in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html) for:
  - `max`
  - `each`
  - `merge`
- [x] Keep raw loop expression editing in advanced mode only.
- [ ] Add publish/readiness checks for missing loop bounds and missing iterable path.

Definition of done:
- Users can configure bounded iteration in guided mode without seeing raw source or generic loop controls.

### P2.3 Trigger-binding Job model
- [ ] Add workflow-trigger binding contract for HTTP/Kafka/Queue in runtime/domain boundary.
- [ ] Add dashboard form flow to create a Job binding referencing workflow revision ID.
- [ ] Ensure job execution output path is explicit in UI and mapped to model context handoff.

Definition of done:
- User can bind a workflow revision to trigger(s) and execute through Job lifecycle with traceable workflow revision provenance.

## Phase 3 Checklist: Simulation + Confidence

### P3.1 Add simulation endpoint and playback model
- [ ] Add route for simulation stream in [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L77) and compose data in a dedicated preview builder.
- [ ] Add simulation timeline panel in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html).
- [ ] Add per-node run result cards and execution path markers.

Definition of done:
- User can run simulation and inspect node-by-node outcomes without raw logs.

### P3.2 Replace raw diagnostics framing in default mode
- [ ] Keep current diagnostics engine in [src/dashboard/service.rs](src/dashboard/service.rs#L771).
- [ ] Add translation layer from diagnostic codes/severity to human remediation text in handlers/template DTO mapping.
- [ ] Show friendly fix actions in default mode; keep raw detail for advanced mode.

Definition of done:
- Default mode issues are expressed as guided fixes, not compiler-style diagnostics.

### P3.3 Expressive productized visual polish
- [ ] Replace generic card-stack composition with distinctive builder surfaces in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html).
- [ ] Apply module-category visual identities (icon badges, accent chips) while preserving readability.
- [ ] Add visual hierarchy pass for top bar, rails, canvas, and tray so canvas remains dominant.

Definition of done:
- Builder feels productized and expressive without harming task clarity.

### P3.4 Calm motion profile and accessibility
- [ ] Introduce calm motion tokens (fast/standard/emphasis) in [dashboard_assets/static/dashboard.css](dashboard_assets/static/dashboard.css).
- [ ] Implement subtle node/edge/simulation animations with reduced amplitude in [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html).
- [ ] Add reduced-motion fallback behavior in [templates/dashboard/index.html](templates/dashboard/index.html).

Definition of done:
- Motion communicates state transitions clearly and remains subtle, calm, and accessible.

## Phase 4 Checklist: Advanced Mode + LSP

### P4.1 Preserve engineering surfaces behind explicit toggle
- [ ] Keep source editor and raw diagnostics under advanced mode flag.
- [ ] Ensure advanced mode uses existing propagation hooks in [templates/dashboard/index.html](templates/dashboard/index.html#L383), [templates/dashboard/index.html](templates/dashboard/index.html#L419), and [templates/dashboard/index.html](templates/dashboard/index.html#L499).

Definition of done:
- Advanced users retain current power without changing default beginner UX.

### P4.2 LSP deepening once upstream spans improve
- [ ] Keep existing parse/reflection fallback in [src/dashboard/service.rs](src/dashboard/service.rs#L771).
- [ ] Add upstream LSP span/code mapping adapter when available.
- [ ] Add parity tests for LSP-vs-reflection error classes.

Definition of done:
- Advanced diagnostics includes precise span/code mapping when upstream supports it.

## Test and Validation Backlog

### Integration tests
- [ ] Add guided-mode render test (no source panel visible by default) in [src/dashboard/integration.rs](src/dashboard/integration.rs).
- [ ] Add canvas action round-trip tests (template select, node add, configure, simulate).
- [ ] Keep and extend state retention checks from [src/dashboard/integration.rs](src/dashboard/integration.rs#L410).
- [ ] Add light/dark theme render parity checks for key builder states.
- [ ] Add integration test for canonical skill template compile path: graph input -> Grapheme source -> saved revision.
- [ ] Add guided loop compile test: loop block config -> iterator `@loop` source artifact.

### Service tests
- [ ] Add graph serialization/deserialization parity tests near [src/dashboard/service.rs](src/dashboard/service.rs#L1039).
- [ ] Add save/execute parity tests for graph-backed revisions across InMemory and SurrealMem.
- [ ] Add service tests for trigger-binding job creation with workflow revision reference integrity.
- [ ] Add service tests for bounded loop validation failures (missing `max`, invalid `each` path).

### Handler tests
- [ ] Add DTO contract tests for mode toggles, readiness messages, and simulation summaries in [src/dashboard/handlers.rs](src/dashboard/handlers.rs).

## Instrumentation Work Items

- [ ] Add client events in [templates/dashboard/index.html](templates/dashboard/index.html) for:
  - builder_opened
  - template_selected
  - first_node_added
  - first_valid_graph
  - simulation_started
  - first_successful_run
  - advanced_mode_opened
- [ ] Add server timing/summary logs in [src/dashboard/handlers.rs](src/dashboard/handlers.rs) for first-run funnel measurements.

## Sprint Sequencing (Recommended)

Sprint A:
- P0.1, P0.2, guided/default mode test coverage.

Sprint B:
- P1.1, P1.2, initial interactive graph + starter templates.

Sprint C:
- P2.1, P2.2, graph-to-runtime parity and revision persistence updates.

Sprint D:
- P2.3, trigger binding and job contract wiring.

Sprint E:
- P3.1, P3.2, simulation timeline + readiness guidance.

Sprint F:
- P4.1, P4.2, advanced mode hardening and future LSP upgrade seam.

## Release Gate Checklist

- [ ] 3-minute first-run funnel instrumentation active.
- [ ] Guided mode hides source and raw diagnostics by default.
- [ ] Visual workflow save and execute parity validated in both backends.
- [ ] Graph-to-Grapheme compilation parity validated for canonical skill templates.
- [ ] Trigger-to-job binding validated with workflow revision provenance.
- [ ] Friendly readiness guidance present for all blocking states.
- [ ] Advanced mode is explicit and isolated.
- [ ] Light/dark theme parity validated for default and simulation states.
- [ ] Expressive visual treatment passes readability and contrast thresholds.
- [ ] Motion profile passes calmness review and reduced-motion accessibility checks.
