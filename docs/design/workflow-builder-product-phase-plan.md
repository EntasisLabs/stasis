# Workflow Builder Product Phase Plan

## Document Metadata

- Document Type: Internal Product + Delivery Plan
- Audience: Product, Design, Runtime, Dashboard Engineering
- Stability: Evolving
- Last Verified: 2026-05-25
- Intent: Convert workflow authoring from engineering-first surfaces into a true no-code, canvas-first product.

## Why This Plan Exists

The current workflow area has strong engineering depth but weak product identity.

Core correction:
- Workflow Builder is not a source editor with extra panels.
- Workflow Builder is not a generic DAG orchestrator.
- Workflow Builder is an agentic skill builder where users compose Grapheme function chains that compile to runnable Grapheme scripts.

## Product Definition

Workflow Builder:
A canvas-first, no-code workflow creation and execution experience where non-technical users can build, test, and publish a useful workflow in under 3 minutes.

## Canonical Runtime Contract

System model:
1. Node = Grapheme function step (for example: `websearch`, `extract_html`, `markdown_transform`, `output`).
2. Edge = function output piped into the next function input.
3. Workflow = versioned AI skill graph compiled into a Grapheme source script.
4. Job = trigger binding that runs a specific workflow revision and routes workflow output to model context.

Trigger examples:
1. HTTP webhook -> workflow revision -> LLM input context.
2. Kafka event -> workflow revision -> LLM input context.
3. Queue message -> workflow revision -> LLM input context.

Design implication:
1. Canvas graph is the source of truth for guided mode.
2. Grapheme source is a compiled artifact (and advanced override), not the default authoring primitive.

Control-flow contract (Grapheme-native):
1. Guided loop behavior maps to Grapheme `iterator ... @loop(...)` blocks.
2. No generic `while` node exists in guided mode.
3. Loop execution must stay bounded and explicit via `@loop` controls such as `max`, `each`, and merge strategy.
4. Any advanced override must still compile to valid Grapheme iterator semantics.

## North Star Outcomes

1. First successful run in under 3 minutes (P50).
2. At least 80% of new workflows created without opening source view.
3. At least 90% of configuration actions performed in form-first node panels (not raw text editing).
4. Publish confidence score visible before release, with actionable guidance.

## Design Principles

1. Canvas first, code second.
2. One primary action per region.
3. Human language over runtime vocabulary.
4. Progressive disclosure for advanced controls.
5. Validation as guidance, not compiler output.
6. Runtime fidelity without exposing implementation complexity by default.
7. Function-first semantics: every guided node maps to a runnable Grapheme function.

## Product Scope

In scope:
1. Visual node catalog and drag/drop canvas.
2. Node configuration forms with defaults and inline guidance.
3. Workflow simulation and run-path visualizer.
4. Publish readiness and remediation guidance.
5. Optional advanced source and diagnostics mode.

Out of scope for first release:
1. Full free-form custom scripting as primary authoring path.
2. Mandatory LSP/developer tooling in default flow.
3. Multi-user live collaboration.

## Experience Architecture

Primary surfaces:
1. Left: Curated function-step catalog and skill templates.
2. Center: Canvas graph with auto-layout and connection guidance.
3. Right: Node inspector with form-first configuration.
4. Footer/overlay: Simulation timeline + run playback.

Secondary surfaces (advanced):
1. Source view toggle.
2. Raw diagnostics panel.
3. Module internals and schema details.

## Phase Plan

### Phase 0: Product Contract and Language Lock (1 sprint)

Outcome:
- Shared definition of Workflow Builder identity and interaction model.

Deliverables:
1. Canonical UX glossary replacing engineering-first labels.
2. Node taxonomy and category model for catalog v1.
3. Golden-path storyboard for "first run in 3 minutes".
4. Acceptance rubric for product polish (clarity, consistency, confidence).

Exit criteria:
1. Product + Engineering sign-off on IA and vocabulary.
2. Every visible control mapped to a specific user decision.

### Phase 1: Canvas Foundation (2 sprints)

Outcome:
- Functional visual authoring baseline.

Deliverables:
1. Interactive canvas (add, connect, move, delete nodes).
2. Starter skill templates with pre-wired function chains.
3. Smart connection hints and validity checks.
4. Node states (empty, configured, invalid, ready).

Exit criteria:
1. User can assemble a runnable graph without source editing.
2. Invalid graph states are obvious and recoverable.

### Phase 2: Functional Node System (2 sprints)

Outcome:
- Pipelines and node types are truly functional in visual mode.

Deliverables:
1. Typed input/output contracts for node ports.
2. Form-first node configuration and defaults.
3. Runtime mapping from visual graph to executable Grapheme script artifact.
4. Capability-safe execution constraints reflected in UI affordances.
5. Controlled loop blocks backed by iterator semantics (bounded, explicit, non-generic).

Loop scope for this phase:
1. Support `for-each` style iteration using Grapheme iterator `@loop(each: ..., max: ..., merge: ...)`.
2. Validate loop bounds and iterable source path before publish.
3. Keep free-form loop constructs out of guided mode.

### Phase 2.5: Job Trigger Binding (1 sprint)

Outcome:
- Workflows become runtime-usable skills through explicit trigger bindings.

Deliverables:
1. Trigger binding model for HTTP, queue, and Kafka sources.
2. Job creation contract referencing workflow revision ID.
3. Output routing contract from workflow result to LLM input context.

Exit criteria:
1. User can bind a workflow revision to at least one trigger type from dashboard.
2. Triggered job execution references immutable workflow revision IDs.

Exit criteria:
1. Visual workflows execute with parity to source-authored workflows.
2. Non-functional node types are removed from active catalog.

### Phase 3: Simulation and Confidence Layer (1-2 sprints)

Outcome:
- Users can understand behavior before publish.

Deliverables:
1. Run simulation with animated path playback.
2. Node-by-node result cards and side-effect preview.
3. Readiness score with guided fixes.
4. Friendly issue language (no raw compiler framing in default mode).
5. Iteration-aware playback for iterator loops (per-iteration step traces + stop reason).

Exit criteria:
1. User can identify and fix blockers without opening advanced mode.
2. Publish action is blocked only with clear, actionable reasons.

### Phase 4: Advanced Authoring and Developer Mode (1 sprint)

Outcome:
- Preserve engineering power without polluting primary UX.

Deliverables:
1. Source/diagnostics toggle as an advanced workspace mode.
2. Round-trip mapping between canvas graph and source where supported.
3. LSP diagnostics integrated behind advanced mode boundary.

Exit criteria:
1. Advanced mode never interrupts no-code flow.
2. Core user outcomes remain measurable without advanced mode usage.

## Delivery Guardrails

1. If a control does not improve first-run success, hide or remove it.
2. If a feature is visible, it must be functional.
3. If a state is invalid, remediation must be one click or one clear instruction away.
4. No internal architecture terms in default UI labels.
5. No generic loop authoring controls in guided mode; loop UX must map directly to Grapheme iterator constraints.

## Metrics and Instrumentation

Track per session:
1. Time to first valid workflow run.
2. Number of panel/context switches before first run.
3. Advanced mode open rate.
4. Publish failure reasons by category.
5. Node-level drop-off points.

Quality gates before broad rollout:
1. P50 first run <= 3:00.
2. P90 first run <= 6:00.
3. At least 70% of first-time users complete run without source mode.

## Risks and Mitigations

1. Risk: Engineering surfaces re-bleed into default UX.
   - Mitigation: Product review gate on every new control.

2. Risk: Visual/runtime drift.
   - Mitigation: Contract tests from graph model -> Grapheme script -> runtime execution receipts.

3. Risk: Node catalog bloat.
   - Mitigation: Tiered catalog curation (core, extended, experimental).

## Immediate Next Actions

1. Write the Workflow Builder glossary and rename current UI labels to product language.
2. Draft a single-screen canvas wireframe with explicit primary/secondary actions.
3. Define v1 node catalog (core only) and remove or mark non-functional nodes as unavailable.
4. Add graph-to-source compile contract tests and trigger-binding contract tests.
5. Add event instrumentation for first-run timing and panel-switch metrics.

## Ownership

- Product: Workflow UX narrative, glossary, success criteria.
- Design: Canvas IA, visual hierarchy, interaction polish.
- Dashboard Engineering: UI implementation and instrumentation.
- Runtime Engineering: Graph-to-runtime mapping and execution parity contracts.
