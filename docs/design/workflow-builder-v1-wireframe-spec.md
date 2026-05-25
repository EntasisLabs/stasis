# Workflow Builder v1 Wireframe Spec

## Document Metadata

- Document Type: Internal UX Spec
- Audience: Product, Design, Dashboard Engineering, Runtime Engineering
- Parent Docs:
  - [docs/design/workflow-builder-product-phase-plan.md](docs/design/workflow-builder-product-phase-plan.md)
  - [docs/design/workflow-builder-execution-checklist.md](docs/design/workflow-builder-execution-checklist.md)
- Last Updated: 2026-05-25

## Objective

Define the first production-ready no-code Workflow Builder experience where users can create, validate, simulate, and publish a workflow in under 3 minutes without opening source mode.

## Product Promise

The default builder is visual and guided.

Source, raw diagnostics, and module internals are optional advanced tools, not primary UI.

## Canonical Model

Definitions:
1. Workflow: A versioned AI skill composed as a Grapheme function chain.
2. Node: A single Grapheme function step shown with user-facing action language.
3. Edge: A pipe from one function step output to the next step input.
4. Build Artifact: Grapheme script assembled by guided graph authoring.
5. Job: Runtime trigger binding (HTTP/Kafka/Queue/etc) that executes a workflow revision and forwards output into model context.

Guided mode translation rule:
1. Show friendly action labels in UI.
2. Preserve module/function IDs as internal metadata.

## Information Architecture

Primary layout:
1. Top Bar (global workflow actions)
2. Left Rail (module catalog and templates)
3. Canvas (workflow graph)
4. Right Rail (node inspector)
5. Bottom Tray (simulation timeline and run playback)

Default visibility:
- Left Rail: open
- Right Rail: collapsed until node selected
- Bottom Tray: collapsed until simulate/run starts
- Advanced Mode: off

## Screen Specs

### Screen A: Empty Builder

Intent:
- Help users start instantly with templates or first module.

Layout behavior:
1. Top Bar: shows workflow name placeholder, Simulate disabled, Publish disabled, History button, Advanced Mode toggle.
2. Left Rail: template cards at top, module catalog below.
3. Canvas: centered empty-state card with two CTA buttons.
4. Right Rail: hidden.
5. Bottom Tray: hidden.

Content:
- Empty-state title: Start your workflow
- Empty-state subtitle: Pick a skill template or add your first function step.
- CTA 1: Use Template
- CTA 2: Add First Function Step

Rules:
- No raw diagnostics shown.
- No raw schema terms shown.

### Screen B: Template Selected (Pre-wired Graph)

Intent:
- Give users momentum with a runnable baseline.

Layout behavior:
1. Canvas displays 3-5 pre-wired nodes.
2. First incomplete node gets focus ring.
3. Right Rail opens with that node config form.

Example baseline chain:
1. Web Search
2. Extract HTML Elements
3. Transform to Markdown
4. Output Result

Content:
- Top Bar readiness pill: Needs setup
- Inline guidance chip on focused node: Complete required fields

Rules:
- Auto-layout graph on template load.
- Keep edge routes readable (no edge overlap where avoidable).
- Preserve edge flow as function output -> next function input.

### Screen C: Node Selected (Configuration)

Intent:
- Allow non-technical configuration through forms.

Right Rail sections:
1. Node Summary:
- Friendly node title
- One-sentence purpose
- Status chip (Ready, Needs Input, Invalid)

2. Inputs:
- Required fields first
- Optional fields collapsed
- Suggested values where available

3. Output Preview:
- Human-readable output shape sample
- Connection hints

4. Validation:
- Inline field-level messages
- Single Fix now actions

Rules:
- Never expose raw parser/reflection wording in default mode.
- Default values must produce valid baseline whenever possible.

### Screen D: Invalid Graph (Guided Fix)

Intent:
- Prevent confusion and provide immediate remediation.

Layout behavior:
1. Top Bar readiness pill turns blocking.
2. A floating guidance panel appears near affected nodes.
3. Right Rail opens to the highest-priority issue.

Message format:
1. What to fix
2. Why it matters
3. Fix now action

Example:
- What to fix: Connect Extract HTML Elements to Web Search output
- Why it matters: This step cannot run without input
- Fix now: Auto-connect from Web Search.output

Rules:
- No compiler/parse wording in guided mode.
- Every blocking issue must have a direct navigation or one-click fix.

### Screen E: Simulation Running

Intent:
- Build confidence before publish.

Layout behavior:
1. Bottom Tray expands automatically.
2. Canvas animates active execution path.
3. Node cards show transient run states (Queued, Running, Completed, Failed).

Bottom Tray tabs:
1. Timeline
2. Node Results
3. Side Effects

Timeline requirements:
- Chronological events with node labels and duration.
- Jump-to-node interaction on click.

Rules:
- Simulation does not modify live production state.
- Clear Run Again action shown.

### Screen F: Ready to Publish

Intent:
- Make publishing safe and decisive.

Layout behavior:
1. Top Bar readiness pill switches to Ready.
2. Publish button enabled and visually primary.
3. Summary modal shown on publish click.

Publish modal sections:
1. Workflow summary
2. Environment/queue target
3. Validation checklist
4. Confirm publish action

Rules:
- Publish is blocked if critical issues remain.
- Block reasons must link to exact node or field.

### Screen G: Advanced Mode

Intent:
- Preserve engineering power without polluting default UX.

Layout behavior changes:
1. Source panel becomes available.
2. Raw diagnostics panel becomes available.
3. Module internals and schema detail become available.

Rules:
- Advanced Mode is opt-in per session.
- Returning to guided mode hides advanced panels and keeps graph state.

## Component Inventory

Core components:
1. ModuleTile
- Fields: name, category, purpose, trust badge, complexity badge
- Actions: drag to canvas, quick add

2. WorkflowNodeCard
- Fields: title, status, port indicators, quick actions
- States: default, selected, invalid, running, complete

3. PortChip
- Fields: direction, label, type family
- States: connected, available, incompatible

4. EdgePath
- States: default, highlighted, invalid

5. ReadinessPill
- Values: Draft, Needs setup, Blocking issues, Ready

6. GuidanceCallout
- Fields: issue title, reason, fix action

7. SimulationTimelineRow
- Fields: timestamp, node, event, duration, result

8. PublishChecklistItem
- Fields: label, pass/fail/warn, jump action

## Interaction Contracts

### Graph interactions
1. Drag module to canvas creates node at drop point.
2. Click node selects and opens inspector.
3. Drag from output port to input port creates connection.
4. Invalid connection snaps back and shows reason.
5. Delete action confirms only when node has downstream dependencies.

### Validation interactions
1. Field validation occurs on blur and on simulate/publish.
2. Graph validation runs on connection changes.
3. Validation priority order:
- blocking
- warning
- info

### Simulation interactions
1. Simulate is enabled only when graph has at least one input/source function and one output function.
2. During simulation, editing is allowed but prompts rerun for stale results.
3. Clicking timeline item highlights corresponding node and edge path.

## Copy System (Guided Mode)

Preferred language:
- Step, Function, Input, Output, Readiness, Fix, Simulate, Publish

Avoid in guided mode:
- parser, lsp, reflection, executable, contract mismatch, schema reference

Tone:
- action-oriented
- plain-language
- specific and concise

## Visual System Notes

Hierarchy rules:
1. Canvas is visual center and largest surface.
2. Right Rail should never overpower canvas width.
3. Bottom Tray should preserve graph context when open.

Color semantics:
1. Neutral: baseline graph
2. Blue: selected/focused
3. Amber: warning
4. Red: blocking
5. Green: success/ready

Motion:
1. Node drop snap (short)
2. Edge draw animation (short)
3. Simulation pulse along active path (medium)

## Accessibility and Responsiveness

Accessibility:
1. Keyboard support for node selection and navigation.
2. Focus-visible styles on interactive graph elements.
3. Readiness and errors not color-only (icon + text).

Responsive behavior:
1. Desktop: full 3-panel layout + bottom tray.
2. Tablet: right rail overlays canvas.
3. Mobile: step-by-step stacked mode (catalog -> canvas -> inspector).

## Telemetry Events (v1)

1. builder_opened
2. template_selected
3. module_added
4. connection_created
5. first_valid_graph
6. simulation_started
7. simulation_completed
8. publish_clicked
9. publish_succeeded
10. advanced_mode_opened

## Acceptance Criteria

1. New user can assemble and simulate a basic workflow without opening Advanced Mode.
2. Blocking issues are understandable and fixable from guided UI.
3. Visual graph and runtime behavior stay in parity for supported node set.
4. Publish flow is explicit, safe, and recoverable.

## Handoff Notes for Engineering

Immediate integration target areas:
- [src/dashboard/handlers.rs](src/dashboard/handlers.rs#L669)
- [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html)
- [templates/dashboard/streams/workflow_reflection.html](templates/dashboard/streams/workflow_reflection.html)
- [templates/dashboard/index.html](templates/dashboard/index.html)
- [src/dashboard/service.rs](src/dashboard/service.rs#L632)

Recommended implementation order:
1. Guided vocabulary + layout shell
2. Interactive graph DTO and rendering
3. Inspector forms and validation layer
4. Simulation tray and playback
5. Advanced Mode segregation
