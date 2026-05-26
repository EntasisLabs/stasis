# Grapheme-Centric Canvas Spec (Loop Containers)

## Intent
Design the workflow canvas as a Grapheme authoring surface, not a generic DAG editor.

The canvas must make Grapheme iterator semantics obvious:
- what repeats
- where repeat scope starts and ends
- how results merge

## Non-Negotiables
- Grapheme primitives only in guided mode.
- Loop containers compile directly to Grapheme iterator semantics.
- Connections represent Grapheme data flow bindings, not generic graph edges.
- All visual state remains source-faithful to Grapheme output.

## Canvas Object Model
- Step Node
- Repeat Container (Grapheme iterator)
- Root Flow Lane

A Repeat Container has:
- Header: label, `each`, `max`, `merge`, scope summary.
- Body: ordered step nodes that execute per item.
- Entry/Exit semantics: body is bounded by start and end step in guided mode.

## Compile Mapping
Guided loop container maps to graph state and compile path as:
- `guided_loop.each` -> iterator `each`
- `guided_loop.max` -> iterator `max`
- `guided_loop.merge` -> iterator `merge`
- `guided_loop.start_node_id`, `guided_loop.end_node_id` -> iterator body range

No additional generic execution semantics are introduced.

## Interaction Model
- User enables Repeat and chooses list source.
- User chooses first and last repeated step.
- Canvas renders a visible Repeat Container around included steps.
- Header preview explains behavior in plain language.
- Selecting a step opens a contextual Step Card near that step in guided mode.
- Hover does not open config; selection is the only primary trigger.
- Contextual Step Card exposes only high-signal inputs first, with deep editing behind a guided drawer or advanced-mode inspector.
- Repeat Container body acts as a mini-canvas zone where users can drop additional steps.
- Multiple containers are a future phase; first slice supports one guided container.

## Validation Model
Readiness strip should block run when:
- repeat enabled but `each` missing/invalid
- repeat start/end missing
- repeat range invalid (start after end)

Validation copy stays user-readable while rooted in Grapheme constraints.

## Visual Direction
- SSIS-style explicit containment, Temporal-style modern polish.
- Low-noise border and tint for container body.
- Clear header with summary chips (`each`, `max`, `merge`).
- Step chips still indicate loop role (start, in loop, end).
- Contextual Step Card appears adjacent to selected node with subtle elevation and minimal controls.
- Full inspector remains available only in advanced mode.

## First Good Slice (Implemented in this sprint)
Scope:
- Single Repeat Container shell rendered in canvas from existing guided loop config.
- Steps in loop range are grouped visually into container body.
- Loop story and readiness remain in place.
- Existing save/run and compile behavior unchanged.
- Action Library includes explicit `Add Repeat Container` affordance.
- Repeat Container supports collapse/expand.
- Repeat Container header/body display loop-scoped readiness with jump-to-fix links.

Out of scope:
- Nested containers
- Multiple independent containers
- Drag-to-create container by marquee selection

## Acceptance Criteria
- Enabling Repeat creates a visible container around target steps.
- Changing start/end updates container bounds immediately.
- Disabling Repeat removes container and restores flat step list.
- Save/Test Run behavior remains backward compatible.
- Grapheme compile mode/output remains unchanged from current semantics.

## Hero-Path Polish Rules (Steve Pass)
- Guided mode centers one path: Add step -> Add Repeat Container -> choose list -> choose range -> Test Run.
- Guided mode hides non-hero controls (theme and execute actions, low-level data-flow internals).
- Readiness language is directive (`Do: ...`) and always one-click fixable.
- Repeat vocabulary is consistent across the guided surface.
- Selection-first editing: click a step, edit near the step, continue building.

## Contextual Step Card Slice (Implemented)
- Guided mode now uses contextual Step Settings presentation and positions it near the selected step.
- Advanced mode keeps full inspector behavior.
- Existing binding/state wiring is preserved while the presentation shifts.
- Loop container now shows explicit drop-target affordance during drag operations.
- Dropping a step into the loop mini-canvas now inserts that step into loop scope and extends loop end bound.
- Loop mini-canvas drop placement is position-aware: top-half drop inserts before a step, bottom-half drop inserts after.
- Guided step card now uses an ultra-compact footprint (narrow width, reduced copy, and denser input rows).
- Quick Inputs stay scrollable inside the card so canvas space stays open while inputs remain editable.
- Parameter rows stay minimal by default and expand into full controls when focused.
- State path controls stay hidden unless a parameter is in `From State` mode.
- Clicking empty canvas space now clears node selection and collapses contextual step settings.
- A single `More` control opens a right-side drawer for deeper details (input/output hints, state explorer, raw payload).
- The guided contextual card no longer expands inline, keeping node-adjacent editing consistently compact.
