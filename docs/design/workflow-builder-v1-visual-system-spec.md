# Workflow Builder v1 Visual System Spec

## Document Metadata

- Document Type: Internal Visual Design Spec
- Audience: Product, Design, Frontend Engineering
- Parent Docs:
  - [docs/design/workflow-builder-v1-wireframe-spec.md](docs/design/workflow-builder-v1-wireframe-spec.md)
  - [docs/design/workflow-builder-product-phase-plan.md](docs/design/workflow-builder-product-phase-plan.md)
- Last Updated: 2026-05-24

## Objective

Define a premium, canvas-first visual language for Workflow Builder v1 that feels intentional, clear, and operationally trustworthy.

## Direction Lock (Approved)

1. Theme strategy: dual light/dark parity is required for v1.
2. Visual tone: expressive and productized at launch.
3. Motion tone: subtle and calm.

## Visual Direction

Design theme:
- Precision Studio

Design intent:
1. The canvas feels like a professional control surface, not a generic dashboard card stack.
2. Node relationships and flow state are visually primary.
3. Readiness and risk are communicated clearly without visual noise.

## Brand Personality

1. Calm confidence
2. High craft
3. Low ambiguity
4. Fast comprehension

## Typography System

Primary typefaces:
1. Headline/UI: Space Grotesk
2. Body/UI: Manrope
3. Mono/Data: IBM Plex Mono

Fallback stacks:
1. UI: Space Grotesk, Manrope, Segoe UI, Arial, sans-serif
2. Mono: IBM Plex Mono, Cascadia Code, Menlo, monospace

Scale:
1. Display: 36/44
2. H1: 28/36
3. H2: 22/30
4. H3: 18/26
5. Body L: 16/24
6. Body M: 14/22
7. Meta: 12/18
8. Micro: 11/16

Usage rules:
1. Canvas node titles use H3 weight 600.
2. Inspector labels use Meta weight 600 with letter spacing.
3. Runtime metrics and IDs use mono only where differentiation is needed.

## Color System

Palette model:
- Neutral-first with signal accents.

Core neutrals:
1. Surface 0: #f7f8fa
2. Surface 1: #ffffff
3. Surface 2: #eef1f5
4. Border soft: #dbe2ea
5. Border strong: #b8c4d3
6. Text primary: #0f1726
7. Text secondary: #475569

Signal colors:
1. Focus/selected: #0b6bcb
2. Success: #0f9f6e
3. Warning: #d98a00
4. Blocking: #d23b3b
5. Running pulse: #5a7bff

Semantic mapping:
1. Draft: neutral
2. Needs setup: warning
3. Blocking issues: blocking
4. Ready: success
5. Simulating: running pulse

Dual-theme parity requirements:
1. Every semantic token has light and dark equivalents.
2. Readiness state colors must preserve contrast in both themes.
3. Node/edge state meaning must not change between themes.

Dark theme core neutrals:
1. Surface 0: #0d121a
2. Surface 1: #131a24
3. Surface 2: #1a2330
4. Border soft: #243244
5. Border strong: #34506e
6. Text primary: #e7eef8
7. Text secondary: #9fb2c9

Dark theme signals:
1. Focus/selected: #5ab0ff
2. Success: #3dd39c
3. Warning: #f7b955
4. Blocking: #ff7373
5. Running pulse: #89a2ff

## Spacing and Layout Rhythm

Base spacing unit:
- 4px

Scale:
1. 4, 8, 12, 16, 24, 32, 40

Layout rules:
1. Minimum page gutter: 24px desktop, 16px tablet/mobile.
2. Canvas always occupies at least 58% of horizontal space on desktop.
3. Left rail width: 280px fixed.
4. Right rail width: 360px fixed.
5. Bottom tray max height: 38vh.

## Shape, Depth, and Borders

Corner radii:
1. Small controls: 8px
2. Cards and node shells: 12px
3. Overlays and modals: 16px

Shadows:
1. Elevation 1: 0 1px 2px rgba(15, 23, 38, 0.08)
2. Elevation 2: 0 6px 18px rgba(15, 23, 38, 0.12)
3. Elevation 3: 0 14px 30px rgba(15, 23, 38, 0.18)

Border policy:
1. Use 1px soft borders by default.
2. Use 2px border only for selected node emphasis.
3. Never stack high-contrast border and heavy shadow simultaneously.

Expressive productized styling rules:
1. Use layered surfaces in key zones (canvas, inspector, timeline) to create depth hierarchy.
2. Use category-tinted icon badges for module identity.
3. Use subtle chroma accents at interaction points (selection, active edge, readiness chips).
4. Prefer distinctive composition over generic card-grid sameness.

## Canvas Styling

Canvas background:
1. Subtle vertical gradient from Surface 0 to Surface 2.
2. Dot-grid overlay at low opacity (4-6%) for spatial orientation.

Node card composition:
1. Header row: icon badge, node title, status chip.
2. Middle row: one-line purpose text.
3. Footer row: input/output port chips and quick action.

Node states:
1. Default: Surface 1, soft border.
2. Hover: slight lift and stronger border.
3. Selected: 2px focus border with glow ring.
4. Invalid: blocking border + inline callout icon.
5. Running: animated edge pulse + active marker.
6. Complete: success tick indicator.

Edge styling:
1. Default edge: 2px muted stroke.
2. Active edge: 3px running color with directional pulse.
3. Invalid edge: dashed blocking color.
4. Compatible drop target preview: focus color highlight.

## Inspector Styling

Panel sections:
1. Node summary
2. Required inputs
3. Optional settings
4. Output preview
5. Guidance

Input controls:
1. Default control height: 40px.
2. Required fields include subtle required dot.
3. Inline helper text appears below controls in Meta size.

Validation presentation:
1. Field-level message under field.
2. Panel-level summary at top with fix action buttons.
3. Avoid terminal-style or compiler-style copy in guided mode.

## Simulation and Timeline Styling

Bottom tray design:
1. Elevated panel with clear separation from canvas.
2. Sticky tab row: Timeline, Node Results, Side Effects.
3. Scrollable event list with mono timestamps and readable event labels.

Timeline rows:
1. Left: timestamp + status icon.
2. Center: node and action text.
3. Right: duration and result chip.

Playback cues:
1. Active timeline row highlights corresponding canvas node.
2. Completed rows dim progressively by recency.

## Motion System

Motion principle:
- Inform, do not decorate.

Motion tone:
- Calm and subtle. Prioritize confidence over spectacle.

Timing tokens:
1. Fast: 140ms
2. Standard: 220ms
3. Emphasis: 320ms

Easing:
1. Enter: cubic-bezier(0.20, 0.70, 0.20, 1.00)
2. Exit: cubic-bezier(0.40, 0.00, 0.80, 0.60)
3. Ambient pulse: ease-in-out

Required animations:
1. Node drop snap: 220ms
2. Edge draw: 220ms
3. Selection ring fade: 140ms
4. Simulation path pulse: 1400ms loop while running

Motion amplitude rules:
1. Max transform distance for micro-motion: 4px.
2. Avoid bounce and spring effects in default mode.
3. Keep opacity shifts within 8-16% range for state transitions.

Accessibility motion rule:
- Honor reduced-motion and replace motion cues with static color/state changes.

## Iconography

Icon style:
1. 2px stroke, rounded joins, consistent optical weight.
2. Filled badges only for state emphasis.

Icon size:
1. Node icon: 18px
2. Toolbar icon: 16px
3. Micro status icon: 12px

## Copy and Tone Integration

Guided mode language:
1. Use user-task language: Add, Connect, Configure, Simulate, Publish.
2. Use issue format: What to fix, Why it matters, Fix now.

Avoid in guided mode:
1. Parser terminology
2. Internal runtime vocabulary
3. Raw diagnostic jargon

## Responsive Behavior

Desktop:
1. Full 3-column layout plus bottom tray.

Tablet:
1. Left rail collapsible.
2. Right rail as overlay drawer.
3. Bottom tray half-height modal.

Mobile:
1. Wizard flow with tabs: Modules, Canvas, Configure, Run.
2. One active surface at a time.

## Implementation Mapping

Existing variable surface to evolve:
- [dashboard_assets/static/dashboard.css](dashboard_assets/static/dashboard.css)

Primary layout and component surface:
- [templates/dashboard/views/workflows.html](templates/dashboard/views/workflows.html)

Interaction behavior surface:
- [templates/dashboard/index.html](templates/dashboard/index.html)

## Token Migration Plan

Phase A:
1. Add workflow-builder-specific CSS token namespace.
2. Introduce typography families and size tokens.
3. Add light+dark state color tokens for readiness/simulation.
4. Add expressive accent tokens for module categories.

Phase B:
1. Update node card and canvas containers to new spacing and border model.
2. Update rail and tray dimensions.
3. Add motion token classes and reduced-motion fallbacks.
4. Add theme switch and persistence behavior with contrast audits.

Phase C:
1. Refine edge and status rendering styles from telemetry feedback.
2. Normalize inspector control spacing and validation visuals.

## Acceptance Checklist

1. Canvas is visually dominant and readable at first glance.
2. Node status is scannable without opening inspector.
3. Simulation flow is understandable from motion + timeline pairing.
4. Guided mode copy remains non-technical.
5. Advanced mode can expose engineering detail without visual contamination.
6. Light and dark themes pass visual parity and contrast checks.
7. Motion reads as calm and intentional during simulation playback.
