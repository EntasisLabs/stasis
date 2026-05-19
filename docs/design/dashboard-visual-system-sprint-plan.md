# Dashboard Visual System Sprint Plan

## Goal

Close the final polish gap between the current Stasis dashboard and the React prototype by standardizing tokens, spacing, typography, interaction states, and section-level visual rhythm.

## Current Gap Summary

- Functional parity is high, but visual hierarchy is still compressed.
- Typography and spacing scales are too small in dense sections.
- Surface styles are not uniformly token-driven across components.
- Interaction polish exists, but not consistently across all controls.

## Strategy

Keep the current stack (Axum + Askama + hx-lite) and run a visual-system sprint with no architecture rewrite.

## Phase Plan

### Phase 1: Core Tokens + Primitive Scale (Immediate)

Scope:
- Establish a durable token direction in SCSS (neutral palette + semantic statuses).
- Increase baseline typography and spacing scales.
- Normalize primitives used everywhere:
  - shell
  - nav item
  - panel/card
  - table
  - badge/status pill
  - button/input/select

Acceptance:
- Job Runtime and Cluster Topology immediately read as product UI instead of debug UI.
- Core text sizes and spacing feel consistent across desktop and mobile breakpoints.

### Phase 2: Section Density and Hierarchy

Scope:
- Tune each section for visual rhythm and content hierarchy.
- Normalize title/subtitle/metadata scales.
- Add section-specific spacing contracts and grid rules.

Acceptance:
- All sections feel part of one visual system.
- Scannability improves without changing backend routes or data contracts.

### Phase 3: Motion and State Polish

Scope:
- Add subtle transitions for interactive controls and section swaps.
- Unify busy/disabled/success/warn/error states across actions.

Acceptance:
- Interactions feel responsive and intentional.
- No abrupt state jumps in primary workflows.

### Phase 4: Final Parity Tune

Scope:
- Side-by-side pass against React screenshots.
- Tighten type scale, spacing, and color contrast for visual parity.

Acceptance:
- Dashboard reads as same product family as prototype.
- No major visual outliers remain.

## Slice 1 Execution Checklist

- [ ] Token baseline and semantic color consistency in SCSS
- [ ] Typography scale increase for headers, body, labels, tables
- [ ] Primitive spacing normalization (cards/panels/controls)
- [ ] Job Runtime readability pass
- [ ] Cluster Topology readability pass
- [ ] Build verification

## Risks and Mitigations

- Risk: over-scaling can reduce data density.
  - Mitigation: tune by section and preserve compact table behavior.
- Risk: dark theme contrast regressions.
  - Mitigation: verify status colors and table contrast in both themes.

## Decision Log

- Chosen path: no frontend framework migration for this sprint.
- Rationale: current architecture already supports high-fidelity parity with lower risk and faster iteration.
