# Medousa Product Revamp Sprint 01

## Context
We are shifting from engineering-first UX to product-first UX.

Current pain points:
- Main chat layout shows too much secondary telemetry by default.
- Observability is valuable but too visually dominant for first-run flow.
- Settings are powerful but feel like a flat config table rather than guided control groups.
- Entry experience starts in chat without a clear onboarding moment.

## Product Direction (Steve-level standard)
- Start with intent, not internals.
- Make one path obvious at boot.
- Keep awareness available, but never noisy.
- Reveal advanced controls progressively.

## UX Principles for this revamp
1. Primary first: chat task dominates by default.
2. Optional depth: observability appears when requested.
3. Guided controls: settings grouped by concern and readability.
4. Friction-light startup: provider/model selection before first chat.
5. Coherent visual language: consistent hierarchy and keyboard affordance.

## First Slice Implemented
Slice: Startup Launch Menu

Delivered behavior:
- At boot, user lands on a dedicated startup screen.
- User can choose provider (cycle with Left/Right).
- User can edit model inline.
- User can press Enter on Start Chat to continue.
- On continue, selected provider/model is applied and chat mode begins.

Why this slice first:
- Creates a clear first-run narrative.
- Reduces cognitive jump from terminal launch to immediate chat complexity.
- Establishes groundwork for future guided settings redesign.

## Second Slice Implemented
Slice: Chat Layout Simplification (Awareness by request)

Delivered behavior:
- Default chat no longer renders persistent right-side observability/job panes.
- Chat remains primary and uncluttered.
- Compact awareness chips are visible in the input title (obs/jobs/drops).
- Ctrl+O opens a unified Awareness Detail overlay containing both observability and job history.

Why this slice now:
- Preserves situational awareness without forcing telemetry scanning.
- Reduces baseline cognitive load in the main canvas.

## Third Slice Implemented
Slice: Guided Settings Information Architecture

Delivered behavior:
- Settings are now grouped by concern:
   - Model & Access
   - Runtime & Thinking
   - Safety & Validation
   - Session Actions
- Section nav is available with Tab/Shift+Tab.
- Existing setting semantics and safety behavior are preserved.

Why this slice now:
- Converts flat configuration scanning into structured navigation.
- Sets up future section-level polish without large behavior risk.

## Fourth Slice Implemented
Slice: Visual Hierarchy + Microcopy Polish (Startup and Settings)

Delivered behavior:
- Settings now includes section helper guidance that changes with active section.
- High-impact actions are visually differentiated (apply/save emphasized, cancel and sensitive actions highlighted).
- Startup copy is clearer and the primary Start Chat row is visually emphasized.
- Startup includes a small tip that provider cycling auto-loads sensible model defaults.

Why this slice now:
- Improves clarity and confidence without changing runtime semantics.
- Increases navigational legibility and reduces decision hesitation.

## Awareness Model (target)
- Lightweight awareness is always nearby:
  - Thinking peek: F2
  - Deep observability overlay: Ctrl+O
- Default view should not force telemetry parsing.

## Next Slices (proposed order)
1. Settings depth pass:
   - Add section-level help text and safer affordances for sensitive actions.
   - Introduce progressive disclosure for advanced fields.
2. Startup polish:
   - Add provider/model presets and last-used quick-pick.
   - Add “advanced setup” branch into guided settings.
3. Component rhythm pass:
   - Tighten modal spacing and visual density to improve scan speed.

## Success Criteria for Sprint 01
- First-run flow feels obvious and calm.
- User can start chatting with chosen provider/model in under 10 seconds.
- Optional introspection remains discoverable but non-intrusive.
- No regressions in runtime behavior or core keybindings.
