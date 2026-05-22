# Medousa Hybrid Loop Kickoff Checklist

## Scope
Execution checklist for implementing the hybrid bounded orchestration loop from the plan.

References:
- docs/design/medousa-orchestration-loop-budget-plan.md

## Phase A: Instrumentation First
- [ ] Add `TurnOrchestrationState` request-local structure in runtime flow.
- [ ] Add counter increments for each call type: classifier, primary tool-loop, primary prompt-only, continuation, retry.
- [ ] Add `orchestration_summary` notice at end of each turn.
- [ ] Add baseline metrics capture script/query notes for before/after comparison.

## Phase B: Budget Enforcement
- [ ] Introduce `TurnBudget` constants and enforcement helpers.
- [ ] Enforce `max_llm_calls_total` hard cap.
- [ ] Enforce `max_tool_loop_calls` hard cap.
- [ ] Enforce `max_classifier_calls` hard cap.
- [ ] Enforce soft-fail response when budget is exhausted.
- [ ] Emit `turn_budget consume` and `turn_budget deny` notices.

## Phase C: Continuation Refactor
- [ ] Replace continuation tool-loop path with prompt-only rewrite path.
- [ ] Add explicit no-tools policy block to continuation prompt.
- [ ] Ensure continuation consumes continuation budget and total call budget.
- [ ] Add tests proving no tool invocations occur in continuation mode.

## Phase D: Loop Guards
- [ ] Implement social-follow-up guard for provisional tool-heavy previous turn.
- [ ] Implement repeated-tool-set guard with low prompt novelty detection.
- [ ] Add guard observability notices with trigger reason.
- [ ] Add clarify fallback wording for guard-triggered turns.

## Tests: Minimum Required
- [ ] Unit: budget consume/deny and no-escalation invariants.
- [ ] Unit: retry mode stability (no prompt-only/tool-loop escalation).
- [ ] Integration: ambiguous prompt with classifier remains within 2-call budget.
- [ ] Integration: explicit tool prompt still reaches tool-loop primary path.
- [ ] Integration: social turn after tool turn routes no-tools.

## Rollout Guardrails
- [ ] Ship Phase A first with behavior unchanged.
- [ ] Run observation window and capture baseline.
- [ ] Enable Phase B with conservative defaults.
- [ ] Enable Phase C only after B shows stable tool-required completion parity.
- [ ] Enable Phase D with log-only or soft mode first if needed.

## Exit Criteria
- [ ] Calls per turn reduced on conversational sessions.
- [ ] No regression in explicit tool-required completion success.
- [ ] Loop incident reports drop across sampled sessions.
- [ ] Clarify-rate does not spike beyond acceptable range.
