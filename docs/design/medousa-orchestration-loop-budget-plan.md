# Medousa Hybrid Loop Plan

## Status
- Draft
- Scope: medousa_tui turn orchestration loop
- Type: architecture and rollout plan

## Thesis
The target design is the sweet spot between classic ReAct and heavyweight multi-pass orchestration.

1. ReAct strengths: simple, interpretable, fast.
2. ReAct gaps: weak call-budget control, weak continuation discipline, easy tool-overfire.
3. Full orchestration strengths: resilient and composable.
4. Full orchestration risk: accidental extra LLM passes and loop amplification.

Medousa should use a **hybrid bounded loop**: one intent gate, one primary execution, optional synthesis-only continuation, strict per-turn budget.

## Problems To Solve
1. Conversational pivots after tool turns still route into tool-capable flows.
2. A turn can fan out into classifier + primary + continuation + retries.
3. Continuation can re-enter tool-loop semantics instead of remaining a rewrite pass.

## Target Loop Shape

### Stage 1: Intent Gate
1. Deterministic routing first.
2. Model classifier only for ambiguous prompts.
3. Output intent: `chat`, `tool`, `clarify`, `mixed`.

### Stage 2: Single Primary Execution
1. If `chat`: prompt-only path.
2. If `tool`: tool-loop path.
3. Exactly one primary execution mode per turn.

### Stage 3: Evidence Synthesis (Optional)
1. Trigger only for large/noisy tool payloads.
2. Must be prompt-only rewrite.
3. No tools allowed in this stage.

### Stage 4: Exit Classification
1. Finalize as `done`, `needs_clarification`, or `explicit_tool_request_needed`.
2. End turn cleanly under budget.

## Turn Budget Model

```text
TurnBudget {
  max_llm_calls_total: usize,
  max_tool_loop_calls: usize,
  max_prompt_only_calls: usize,
  max_classifier_calls: usize,
  max_retries: usize,
  max_continuations: usize,
}
```

Recommended defaults:
1. `max_llm_calls_total = 2`
2. `max_tool_loop_calls = 1`
3. `max_prompt_only_calls = 1`
4. `max_classifier_calls = 1`
5. `max_retries = 1`
6. `max_continuations = 1`

Interpretation:
1. Most turns should use one call.
2. Ambiguous turns may use classifier + one primary call.
3. Synthesis occurs only if budget remains and never via tool-loop.

## Hard Invariants
1. Never exceed `max_llm_calls_total` per turn.
2. Never run more than one tool-loop primary call per turn.
3. Continuation is rewrite-only and no-tools.
4. Retry must be mode-stable (no escalation between path types).
5. Budget exhaustion must fail soft with a coherent answer.

## Loop Guard Rules
1. If short social turn follows a provisional tool-heavy response, force no-tools.
2. If tool-loop repeats across turns without fresh explicit tool intent, force no-tools and ask clarify.
3. If tool set repeats with low prompt novelty, skip tool-loop for that turn.

## Observability Contract
Emit notices for:
1. heuristic decision
2. classifier decision
3. final activation
4. budget consume/deny events
5. continuation mode
6. retry mode
7. per-turn orchestration summary

Summary format:

```text
orchestration_summary
calls_total=...
classifier_calls=...
tool_loop_calls=...
prompt_only_calls=...
continuations=...
retries=...
final_mode=...
```

## Data Shape

```text
TurnOrchestrationState {
  calls_total,
  classifier_calls,
  tool_loop_calls,
  prompt_only_calls,
  continuations,
  retries,
  loop_guard_tripped,
  final_mode,
}
```

## Rollout Phases

### Phase A: Instrumentation
1. Add turn-level call accounting and summary logs.
2. Keep behavior unchanged.

### Phase B: Budget Enforcement
1. Enforce hard call ceilings.
2. Deny excess calls with explicit notices.

### Phase C: Continuation Refactor
1. Move continuation to prompt-only rewrite path.
2. Disallow tool-loop continuation.

### Phase D: Loop Guards
1. Add social-turn and repeat-tool guardrails.
2. Tune thresholds using observability.

## Test Plan

### Unit
1. Budget consume/deny behavior.
2. No-escalation invariants.
3. Continuation path uses prompt-only mode.
4. Retry remains mode-stable.

### Integration
1. Ambiguous prompt obeys 2-call max under classifier.
2. Tool-heavy turn allows one primary tool-loop and optional prompt-only synthesis.
3. Social follow-up bypasses tool-loop.

### Runtime
1. Track calls per turn distribution before and after each phase.
2. Track repeated tool-loop incidence in conversational sessions.

## Implementation Anchors
1. `medousa/src/bin/medousa_tui/agent_runtime.rs`
2. `medousa/src/bin/medousa_tui/event_reducer.rs`
3. `medousa/src/bin/medousa_tui/settings_ui.rs` and settings runtime for future tunables

## Success Criteria
1. Calls per turn drop on conversational workloads.
2. Tool-required tasks still complete at parity.
3. Loop incident rate falls without clarify-rate explosion.
