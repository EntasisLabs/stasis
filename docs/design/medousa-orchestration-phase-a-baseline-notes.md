# Medousa Orchestration Phase A Baseline Notes

## Purpose
Baseline capture guidance before and during Phase A instrumentation rollout.

## What To Observe
Use `UiNotice` stream and observability panel entries to capture:

1. `activation heuristic ...`
2. `intent classifier ...`
3. `activation final ...`
4. `orchestration_summary ...`

## Suggested Session Sampling
1. Conversational-only session (20+ turns).
2. Tool-heavy lookup session (20+ turns).
3. Mixed session with pivots between social and lookup intent (20+ turns).

## Key Metrics (Manual/Log-Derived)
1. Calls per turn (`calls_total`).
2. Classifier frequency (`classifier_calls`).
3. Tool-loop frequency (`tool_loop_calls`).
4. Continuation frequency (`continuations`).
5. Retry frequency (`retries`).
6. Final mode distribution (`final_mode`).

## Red Flags
1. `calls_total > 2` on conversational turns.
2. Repeated `tool_loop_with_continuation` on short prompts.
3. Rising `tool_loop_retry_exhausted` rates.

## Target Baseline Snapshot Format
```text
session_id=...
sample_turns=...
avg_calls_per_turn=...
tool_loop_rate=...
continuation_rate=...
retry_rate=...
final_mode_distribution={...}
notes=...
```

## Next Step
After collecting baseline, proceed to Phase B budget enforcement and compare delta against the same sampling shape.
