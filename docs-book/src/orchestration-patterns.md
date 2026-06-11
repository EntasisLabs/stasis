# Orchestration Patterns

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Stable
- Last Verified: 2026-05-15
- Verified Against:
  - src/application/orchestration/sequential_pattern_pipeline.rs
  - src/application/orchestration/concurrent_pattern_pipeline.rs
  - src/application/orchestration/handoff_pattern_pipeline.rs
  - src/application/orchestration/orchestrator_pattern_pipeline.rs
  - src/application/orchestration/agent_session_payload.rs
  - src/application/runtime/sequential_pattern_job_handler.rs
  - src/application/runtime/concurrent_pattern_job_handler.rs
  - src/application/runtime/handoff_pattern_job_handler.rs
  - src/application/runtime/orchestrator_pattern_job_handler.rs

## Purpose

Define the four durable orchestration patterns available in Stasis, their execution semantics, payload contracts, and failure behavior. Each pattern maps to a registered job handler and a corresponding pipeline that runs inside the handler's execution boundary.

## Scope

This document covers the four workflow-level patterns: Sequential, Concurrent, Handoff, and Orchestrator-Routed. Agent-level coordination (`workflow.stasis.agent_session`) is covered separately in the Agent Coordination reference.

## Invariants

1. Every pattern is submitted as a durable job and inherits retry, dead-letter, and lineage semantics from the runtime.
2. All **prompt** branches and the prompt rounds inside **tool_loop** branches execute through `PromptExecutionPipeline` — the chat middleware chain applies uniformly.
3. `{{input}}` and `{input}` template tokens are substituted with the output of the prior stage or the initial prompt.
4. `policy_profile` and `model_hint` set at the pattern level act as defaults and are overridden per-stage/branch/turn if provided.
5. Thread records are created per execution for concurrent, handoff, and orchestrator patterns when a `ThreadStore` is wired.

---

## Pattern 1: Sequential

**Job type:** `workflow.stasis.sequential`

Each stage executes in order. The output of stage N becomes the input to stage N+1 via `{{input}}` substitution. All stages must complete for the job to succeed.

### When to use

- Multi-step pipelines where each step depends on the previous result (e.g. extract → summarize → format).
- Workflows where intermediate outputs must be inspectable in lineage.

### Payload

```json
{
  "initial_user_prompt": "string",
  "trace_id": "string | null",
  "correlation_id": "string | null",
  "policy_profile": "string | null",
  "model_hint": "string | null",
  "stages": [
    {
      "stage_id": "string",
      "user_prompt_template": "string",
      "system_prompt": "string | null",
      "policy_profile": "string | null",
      "model_hint": "string | null"
    }
  ]
}
```

Rust type: `SequentialPatternJobPayload` → `SequentialStageJobPayload`

### Response fields

| Field | Type | Description |
|---|---|---|
| `final_text` | string | Output of the last stage |
| `stages` | array | Per-stage results including `stage_id`, `rendered_prompt`, `output_text` |
| `termination_reason` | string | Always `completed_all_stages` on success |

### Failure semantics

If any stage fails, the job fails at that stage. No partial results are persisted. The job is retried from the beginning per its `BackoffPolicy`.

---

## Pattern 2: Concurrent

**Job type:** `workflow.stasis.concurrent`

All branches execute simultaneously using `tokio::task::JoinSet`. Branch results are merged after all branches complete. Branch order in the response is deterministic (sorted by `branch_id`).

### When to use

- Fan-out analysis where independent perspectives on the same input are needed (e.g. risk assessment from multiple domains simultaneously).
- Latency-sensitive workflows where branches have no inter-dependency.

### Payload

```json
{
  "initial_user_prompt": "string",
  "trace_id": "string | null",
  "correlation_id": "string | null",
  "policy_profile": "string | null",
  "model_hint": "string | null",
  "merge_strategy": "join_with_headers | join_lines | null",
  "tool_call_mode": "auto | strict | null",
  "memory_policy": "MemoryPolicyPayload | null",
  "branches": [
    {
      "branch_id": "string",
      "execution_mode": "prompt | tool_loop",
      "user_prompt_template": "string",
      "system_prompt": "string | null",
      "policy_profile": "string | null",
      "model_hint": "string | null",
      "tool_name": "string | null",
      "tool_input": "object | null",
      "tool_call_mode": "auto | strict | null",
      "memory_policy": "MemoryPolicyPayload | null"
    }
  ]
}
```

`execution_mode` defaults to `prompt` when omitted. When `execution_mode` is `tool_loop`, `tool_name` is required.

Use `ConcurrentBranchJobPayload::prompt(...)` or `::tool_loop(...)` in Rust for ergonomic construction.

Rust type: `ConcurrentPatternJobPayload` → `ConcurrentBranchJobPayload` → `ConcurrentBranchExecutionMode`

### Merge strategies

| Strategy | Behavior |
|---|---|
| `join_with_headers` (default) | Sections separated by `[branch_id]\n{output}` |
| `join_lines` | Outputs joined with `\n` only, no headers |

### Response fields

| Field | Type | Description |
|---|---|---|
| `final_text` | string | Merged output across all branches |
| `branches` | array | Per-branch results: `branch_id`, `execution_mode`, `rendered_prompt`, `output_text`, optional tool metadata (`tool_output`, `tool_invocations`, `rounds_executed`, `branch_termination_reason`) |
| `termination_reason` | string | Always `completed_all_branches` on success |
| `merge_strategy` | string | Strategy applied |

### Failure semantics

If any branch task fails (including a `JoinError`), the entire job fails. All branch tasks are awaited before any merge occurs — there is no partial merge.

---

## Pattern 3: Handoff

**Job type:** `workflow.stasis.handoff`

Turns execute sequentially, each passing its output to the next via `{{input}}` substitution. Unlike Sequential (which is stage-based without actor identity), Handoff models distinct actors with explicit transition records emitted per transfer.

### When to use

- Specialist-chain workflows where each actor has a distinct role and handoff transitions must be traceable (e.g. analyst → reviewer → formatter).
- Workflows where actor identity matters for lineage or audit.

### Payload

```json
{
  "initial_user_prompt": "string",
  "trace_id": "string | null",
  "correlation_id": "string | null",
  "policy_profile": "string | null",
  "model_hint": "string | null",
  "turns": [
    {
      "actor_id": "string",
      "user_prompt_template": "string",
      "system_prompt": "string | null",
      "policy_profile": "string | null",
      "model_hint": "string | null"
    }
  ]
}
```

Rust type: `HandoffPatternJobPayload` → `HandoffTurnJobPayload`

### Response fields

| Field | Type | Description |
|---|---|---|
| `final_text` | string | Output of the last turn |
| `turns` | array | Per-turn results: `actor_id`, `rendered_prompt`, `output_text` |
| `handoffs` | array | Transition records: `{ from_actor_id, to_actor_id }` |
| `termination_reason` | string | Always `completed_all_turns` on success |

### Distinction from Sequential

| | Sequential | Handoff |
|---|---|---|
| Unit | stage | actor turn |
| Identity | stage_id | actor_id |
| Transitions | none | `HandoffTransition` records |
| Use case | pipeline steps | specialist hand-off chains |

### Failure semantics

Same as Sequential — failure at any turn fails the job; retried from the beginning.

---

## Pattern 4: Orchestrator-Routed

**Job type:** `workflow.stasis.orchestrator`

Routes the initial prompt to exactly one route based on keyword scoring. Each route declares `selector_keywords`. The route with the highest keyword match count wins. If no keywords match, the first route is used as fallback.

### When to use

- Dynamic dispatch where the appropriate handler for a prompt is not known statically (e.g. query routing, intent classification, domain dispatch).
- Single-execution workflows where only one specialized path should run per invocation.

### Payload

```json
{
  "initial_user_prompt": "string",
  "trace_id": "string | null",
  "correlation_id": "string | null",
  "policy_profile": "string | null",
  "model_hint": "string | null",
  "routes": [
    {
      "route_id": "string",
      "selector_keywords": ["string"],
      "user_prompt_template": "string",
      "system_prompt": "string | null",
      "policy_profile": "string | null",
      "model_hint": "string | null"
    }
  ]
}
```

Rust type: `OrchestratorPatternJobPayload` → `OrchestratorRouteJobPayload`

### Route selection algorithm

1. Lowercase the `initial_user_prompt`.
2. For each route, count non-empty `selector_keywords` that appear as substrings in the prompt.
3. Select the route with the highest score.
4. If no route scores above zero, select `routes[0]` as fallback.

The `selection_reason` field in the response carries `keyword_match score=N route_id=X` or `fallback_first_route route_id=X`.

### Payload constraint

At least one route must be defined. An empty `routes` array returns a `PortFailure` error and the job is dead-lettered after max attempts.

### Response fields

| Field | Type | Description |
|---|---|---|
| `selected_route_id` | string | Route that was executed |
| `selection_reason` | string | Selection trace: match score or fallback indicator |
| `rendered_prompt` | string | Final prompt sent to the model |
| `output_text` | string | Model output |
| `termination_reason` | string | Always `completed_selected_route` on success |

---

## Submitting Pattern Jobs

Use `StasisWorkflowJobBuilder` to construct a `NewJob` for any pattern:

```rust
let job = StasisWorkflowJobBuilder::sequential()
    .payload(payload.to_payload_ref()?)
    .build();

runtime.enqueue(job).await?;
```

Each pattern has a matching builder method on `StasisWorkflowJobBuilder`:
- `.sequential()`
- `.concurrent()`
- `.handoff()`
- `.orchestrator()`

## Non-Goals

- These patterns do not manage state between separate job invocations. Cross-job continuity is handled via `thread_id` and Locus memory.
- Pattern pipelines do not perform tool invocations. Tool-augmented workflows use `workflow.stasis.tool_loop` or `workflow.stasis.agent_session`.
