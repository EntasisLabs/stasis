# Concurrent Capabilities 0.5.0 Roadmap and Internal Plan

Status: **Approved — In Implementation**
Date: 2026-06-02
Owner: Stasis Core
Target Release: **0.5.0**
Feedback source: Post-0.4.0 operator and integrator review

Depends on:

- [orchestration-patterns.md](../../docs-book/src/orchestration-patterns.md)
- `src/application/orchestration/concurrent_pattern_pipeline.rs`
- `src/application/orchestration/tool_loop_pipeline.rs`
- `src/application/runtime/concurrent_pattern_job_handler.rs`
- `src/application/orchestration/runtime_job_payloads.rs`

## 1. Purpose

Extend the **Concurrent orchestration pattern** so each branch can execute a full **tool loop** (prompt → tool call → prompt), not only a single prompt completion. Today all four orchestration patterns fan out through `PromptExecutionPipeline` only; tool execution exists separately on `workflow.stasis.tool_loop` and agent jobs.

This release delivers **Track A only** — concurrent tool branches — as the foundation for a future model-routing / per-agent override track (deferred).

## 2. Problem Statement

Today:

1. `ConcurrentPatternPipeline` spawns one `PromptExecutionPipeline::execute()` per branch in a `JoinSet`.
2. Per-branch `model_hint` and `system_prompt` exist but branches cannot invoke tools.
3. Operators who want parallel research / validation / extraction with tools must run sequential agent sessions or external orchestration.

## 3. Architecture

### 3.1 Branch execution modes

```text
ConcurrentPatternJob
├── shared: initial_user_prompt, merge_strategy, defaults (policy, model, tool_call_mode)
└── branches[] (JoinSet — parallel)
    ├── mode=prompt     → PromptExecutionPipeline
    └── mode=tool_loop  → ToolLoopPipeline (same as workflow.stasis.tool_loop)
```

### 3.2 Contract (backward compatible)

Existing payloads deserialize unchanged — `execution_mode` defaults to `prompt`.

```json
{
  "branch_id": "research",
  "execution_mode": "tool_loop",
  "user_prompt_template": "Research {{input}}",
  "tool_name": "stasis.web.search.mock",
  "tool_input": { "query": "{{input}}" },
  "tool_call_mode": "auto",
  "system_prompt": "be factual",
  "policy_profile": null,
  "model_hint": null
}
```

Pattern-level defaults (optional, branch overrides):

| Field | Scope |
|---|---|
| `tool_call_mode` | Default for tool_loop branches |
| `memory_policy` | Default for tool_loop branches (Slice 5) |

### 3.3 Merge semantics

- Merge strategies (`join_with_headers`, `join_lines`) operate on branch **`output_text`** (final model text from tool loops).
- Tool metadata lives on **`ConcurrentPatternBranchResult`** (`tool_output`, `tool_invocations`, `rounds_executed`, `termination_reason`) and in job diagnostics.

### 3.4 Invariant updates

Orchestration invariant #2 becomes:

> All **prompt** branches and the prompt rounds inside **tool_loop** branches execute through `PromptExecutionPipeline` — the chat middleware chain applies uniformly.

## 4. Implementation Slices

Each slice lands independently, builds on the prior slice, and keeps tests green.

### Slice 1 — Payload + validation ✅

- [x] `ConcurrentBranchExecutionMode` enum (`prompt` / `tool_loop`, serde default `prompt`)
- [x] Extend `ConcurrentBranchJobPayload` with tool fields + optional `memory_policy`
- [x] Extend `ConcurrentPatternJobPayload` with pattern-level `tool_call_mode`, `memory_policy`
- [x] Handler validation: `tool_loop` requires non-empty `tool_name`

### Slice 2 — Pipeline dispatch ✅

- [x] `ConcurrentPatternPipeline::new_with_tool_loop(...)`
- [x] Per-branch dispatch in existing `JoinSet`
- [x] Extend `ConcurrentPatternBranch` / `ConcurrentPatternBranchResult` with execution metadata
- [x] Unit tests: mixed prompt + tool_loop branches

### Slice 3 — Handler + runtime builder ✅

- [x] `ConcurrentPatternJobHandler` accepts `Arc<dyn ToolRegistry>`
- [x] Map payload → pipeline request (tool_call_mode resolution)
- [x] `StasisRuntimeBuilder` passes `tool_registry` to concurrent handler
- [x] Diagnostics: `execution_mode`, tool branch counts

### Slice 4 — Integration tests ✅

- [x] `runtime_backend_parity`: mixed-branch concurrent job (prompt + tool_loop)
- [x] Policy violation: `tool_loop` branch missing `tool_name`
- [x] Thread lineage unchanged for tool branches

### Slice 5 — Memory on tool branches (optional per branch)

- [ ] Resolve `memory_policy` (pattern default → branch override) for tool_loop branches
- [ ] Reuse `ToolLoopJobHandler` memory/identity prepend path in concurrent handler
- [ ] Parity test with mock memory reader

### Slice 6 — Docs and release

- [x] Update `docs-book/src/orchestration-patterns.md`
- [x] Update cookbook / examples (`agentic_workflows_production.rs`, `team_role_workflows.rs`)
- [ ] `CHANGELOG [Unreleased]` → `[0.5.0]` at tag time
- [ ] `mdbook build`

## 5. Test Plan

| Test | Validates |
|---|---|
| `concurrent_pattern_mixed_branches_execute` | Prompt + tool_loop in one JoinSet |
| `concurrent_tool_loop_branch_missing_tool_name_rejects` | Policy violation |
| `surreal_orchestration_concurrent_pattern_executes_all_branches` | Backward compat (prompt-only) |
| `in_memory_orchestration_concurrent_pattern_persists_branch_thread_lineage` | Thread events for tool branches |
| `concurrent_tool_loop_branch_memory_recall` (Slice 5) | Per-branch memory_policy |

## 6. Non-Goals (0.5.0)

- Sequential / handoff tool stages (reuse same enum later)
- Concurrent agent session turns (parallel participants)
- Per-agent model overrides / `model_hint` routing (Track B — future release)
- Durable agent registry with model fields
- Parallel tool calls within a single tool-loop round

## 7. Release Gate

1. All `runtime_backend_parity` orchestration tests pass (in-memory + surreal).
2. New mixed-branch concurrent tests pass.
3. `architecture_conformance` passes (handler registers with tool registry).
4. `mdbook build` succeeds.
5. Examples compile with extended branch payload fields.

## 8. Deferred — Track B (post-0.5.0)

- Model hint routing in `GenaiChatClient` / middleware
- `RegisterAgentRequest.model_hint` + runtime resolution from agent repository
- Per-participant model on `AgentSessionParticipantPayload`
