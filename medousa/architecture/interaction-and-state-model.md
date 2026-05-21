# Interaction and State Model

## End-to-End Interaction Flows

## 1) TUI Chat Turn

1. User submits prompt in TUI.
2. TUI calls `start_prompt_run(...)`.
3. Tool loop executes with streaming chunks.
4. `TuiEvent::AgentChunk` updates incremental UI text.
5. Tool invocations emit `ToolPayload`/`ToolInvoked` to observability.
6. Final `AgentResponse` is persisted to session history.

State touched:

- in-memory: conversation buffers, processing flags, scroll positions
- durable: history jsonl append

## 2) TUI Grapheme Script Run

1. `/run` or `/run-current` resolves source.
2. allowlist precheck validates referenced ops.
3. enqueue `workflow.grapheme.run` job.
4. run `process_once`.
5. fetch attempts + diagnostics.
6. update job history/observability and Grapheme console.

State touched:

- in-memory: job list, obs log, grapheme console
- runtime backend: job + attempt records

## 3) CLI Local Prompt/Ask

1. CLI builds runtime.
2. CLI enqueues prompt or agent-session job.
3. CLI runs one processing cycle.
4. CLI prints diagnostics/result.

State touched:

- process-local only during command
- runtime backend for job lifecycle data

## 4) Daemon Enqueue + Scheduler

1. API call enqueues job/recurring definition.
2. scheduler tick materializes recurring due jobs.
3. scheduler processes one job.
4. scheduler publishes outbox events.

State touched:

- runtime backend stores
- daemon `last_tick_at` in-memory metric

## State Domains

## A. UI Domain (TUI only)

Owned by `TuiState`:

- rendering mode and panel state
- drafts/editor buffers/scroll offsets
- transient traces and console outputs

Properties:

- volatile
- deterministic reducer-style updates from key events + `TuiEvent`

## B. User Persistence Domain

Owned by `session.rs`:

- session history
- defaults
- last session id
- API key secret storage

Properties:

- file + keyring backed
- independent of runtime backend selection

## C. Runtime Execution Domain

Owned by Stasis runtime backend:

- jobs and transitions
- attempts and diagnostics
- recurring definitions
- outbox events

Properties:

- source of truth for orchestration outcomes
- shared by CLI/TUI/daemon when pointed at same backend context

## Configuration Precedence (Observed Pattern)

1. explicit CLI/TUI args
2. saved defaults (TUI)
3. environment variables
4. hardcoded defaults

For TUI runtime/env overrides:

- overrides are applied before runtime rebuild so new runtime instances read updated env values.

## Coupling Boundaries

- TUI <-> runtime: via `TuiRuntime` and events channel
- CLI <-> runtime: direct synchronous orchestration calls
- CLI <-> daemon: HTTP API only
- daemon <-> runtime: direct runtime orchestration + scheduler tick

This keeps transport boundaries explicit:

- in-process function calls for local orchestration
- HTTP for remote daemon orchestration
