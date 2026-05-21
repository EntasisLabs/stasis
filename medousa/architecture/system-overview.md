# System Overview

## High-Level Topology

Medousa has three executable surfaces:

- CLI (`medousa_cli`): one-shot command runner and daemon client
- TUI (`medousa_tui`): interactive session UI with local runtime + tool loop
- Daemon (`medousa_daemon`): long-running HTTP control plane + scheduler loop

All three rely on shared Medousa library glue (`medousa/src/lib.rs`) and Stasis runtime abstractions.

## Runtime Composition Layer

Shared runtime helpers:

- `build_runtime(...)` in `medousa/src/lib.rs`
- `parse_backend(...)` in `medousa/src/lib.rs`
- `process_once(...)` and `publish_pending(...)` in `medousa/src/lib.rs`

Runtime backend options:

- `in-memory`
- `surreal-mem`

LLM target resolution:

- provider/model/base-url resolved from flags first, then environment
- defaults to `openai` + `gpt-4o-mini`

## Responsibility Split

### CLI

- creates runtime per command
- submits and processes jobs synchronously
- optionally calls daemon HTTP API endpoints

### TUI

- owns interactive app state (`TuiState`)
- builds runtime + tool loop pipeline (`build_tui_runtime`)
- uses internal event channel (`TuiEvent`) to bridge background tool activity to UI
- persists sessions/settings/api key

### Daemon

- exposes HTTP API for enqueue + recurring registration + health/stats
- runs periodic scheduler loop:
  - materialize recurring
  - process one job
  - publish pending outbox

## Shared Concepts

Job-oriented execution is the backbone:

- submit `NewJob`
- execute through runtime handlers
- inspect `JobAttempt` diagnostics/outcome

TUI and CLI can both execute directly against runtime. Daemon adds networked orchestration and scheduling over the same primitives.
