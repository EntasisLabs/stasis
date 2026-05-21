# Component: medousa-tui

## Entry

- Binary: `medousa/src/bin/medousa_tui.rs`

## Core Runtime Assembly

TUI runtime is built by `build_tui_runtime(...)` in `medousa/src/tools.rs`:

- creates Stasis runtime composition
- creates memory reader/writer (Locus in-memory store)
- registers tool set in `InMemoryToolRegistry`
- wraps registry with `PolicyAwareToolRegistry` for Grapheme module allowlist enforcement
- composes `PromptExecutionPipeline + ToolLoopPipeline`

Result surface:

- `TuiRuntime { runtime, tool_loop_pipeline, memory_reader, memory_writer }`

## Main State Owner

`TuiState` in `medousa_tui.rs` is the central in-memory state machine:

- conversation + scroll state
- observability log + jobs list
- settings + settings draft
- editor buffer/file state
- thinking trace
- grapheme console output
- UI mode (`UiMode`)

## Event Loop Model

TUI has a multiplexed async loop:

1. Keyboard events (crossterm) -> key handlers
2. Tool/runtime events (`mpsc::Receiver<TuiEvent>`) -> `handle_tui_event`
3. periodic redraw tick

`TuiEvent` variants (`medousa/src/events.rs`):

- `ToolInvoked`
- `ToolPayload`
- `JobEnqueued`
- `JobProcessed`
- `AgentResponse`
- `AgentChunk`
- `AgentError`

This is the boundary between asynchronous tool execution and deterministic UI state updates.

## Interaction Surfaces

1. Chat loop
- executes prompt through `ToolLoopPipeline`
- streams partial output chunks into conversation

2. Slash commands
- session control (`/new`, `/history`, `/settings`, etc.)
- editor (`/open`, `/save`, `/run`, `/run-current`)
- daemon proxy commands (`/daemon ...`, `/watch add ...`)

3. Overlays/panels
- history
- command palette
- settings + runtime/env submenu
- observability detail
- thinking overlays
- grapheme console

## Persistence and Secrets

In `medousa/src/session.rs`:

- conversation history: `~/.local/share/medousa/history/<session>.jsonl`
- last session id: `~/.local/share/medousa/last_session`
- TUI defaults: `~/.local/share/medousa/tui_defaults.json`
- API key: keyring first, file fallback at `~/.local/share/medousa/secrets/api_key`

## Settings + Env Override Semantics

- settings draft is validated before apply
- env overrides parsed as `KEY=VALUE`
- env overrides are applied before runtime rebuild to ensure new runtime picks up values
- applied settings are persisted to defaults

## State Boundary Summary

- Ephemeral UI state: `TuiState`
- Durable user state: session/defaults/key storage
- Runtime durable/execution state: backend job stores (in-memory or surreal-mem)
