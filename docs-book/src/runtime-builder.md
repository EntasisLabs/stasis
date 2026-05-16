# Runtime Builder and Wiring Guide

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-05-15
- Verified Against:
  - src/application/runtime/stasis_runtime_builder.rs
  - src/application/runtime/runtime_factory.rs
  - src/infrastructure/runtime/in_memory_thread_store.rs
  - src/infrastructure/runtime/surreal_thread_store.rs
  - src/infrastructure/memory/locus_node_store_factory.rs
  - src/infrastructure/llm/genai_chat_client.rs

## Purpose

Document the `StasisRuntimeBuilder` API — the single composition point for wiring a fully-functional Stasis runtime. Covers backend selection, handler sets, middleware, tool registration, memory integration, and extensibility.

## Invariants

1. `StasisRuntimeBuilder::new(backend)` is the only supported entry point for runtime composition.
2. All handler groups are **included by default**. Use `without_*` methods to opt out.
3. If no chat client is provided, `GenaiChatClient::from_env()` is used — model selection is then driven by environment variables.
4. If `.with_locus_memory()` is called and no explicit memory ports are provided, an in-memory Locus store is bootstrapped automatically.
5. Middleware is applied in registration order — first registered wraps outermost.
6. `build()` is async and must be awaited. It wires all handlers into the runtime before returning `RuntimeComposition`.

---

## Backend Selection

`RuntimeBackend` controls the persistence layer for jobs, attempts, outbox, and threads.

| Variant | Description |
|---|---|
| `RuntimeBackend::InMemory` | In-process store. No persistence across restarts. Use for testing and development. |
| `RuntimeBackend::Surreal` | SurrealDB-backed durable store. Use for production. |

```rust
use stasis::prelude::*;

// Development / testing
let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory);

// Production
let builder = StasisRuntimeBuilder::new(RuntimeBackend::Surreal);
```

---

## Minimal Working Setup

This composes a runtime with all default handlers and `GenaiChatClient::from_env()` as the chat client:

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .build()
    .await?;
```

All handler groups are active. No middleware is applied beyond the chat client itself.

---

## Chat Client

Provide a custom `AiChatClient` implementation or rely on the default:

```rust
// Custom client
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_chat_client(Arc::new(MyCustomChatClient::new()))
    .build()
    .await?;

// Default: GenaiChatClient::from_env()
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .build()
    .await?;
```

---

## Chat Middleware

Middleware wraps the chat client in registration order. See [Chat Middleware Pipeline](./chat-middleware.md) for behavioral details on each middleware.

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_logging_chat_middleware()
    .with_telemetry_chat_middleware(metrics.clone())
    .with_cache_chat_middleware(Arc::new(InMemoryAiChatResponseCache::default()))
    .with_tool_call_interception_chat_middleware(Arc::new(my_interceptor))
    .build()
    .await?;
```

Convenience methods:

| Method | Middleware registered |
|---|---|
| `.with_logging_chat_middleware()` | `LoggingChatMiddleware` |
| `.with_telemetry_chat_middleware(metrics)` | `TelemetryChatMiddleware` |
| `.with_cache_chat_middleware(cache)` | `CacheChatMiddleware` |
| `.with_tool_call_interception_chat_middleware(interceptor)` | `ToolCallInterceptionChatMiddleware` |
| `.with_chat_middleware(m)` | Any `ChatClientMiddleware` implementation |
| `.with_chat_middleware_arc(arc)` | Pre-boxed `Arc<dyn ChatClientMiddleware>` |

---

## Tool Registration

Tools are registered on the builder and made available to `workflow.stasis.tool_loop` and `workflow.stasis.agent_*` handlers.

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_tool(MyWebSearchTool)?
    .with_tool(MyDatabaseTool)?
    .build()
    .await?;
```

Tools must implement the `StasisTool` trait. Input schema validation (required fields, types, enums, `additionalProperties`) is enforced automatically at invocation time.

---

## Memory Integration

### Automatic Locus bootstrap

`.with_locus_memory()` bootstraps an in-memory Locus store and wires `LocusContextReader`, `LocusContextWriter`, and `LocusMemoryOperations` if none are provided explicitly:

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_locus_memory()
    .build()
    .await?;
```

### Explicit memory ports

Provide custom implementations for any combination of the three memory ports:

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::Surreal)
    .with_memory_context_reader(Arc::new(my_reader))
    .with_memory_context_writer(Arc::new(my_writer))
    .with_memory_operations(Arc::new(my_ops))
    .build()
    .await?;
```

If `.with_locus_memory()` is also set, explicit ports take precedence over auto-bootstrapped ones.

---

## Thread Store

Thread records track execution continuity for orchestration patterns and agent sessions. If not provided, `InMemoryThreadStore` is used for `InMemory` backends and `SurrealThreadStore` for `Surreal` backends.

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::Surreal)
    .with_thread_store(Arc::new(SurrealThreadStore::new(db.clone())))
    .build()
    .await?;
```

---

## Handler Groups

All handler groups are included by default. Opt out with `without_*` methods:

| Method | Handlers disabled |
|---|---|
| `.without_grapheme_handlers()` | `GraphemeJobHandler`, `GraphemeHealthcheckJobHandler`, `GraphemeEchoJobHandler`, `GraphemeTextOpsJobHandler` |
| `.without_prompt_handler()` | `PromptChatJobHandler` |
| `.without_tool_loop_handler()` | `ToolLoopJobHandler` |
| `.without_agent_handlers()` | `AgentTurnJobHandler`, `AgentSessionJobHandler` |
| `.without_memory_operation_handlers()` | `MemoryRecallJobHandler`, `MemoryAggregateJobHandler`, `MemoryTransformJobHandler`, `MemoryRollupJobHandler`, `MemorySchemaJobHandler` |
| `.without_orchestration_pattern_handlers()` | `SequentialPatternJobHandler`, `ConcurrentPatternJobHandler`, `HandoffPatternJobHandler`, `OrchestratorPatternJobHandler` |

Memory operation handlers are only registered when the corresponding memory ports are wired. Calling `.without_memory_operation_handlers()` is a no-op if memory ports are absent.

### Custom handlers

Add application-specific job handlers:

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_extra_handler(MyReportGenerationHandler)
    .build()
    .await?;
```

Custom handlers must implement `JobHandler`. They are registered after all built-in handlers.

---

## Build Output: `RuntimeComposition`

`build()` returns `RuntimeComposition`, which carries the assembled runtime and all wired dependencies:

```rust
let composition = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .build()
    .await?;

// Start the job processing loop
composition.runtime.start().await?;
```

`RuntimeComposition` exposes the underlying `InMemoryRuntime` or `SurrealRuntime` depending on the backend selected.

---

## Non-Goals

- `StasisRuntimeBuilder` does not manage the job processing loop lifecycle. Callers start and stop the runtime independently.
- Builder instances are not reusable after `.build()` is called.
- Middleware order across process restarts is the caller's responsibility.
