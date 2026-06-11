# Runtime Builder and Wiring Guide

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Stable
- Last Verified: 2026-06-02
- Verified Against:
  - src/application/runtime/stasis_runtime_builder.rs
    - src/application/composition/runtime_composition.rs
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
| `RuntimeBackend::SurrealMem { namespace, database, auth }` | Surreal in-memory engine with explicit namespace/database and optional root credentials. |
| `RuntimeBackend::SurrealWs { endpoint, namespace, database, auth }` | Remote SurrealDB over websocket. Use for shared/dev/staging environments where runtime state is centralized. |
| `RuntimeBackend::SurrealKv { path, namespace, database, auth }` | Embedded SurrealKV on local disk. Use for durable single-node deployments without a remote Surreal service. |

Helper constructors (`RuntimeBackend::surreal_mem`, `surreal_ws`, `surreal_kv`) default `auth` to `None`. Use `.with_surreal_auth(SurrealAuth::new(user, pass))` for authenticated remote databases.

When `auth` is set, Stasis signs in as a **root user** (`username`, `password`), then selects the configured namespace and database.

```rust
use stasis::prelude::*;

// Development / testing
let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory);

// Surreal-backed behavior tests
let builder = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem("stasis", "runtime"));

// Remote websocket backend with root credentials
let backend = RuntimeBackend::surreal_ws(
    "ws://127.0.0.1:8000/rpc",
    "stasis",
    "runtime",
)
.with_surreal_auth(SurrealAuth::new("root", "root"));
let builder = StasisRuntimeBuilder::new(backend);

// Embedded SurrealKV backend
let builder = StasisRuntimeBuilder::new(RuntimeBackend::surreal_kv(
    "./data/stasis-runtime",
    "stasis",
    "runtime",
));
```

Environment variables (dashboard and examples):

| Variable | Purpose |
|---|---|
| `STASIS_DASHBOARD_SURREAL_NAMESPACE` / `STASIS_EXAMPLE_SURREAL_NAMESPACE` | Namespace (default: `stasis`) |
| `STASIS_DASHBOARD_SURREAL_DATABASE` / `STASIS_EXAMPLE_SURREAL_DATABASE` | Database (default: `runtime`) |
| `STASIS_DASHBOARD_SURREAL_USERNAME` / `STASIS_EXAMPLE_SURREAL_USERNAME` | Root user for sign-in |
| `STASIS_DASHBOARD_SURREAL_PASSWORD` / `STASIS_EXAMPLE_SURREAL_PASSWORD` | Root password for sign-in |
| `STASIS_DASHBOARD_SURREAL_ENDPOINT` | Websocket endpoint (required for `surreal-ws`) |
| `STASIS_DASHBOARD_SURREAL_KV_PATH` | Local KV path (required for `surreal-kv`) |

Both username and password must be set for authentication to run. If either is omitted, Stasis connects without sign-in (suitable for local/unauthenticated setups).

Legacy struct-literal form remains available when you need full control:

```rust
let builder = StasisRuntimeBuilder::new(RuntimeBackend::SurrealWs {
    endpoint: "ws://127.0.0.1:8000/rpc".to_string(),
    namespace: "stasis".to_string(),
    database: "runtime".to_string(),
    auth: Some(SurrealAuth::new("root", "root")),
});
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

## Production Environment Variables

Call `stasis::config_prelude::bootstrap()` once at process entry to load `.env` (when present) and file secrets from `STASIS_SECRETS_DIR`. See [Environment Configuration](./environment-configuration.md).

When using the default chat client (`GenaiChatClient::from_env()`), configure provider and model routing via environment variables:

```bash
export STASIS_LLM_PROVIDER=openai
export STASIS_LLM_MODEL=gpt-4o-mini
```

Then provide credentials using either the generic fallback key or provider-specific keys:

```bash
export STASIS_LLM_API_KEY=...
# or one of:
export STASIS_OPENAI_API_KEY=...
export STASIS_ANTHROPIC_API_KEY=...
export STASIS_OLLAMA_API_KEY=...
```

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

Tools are registered on the builder and made available to `workflow.stasis.tool_loop`, `workflow.stasis.agent_*`, and **concurrent orchestration branches** with `execution_mode: tool_loop`.

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
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::SurrealMem {
    namespace: "stasis".to_string(),
    database: "runtime".to_string(),
})
    .with_memory_context_reader(Arc::new(my_reader))
    .with_memory_context_writer(Arc::new(my_writer))
    .with_memory_operations(Arc::new(my_ops))
    .build()
    .await?;
```

If `.with_locus_memory()` is also set, explicit ports take precedence over auto-bootstrapped ones.

---

## Thread Store

Thread records track execution continuity for orchestration patterns and agent sessions. If not provided, `InMemoryThreadStore` is used for `InMemory` backends and `SurrealThreadStore` for `Surreal` runtime compositions.

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::SurrealMem {
    namespace: "stasis".to_string(),
    database: "runtime".to_string(),
})
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
use stasis::prelude::RuntimeSdk;

let composition = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .build()
    .await?;

let runtime = RuntimeSdk::new(composition); // StasisRuntime public naming

// Process one queue tick
let processed = runtime.process_once("default", "worker-1").await?;
println!("processed={:?}", processed);
```

`RuntimeComposition` exposes the underlying `InMemoryRuntime` or `SurrealRuntime` depending on the backend selected, while `StasisRuntime` (currently implemented by `RuntimeSdk`) provides the backend-agnostic operational facade.

---

## Non-Goals

- `StasisRuntimeBuilder` does not manage the job processing loop lifecycle. Callers start and stop the runtime independently.
- Builder instances are not reusable after `.build()` is called.
- Middleware order across process restarts is the caller's responsibility.
