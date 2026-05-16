# Extension Points and Port Contracts

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-05-15
- Verified Against:
  - src/ports/outbound/ai_chat_client.rs
  - src/ports/outbound/ai_chat_response_cache.rs
  - src/ports/outbound/ai_chat_tool_interceptor.rs
  - src/ports/outbound/runtime/runtime_metrics.rs
  - src/ports/outbound/runtime/thread_store.rs
  - src/ports/outbound/runtime/workflow_engine.rs
  - src/ports/outbound/memory/memory_context_reader.rs
  - src/ports/outbound/memory/memory_context_writer.rs
  - src/ports/outbound/memory/memory_operations.rs
  - src/application/orchestration/tool_registry.rs

## Purpose

Document all port contracts that SDK consumers can implement to extend or replace Stasis infrastructure. Each entry covers the trait signature, what Stasis guarantees about call semantics, and the built-in implementation(s) provided.

## Architecture Principle

Stasis follows hexagonal architecture. All external dependencies are hidden behind ports (traits). Consumers implement ports to substitute infrastructure without touching domain or orchestration logic. The runtime builder (`StasisRuntimeBuilder`) is the single wiring point where port implementations are injected.

---

## AI Layer Ports

### AiChatClient

The primary interface for all model interactions. Every prompt, tool loop, and agent turn goes through this port.

```rust
#[async_trait]
pub trait AiChatClient: Send + Sync {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse>;
}
```

**Stasis guarantees:**
- `complete` is called for every model interaction, including all pipeline patterns.
- The middleware chain wraps this port — all registered middleware decorates the implementation provided at build time.
- `ChatRequest`, `ChatOptions`, and `ChatResponse` are from the `genai` crate.

**Built-in implementations:**

| Type | Description |
|---|---|
| `GenaiChatClient` | Production client backed by `genai`. Uses `from_env()` for model selection via environment variables |
| `MockLlmGateway` | Test stub with configurable canned responses |

**Wiring:**

```rust
builder.with_chat_client(Arc::new(GenaiChatClient::from_env()))
```

---

### AiChatResponseCache

Backing store for `CacheChatMiddleware`. Implement this to use a persistent or distributed cache.

```rust
pub trait AiChatResponseCache: Send + Sync {
    fn get(&self, key: &str) -> Option<ChatResponse>;
    fn set(&self, key: &str, response: ChatResponse);
}
```

**Stasis guarantees:**
- Keys are deterministic SHA-256 hashes of `ChatRequest` + `ChatOptions` — key format is `"chat:<hex>"`.
- `get` and `set` are called synchronously. Implementations must not block the async runtime.

**Built-in implementations:**

| Type | Description |
|---|---|
| `InMemoryAiChatResponseCache` | In-process cache with no expiry. For testing and single-process deployments |

---

### AiChatToolInterceptor

Receives an envelope for every model response that contains tool calls.

```rust
pub trait AiChatToolInterceptor: Send + Sync {
    fn on_tool_calls(&self, envelope: AiToolCallEnvelope);
}
```

**AiToolCallEnvelope fields:**

| Field | Type | Description |
|---|---|---|
| `request_fingerprint` | `String` | Hash of the originating request |
| `tool_call_count` | `usize` | Number of tool calls in the response |
| `tool_names` | `Vec<String>` | Names of the tools called |

**Stasis guarantees:**
- `on_tool_calls` is called once per response that contains at least one tool call.
- The call is synchronous and happens before the tool loop processes the calls.
- Return value is `()` — this is an observation hook, not a veto.

---

## Observability Port

### RuntimeMetrics

Receives counter increments and duration observations from the runtime's job processing loop and chat middleware.

```rust
pub trait RuntimeMetrics: Send + Sync {
    fn incr_counter(&self, name: &str, value: u64);
    fn observe_duration_ms(&self, name: &str, duration_ms: u64);
}
```

**Stasis guarantees:**
- `incr_counter` and `observe_duration_ms` are called on hot paths. Implementations must not block.
- All metric key strings are stable `&'static str` constants. See [Lineage and Observability](./lineage-observability.md) for the full key table.

**Built-in implementations:**

| Type | Description |
|---|---|
| `InMemoryRuntimeMetrics` | In-process counter/duration store for testing |
| `NoopRuntimeMetrics` | Silent discard — used by default when no metrics port is provided |

**Wiring:**

```rust
builder.with_telemetry_chat_middleware(Arc::new(my_metrics_impl))
```

---

## Thread Store Port

### ThreadStore

Manages execution continuity records for orchestration patterns and agent sessions.

```rust
#[async_trait]
pub trait ThreadStore: Send + Sync {
    async fn create_thread(&self, thread: NewThread) -> Result<ThreadRecord>;
    async fn get_thread(&self, thread_id: &str) -> Result<Option<ThreadRecord>>;
    async fn append_event(&self, event: NewThreadEvent) -> Result<ThreadEvent>;
    async fn list_events(&self, thread_id: &str) -> Result<Vec<ThreadEvent>>;
    async fn fork_thread(
        &self,
        parent_thread_id: &str,
        child_thread_id: &str,
        branch_label: Option<String>,
        created_at: DateTime<Utc>,
    ) -> Result<ThreadRecord>;
    async fn list_lineage(&self, thread_id: &str) -> Result<Vec<ThreadRecord>>;
}
```

**Stasis guarantees:**
- `fork_thread` creates a child `ThreadRecord` with `parent_thread_id` set — used by concurrent pattern to track branches.
- `list_lineage` returns the full ancestry chain from the given thread ID back to the root.
- `list_events` returns events in store order for the given thread.

**Built-in implementations:**

| Type | Description |
|---|---|
| `InMemoryThreadStore` | In-process store. Used automatically with `RuntimeBackend::InMemory` |
| `SurrealThreadStore` | SurrealDB-backed durable store. Used automatically with `RuntimeBackend::Surreal` |

**Wiring:**

```rust
builder.with_thread_store(Arc::new(SurrealThreadStore::new(db.clone())))
```

---

## Workflow Engine Port

### WorkflowEngine

Executes Grapheme workflow source programs. Required by Grapheme job handlers.

```rust
#[async_trait]
pub trait WorkflowEngine: Send + Sync {
    async fn execute_grapheme_source(&self, source: &str) -> Result<WorkflowExecutionOutput>;
}
```

**WorkflowExecutionOutput:**

| Field | Type | Description |
|---|---|---|
| `run_id` | `String` | Execution run identifier |

**Built-in implementations:**

| Type | Description |
|---|---|
| `GraphemeSdkWorkflowEngine` | Production Grapheme SDK execution engine. Wired automatically by the builder |

---

## Memory Ports

Memory ports are documented in full in [Memory Operations Reference](./memory-operations.md). Summary:

| Port | Method | Purpose |
|---|---|---|
| `MemoryContextReader` | `recall(request)` | Retrieve prior context before job execution |
| `MemoryContextWriter` | `store_context(request)` | Persist execution output after job completion |
| `MemoryOperations` | `aggregate`, `transform`, `rollup`, `schema` | Bulk memory maintenance operations |

**Built-in implementations:**

| Type | Ports implemented | Description |
|---|---|---|
| `LocusContextReader` | `MemoryContextReader` | Locus-backed recall |
| `LocusContextWriter` | `MemoryContextWriter` | Locus-backed store |
| `LocusMemoryOperations` | `MemoryOperations` | Locus-backed aggregate/transform/rollup/schema |

---

## Tool Registry

### StasisTool

Implement this trait to register a tool with the runtime for use in tool loop and agent handlers.

```rust
#[async_trait]
pub trait StasisTool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&'static str> { None }
    fn input_schema(&self) -> Option<Value> { None }
    async fn invoke(&self, input: Value) -> Result<Value>;
}
```

**Stasis guarantees:**
- `input_schema` is used to validate tool input at invocation time. Schema validation enforces: `required` fields, `type` constraints, `enum` values, and `additionalProperties`.
- `name` must be unique across all registered tools. Duplicate registration returns a `PortFailure` error.
- `description` and `input_schema` are exposed to the model via the `genai` tool interface — descriptive values improve model tool-call accuracy.

**Wiring:**

```rust
builder.with_tool(MyWebSearchTool)?
       .with_tool(MyDatabaseTool)?
```

**Built-in implementations:**

The `InMemoryToolRegistry` is the only provided registry. It is always used internally by the builder. Custom `ToolRegistry` implementations can be provided to the pipeline constructors directly if needed outside the builder path.

---

## Summary Table

| Port | Required | Default | Builder method |
|---|---|---|---|
| `AiChatClient` | No | `GenaiChatClient::from_env()` | `.with_chat_client(...)` |
| `AiChatResponseCache` | No | None | `.with_cache_chat_middleware(cache)` |
| `AiChatToolInterceptor` | No | None | `.with_tool_call_interception_chat_middleware(interceptor)` |
| `RuntimeMetrics` | No | `NoopRuntimeMetrics` | `.with_telemetry_chat_middleware(metrics)` |
| `ThreadStore` | No | `InMemoryThreadStore` / `SurrealThreadStore` | `.with_thread_store(...)` |
| `WorkflowEngine` | No | `GraphemeSdkWorkflowEngine` | Auto-wired |
| `MemoryContextReader` | No | Auto-bootstrapped with `.with_locus_memory()` | `.with_memory_context_reader(...)` |
| `MemoryContextWriter` | No | Auto-bootstrapped with `.with_locus_memory()` | `.with_memory_context_writer(...)` |
| `MemoryOperations` | No | Auto-bootstrapped with `.with_locus_memory()` | `.with_memory_operations(...)` |
| `StasisTool` | No | None | `.with_tool(tool)?` |

No port is strictly required. The runtime boots with all defaults when `StasisRuntimeBuilder::new(backend).build().await?` is called with no additional configuration.
