# Chat Middleware Pipeline

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Architect
- Stability: Stable
- Last Verified: 2026-05-15
- Verified Against:
  - src/application/runtime/chat_client_middleware.rs
  - src/application/runtime/default_chat_middlewares.rs
  - tests/chat_middleware_pipeline.rs

## Purpose

Document the chat middleware pipeline — the composable decorator chain that wraps every `AiChatClient` invocation inside the Stasis runtime. Covers the four built-in middleware, composition semantics, telemetry key contracts, and the `ChatClientMiddleware` extension interface.

## Invariants

1. Middleware is applied in registration order. The first registered middleware is the outermost wrapper — it receives the request first and the response last.
2. All middleware operates at the `AiChatClient` boundary. Every `complete(request, options)` call passes through the full chain.
3. Middleware is wired once at builder time (`StasisRuntimeBuilder`) and shared across all handlers that use the chat client.
4. The `deterministic_cache_key` function produces a SHA-256 hex digest over the serialized `ChatRequest` and `ChatOptions`. Cache keys are stable across restarts for identical inputs.
5. `TelemetryChatMiddleware` and `CacheChatMiddleware` do not emit metrics unless a `RuntimeMetrics` port is provided.

---

## Middleware Execution Order

Middleware registered with `StasisRuntimeBuilder` wraps in LIFO order relative to the inner client:

```
Request → [Middleware A] → [Middleware B] → [Middleware C] → AiChatClient
Response ← [Middleware A] ← [Middleware B] ← [Middleware C] ← AiChatClient
```

Recommended registration order:

```rust
builder
    .with_logging_chat_middleware()           // outermost — sees all requests and errors
    .with_telemetry_chat_middleware(metrics)  // records duration and counters
    .with_cache_chat_middleware(cache)        // short-circuits on hit before reaching model
    .with_tool_call_interception_chat_middleware(interceptor) // innermost — intercepts model tool calls
```

---

## Built-in Middleware

### LoggingChatMiddleware

Logs request and response metadata to stderr using the `stasis.chat` prefix.

**On request:**
```
stasis.chat request messages=N options_present=true|false
```

**On success:**
```
stasis.chat response ok elapsed_ms=N
```

**On error:**
```
stasis.chat response error elapsed_ms=N error=<message>
```

No configuration required:

```rust
builder.with_logging_chat_middleware()
```

---

### TelemetryChatMiddleware

Emits counters and duration observations through the `RuntimeMetrics` port.

```rust
builder.with_telemetry_chat_middleware(metrics)
```

#### Telemetry keys

All keys are `&'static str` constants exported from `stasis::prelude`:

| Constant | Key | Type | Emitted on |
|---|---|---|---|
| `CHAT_REQUESTS_TOTAL` | `runtime.chat.requests.total` | counter | Every `complete()` call |
| `CHAT_ERRORS_TOTAL` | `runtime.chat.errors.total` | counter | Every failed `complete()` call |
| `CHAT_DURATION_MS` | `runtime.chat.duration_ms` | duration | Every `complete()` call (success or error) |
| `CHAT_CACHE_HIT_TOTAL` | `runtime.chat.cache.hit.total` | counter | Cache hit in `CacheChatMiddleware` |
| `CHAT_CACHE_MISS_TOTAL` | `runtime.chat.cache.miss.total` | counter | Cache miss in `CacheChatMiddleware` |
| `CHAT_TOOL_CALLS_TOTAL` | `runtime.chat.tool_calls.total` | counter | Tool call intercepted in `ToolCallInterceptionChatMiddleware` |

Note: `CHAT_CACHE_HIT_TOTAL`, `CHAT_CACHE_MISS_TOTAL`, and `CHAT_TOOL_CALLS_TOTAL` are emitted by their respective middleware, not by `TelemetryChatMiddleware`. All require a `RuntimeMetrics` port to be provided to that middleware.

---

### CacheChatMiddleware

Caches `ChatResponse` values keyed by a deterministic SHA-256 hash of the request. Returns the cached response immediately on hit without calling the model.

```rust
builder.with_cache_chat_middleware(Arc::new(InMemoryAiChatResponseCache::default()))
```

#### With metrics

```rust
let cache_middleware = CacheChatMiddleware::new(cache)
    .with_metrics(metrics.clone());

builder.with_chat_middleware(cache_middleware)
```

#### Cache key

The cache key is computed by `deterministic_cache_key(request, options)`:

```
SHA-256(format!("request={:?}|options={:?}", request, options))
→ "chat:<hex>"
```

Keys are stable for structurally identical requests. Any change to message content, model hint, or options produces a different key.

#### Cache interface

The `AiChatResponseCache` port must be implemented to back this middleware. `InMemoryAiChatResponseCache` is provided for development and testing. Production deployments should implement a durable or distributed cache backend.

---

### ToolCallInterceptionChatMiddleware

Intercepts responses that contain tool calls and routes them through an `AiChatToolInterceptor` implementation. Emits `CHAT_TOOL_CALLS_TOTAL` per intercepted tool call when metrics are configured.

```rust
builder.with_tool_call_interception_chat_middleware(Arc::new(my_interceptor))
```

#### With metrics

```rust
let interception_middleware = ToolCallInterceptionChatMiddleware::new(interceptor)
    .with_metrics(metrics.clone());

builder.with_chat_middleware(interception_middleware)
```

The `AiChatToolInterceptor` port receives an `AiToolCallEnvelope` for each intercepted call. Implement this port to log, audit, filter, or transform tool calls before they are processed by the tool loop.

---

## Implementing Custom Middleware

Implement `ChatClientMiddleware` to inject custom behavior into the pipeline:

```rust
use std::sync::Arc;
use stasis::prelude::{AiChatClient, ChatClientMiddleware};

pub struct MyMiddleware;

impl ChatClientMiddleware for MyMiddleware {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient> {
        Arc::new(MyWrappedClient { inner })
    }
}
```

Register with the builder:

```rust
builder.with_chat_middleware(MyMiddleware)
```

or with a pre-boxed arc:

```rust
builder.with_chat_middleware_arc(Arc::new(MyMiddleware))
```

---

## Non-Goals

- Middleware does not control routing between models or providers. Model selection is governed by `model_hint` in the job payload and the `AiChatClient` implementation.
- Middleware does not have access to job metadata (job_id, thread_id, trace_id). Those fields are available in handler diagnostics via the outbox, not in the chat pipeline.
