# OpenTelemetry Integration RFC and Delivery Plan

Status: **Frozen Contract — Ready for Implementation**
Date: 2026-06-04
Owner: Stasis Core
Target Release: **0.3.0** (single release — metrics + traces + propagation)
ADR: [ADR-0006-opentelemetry-first-class-observability.md](../adr/ADR-0006-opentelemetry-first-class-observability.md)

Depends on:

- [ADR-0006](../adr/ADR-0006-opentelemetry-first-class-observability.md)
- [job-runtime-design.md](job-runtime-design.md)
- [../architecture/overview.md](../architecture/overview.md)
- `src/ports/outbound/runtime/runtime_metrics.rs`
- `src/application/runtime/in_memory_runtime.rs`
- `src/application/runtime/default_chat_middlewares.rs`

## 1. Purpose

Define the **frozen OpenTelemetry contract** for Stasis runtime observability and the **implementation plan** to deliver it in one release:

- OTLP metrics export for all existing `RuntimeMetrics` keys plus new memory keys
- Distributed tracing with stable span names across job execution, chat, memory, outbox, and grapheme paths
- W3C trace context propagation into jobs and child spans
- Builder and environment configuration with minimal operator setup

This document is the **compatibility specification**. Implementation must not ship instruments, span names, or attribute keys that diverge from this RFC without a contract revision.

## 2. Problem Statement

Today:

1. `RuntimeMetrics` observations are emitted in `InMemoryRuntime` / `SurrealRuntime` but default to `NoopRuntimeMetrics`.
2. `StasisRuntimeBuilder::with_telemetry_chat_middleware()` only covers chat middleware — not the job loop.
3. `Job.trace_id` is an opaque string with no W3C linkage.
4. No OTLP export path exists.

Operators cannot answer: *Which trace shows the slow LLM call inside a retried agent job that recalled memory and failed outbox publish?*

## 3. Scope

### In scope (0.3.0)

1. Optional Cargo feature `otel`.
2. `RuntimeTracing` port + `OpenTelemetryTelemetry` adapter.
3. Builder wiring: `with_runtime_telemetry`, `with_otel_from_env`.
4. Span instrumentation at all contract-defined touchpoints.
5. Metric export for all contract-defined instruments.
6. W3C `traceparent` propagation on enqueue and HTTP dashboard entry (when feature enabled).
7. Documentation, `.env.example` OTEL vars, parity tests with in-memory OTEL exporters.

### Out of scope (0.3.0)

- Python SDK telemetry pass-through.
- Replacing diagnostics JSON on job attempts (kept; spans complement).
- Auto-instrumentation of third-party crates (Surreal driver, lapin, rskafka).
- Custom OTEL views/aggregations beyond SDK defaults.

## 4. Architecture

### 4.1 Port model

```text
┌─────────────────────────────────────────────────────────────┐
│                  StasisRuntimeBuilder                       │
│  with_otel_from_env() / with_runtime_telemetry(handle)      │
└──────────────────────────┬──────────────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
   InMemoryRuntime / SurrealRuntime   ChatClient middleware stack
   (job loop, outbox)                 (TelemetryChatMiddleware, …)
           │                               │
           └───────────────┬───────────────┘
                           ▼
              Arc<dyn RuntimeTelemetry>
                           │
                           ▼
              OpenTelemetryTelemetry (feature otel)
                    │            │
                    ▼            ▼
              OTEL Metrics   OTEL Tracer
                    └─────┬─────┘
                          ▼
                    OTLP Exporter
```

### 4.2 Public Rust API (frozen)

New module: `stasis::application::telemetry` (also exported via `stasis::telemetry_prelude`).

```rust
/// Combined observability handle wired into runtime + chat middleware.
pub trait RuntimeTelemetry: RuntimeMetrics + RuntimeTracing {}

pub trait RuntimeTracing: Send + Sync {
    /// Starts a span; ends automatically when guard is dropped.
    fn start_span(&self, name: &'static str, attributes: &[OtelAttribute]) -> SpanGuard;

    /// Returns the active W3C trace context, if any.
    fn active_trace_context(&self) -> Option<TraceContext>;

    /// Runs a closure inside a child span of the current context.
    fn in_span<F, R>(&self, name: &'static str, attributes: &[OtelAttribute], f: F) -> R
    where
        F: FnOnce() -> R;
}

pub struct TraceContext {
    pub trace_id: String,   // 32 lowercase hex
    pub span_id: String,    // 16 lowercase hex
    pub trace_flags: u8,
}

pub struct OtelAttribute {
    pub key: &'static str,
    pub value: OtelAttributeValue,
}

pub enum OtelAttributeValue {
    String(String),
    Int(i64),
    Bool(bool),
}
```

**Builder methods (frozen names):**

| Method | Behavior |
|---|---|
| `StasisRuntimeBuilder::with_runtime_telemetry(Arc<dyn RuntimeTelemetry>)` | Wires telemetry into runtime job loop **and** chat middleware telemetry |
| `StasisRuntimeBuilder::with_otel_from_env() -> Result<Self>` | Builds `OpenTelemetryTelemetry` from env; no-op error if disabled/misconfigured per policy below |

**Existing method (unchanged, deprecated path):**

- `with_telemetry_chat_middleware(metrics)` — superseded by `with_runtime_telemetry` for new code; remains for backward compatibility.

### 4.3 Cargo feature

```toml
[features]
otel = [
  "dep:opentelemetry",
  "dep:opentelemetry_sdk",
  "dep:opentelemetry-otlp",
]

[dependencies]
opentelemetry = { version = "0.30", optional = true }
opentelemetry_sdk = { version = "0.30", features = ["rt-tokio"], optional = true }
opentelemetry-otlp = { version = "0.30", features = ["grpc-tonic", "http-proto"], optional = true }
```

Exact crate versions may float within 0.30.x at implementation time; API surface in this RFC is what is frozen.

### 4.4 Enablement policy

| Condition | Behavior |
|---|---|
| `otel` feature **disabled** | No OTEL deps; `with_otel_from_env()` returns `Err` with clear message or builder helper unavailable |
| Feature enabled, `STASIS_OTEL_ENABLED=false` | Telemetry disabled; `NoopRuntimeMetrics` + no-op tracing |
| Feature enabled, `STASIS_OTEL_ENABLED` unset or true, no OTLP endpoint | Use OTEL SDK defaults (stdout/noop per SDK env) — document recommended explicit endpoint |
| Feature enabled + `OTEL_EXPORTER_OTLP_ENDPOINT` set | Full OTLP export |

**Stasis-specific env (additive to standard OTEL):**

| Variable | Default | Description |
|---|---|---|
| `STASIS_OTEL_ENABLED` | `true` (when feature on) | Master switch |
| `STASIS_OTEL_SERVICE_NAME` | falls back to `OTEL_SERVICE_NAME`, then `stasis-runtime` | Service name resource attribute |
| `STASIS_OTEL_TRACE_PROPAGATION` | `w3c` | `w3c` \| `legacy` \| `both` — see §6 |

Standard OTEL variables honored without renaming:

- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc` \| `http/protobuf`)
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`
- `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`
- `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`

Integrate with existing `config_prelude::bootstrap()` — OTEL init runs **after** env bootstrap in `with_otel_from_env()`.

## 5. Metrics Contract (Frozen)

### 5.1 Mapping rules

1. **Counter keys** ending in `.total` → OTEL `Counter<u64>`.
2. **Duration keys** ending in `.duration_ms` → OTEL `Histogram<f64>` recording milliseconds.
3. Instrument **name** equals the existing `RuntimeMetrics` string key (dot-separated).
4. Unit: `{duration}` → `ms`; counters → `{request}` or dimensionless.

### 5.2 Existing instruments (unchanged keys)

| Key | Type | Emitted from |
|---|---|---|
| `runtime.job.succeeded.total` | counter | Job success |
| `runtime.job.retryable_failure.total` | counter | Retryable failure |
| `runtime.job.fatal_failure.total` | counter | Fatal failure |
| `runtime.job.dead_letter.total` | counter | Dead letter |
| `runtime.job.retry_scheduled.total` | counter | Retry scheduled |
| `runtime.job.process.duration_ms` | histogram | Job handler wall time |
| `runtime.outbox.publish.success.total` | counter | Outbox publish OK |
| `runtime.outbox.publish.failure.total` | counter | Outbox publish fail |
| `runtime.grapheme.guardrail_failure.total` | counter | Grapheme guardrail |
| `runtime.chat.requests.total` | counter | Chat middleware |
| `runtime.chat.errors.total` | counter | Chat middleware |
| `runtime.chat.duration_ms` | histogram | Chat middleware |
| `runtime.chat.cache.hit.total` | counter | Cache middleware |
| `runtime.chat.cache.miss.total` | counter | Cache middleware |
| `runtime.chat.tool_calls.total` | counter | Tool interception middleware |

### 5.3 New instruments (0.3.0 — additive)

| Key | Type | Emitted from |
|---|---|---|
| `runtime.memory.recall.total` | counter | Memory recall attempts (success + failure) |
| `runtime.memory.recall.errors.total` | counter | Memory recall failures |
| `runtime.memory.recall.duration_ms` | histogram | Memory recall latency |
| `runtime.memory.store.total` | counter | Memory store attempts |
| `runtime.memory.store.errors.total` | counter | Memory store failures |
| `runtime.memory.store.duration_ms` | histogram | Memory store latency |
| `runtime.worker.process_once.total` | counter | Worker ticks |
| `runtime.worker.process_once.duration_ms` | histogram | Full `process_once` wall time |

All new keys must be exported as `pub const` from `stasis::application::telemetry::keys` (single source of truth — runtime constants migrate there).

## 6. Tracing Contract (Frozen)

### 6.1 Span names

All span names are `snake.case` prefixed with `stasis.`.

| Span name | Parent | When started | When ended |
|---|---|---|---|
| `stasis.worker.process_once` | active or remote | Start of `process_once` | Return |
| `stasis.job.execute` | `process_once` or remote | Before handler `execute` | After outcome resolved |
| `stasis.chat.complete` | `job.execute` | Before `AiChatClient::complete` | After response/error |
| `stasis.memory.recall` | `job.execute` | Before `MemoryContextReader::recall` | After response/error |
| `stasis.memory.store` | `job.execute` | Before `MemoryContextWriter::store_context` | After response/error |
| `stasis.outbox.publish` | `process_once` or background task | Before publisher call | After result |
| `stasis.grapheme.execute` | `job.execute` | Before workflow engine execute | After result |

Nested agent/session/tool-loop spans reuse `stasis.job.execute` on the handler boundary; pipeline internals use `stasis.chat.complete` and memory spans as children.

### 6.2 Required span attributes

Present on **`stasis.job.execute`** when available:

| Attribute | Type | Source |
|---|---|---|
| `stasis.job.id` | string | `Job.id` |
| `stasis.job.type` | string | `Job.job_type` |
| `stasis.job.queue` | string | `Job.queue` |
| `stasis.job.attempt` | int | attempt number |
| `stasis.correlation_id` | string | `Job.correlation_id` |
| `stasis.causation_id` | string | `Job.causation_id` |
| `stasis.trace_id` | string | `Job.trace_id` (stored value) |
| `stasis.worker.id` | string | worker_id argument |
| `stasis.job.outcome` | string | `success` \| `retryable_failure` \| `fatal_failure` (set before span end) |

**`stasis.chat.complete`:**

| Attribute | Type |
|---|---|
| `stasis.chat.model` | string |
| `stasis.chat.provider` | string |
| `stasis.chat.messages_count` | int |

**`stasis.memory.recall` / `stasis.memory.store`:**

| Attribute | Type |
|---|---|
| `stasis.memory.retrieved_count` | int (recall only) |
| `stasis.memory.retrieval_path` | string (recall only) |
| `stasis.memory.store_valid` | bool (store only) |
| `stasis.memory.store_node_id` | string (store only, non-secret id) |

**`stasis.outbox.publish`:**

| Attribute | Type |
|---|---|
| `stasis.outbox.event_type` | string |
| `stasis.outbox.job_id` | string |

### 6.3 Forbidden span attributes

Never attach:

- Raw prompts, completions, STTP `raw` node content
- API keys, passwords, bearer tokens
- Full job `payload_ref` JSON

Use counts, ids, paths, and outcome enums only.

### 6.4 Trace propagation contract

**Canonical format:** W3C Trace Context (`traceparent` header).

**On enqueue (`NewJob` / `RuntimeWorkflowJobBuilder`):**

1. If caller supplies valid W3C `traceparent`, extract trace-id → store in `Job.trace_id` (32 hex chars).
2. If caller supplies legacy opaque `trace_id` (non-W3C), store as-is and set span attribute `stasis.legacy_trace_id=true` on `stasis.job.execute`.
3. If no parent context, generate new trace; `Job.trace_id` = new trace id.

**New builder helper (frozen):**

```rust
RuntimeWorkflowJobBuilder::with_trace_context(trace_context: TraceContext)
RuntimeWorkflowJobBuilder::with_traceparent(header: &str) -> Result<Self>
```

**On `process_once`:**

1. Rehydrate OTEL parent from `Job.trace_id` + stored span context when `STASIS_OTEL_TRACE_PROPAGATION` is `w3c` or `both`.
2. Legacy mode (`legacy`): start new root span per job; link via attributes only.

**Dashboard HTTP (when `otel` + dashboard binary):**

- Extract `traceparent` from incoming request → propagate to any jobs enqueued during that request (future dashboard actions).

## 7. Implementation Plan (Single Release 0.3.0)

### Workstream A — Ports and adapter (foundation)

| Task | Files | Done when |
|---|---|---|
| A1 Define `RuntimeTracing`, `RuntimeTelemetry`, `TraceContext`, attribute types | `src/ports/outbound/runtime/runtime_tracing.rs`, `runtime_telemetry.rs` | Traits compile; noop impl |
| A2 Centralize metric key constants | `src/application/telemetry/keys.rs` | All keys single-sourced |
| A3 `NoopRuntimeTracing`, `NoopRuntimeTelemetry` | `src/infrastructure/telemetry/` | Default behavior unchanged without feature |
| A4 `OpenTelemetryTelemetry` + `init_from_env` | `src/infrastructure/telemetry/otel.rs` | In-memory OTEL exporter tests pass |
| A5 Add `otel` feature + deps | `Cargo.toml` | `cargo build --features otel` green |

### Workstream B — Builder and runtime wiring

| Task | Files | Done when |
|---|---|---|
| B1 `StasisRuntimeBuilder::with_runtime_telemetry` | `stasis_runtime_builder.rs` | Same Arc wired to runtime + chat |
| B2 `with_otel_from_env()` | builder + config | Reads env after `bootstrap()` |
| B3 Pass telemetry into `InMemoryRuntime::with_dependencies_and_metrics` → rename to `with_dependencies_and_telemetry` | `in_memory_runtime.rs`, `surreal_runtime.rs`, `runtime_composition.rs` | Job metrics non-noop when wired |
| B4 Export `telemetry_prelude` | `src/lib.rs` | Public API documented |

### Workstream C — Span instrumentation

| Task | Location | Done when |
|---|---|---|
| C1 `stasis.worker.process_once` | `process_once` entry/exit | Span in parity test |
| C2 `stasis.job.execute` | Runtime loop around handler dispatch | Attributes populated |
| C3 `stasis.chat.complete` | `TelemetryChatMiddleware` or chat pipeline wrapper | Nested under job span |
| C4 `stasis.memory.recall` / `store` | Shared helper used by memory-enabled handlers | Memory parity test asserts span |
| C5 `stasis.outbox.publish` | Outbox publish loop | Attributes on event_type |
| C6 `stasis.grapheme.execute` | `GraphemeSdkWorkflowEngine` or handler | Guardrail failures tagged |

### Workstream D — Propagation

| Task | Location | Done when |
|---|---|---|
| D1 `with_traceparent` on job builder | `runtime_workflow_job_builder.rs` | Unit tests for W3C parse |
| D2 Rehydrate context in `process_once` | runtime loop | Child spans link to parent trace |
| D3 Dashboard HTTP middleware (optional) | `stasis_dashboard.rs` | traceparent extracted when present |

### Workstream E — Memory metrics

| Task | Location | Done when |
|---|---|---|
| E1 Emit new memory metric keys | memory handler helper | Keys in OTEL test snapshot |
| E2 Align with recall/store spans | same helper | One code path for metrics + traces |

### Workstream F — Documentation and verification

| Task | Deliverable | Done when |
|---|---|---|
| F1 docs-book `opentelemetry.md` | Reference aligned to this RFC | Published in SUMMARY |
| F2 Update `lineage-observability.md` | Cross-link OTEL + lineage | |
| F3 Update `.env.example` | OTEL vars | |
| F4 `tests/otel_runtime_parity.rs` | In-memory exporter asserts metrics + span names | CI green |
| F5 CHANGELOG 0.3.0 | Release notes | |

### Suggested implementation order

```text
A1 → A2 → A3 → A5 → A4 → B1 → B3 → B2 → C1 → C2 → C3 → C4 → C5 → C6 → E1 → D1 → D2 → F*
```

Parallelizable after A4: C*, E*, D3.

## 8. Acceptance Criteria (Release Gate)

0.3.0 OTEL is **done** when all of the following hold:

1. `cargo test --features otel` passes including `tests/otel_runtime_parity.rs`.
2. `StasisRuntimeBuilder::new(backend).with_otel_from_env()?.build().await?` exports job + chat metrics to OTLP in a local collector smoke test.
3. A single `process_once` run produces span tree: `process_once` → `job.execute` → (`chat.complete` \| `memory.recall` \| …) with required attributes.
4. Enqueue with `traceparent` yields linked traces in collector (same trace id as header).
5. No secret values in span attributes (automated test scans attribute keys against denylist patterns).
6. Default build (`cargo test` without feature) behavior unchanged — no OTEL deps, no panics.
7. Documentation and `.env.example` match this RFC.

## 9. Compatibility and Versioning

| Change type | Policy |
|---|---|
| New metric key | Minor release, RFC appendix update |
| New span (additive) | Minor release |
| Rename metric key or span | **Breaking** — major release + migration doc |
| New span attribute | Minor release (additive) |
| Remove span attribute | Avoid; deprecate in docs first |

## 10. Operator Quick Start (Target DX)

```bash
cp .env.example .env
# enable otel feature at build time; at runtime:
export STASIS_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_SERVICE_NAME=stasis-runtime
```

```rust
use stasis::config_prelude::bootstrap;
use stasis::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    bootstrap()?;
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_otel_from_env()?
        .build()
        .await?;
    // ...
    Ok(())
}
```

## 11. Open Questions (Resolve Before Coding Starts)

| # | Question | Default if unresolved |
|---|---|---|
| 1 | gRPC vs HTTP/protobuf as documented default? | gRPC (`4317`) |
| 2 | Shutdown hook to flush OTEL providers on process exit? | Yes — `StasisRuntime` drop or explicit `telemetry.shutdown()` |
| 3 | Span events for job attempt diagnostics vs attributes only? | Attributes only in 0.3.0; events as follow-up |

**Decision deadline:** resolve Q1–Q3 in PR that starts Workstream A (record answers in this section).

## 12. Relationship to Lineage

| Concern | Lineage (existing) | OTEL (0.3.0) |
|---|---|---|
| Post-hoc investigation | `InvestigateRuntimeLineage` | Trace UI in collector |
| Correlation | `correlation_id`, `trace_id` fields | Span attributes + W3C trace |
| Metrics | `RuntimeMetrics` port | OTLP instruments (same keys) |
| Storage | Surreal/in-memory job stores | External collector |

Both systems run together — OTEL does not replace lineage queries or job attempt diagnostics.

---

**Contract status:** Frozen for implementation. Changes require ADR amendment and explicit changelog migration notes.
