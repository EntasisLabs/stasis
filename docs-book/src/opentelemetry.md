# OpenTelemetry

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Operator
- Stability: Stable
- Last Verified: 2026-06-04
- Verified Against:
  - docs/design/opentelemetry-integration-rfc-plan.md
  - docs/adr/ADR-0006-opentelemetry-first-class-observability.md

## Purpose

Document the stable OpenTelemetry contract for Stasis runtime: metrics, traces, propagation, and configuration.

Full specification: [OpenTelemetry Integration RFC](../../docs/design/opentelemetry-integration-rfc-plan.md)  
ADR: [ADR-0006 OpenTelemetry First-Class Observability](../../docs/adr/ADR-0006-opentelemetry-first-class-observability.md)

## Status

**Shipped in 0.3.0** behind optional Cargo feature `otel`. Metrics, traces, and W3C propagation ship together in one release.

## Enablement

Build with the optional feature:

```bash
cargo build --features otel
```

Runtime configuration (after `config_prelude::bootstrap()`):

```bash
export STASIS_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_SERVICE_NAME=stasis-runtime
```

Builder entry point:

```rust
use stasis::config_prelude::bootstrap;
use stasis::prelude::*;

bootstrap()?;
let composition = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_otel_from_env()?
    .build()
    .await?;
```

## Contract Summary

### Ports

| Port | Role |
|---|---|
| `RuntimeMetrics` | Existing counter/duration keys (unchanged) |
| `RuntimeTracing` | **New** — span lifecycle |
| `RuntimeTelemetry` | Combined handle wired through builder |

First-party adapter: `OpenTelemetryTelemetry` (feature `otel`).

### Span names (frozen)

| Span | Scope |
|---|---|
| `stasis.worker.process_once` | Worker tick |
| `stasis.job.execute` | Handler execution |
| `stasis.chat.complete` | LLM call |
| `stasis.memory.recall` | Memory recall |
| `stasis.memory.store` | Memory store |
| `stasis.outbox.publish` | Outbox delivery |
| `stasis.grapheme.execute` | Grapheme workflow |

### Required job attributes

`stasis.job.id`, `stasis.job.type`, `stasis.job.queue`, `stasis.job.attempt`, `stasis.correlation_id`, `stasis.causation_id`, `stasis.trace_id`, `stasis.worker.id`, `stasis.job.outcome`

Secrets and raw prompts/STTP bodies are **never** attached to spans.

### Trace propagation

W3C `traceparent` is canonical. `Job.trace_id` stores the trace id (32 hex chars when propagated). Legacy opaque trace ids remain supported via span attributes.

Job builder helpers:

- `RuntimeWorkflowJobBuilder::with_traceparent(header)`
- `RuntimeWorkflowJobBuilder::with_trace_context(ctx)`

### Metrics

Existing keys (e.g. `runtime.job.succeeded.total`, `runtime.chat.duration_ms`) are unchanged.

New keys in 0.3.0 include `runtime.memory.recall.duration_ms`, `runtime.memory.store.duration_ms`, `runtime.worker.process_once.duration_ms` — see RFC §5.3.

## Relationship to lineage

| Lineage | OTEL |
|---|---|
| `InvestigateRuntimeLineage` queries | Live traces in your collector |
| Stored `Job` / outbox fields | Span attributes + W3C context |
| Job attempt diagnostics JSON | Complements spans (not replaced) |

## Implementation plan

Single release **0.3.0** workstreams (see RFC §7):

1. **A** — Ports + OTEL adapter + `otel` feature
2. **B** — Builder wiring (`with_runtime_telemetry`, `with_otel_from_env`)
3. **C** — Span instrumentation (job, chat, memory, outbox, grapheme)
4. **D** — W3C propagation on enqueue + `process_once`
5. **E** — Memory metrics
6. **F** — Docs, `.env.example`, parity tests

Release gate: RFC §8 acceptance criteria (OTLP smoke test, span tree, no secrets in attributes, default build unchanged).

## Local collector smoke test

Verify OTLP export end-to-end before tagging a release:

1. Start an OTLP gRPC collector (Jaeger all-in-one is enough for local dev):

```bash
docker run --rm -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one:1.62.0
```

2. Build and run a runtime or the dashboard with OTEL enabled:

```bash
export STASIS_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_SERVICE_NAME=stasis-runtime

cargo run --features otel --bin stasis_dashboard
```

3. Trigger work that emits spans (enqueue/process a job, or call a dashboard action with a W3C header):

```bash
curl -i -X POST http://127.0.0.1:3007/action/scheduler/materialize \
  -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
```

4. Open Jaeger UI at http://127.0.0.1:16686 and confirm spans such as `stasis.worker.process_once` and `stasis.job.execute` for service `stasis-runtime`.

5. Run parity tests without a live collector:

```bash
cargo test --features otel --test otel_runtime_parity
```

## Out of scope (0.3.0)

- Python SDK telemetry
- Third-party driver auto-instrumentation (Surreal, Kafka, RabbitMQ)
- Replacing diagnostics JSON or lineage queries
