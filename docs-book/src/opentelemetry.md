# OpenTelemetry

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Operator
- Stability: **Frozen** (contract for 0.3.0)
- Last Verified: 2026-06-04
- Verified Against:
  - docs/design/opentelemetry-integration-rfc-plan.md
  - docs/adr/ADR-0006-opentelemetry-first-class-observability.md

## Purpose

Document the **frozen OpenTelemetry contract** for Stasis runtime: metrics, traces, propagation, configuration, and the single-release implementation plan.

Full specification: [OpenTelemetry Integration RFC](../../docs/design/opentelemetry-integration-rfc-plan.md)  
ADR: [ADR-0006 OpenTelemetry First-Class Observability](../../docs/adr/ADR-0006-opentelemetry-first-class-observability.md)

## Status

**Planned for 0.3.0** — contract frozen; implementation not yet shipped.

OTEL ships in **one release** (metrics + traces + propagation). No partial metrics-only release.

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

## Out of scope (0.3.0)

- Python SDK telemetry
- Third-party driver auto-instrumentation (Surreal, Kafka, RabbitMQ)
- Replacing diagnostics JSON or lineage queries
