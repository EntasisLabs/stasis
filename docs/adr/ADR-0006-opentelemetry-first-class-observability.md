# ADR-0006 OpenTelemetry as First-Class Runtime Observability

## Document Metadata

- Document Type: Architecture Decision Record
- Status: Accepted
- Date: 2026-06-04
- Owners: Runtime Engineering, SRE
- Implementation Plan: [../design/opentelemetry-integration-rfc-plan.md](../design/opentelemetry-integration-rfc-plan.md)

## Context

Stasis already emits structured lineage (`correlation_id`, `causation_id`, `trace_id`), job attempt diagnostics, and stable counter/duration keys through the `RuntimeMetrics` port. However:

1. Default wiring uses `NoopRuntimeMetrics` — observations are discarded unless consumers implement a custom adapter.
2. `StasisRuntimeBuilder` can attach chat telemetry middleware but does not wire metrics/tracing into the job processing loop by default.
3. There is no OpenTelemetry integration — no OTLP export, no spans, no W3C trace context propagation.
4. Operations teams expect one switch (`OTEL_EXPORTER_OTLP_ENDPOINT`) and full visibility across job execution, LLM calls, memory operations, and outbox delivery.

Partial OTEL (metrics-only or traces-only) would force consumers to maintain two observability paths and delay a stable contract.

## Decision

Adopt **OpenTelemetry as the first-class observability surface for Stasis runtime** in a **single release (target 0.3.0)**:

1. **Keep `RuntimeMetrics`** as the stable counter/duration port; add a **`RuntimeTracing`** port for span lifecycle.
2. Ship a first-party **`OpenTelemetryTelemetry`** adapter (behind `feature = "otel"`) that implements both ports and initializes from standard OTEL environment variables.
3. Wire telemetry through **`StasisRuntimeBuilder::with_runtime_telemetry(...)`** so job loop, chat middleware, memory handlers, and outbox publishing share one provider.
4. **Freeze span names, metric keys, and attribute keys** in [opentelemetry-integration-rfc-plan.md](../design/opentelemetry-integration-rfc-plan.md). Treat that document as the compatibility contract (SemVer: additive attributes OK; renames require major).
5. **W3C Trace Context propagation** is canonical for cross-service correlation. Existing `Job.trace_id` remains stored and is mapped to OTEL trace ID when valid; legacy opaque IDs are preserved as span attributes.

## Alternatives Considered

| Alternative | Rejected because |
|---|---|
| Metrics-only OTEL in v1 | Leaves blind spots in LLM/memory latency debugging; users would still need custom tracing. |
| Replace `RuntimeMetrics` with OTEL-only API | Breaks existing test doubles and custom adapters; port pattern is a core Stasis convention. |
| Mandatory OTEL dependency | Increases compile surface for embedded/test consumers; optional feature is sufficient. |
| Phased releases (metrics then traces) | User preference: ship one complete observability story. |

## Consequences

Positive:

1. One builder call + standard env vars → OTLP metrics and traces.
2. Stable instrument and span names for dashboards and alerts.
3. Job lineage fields correlate with OTEL traces in backends (Datadog, Grafana Tempo, Honeycomb, etc.).
4. Custom `RuntimeMetrics` implementations remain supported; OTEL is additive.

Tradeoffs:

1. New optional dependency tree when `otel` feature is enabled.
2. Instrumentation touchpoints across runtime loop, handlers, and middleware (implementation work concentrated in one release).
3. Contract maintenance obligation — changes require RFC/ADR update.

## Guardrails

1. OTEL must be **opt-in via Cargo feature** (`otel`); default builds unchanged.
2. Hot paths must not block on export; batch/async OTLP exporters only.
3. Span and log attributes must **never include secret values** (API keys, passwords, raw STTP bodies).
4. Frozen contract changes require updating the RFC plan and a changelog migration note.

## Out of Scope (This ADR)

- Python SDK / polyglot telemetry bridges (deferred).
- Log-trace correlation via `tracing`/structured logging crate (follow-up if needed).
- Auto-instrumentation of SurrealDB or external transports beyond Stasis-owned code paths.
