# Lineage and Observability

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Architect
- Stability: Evolving
- Last Verified: 2026-05-15
- Verified Against:
  - src/application/use_cases/investigate_runtime_lineage.rs
  - src/application/runtime/in_memory_runtime.rs
  - src/domain/runtime/outbox.rs
  - src/domain/runtime/job_attempt.rs
  - tests/runtime_backend_parity.rs

## Purpose

Document the Stasis lineage and observability surface: the `InvestigateRuntimeLineage` query interface, the outbox event model, `JobAttempt` records, and all runtime metric keys emitted by the job processing loop.

## Invariants

1. Every completed job execution produces at least one `JobAttempt` record and at least one `OutboxEvent`.
2. A lineage query requires at least one selector. An empty query returns a `PortFailure` error.
3. `OutboxEvent` records are retained until pruned by the retention policy. Lineage queries operate on live store state.
4. `thread_id` queries with `include_thread_ancestry: true` expand to all ancestor thread IDs before filtering.
5. `execution_id` and `guardrail_code` filters are applied as secondary refinements on top of the primary selector result — they do not replace the primary selector.

---

## InvestigateRuntimeLineage

The `InvestigateRuntimeLineage` use case is the canonical entry point for correlating job attempts with outbox events across any selector dimension.

### Query: `RuntimeLineageQuery`

| Field | Type | Description |
|---|---|---|
| `job_id` | `Option<String>` | Select by exact job ID |
| `execution_id` | `Option<String>` | Select by execution ID emitted in handler diagnostics |
| `guardrail_code` | `Option<String>` | Select by guardrail policy code from attempt record |
| `thread_id` | `Option<String>` | Select all jobs belonging to a thread |
| `include_thread_ancestry` | `bool` | When `thread_id` is set, expand to include all ancestor threads |

Exactly one primary selector is required. `execution_id` and `guardrail_code` can be combined with any primary selector for secondary filtering.

### Selector precedence

The primary selector determines how records are fetched from the store. Secondary filters are applied in-memory after the initial fetch:

```
Primary:    job_id | execution_id | guardrail_code | thread_id
Secondary:  execution_id, guardrail_code, job_id (applied as retain filters)
```

### Response: `RuntimeLineageReport`

| Field | Type | Description |
|---|---|---|
| `attempts` | `Vec<JobAttempt>` | All matching attempt records |
| `lineage_events` | `Vec<OutboxEvent>` | All matching outbox events, cross-filtered to matched attempt job IDs |
| `thread_ancestry` | `Vec<String>` | Thread IDs in the ancestry chain when `include_thread_ancestry` is true |

### Runtime API

`InvestigateRuntimeLineage` is exposed directly on `InMemoryRuntime` and `SurrealRuntime`:

```rust
let report = runtime.investigate_lineage(RuntimeLineageQuery {
    job_id: Some("job-abc-123".to_string()),
    ..Default::default()
}).await?;

// By thread, with ancestry expansion
let report = runtime.investigate_lineage(RuntimeLineageQuery {
    thread_id: Some("thread-root-001".to_string()),
    include_thread_ancestry: true,
    ..Default::default()
}).await?;
```

Focused query methods are also available for direct store access without cross-filtering:

```rust
runtime.list_job_attempts("job-id").await?
runtime.list_attempts_by_execution_id("exec-id").await?
runtime.list_attempts_by_guardrail_code("guardrail-code").await?
runtime.list_lineage_events("job-id").await?
runtime.list_lineage_events_by_execution_id("exec-id").await?
runtime.list_lineage_events_by_thread_id("thread-id").await?
```

---

## JobAttempt Record

One `JobAttempt` is written per execution attempt of a job.

| Field | Type | Description |
|---|---|---|
| `attempt_id` | `String` | Unique attempt identifier |
| `job_id` | `String` | Parent job ID |
| `attempt_number` | `u32` | 1-based attempt counter |
| `worker_id` | `String` | Identity of the worker that executed the attempt |
| `started_at` | `DateTime<Utc>` | Attempt start time |
| `finished_at` | `DateTime<Utc>` | Attempt finish time |
| `outcome` | `JobAttemptOutcome` | `Succeeded`, `RetryableFailure`, or `FatalFailure` |
| `error_message` | `Option<String>` | Error text on failure |
| `sttp_output_node_id` | `Option<String>` | STTP output node ID from the handler result |
| `execution_id` | `Option<String>` | Handler-emitted execution identifier for correlation |
| `guardrail_code` | `Option<String>` | Policy violation code if a guardrail triggered |
| `policy_reason` | `Option<String>` | Human-readable guardrail reason |
| `duration_ms` | `Option<u64>` | Attempt wall-clock duration in milliseconds |
| `diagnostics` | `Option<String>` | Handler-emitted JSON diagnostics blob |

### JobAttemptOutcome

| Variant | Meaning |
|---|---|
| `Succeeded` | Handler returned `JobExecutionOutcome::Success` |
| `RetryableFailure` | Handler returned `JobExecutionOutcome::RetryableFailure` — job scheduled for retry |
| `FatalFailure` | Handler returned `JobExecutionOutcome::FatalFailure` or max attempts exceeded — job dead-lettered |

---

## Outbox Event Model

Every terminal job state transition emits an `OutboxEvent` to the durable outbox.

### OutboxEvent

| Field | Type | Description |
|---|---|---|
| `event_id` | `String` | Unique event identifier |
| `status` | `OutboxStatus` | `Pending`, `Published`, or `Failed` |
| `publish_attempts` | `u32` | Number of publish delivery attempts |
| `published_at` | `Option<DateTime<Utc>>` | Time of successful publish |
| `next_attempt_at` | `Option<DateTime<Utc>>` | Scheduled time for next publish retry |
| `last_publish_error` | `Option<String>` | Last publish delivery error |
| `event` | `RuntimeEvent` | The event payload (see below) |

### RuntimeEvent

| Field | Type | Description |
|---|---|---|
| `event_type` | `RuntimeEventType` | `JobSucceeded`, `JobRetryScheduled`, or `JobDeadLettered` |
| `job_id` | `String` | Job that produced this event |
| `thread_id` | `Option<String>` | Thread the job belongs to |
| `correlation_id` | `String` | Propagated from the job |
| `causation_id` | `String` | Propagated from the job |
| `trace_id` | `String` | Propagated from the job |
| `sttp_input_node_id` | `String` | STTP input node at job creation |
| `sttp_output_node_id` | `Option<String>` | STTP output node set by the handler on success |
| `execution_id` | `Option<String>` | Handler-emitted execution correlation ID |
| `input_memory_query_id` | `Option<String>` | Locus memory recall query ID (memory-enabled paths) |
| `input_memory_query_fingerprint` | `Option<String>` | Deterministic fingerprint of the memory query |
| `output_memory_node_id` | `Option<String>` | Locus memory store output node ID (memory-enabled paths) |
| `retrieval_path` | `Option<String>` | Memory retrieval path descriptor |
| `occurred_at` | `DateTime<Utc>` | Event timestamp |
| `message` | `Option<String>` | Optional context message |

### OutboxPublishPolicy

Controls retry behavior for outbox event delivery:

| Field | Default | Description |
|---|---|---|
| `max_attempts` | `8` | Maximum publish delivery attempts |
| `base_delay_seconds` | `2` | Initial retry delay |
| `max_delay_seconds` | `300` | Maximum retry delay cap |

---

## Runtime Metric Keys

All metric keys are emitted through the `RuntimeMetrics` port. Keys are stable `&'static str` constants.

### Job lifecycle metrics

| Key | Type | Emitted on |
|---|---|---|
| `runtime.job.succeeded.total` | counter | Job handler returned `Success` |
| `runtime.job.retryable_failure.total` | counter | Handler returned `RetryableFailure` |
| `runtime.job.fatal_failure.total` | counter | Handler returned `FatalFailure` |
| `runtime.job.dead_letter.total` | counter | Job moved to dead-letter (fatal failure or max attempts exceeded) |
| `runtime.job.retry_scheduled.total` | counter | Job scheduled for retry after retryable failure |
| `runtime.job.process.duration_ms` | duration | Per-job wall-clock processing time |

### Outbox metrics

| Key | Type | Emitted on |
|---|---|---|
| `runtime.outbox.publish.success.total` | counter | Outbox event successfully delivered |
| `runtime.outbox.publish.failure.total` | counter | Outbox event delivery attempt failed |

### Grapheme metrics

| Key | Type | Emitted on |
|---|---|---|
| `runtime.grapheme.guardrail_failure.total` | counter | Grapheme job rejected by guardrail policy |

### Chat metrics

Chat-layer metrics are documented in [Chat Middleware Pipeline](./chat-middleware.md).

---

## Non-Goals

- `InvestigateRuntimeLineage` does not provide real-time streaming. It queries stored state.
- Metric emission requires a `RuntimeMetrics` implementation to be wired. The default `NoopRuntimeMetrics` silently discards all observations.
- Outbox event delivery to external subscribers requires an `EventPublisher` implementation registered via `register_event_publisher`.
