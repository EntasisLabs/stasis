# Stasis Job Runtime Design

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Architect
- Stability: Stable
- Last Verified: 2026-05-15
- Verified Against:
	- src/application/runtime/in_memory_runtime.rs
	- src/application/runtime/surreal_runtime.rs
	- src/application/use_cases/investigate_runtime_lineage.rs
	- src/domain/runtime/job.rs
	- src/domain/runtime/job_attempt.rs
	- src/domain/runtime/outbox.rs
	- src/domain/runtime/thread.rs
	- tests/runtime_backend_parity.rs

## Scope

This document defines the implementation-facing design for the distributed Stasis runtime across in-memory and Surreal backends.

The runtime coordinates:
- core job lifecycle and scheduling
- orchestration pattern handlers
- Grapheme workflow handlers
- Locus memory-enabled handlers and memory operation workflows
- thread-aware lineage and diagnostics

## Domain Concepts

Job:
- Durable execution unit for agent work.

Recurring Definition:
- Schedule template that generates concrete jobs.

Run Attempt:
- Single execution attempt of a job with timing and outcome metadata.

Thread:
- First-class execution continuity record used by orchestration patterns.

Thread Event:
- Time-ordered event stream bound to a thread, including branch/merge lifecycle semantics.

Lease:
- Time-bounded ownership token for safe distributed processing.

## Data Model

### Job Record

Suggested fields:
- job_id: string
- queue: string
- job_type: string
- payload_ref: string
- state: string
- priority: int
- attempts: int
- max_attempts: int
- backoff_policy: object
- idempotency_key: string
- correlation_id: string
- causation_id: string
- trace_id: string
- sttp_input_node_id: string
- sttp_output_node_id: string | null
- lease_owner: string | null
- lease_expires_at: datetime | null
- heartbeat_at: datetime | null
- scheduled_at: datetime
- started_at: datetime | null
- finished_at: datetime | null
- last_error: object | null

Notes:
- `sttp_input_node_id` and `sttp_output_node_id` preserve cross-job context continuity.
- `correlation_id`, `causation_id`, and `trace_id` are mandatory observability fields.

### Recurring Definition

Suggested fields:
- recurring_id: string
- queue: string
- job_type: string
- payload_template_ref: string
- cron_expr: string
- timezone: string
- jitter_seconds: int
- enabled: bool
- next_run_at: datetime
- last_run_at: datetime | null
- lease_owner: string | null
- lease_expires_at: datetime | null

### Run Attempt Record

Suggested fields:
- attempt_id: string
- job_id: string
- attempt_number: int
- worker_id: string
- started_at: datetime
- finished_at: datetime
- outcome: enum(succeeded|retryable_failure|fatal_failure)
- error_message: string | null
- sttp_output_node_id: string | null
- execution_id: string | null
- guardrail_code: string | null
- policy_reason: string | null
- duration_ms: int | null
- diagnostics: json string | null

### Thread Record

Suggested fields:
- thread_id: string
- parent_thread_id: string | null
- branch_label: string | null
- created_at: datetime
- updated_at: datetime

### Thread Event Record

Suggested fields:
- event_id: string
- thread_id: string
- event_kind: string
- payload_ref: string
- occurred_at: datetime

### Job Event (Outbox)

Suggested fields:
- event_id: string
- event_type: string
- job_id: string
- aggregate_type: string
- aggregate_id: string
- payload: object
- status: string
- created_at: datetime
- published_at: datetime | null

Runtime event envelope fields:
- event_type
- job_id
- thread_id
- correlation_id
- causation_id
- trace_id
- sttp_input_node_id
- sttp_output_node_id
- execution_id
- input_memory_query_id
- input_memory_query_fingerprint
- output_memory_node_id
- retrieval_path
- occurred_at
- message

## Ports

Outbound ports:
- JobStore
- JobAttemptStore
- RecurringStore
- OutboxStore
- ThreadStore
- EventPublisher
- Clock
- IdGenerator

Inbound ports:
- JobCommands
- SchedulerCommands
- LineageInvestigation

## Core Algorithms

### Lease Acquisition

1. Select due enqueued job where scheduled_at <= now and lease_expires_at is null or expired.
2. Attempt compare-and-set update to assign lease_owner and lease_expires_at.
3. Proceed only when update affects one record.

### Worker Execution

1. Move leased job to running.
2. Heartbeat every lease_interval/2.
3. Execute handler with idempotency context.
4. On success:
- persist result reference sttp_output_node_id
- mark succeeded
- write outbox events
 - include standardized diagnostics for orchestration/policy outcomes when applicable
5. On failure:
- increment attempts
- if attempts < max_attempts, compute backoff and re-enqueue
- otherwise mark dead_letter and write failure event

### Orchestration Pattern Execution

Supported typed pattern handlers:
- sequential
- concurrent fan-out + merge
- handoff
- orchestrator-routed

Pattern invariants:
1. Emit deterministic diagnostics with provider, status, pattern, and termination fields.
2. Emit `guardrail_code=POLICY_VIOLATION` and `policy_reason` for policy failures.
3. Persist `thread_id` in success diagnostics and outbox runtime events.

Concurrent-specific invariants:
1. Branch thread IDs follow `root::branch::<branch_id>` naming.
2. Branch completion events are persisted per branch thread.
3. Merge metadata is emitted via `ThreadMergeMetadata`.

### Grapheme Execution

Grapheme handlers execute policy-governed workflow jobs and classify policy failures into guardrail outcomes.

### Locus Memory Execution

Memory-enabled handlers project memory lineage metadata into runtime outbox events.
Dedicated memory operation handlers support recall, find, graph, aggregate, transform, rollup, schema, and evict workflows.

### Lineage Investigation

Selectors supported:
- job_id
- execution_id
- guardrail_code
- thread_id (+ optional ancestry expansion)

Thread selector behavior:
1. Branch selectors can include parent ancestry.
2. Root selectors can include descendant branch contexts through outbox thread-prefix expansion.
3. Results are constrained to selected job IDs for deterministic reports.

### Recurring Materialization

1. Scheduler leases recurring definitions that are due.
2. For each definition, enqueue concrete job with scheduled_at = now + jitter.
3. Compute and persist next_run_at from cron_expr and timezone.
4. Release definition lease.

## Idempotency Contract

- idempotency_key must be unique per logical operation.
- Handlers must be safe under duplicate delivery.
- Store execution fingerprints to detect duplicate side effects.

## Event Contract

Minimum event envelope:
- event_type
- occurred_at
- correlation_id
- causation_id
- trace_id
- job_id
- thread_id
- sttp_input_node_id
- sttp_output_node_id

Optional lineage extensions:
- execution_id
- input_memory_query_id
- input_memory_query_fingerprint
- output_memory_node_id
- retrieval_path
- message

## Failure Semantics

- At-least-once processing is expected.
- Lease expiration enables recovery after worker crash.
- Dead-letter records must retain full diagnostics and replay metadata.
- Policy violations are terminal and classified explicitly for lineage querying.

## SurrealDB Notes

- Prefer indexed fields: state, queue, scheduled_at, lease_expires_at.
- Keep payload_ref small and immutable.
- Keep state changes append-observable via outbox/event rows.

## Testing Matrix

1. Lease contention with multiple workers.
2. Heartbeat expiry and recovery.
3. Retry and backoff correctness.
4. Dead-letter transition on attempt exhaustion.
5. Recurring materialization correctness with timezone and jitter.
6. Idempotent replay behavior.
7. STTP input/output reference continuity across continuation jobs.
8. Orchestration pattern success and policy-failure parity across backends.
9. Thread store parity for create/append/fork/lineage behavior.
10. Thread-only lineage investigation parity for root and branch selectors.
11. Middleware chain parity (ordering, failure propagation, cache, telemetry, interception).

## Open Questions

1. Cron parser choice for Rust runtime.
2. Preferred queue partition strategy.
3. Replay API shape for dead-letter processing.
4. Backoff policy defaults per queue class.
