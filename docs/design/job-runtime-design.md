# Stasis Job Runtime Design

## Scope

This document defines the implementation-facing design for a SurrealDB-backed distributed job runtime used by Stasis.

## Domain Concepts

Job:
- Durable execution unit for agent work.

Recurring Definition:
- Schedule template that generates concrete jobs.

Run Attempt:
- Single execution attempt of a job with timing and outcome metadata.

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

## Ports

Outbound ports:
- JobStore
- RecurringStore
- OutboxStore
- EventPublisher
- Clock
- IdGenerator

Inbound ports:
- JobCommands
- SchedulerCommands

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
5. On failure:
- increment attempts
- if attempts < max_attempts, compute backoff and re-enqueue
- otherwise mark dead_letter and write failure event

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
- sttp_input_node_id
- sttp_output_node_id

## Failure Semantics

- At-least-once processing is expected.
- Lease expiration enables recovery after worker crash.
- Dead-letter records must retain full diagnostics and replay metadata.

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

## Open Questions

1. Cron parser choice for Rust runtime.
2. Preferred queue partition strategy.
3. Replay API shape for dead-letter processing.
4. Backoff policy defaults per queue class.
