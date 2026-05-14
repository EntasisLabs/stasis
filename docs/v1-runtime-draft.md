# Stasis V1 Runtime Draft

## Purpose

Define a V1 execution runtime for Stasis that combines DDD + Hexagonal boundaries with a durable, SurrealDB-backed job system inspired by Hangfire.

## Problem Statement

Linear orchestration is insufficient for agentic workloads that require retries, fan-out, delayed execution, and recurring automation. We need a durable runtime that can survive process crashes while preserving context continuity via STTP references.

## V1 Goals

1. Durable job queue backed by SurrealDB.
2. Event-driven orchestration between agent capabilities.
3. STTP node references as job input and output handles.
4. Reliable retry, leasing, and dead-letter behavior.
5. Recurring job support for periodic automations such as web scraping.

## V1 Non-Goals

1. Multi-region consensus scheduling.
2. Exactly-once delivery guarantees.
3. Full visual dashboard UI.
4. Hard real-time execution guarantees.

## Architecture Summary

- Local request path uses chain-of-responsibility for deterministic per-request processing.
- Cross-capability orchestration uses jobs and events.
- Jobs are persisted in SurrealDB and processed by leased workers.
- Large payloads are not inlined in job records; STTP and artifact references are stored instead.

## Job Classes

1. Fire-and-forget
2. Delayed
3. Recurring
4. Continuation
5. Compensation

## Runtime Components

1. Job Store
- Persists jobs, state transitions, leases, and run metadata.

2. Queue Dispatcher
- Polls due jobs by queue and attempts lease acquisition.

3. Worker Runtime
- Executes jobs, updates heartbeats, records outputs, and emits events.

4. Scheduler
- Materializes recurring definitions into concrete jobs.

5. Event Bus Port
- Publishes domain and runtime events to internal subscribers.

6. Outbox Port
- Ensures event publication consistency with state transitions.

## State Machine

Allowed states:
- enqueued
- leased
- running
- succeeded
- failed
- dead_letter
- canceled

Core transitions:
- enqueued -> leased
- leased -> running
- running -> succeeded
- running -> failed
- failed -> enqueued (retry)
- failed -> dead_letter (attempts exhausted)

## Reliability Model

- Delivery semantics: at-least-once.
- Safety mechanism: idempotency_key and deterministic handlers.
- Recovery: lease timeout and heartbeat expiry trigger re-queue.
- Failure handling: bounded retries + exponential backoff + dead-letter queue.

## STTP and Locus Integration

Input contract:
- sttp_input_node_id references the memory/context node required for execution.

Output contract:
- sttp_output_node_id references a node produced by the worker after execution.

A2A continuity:
- Downstream jobs consume the upstream output reference via correlation identifiers.

## Recurring Jobs V1

Recurring definition fields:
- recurring_id
- cron_expr
- timezone
- jitter_seconds
- enabled
- next_run_at
- last_run_at

Execution rule:
- Scheduler acquires a lease, computes due definitions, and enqueues concrete jobs.
- Jitter spreads schedule bursts to avoid thundering-herd effects.

## Observability V1

Metrics:
- queue_depth by queue
- job_latency_ms
- success_rate
- retry_count
- dead_letter_count
- lease_timeout_recoveries

Tracing fields:
- correlation_id
- causation_id
- trace_id

## Security and Safety

1. Avoid inline secret payloads in job rows.
2. Encrypt sensitive artifacts outside job metadata if needed.
3. Enforce queue-specific execution policies.
4. Keep worker behavior idempotent.

## Rollout Plan

1. Implement in-memory behavior-compatible adapters for fast tests.
2. Implement SurrealDB job store and recurring definitions.
3. Add worker lease and heartbeat loop.
4. Add outbox and event bus publication path.
5. Enable recurring jobs for web scraping workloads.

## Exit Criteria for V1

1. Jobs survive process restart and continue execution.
2. Retry/backoff/dead-letter paths are validated by tests.
3. Recurring jobs materialize and execute on schedule.
4. STTP input/output references are persisted per run.
5. Event-driven continuation jobs are demonstrated end-to-end.

## Implementation Checkpoint

For current status against P0-P3 and the first-class Grapheme integration track, see:
- [design/runtime-phase-plan.md](design/runtime-phase-plan.md)
