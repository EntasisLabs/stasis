# ADR-0011 Typed Job Framework

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-09-22
- Verified Against:
  - src/domain/runtime/typed_contract.rs
  - src/domain/runtime/job_continuation.rs
  - src/application/runtime/typed_job.rs
  - src/application/runtime/job_continuation.rs
  - src/application/runtime/job_context.rs
  - src/sdk/runtime_sdk.rs
  - docs/design/typed-job-framework.md
  - tests/typed_job_framework.rs

## Status

Accepted

## Date

2026-09-22

## Context

Stasis already has a durable job kernel: leases, retries, dead-letter, recurring materialization, outbox, and lineage. Release 0.9.0 added typed payloads (`StasisJob` / `JobConsumer`) on top of that kernel, but the decision was never recorded, and the API still behaves like a queue client.

Callers repeat queue, retry, and placement on every `enqueue_job`. A follow-up job is an immediate child insert, not a durable edge that fires when the parent finishes. Recurring definitions still carry an untyped `payload_template_ref` and stamp that string as STTP provenance. There is no batch, no declared concurrency cap, and no filter pipeline.

That is the gap relative to Hangfire. Hangfire is not just a durable queue. A job is declared (method, queue, retry, concurrency) and then run (fire-and-forget, delay, recurrence, continuation, batch). Stasis should grow that declaration-and-run layer without turning the kernel into a .NET port and without folding agent-vendor adapters into core (ADR-0007).

## Decision

Stasis adopts a **typed job framework** above the durable kernel. A job is a declared type. The framework runs instances of that type.

### 1) Declaration lives on the type

`StasisJob::declaration()` returns queue, priority, retry, and placement. `enqueue_job` and `JobContext::enqueue` start from that declaration. Builder methods (`.queue()`, `.retry()`, `.priority()`, `.placement()`) override one field at a time. Raw `NewJob` / `JobHandler::execute` stay supported.

`JobContext::enqueue` no longer copies the parent attempt's queue, priority, or retry. Correlation, causation, and trace still follow the parent execution.

### 2) Continuations are durable edges, not eager child inserts

`RuntimeSdk::continue_with(parent_id, child)` and `JobContext::continue_with(child)` register a `JobContinuation`. The child job row is inserted only when the parent reaches the trigger:

- `Succeeded` (default)
- `Failed` (dead-letter, including fatal failure and operator `fail`)
- `Canceled`
- `AnyTerminal`

Non-matching triggers are discarded. If the parent is already terminal when the continuation is registered, the child is inserted immediately. The child keeps the parent's trace, uses the parent id as `causation_id`, and can read `JobContext::parent_job_id` / `parent_output_json`. Success output is the handler diagnostics JSON. Failure and cancel pass the terminal message.

Settlement runs from the lifecycle hook on both in-memory and Surreal runtimes, including stale-lease dead-letter. Claims are compare-and-set so a continuation materializes once.

### 3) What stays later

These Hangfire-shaped pieces are in the framework contract but are not part of this decision's first implementation:

1. Typed recurring schedules. Today's materializer writes `payload_template_ref` into STTP provenance, so a typed cron helper would lie about lineage.
2. Batches (a set of jobs plus one continuation when the set reaches a terminal condition).
3. Declared concurrency limits (`DisableConcurrentExecution`). Fenced resource leases remain the manual tool.
4. A filter pipeline beyond `JobConsumer::on_lifecycle`.

## Non-Goals

1. No expression-tree or method-pointer job capture. Rust jobs stay typed payloads plus a consumer.
2. No removal of raw `NewJob` handlers.
3. No change to ADR-0007 comms, translation, or MCP contracts.
4. No requirement that recurring jobs become typed in this slice.

## Consequences

### Positive

1. A job's queue and retry are declared once and reused by every enqueue.
2. Success, failure, and cancel chains are durable and re-entrant across process restarts.
3. The kernel (lease, retry, outbox, provenance) stays the execution engine.

### Tradeoffs

1. In-job `enqueue` of a type that uses the default declaration now lands on queue `default`, even when the parent ran elsewhere.
2. Continuations add a `job_continuation` record beside the job row. Surreal creates that table on write (schemaless), same as durable waits.
3. Parent success output is diagnostics JSON, not a separately typed channel into the child payload.

## Guardrails

1. A continuation must not insert its child before the trigger matches, except when the parent is already in that state at registration.
2. Settlement must claim a pending row before insert, and must not insert a second child if the claim loses.
3. Generic typed success must keep scheme-neutral output provenance (ADR-0010). Continuations copy the parent's `output_provenance` as the child's input provenance.
4. Declaration defaults must not silently override an explicit builder call.
