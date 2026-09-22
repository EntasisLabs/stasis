# Typed Job Framework

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-09-22
- Verified Against:
  - docs/adr/ADR-0011-typed-job-framework.md
  - src/domain/runtime/typed_contract.rs
  - src/domain/runtime/job_continuation.rs
  - src/application/runtime/job_continuation.rs
  - src/sdk/runtime_sdk.rs
  - tests/typed_job_framework.rs

## Purpose

Define the layer where a Stasis job is declared and run, and record which Hangfire-shaped features that layer owns versus which stay on the durable kernel.

## Hangfire gap map

| Capability | Stasis |
| --- | --- |
| Fire-and-forget | `RuntimeSdk::enqueue_job` |
| Delayed job | `TypedEnqueueBuilder::scheduled_at` |
| Declared queue, retry, priority, placement | `StasisJob::declaration` (ADR-0011) |
| Continuation | `continue_with` (ADR-0011) |
| Recurring | `RecurringDefinition` materializes an untyped template. Typed cron is deferred because materialization stamps `payload_template_ref` as STTP provenance. |
| Batch | Not implemented. A batch is a set of job ids plus one continuation when the set succeeds, fails, or finishes. |
| Cancellation | `RuntimeSdk::cancel`, plus `ContinuationTrigger::Canceled` |
| Disable concurrent execution | Not implemented. Callers use fenced resource leases directly. |
| Job filters | `JobConsumer::on_lifecycle` only |
| Dashboard | Existing command center |

## Declare

```rust
impl StasisJob for PrepareReplica {
    const NAME: &'static str = "prepare_replica";
    const VERSION: u32 = 1;
    type Output = ();

    fn declaration() -> JobDeclaration {
        JobDeclaration::default()
            .queue("replicas")
            .retry(RetryPolicy::exponential(8))
    }
}
```

`enqueue_job(payload).send()` uses that declaration. `.queue("other")` replaces only the queue.

`JobContext::enqueue` uses the child type's declaration for queue, priority, retry, and placement. It keeps the parent correlation id, causation id, and trace id.

## Continue

```rust
let parent_id = runtime.enqueue_job(PrepareReplica { replica_id }).send().await?;
runtime
    .continue_with(parent_id, NotifyReady { replica_id })
    .trigger(ContinuationTrigger::Succeeded)
    .send()
    .await?;
```

The child row does not exist until the parent succeeds. From inside a consumer, `ctx.continue_with(child).send().await?` registers the edge against the current job. The child reads `ctx.parent_job_id()` and `ctx.parent_output_json()`.

Triggers that do not match the parent outcome are stored as discarded and do not enqueue.

## Later slices

1. Typed recurring: teach materialization to store a typed envelope without fabricating STTP provenance, then accept a cron on `JobDeclaration` or `register_recurring_job`.
2. Batches: `start_batch` returns a batch id; each member is a normal job; one continuation fires on all-succeeded, any-failed, or all-terminal.
3. Concurrency: `JobDeclaration::max_concurrent` acquired as a fenced lease keyed by job type (or an explicit resource) for the attempt, released on every terminal path.
4. Filters: ordered hooks around claim and state change, still outside the handler body.
