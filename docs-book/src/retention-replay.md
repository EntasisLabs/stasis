# Retention and Replay

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE
- Stability: Stable
- Last Verified: 2026-10-12
- Verified Against:
  - src/application/runtime/retention.rs
  - src/application/runtime/replay_report.rs
  - src/application/runtime/in_memory_runtime.rs

## Purpose

Document the Stasis retention policy contract and the dead-letter replay interface. Covers policy configuration, what is pruned, prune report fields, replay semantics, and the `ReplayReport` shape.

## Invariants

1. Retention operates on **terminal** records only. Jobs in `Enqueued`, `Leased`, or `Running` state are never pruned.
2. The job cutoff is `now - terminal_ttl_days`. Receipts and cleanup records use their own cutoffs. A receipt is eligible only when `retained_at` is set and at or before `now - receipt_ttl_days`. A missing `retained_at` is kept.
3. Job, attempt and outbox prunes are separate store calls. A later failure does not restore rows the earlier call already deleted. Receipt and cleanup pruning is its own call and does not run inside `prune_terminal_records`.
4. Dead-letter replay resets the job state to `Enqueued` with `attempts = 0`. Prior attempt records are preserved.
5. `get_replay_report` is a read-only operation — it does not trigger re-execution.

---

## RetentionPolicy

`RetentionPolicy` controls how long terminal records are kept before they become eligible for pruning.

| Field | Type | Default | Description |
|---|---|---|---|
| `terminal_ttl_days` | `i64` | `30` | Days to retain terminal job records, attempt records, and non-pending outbox events |
| `receipt_ttl_days` | `i64` | `30` | Days to retain completion and cancellation receipts after `retained_at` |
| `cleanup_ttl_days` | `i64` | `30` | Days to retain cleanup observations and acknowledgements after `retained_at` |

```rust
use stasis::application::runtime::retention::RetentionPolicy;

let policy = RetentionPolicy {
    terminal_ttl_days: 14,
    ..RetentionPolicy::default()
};

runtime.configure_retention_policy(policy)?;
```

`RetentionPolicy::default()` sets all three fields to 30. Deleting a terminal job does not delete its receipt. Inside the receipt window, a duplicate completion of that job still returns the original receipt. After the receipt is pruned, the duplicate is `Stale` and does not republish effects.

### What "terminal" means per store

| Store | Eligible records |
|---|---|
| Job store | Jobs in `Succeeded`, `Failed`, `DeadLetter`, or `Canceled` state |
| Attempt store | Attempts with a `finished_at` before the cutoff |
| Outbox store | Events in `Published` or `Failed` status (not `Pending`) |

---

## Enforcing Retention

Three methods are available on `InMemoryRuntime` and `SurrealRuntime`:

### `enforce_retention_now()`

Reads the current `RetentionPolicy`, computes the cutoff against the runtime clock, and prunes all three stores:

```rust
let report = runtime.enforce_retention_now().await?;
```

### `enforce_retention(now)`

Same as above but with an explicit `DateTime<Utc>` — useful for deterministic testing:

```rust
let cutoff_reference = Utc::now();
let report = runtime.enforce_retention(cutoff_reference).await?;
```

### `prune_terminal_records(cutoff)`

Direct prune with an explicit cutoff timestamp, bypassing the policy:

```rust
let cutoff = Utc::now() - chrono::Duration::days(7);
let report = runtime.prune_terminal_records(cutoff).await?;
```

---

## RetentionPruneReport

Returned by all retention enforcement methods.

| Field | Type | Description |
|---|---|---|
| `jobs_pruned` | `usize` | Number of terminal job records removed |
| `attempts_pruned` | `usize` | Number of finished attempt records removed |
| `outbox_events_pruned` | `usize` | Number of non-pending outbox events removed |
| `receipts_pruned` | `usize` | Completion and cancellation receipts removed by `enforce_retention` |
| `cleanups_pruned` | `usize` | Cleanup records removed by `enforce_retention` |

`prune_terminal_records` leaves `receipts_pruned` and `cleanups_pruned` at zero. A zero report is not an error.

---

## Dead-Letter Replay

Jobs that reach `DeadLetter` state (exhausted `max_attempts` or returned `FatalFailure`) can be re-enqueued for execution.

### `replay_dead_letter_now(job_id)`

Resets a dead-lettered job to `Enqueued` state with `attempts = 0`, making it eligible for the next processing cycle:

```rust
let replayed = runtime.replay_dead_letter_now("job-abc-123").await?;

if replayed {
    // Job was found in DeadLetter state and re-enqueued
} else {
    // Job was not in DeadLetter state — no action taken
}
```

Returns `true` if the job was found and reset, `false` if the job was not in dead-letter state.

Prior `JobAttempt` records from the original execution are preserved. The re-enqueued job starts a new attempt sequence from `attempt_number = 1`.

---

## ReplayReport

`ReplayReport` provides a pre-correlated view of all attempt records and outbox events for a given job, without triggering re-execution:

| Field | Type | Description |
|---|---|---|
| `job_id` | `String` | The job being inspected |
| `attempts` | `Vec<JobAttempt>` | All attempt records for this job, in store order |
| `lineage_events` | `Vec<OutboxEvent>` | All outbox events associated with this job |

```rust
let report = runtime.get_replay_report("job-abc-123").await?;

println!("attempts: {}", report.attempts.len());
println!("events: {}", report.lineage_events.len());
```

### Difference from `InvestigateRuntimeLineage`

| | `ReplayReport` | `RuntimeLineageReport` |
|---|---|---|
| Selector | `job_id` only | job_id, execution_id, guardrail_code, thread_id |
| Cross-filtering | None | Secondary filter refinement |
| Thread ancestry | No | Yes (with `include_thread_ancestry`) |
| Use case | Quick job-level inspection before replay | Multi-dimensional investigation |

---

## Non-Goals

- Retention enforcement is not scheduled automatically. Callers are responsible for running `enforce_retention_now()` on an appropriate cadence (e.g. a recurring job, a cron task, or a periodic operator call).
- `replay_dead_letter_now` does not bypass `max_attempts`. The replayed job will dead-letter again if it fails the same number of times without a fix.
