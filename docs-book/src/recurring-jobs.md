# Recurring Jobs

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE
- Stability: Stable
- Last Verified: 2026-05-15
- Verified Against:
  - src/domain/runtime/recurring.rs
  - src/application/runtime/in_memory_runtime.rs

## Purpose

Document the `RecurringDefinition` contract, schedule materialization semantics, cron expression format, timezone handling, jitter, and the runtime methods used to register and tick recurring schedules.

## Invariants

1. A `RecurringDefinition` is a schedule template — it does not execute work itself. Each tick materializes a concrete `NewJob` enqueued into the target queue.
2. `cron_expr` is validated at materialization time using the `cron` crate's `Schedule::from_str`. Invalid expressions cause a `PortFailure` error on the first tick.
3. `timezone` must be a valid IANA timezone string (e.g. `America/New_York`, `UTC`, `Europe/London`). Invalid timezone strings cause a `PortFailure` error.
4. `next_run_at` is updated after each materialization by calling `compute_next_run_at(now)`. The definition is persisted back to the store with the new `next_run_at` and `last_run_at`.
5. Disabled definitions (`enabled: false`) are skipped during materialization without error.
6. Definitions are leased before materialization to prevent duplicate jobs across concurrent scheduler instances.

---

## RecurringDefinition

| Field | Type | Description |
|---|---|---|
| `id` | `String` | Unique identifier for this recurring schedule |
| `queue` | `String` | Target queue for materialized jobs |
| `job_type` | `String` | Job type registered on the runtime |
| `payload_template_ref` | `String` | Payload string passed as `payload_ref` and `sttp_input_node_id` on materialized jobs |
| `cron_expr` | `String` | Cron expression defining the schedule |
| `timezone` | `String` | IANA timezone for schedule evaluation |
| `jitter_seconds` | `i64` | Seconds added to `scheduled_at` to spread load. Use `0` for no jitter |
| `enabled` | `bool` | Whether this schedule is active |
| `max_attempts` | `u32` | `max_attempts` applied to each materialized job |
| `next_run_at` | `DateTime<Utc>` | Next scheduled materialization time (UTC) |
| `last_run_at` | `Option<DateTime<Utc>>` | Last materialization time (UTC) |
| `lease_owner` | `Option<String>` | Scheduler instance holding the current lease |
| `lease_expires_at` | `Option<DateTime<Utc>>` | Lease expiry time |

---

## Cron Expression Format

`cron_expr` uses the standard 5-field cron format as supported by the `cron` crate:

```
┌─────────── minute        (0–59)
│ ┌───────── hour           (0–23)
│ │ ┌─────── day of month   (1–31)
│ │ │ ┌───── month          (1–12)
│ │ │ │ ┌─── day of week    (0–6, Sun=0)
│ │ │ │ │
* * * * *
```

### Examples

| Expression | Schedule |
|---|---|
| `0 * * * *` | Every hour at minute 0 |
| `*/15 * * * *` | Every 15 minutes |
| `0 9 * * 1-5` | Weekdays at 09:00 |
| `30 2 * * 0` | Sundays at 02:30 |
| `0 0 1 * *` | First day of each month at midnight |

---

## Timezone Handling

`timezone` accepts any valid IANA timezone identifier. Schedule evaluation converts the reference time into the specified timezone before computing the next occurrence, then converts the result back to UTC for storage.

```rust
// Schedule evaluated in US Eastern time
RecurringDefinition {
    timezone: "America/New_York".to_string(),
    cron_expr: "0 9 * * 1-5".to_string(), // 09:00 ET on weekdays
    ..
}
```

Use `"UTC"` for schedules that should not adjust for local time or daylight saving transitions.

---

## Registering a Recurring Definition

```rust
use stasis::prelude::*;

let definition = RecurringDefinition {
    id: "daily-report".to_string(),
    queue: "reports".to_string(),
    job_type: "workflow.report.generate".to_string(),
    payload_template_ref: "{}".to_string(),
    cron_expr: "0 6 * * *".to_string(),
    timezone: "UTC".to_string(),
    jitter_seconds: 0,
    enabled: true,
    max_attempts: 3,
    next_run_at: Utc::now(),
    last_run_at: None,
    lease_owner: None,
    lease_expires_at: None,
};

runtime.register_recurring(definition).await?;
```

---

## Schedule Materialization

`materialize_recurring_now` processes all due definitions and enqueues jobs:

```rust
let count = runtime.materialize_recurring_now("scheduler-instance-1").await?;
// count = number of jobs materialized this tick
```

The scheduler ID (`"scheduler-instance-1"`) identifies the calling instance for lease ownership. Use a stable, unique identifier per scheduler process.

### What happens per tick

For each due definition with `enabled: true`:

1. A `NewJob` is created with:
   - `job_type` from the definition
   - `queue` from the definition
   - `payload_ref` = `payload_template_ref`
   - `max_attempts` from the definition
   - `idempotency_key` = `recurring:{id}:{unix_timestamp}` — prevents duplicate jobs if ticked twice within the same second
   - `scheduled_at` = `now + jitter_seconds`
2. The job is enqueued.
3. `last_run_at` is set to `now`.
4. `next_run_at` is updated via `compute_next_run_at(now)`.
5. The lease is released on the definition.

### Jitter

`jitter_seconds` offsets the `scheduled_at` of materialized jobs. Use it to spread load when multiple recurring definitions fire simultaneously:

```rust
// Spread three definitions by 10 seconds each
definition_a.jitter_seconds = 0;
definition_b.jitter_seconds = 10;
definition_c.jitter_seconds = 20;
```

---

## Disabling a Schedule

Set `enabled: false` and save the definition. The next `materialize_recurring_now` tick will skip it without error:

```rust
definition.enabled = false;
runtime.recurring_store.save(definition).await?;
```

---

## Non-Goals

- `RecurringDefinition` does not support sub-minute schedules. The minimum granularity is one minute.
- Materialization does not guarantee exactly-once job creation in the presence of network partitions. The `idempotency_key` is the mechanism that deduplicates jobs created within the same second.
- The `payload_template_ref` is passed as a literal string — no template interpolation is performed at materialization time. Dynamic payloads require a custom scheduler that builds and registers definitions at runtime.
