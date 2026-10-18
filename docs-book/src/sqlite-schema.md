# SQLite Schema

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Architect
- Stability: Stable
- Last Verified: 2026-10-12
- Verified Against:
    - src/infrastructure/sqlite/schema.rs
    - src/infrastructure/sqlite/session.rs
    - src/application/runtime/sqlite_runtime.rs
    - src/application/runtime/store_runtime.rs
    - src/infrastructure/sqlite/durable.rs
    - examples/cloudflare-durable-object/wrangler.toml

## Purpose

Native SQLite is a durable alternative to SurrealDB for single-process Stasis runtimes. `SqliteRuntime` is a `StoreRuntime` alias over the SQLite stores; job orchestration is shared with the in-memory and Surreal backends. `schema.rs` has no rusqlite types. The Durable Object kernel applies the same DDL through `SqlExecutor` and calls `StoreRuntime::from_stores`. Store traits allow a `!Send` implementation on `wasm32`.

## Selection

The `sqlite` cargo feature is included in `native` and `dashboard`. It is not enabled for `wasm32`.

```rust
use stasis::prelude::*;

let runtime = StasisRuntimeBuilder::new(RuntimeBackend::sqlite_file("./stasis.sqlite"))
    .build()
    .await?;

let runtime = StasisRuntimeBuilder::new(RuntimeBackend::sqlite_memory())
    .build()
    .await?;
```

Environment selectors accept `sqlite`, `sqlite-file`, and `sqlite-memory`:

- `STASIS_DASHBOARD_RUNTIME_BACKEND`
- `STASIS_STASISD_RUNTIME_BACKEND`
- `STASIS_EXAMPLE_RUNTIME_BACKEND`

File paths come from `STASIS_SQLITE_PATH`.

## Migrations

`SqliteSession::open` applies `MIGRATIONS` from `schema.rs` and records each version in `stasis_schema_migration (version, applied_at)`. Re-opening a file does not re-run applied SQL. Timestamps are integer nanoseconds since the Unix epoch (UTC).

File databases use WAL and a 5 second busy timeout. `:memory:` is a private connection, shared by every store cloned from that session.

Version 3, in the same transaction as its DDL, imports one legacy source into the `runtime_*` tables and then deletes that source:

- Populated v1 runtime tables (`job`, `job_attempt`, `outbox_event`, `durable_wait`, `durable_signal`, `job_continuation`, `recurring_definition`) are copied. `Leased` and `Running` jobs are re-enqueued with their lease columns cleared. They are not trusted as current grants. Each imported job gets incarnation 1, terminal cycle 1, execution epoch 0 and version 0. No receipt is invented. A wait or continuation with no parent, or an unknown enum, refuses the upgrade.
- A format-1 `runtime_safety_ledger` row (schema version 2) is copied as stored, including an unexpired grant. Any other format, or JSON that does not decode, refuses the upgrade.
- Both sources populated refuses the upgrade. The import does not choose a winner.
- `runtime_id_alloc` is set from the highest of the ledger sequence, `stasis_id_seq`, and existing `{prefix}-{n}` suffixes.

A refusal rolls the version stamp back, so the next open tries again after the data is repaired. Back up the file before opening it with this runtime. The old tables remain after a successful import so they can be inspected; they are not the live runtime. Non-runtime tables (inbound keys, resource leases, workflows, threads, identity) are left in place. Durable Objects run this same import. Surreal uses `reconcile_legacy_identity` instead; see [store-runtime-safety-integration.md](../../docs/store-runtime-safety-integration.md).

## Tables

Version 1 creates the same durable surfaces as the Surreal backend:

- `job`, `outbox_event`, `job_attempt`, `durable_wait`, `durable_signal`, `job_continuation`
- `recurring_definition`, `inbound_trigger` (primary key `idempotency_key`), `resource_lease`
- `thread`, `thread_event`, `cluster_node`, `delivery_endpoint`, `endpoint_delivery_status`
- `workflow_definition`, `workflow_revision`, `cluster_forward_outcome`
- Identity tables (`identity_persona`, `identity_user`, `identity_contact`, `identity_channel_profile`, `identity_policy_profile`, `identity_relationship`, `identity_relationship_version`, `identity_entity_update_proposal`, `identity_relationship_transition`)

Identity rows store the domain record as JSON `payload` plus the columns used in filters (relationship endpoints, status, kind; proposal entity type, entity id, state, created time).

Version 2 creates `runtime_safety_ledger`, the historical one-row snapshot. Version 3 creates the live runtime tables and leaves the ledger table in place only so an existing row can be imported:

- `runtime_job`, `runtime_epoch`, `runtime_attempt`, `runtime_event`
- `runtime_receipt`, `runtime_cancellation`, `runtime_cleanup`, `runtime_staged`
- `runtime_wait`, `runtime_signal`, `runtime_continuation`, `runtime_recurring`
- `runtime_id_alloc` (one row, `id = 1`)

Indexes cover the next runnable job (`queue, state, scheduled_at, priority, job_id`), expired leases (`state, lease_expires_at`), pending outbox order, waits and signals by key, and receipt/cleanup retention. Each `payload` is limited to 1 MiB. There is no limit on how many rows the database holds.

Version 4 creates the Locus tables used by `.with_locus_memory()` on SQLite and on Durable Objects. Native SQLite reads and writes them with `locus-sqlite-adapter` 0.1.0. Durable Objects use the `SqlExecutor` store. Timestamps in these tables are unix milliseconds, matching the adapter:

- `temporal_node`, `calibration`, `sync_checkpoint`, `semantic_tag_index`

Identity rows from version 1 are read by `SqliteIdentityMemoryStore`, which the SQLite runtime attaches when the builder does not set another identity store.

## Durability

Runtime operations (insert, claim, heartbeat, complete, cancel, recover, replay, waits, continuations, cleanup, lifecycle delivery, id allocation) run in one `BEGIN IMMEDIATE` transaction. The transaction loads the indexed rows that transition needs, runs the in-memory transition, and compare-and-swaps each changed payload. A mismatch or a failed commit rolls the transition back. Heartbeats require an execution grant. Inbound accept uses `INSERT ... ON CONFLICT(idempotency_key) DO NOTHING` and returns the original receipt. Resource fences follow the Surreal outcomes: stale fences, expired leases, and duplicate inbound keys do not take over.

`RetentionPolicy` defaults are 30 days for terminal jobs, completion receipts, and cleanup records. Pruning a terminal job does not delete its receipt. A duplicate completion returns the original receipt while that receipt is retained.

Turn waits stay process-local, matching the Surreal runtime.

## Cloudflare Durable Objects

The same DDL is applied by `DurableSqliteKernel` on wasm32. Workers storage cannot run `BEGIN` or `PRAGMA`, so those stay out of `schema.rs` and the kernel uses `SqlExecutor::transaction` (`ctx.storage.transactionSync`). Integer columns are selected with `CAST(col AS TEXT)` because JavaScript numbers cannot hold nanosecond timestamps. See [Workers Durable Object SQLite](./workers-durable-sqlite.md).
