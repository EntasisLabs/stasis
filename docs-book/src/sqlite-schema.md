# SQLite Schema

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, SRE, Architect
- Stability: Stable
- Last Verified: 2026-10-11
- Verified Against:
    - src/infrastructure/sqlite/schema.rs
    - src/infrastructure/sqlite/session.rs
    - src/application/runtime/sqlite_runtime.rs
    - src/infrastructure/sqlite/durable.rs
    - examples/cloudflare-durable-object/wrangler.toml

## Purpose

Native SQLite is a durable alternative to SurrealDB for single-process Stasis runtimes. The DDL is the reuse boundary for a later Cloudflare Durable Object / wasm SQLite store: `schema.rs` has no rusqlite types.

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

## Tables

Version 1 creates the same durable surfaces as the Surreal backend:

- `job`, `outbox_event`, `job_attempt`, `durable_wait`, `durable_signal`, `job_continuation`
- `recurring_definition`, `inbound_trigger` (primary key `idempotency_key`), `resource_lease`
- `thread`, `thread_event`, `cluster_node`, `delivery_endpoint`, `endpoint_delivery_status`
- `workflow_definition`, `workflow_revision`, `cluster_forward_outcome`
- Identity tables (`identity_persona`, `identity_user`, `identity_contact`, `identity_channel_profile`, `identity_policy_profile`, `identity_relationship`, `identity_relationship_version`, `identity_entity_update_proposal`, `identity_relationship_transition`)

Identity rows store the domain record as JSON `payload` plus the columns used in filters (relationship endpoints, status, kind; proposal entity type, entity id, state, created time).

## Durability

Lease claims, continuation claims, and resource-lease mutations run in `BEGIN IMMEDIATE` transactions. Inbound accept uses `INSERT ... ON CONFLICT(idempotency_key) DO NOTHING` and returns the original receipt. Job heartbeats update only the current lease owner. Recurring leases and resource fences follow the Surreal outcomes: stale fences, expired leases, and duplicate inbound keys do not take over.

Turn waits stay process-local, matching the Surreal runtime.

## Cloudflare Durable Objects

The same DDL is applied by `DurableSqliteKernel` on wasm32. Workers storage cannot run `BEGIN` or `PRAGMA`, so those stay out of `schema.rs` and the kernel uses `SqlExecutor::transaction` (`ctx.storage.transactionSync`). Integer columns are selected with `CAST(col AS TEXT)` because JavaScript numbers cannot hold nanosecond timestamps. See [Workers Durable Object SQLite](./workers-durable-sqlite.md).
