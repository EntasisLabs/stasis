# Workers Durable Object SQLite

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-10-12
- Verified Against:
    - src/infrastructure/sqlite/schema.rs
    - src/infrastructure/sqlite/durable.rs
    - src/infrastructure/sqlite/executor.rs
    - stasis-wasm/src/durable_sqlite.rs
    - examples/cloudflare-durable-object/wrangler.toml
    - examples/cloudflare-durable-object/src/index.js

## Purpose

Run the Stasis wasm kernel on Cloudflare Workers and keep the durable job path in the Durable Object's built-in SQLite (`ctx.storage.sql`). This is an alternative to the in-memory `StasisWasmClient` and to remote SurrealDB over WebSocket. It does not replace the native `sqlite` feature, which still links rusqlite and stays off wasm32.

## What is durable

| Surface | Durable Object SQLite |
| --- | --- |
| Jobs, including payload, grant, incarnation and lease columns | Yes, in `runtime_job` |
| Heartbeat of an execution grant | Yes |
| Fenced resource leases (acquire, renew, stale fence, expiry) | Yes |
| Job attempts | Yes |
| Inbound idempotency keys | Yes |
| Grapheme workflow graph state (`workflow_definition` / `workflow_revision`, including `state_current` from a `grapheme:json:` payload) | Yes |
| Outbox, durable waits and signals, continuations, recurring jobs, receipts, cleanup | Yes, in the `runtime_*` tables |
| Threads, cluster nodes, delivery endpoints, identity memory | Not on this backend |
| Turn waits | Process-local, same as Surreal and native SQLite |
| Grapheme script execution inside the lease | The checkpoint handler stores `state_current`. `StasisWasmClient` still runs `grapheme-wasm` in memory |

`DurableSqliteKernel::open` builds a `StoreRuntime` and registers a success handler for `durable.job` and `grapheme.workflow`. See [Wiring into StoreRuntime](#wiring-into-storeruntime).

## Design

Durable Object storage handles are not `Send`. Store traits use `StoreBound` and `stasis::async_store!`: `Send + Sync` on native, no auto-trait bound and `#[async_trait(?Send)]` on `wasm32`. The kernel still does not take `worker::SqlStorage`. The wasm guest keeps the `exec` and `transaction` functions in a thread-local, the same way host tools keep callbacks, and `JsSqlExecutor` is an empty `Send` struct. `sql.exec` is synchronous, so those futures never hold a JavaScript value across an await. A later store can keep the handle inside the struct because the wasm trait allows `!Send`.

Schema DDL is `MIGRATIONS` from `schema.rs`. Durable Object adjustments stay outside that file:

- Statements are applied one at a time. `BEGIN`, `COMMIT`, `SAVEPOINT`, and `PRAGMA` are rejected. Atomic claim and lease updates go through `SqlExecutor::transaction`, which the Worker maps to `ctx.storage.transactionSync`.
- Integer columns are read with `CAST(col AS TEXT)`. Nanosecond timestamps do not fit in a JavaScript number. Parameters travel as decimal strings and SQLite INTEGER affinity stores them as 64-bit integers.
- Runtime ids come from `runtime_id_alloc` (schema version 3) as `{prefix}-{n}`. A process-local counter would repeat after the isolate is evicted. `stasis_id_seq` remains for `next_sequence` and is read during upgrade so new ids do not reuse an old counter.
- Each runtime operation is one SQL transaction over the rows it needs. Updates compare-and-swap the previously read payload. A row payload above 1 MiB is refused. There is no cap on the number of rows.
- Opening the kernel runs the version 3 import. A v1 database re-enqueues in-flight leases and copies auxiliary rows. A format-1 `runtime_safety_ledger` is copied as stored, including an unexpired grant. Both sources populated, or a row the import cannot reconcile, fails the open and does not advance the schema version. Back up the database first. Details are in [store-runtime-safety-integration.md](../../docs/store-runtime-safety-integration.md).

Compare-and-swap uses `RETURNING`, not `rowsWritten`. Workers counts index maintenance in `rowsWritten`, so a one-row update can report more than one.

## Example Worker

The example is `examples/cloudflare-durable-object`. Build the bundler package first:

```bash
npm --prefix stasis-wasm run build
cd examples/cloudflare-durable-object
npx wrangler dev
```

`wrangler.toml` binds `STASIS` to `StasisDurableObject` and migrates it with `new_sqlite_classes`. One named object (`idFromName("stasis")`) holds the database.

Enqueue an idempotent job, process it, evict the in-memory kernel, and read the row back from SQLite:

```bash
curl -s -X POST localhost:8787/jobs \
  -H 'content-type: application/json' \
  -d '{"idempotencyKey":"order-17","payload":"ship-it"}'

curl -s -X POST localhost:8787/jobs \
  -H 'content-type: application/json' \
  -d '{"idempotencyKey":"order-17","payload":"ship-it-again"}'

curl -s -X POST localhost:8787/process \
  -H 'content-type: application/json' \
  -d '{"workerId":"worker-a"}'

curl -s -X POST localhost:8787/evict
curl -s localhost:8787/jobs/JOB_ID
```

The second enqueue returns `disposition: "duplicate"` and the original `jobId`. `/evict` drops the wasm kernel object and opens a new one on the same `ctx.storage.sql`. The job is still `succeeded`.

A Grapheme checkpoint uses the same job payload the native handler accepts. After process and evict, `GET /workflows/wf-1` returns `state_current`:

```bash
curl -s -X POST localhost:8787/jobs \
  -H 'content-type: application/json' \
  -d '{"idempotencyKey":"wf-1","payload":"grapheme:json:{\"source\":\"op main()\",\"workflow_id\":\"wf-1\",\"state_current\":{\"step\":2}}"}'
```

`POST /workflows` writes a checkpoint without enqueueing a job.

Store logic is tested on native rusqlite (`tests/durable_object_sqlite.rs`) and through the JavaScript bridge against `node:sqlite` (`stasis-wasm/test/durable-sqlite.mjs`), including a kernel reconstructed against the same database. `node:sqlite` binds `?1` as the named key `"1"`. The test adapter maps the kernel's numbered placeholders onto that object. Durable Object `sql.exec(sql, ...bindings)` binds the same placeholders in order.

## Wiring into StoreRuntime

`DurableSqliteKernel::open` is the backend constructor. The four SQLite stores use `stasis::async_store!`, matching the trait definitions. Mixing `#[async_trait]` with `#[async_trait(?Send)]` does not compile.

| Port | Store |
| --- | --- |
| `JobStore` | `ExecutorJobStore` |
| `JobAttemptStore` | `ExecutorJobAttemptStore` |
| `InboundTriggerStore` | `ExecutorInboundTriggerStore` |
| `ResourceLeaseStore` | `ExecutorResourceLeaseStore` |
| `RecurringStore` | `ExecutorRecurringStore` |
| `OutboxStore` | `ExecutorOutboxStore` |
| `DurableWaitStore` | `ExecutorDurableWaitStore` |
| `JobContinuationStore` | `ExecutorJobContinuationStore` |

`ExecutorWorkflowDefinitionStore` implements `WorkflowDefinitionStore`, which is not one of the eight ports. The checkpoint handler writes it when a `grapheme:json:` payload is processed. `state_current` also stays in the job payload.

`ids` is `DurableIds`, which allocates `{prefix}-{n}` from `runtime_id_alloc`. A process-local `AtomicIdGenerator` repeats after eviction. `exec` is `JsSqlExecutor` in the Worker and `RusqliteExecutor` in the native tests. `process_once` is `StoreRuntime::process_once_now`. A successful job leaves `job.attempts` at 0 and writes one attempt row; the engine increments `attempts` on retry and dead-letter. Heartbeats use the execution grant from the leased snapshot. An owner name alone is not authority.

```rust
StoreRuntime::from_stores(
    ExecutorJobStore::new(exec.clone()),
    ExecutorRecurringStore::new(exec.clone()),
    ExecutorOutboxStore::new(exec.clone()),
    ExecutorJobAttemptStore::new(exec.clone()),
    ExecutorDurableWaitStore::new(exec.clone()),
    ExecutorJobContinuationStore::new(exec.clone()),
    ExecutorInboundTriggerStore::new(exec.clone()),
    ExecutorResourceLeaseStore::new(exec),
    clock,
    ids,
    metrics,
    tracing,
)
```
