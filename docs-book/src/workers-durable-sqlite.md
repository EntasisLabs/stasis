# Workers Durable Object SQLite

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-10-11
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
| Jobs, including payload and lease columns | Yes |
| Job-lease heartbeat (only the current owner) | Yes |
| Fenced resource leases (acquire, renew, stale fence, expiry) | Yes |
| Job attempts | Yes |
| Inbound idempotency keys | Yes |
| Grapheme workflow graph state (`workflow_definition` / `workflow_revision`, including `state_current` from a `grapheme:json:` payload) | Yes |
| Outbox, durable waits and signals, continuations, recurring jobs | In memory, as the `StoreRuntime` fillers |
| Threads, cluster nodes, delivery endpoints, identity memory | Not on this backend |
| Turn waits | Process-local, same as Surreal and native SQLite |
| Grapheme script execution inside the lease | The checkpoint handler stores `state_current`. `StasisWasmClient` still runs `grapheme-wasm` in memory |

`DurableSqliteKernel::open` builds a `StoreRuntime` and registers a success handler for `durable.job` and `grapheme.workflow`. See [Wiring into StoreRuntime](#wiring-into-storeruntime).

## Design

Durable Object storage handles are not `Send`. Store traits use `StoreBound` and `stasis::async_store!`: `Send + Sync` on native, no auto-trait bound and `#[async_trait(?Send)]` on `wasm32`. The kernel still does not take `worker::SqlStorage`. The wasm guest keeps the `exec` and `transaction` functions in a thread-local, the same way host tools keep callbacks, and `JsSqlExecutor` is an empty `Send` struct. `sql.exec` is synchronous, so those futures never hold a JavaScript value across an await. A later store can keep the handle inside the struct because the wasm trait allows `!Send`.

Schema DDL is `MIGRATIONS` from `schema.rs`. Durable Object adjustments stay outside that file:

- Statements are applied one at a time. `BEGIN`, `COMMIT`, `SAVEPOINT`, and `PRAGMA` are rejected. Atomic claim and lease updates go through `SqlExecutor::transaction`, which the Worker maps to `ctx.storage.transactionSync`.
- Integer columns are read with `CAST(col AS TEXT)`. Nanosecond timestamps do not fit in a JavaScript number. Parameters travel as decimal strings and SQLite INTEGER affinity stores them as 64-bit integers.
- Job and attempt ids come from `stasis_id_seq`, a table created next to the shared schema. A process-local counter would repeat after the isolate is evicted. That table is not part of `MIGRATIONS`.

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
| `RecurringStore` | `InMemoryRecurringStore` |
| `OutboxStore` | `InMemoryOutboxStore` |
| `DurableWaitStore` | `InMemoryDurableWaitStore` |
| `JobContinuationStore` | `InMemoryJobContinuationStore` |

`ExecutorWorkflowDefinitionStore` implements `WorkflowDefinitionStore`, which is not one of the eight ports. The checkpoint handler writes it when a `grapheme:json:` payload is processed. `state_current` also stays in the job payload.

`ids` is the `stasis_id_seq` generator (`SequenceIds` in `durable.rs`). A process-local `AtomicIdGenerator` repeats after eviction. `exec` is `JsSqlExecutor` in the Worker and `RusqliteExecutor` in the native tests. `process_once` is `StoreRuntime::process_once_now`. A successful job leaves `job.attempts` at 0 and writes one attempt row; the engine increments `attempts` on retry and dead-letter.

```rust
StoreRuntime::from_stores(
    ExecutorJobStore::new(exec.clone()),
    InMemoryRecurringStore::default(),
    InMemoryOutboxStore::default(),
    ExecutorJobAttemptStore::new(exec.clone()),
    InMemoryDurableWaitStore::default(),
    InMemoryJobContinuationStore::default(),
    ExecutorInboundTriggerStore::new(exec.clone()),
    ExecutorResourceLeaseStore::new(exec),
    clock,
    ids,
    metrics,
    tracing,
)
```
