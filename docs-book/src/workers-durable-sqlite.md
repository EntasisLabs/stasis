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
| Outbox, durable waits and signals, continuations, recurring jobs | Not on this backend |
| Threads, cluster nodes, delivery endpoints, identity memory | Not on this backend |
| Turn waits | Process-local, same as Surreal and native SQLite |
| Grapheme script execution inside the lease | No. The kernel stores the checkpoint. `StasisWasmClient` still runs `grapheme-wasm` in memory |

This branch stays on `main` and keeps a small `DurableSqliteKernel` for the Worker demo. Draft PR #29 (`cursor/store-backed-runtime-d34f`) moves orchestration into one `StoreRuntime<J, R, O, A, W, Cn, I, L>`. The executor stores below already implement the ports that engine takes, so they can be passed to `StoreRuntime::from_stores` after that PR merges. See [Wiring into StoreRuntime](#wiring-into-storeruntime).

## Design

Durable Object storage handles are not `Send`. The store traits stay `Send + Sync`, so the kernel does not take `worker::SqlStorage` (that type holds a JavaScript value). The wasm guest keeps the `exec` and `transaction` functions in a thread-local, the same way host tools keep callbacks, and the executor type is an empty `Send` struct. `sql.exec` is synchronous, so those futures never hold a JavaScript value across an await.

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

`StoreRuntime` (draft PR #29, branch `cursor/store-backed-runtime-d34f`) is generic over eight store ports. A backend builds them and calls `StoreRuntime::from_stores`. Store traits on that branch use `StoreBound` and `stasis::async_store!`: `Send + Sync` and `#[async_trait]` on native, no auto-trait bound and `#[async_trait(?Send)]` on `wasm32`. Mixing `#[async_trait]` with `#[async_trait(?Send)]` does not compile, so every impl of those traits has to use the same macro.

On this branch the executor stores still use `#[async_trait]`, matching `main`. After #29 merges, switch these impls to `stasis::async_store!`:

- `ExecutorJobStore` (`JobStore`)
- `ExecutorJobAttemptStore` (`JobAttemptStore`)
- `ExecutorInboundTriggerStore` (`InboundTriggerStore`)
- `ExecutorResourceLeaseStore` (`ResourceLeaseStore`)

`JsSqlExecutor` stays `Send`. The JavaScript functions live in a thread-local, and the struct does not hold a `JsValue`. The `?Send` attribute is what the wasm trait requires; it also leaves room for a later store that keeps the Durable Object handle inside the struct.

`ExecutorWorkflowDefinitionStore` implements `WorkflowDefinitionStore`. That port is not one of the eight. Keep it beside the engine. Grapheme `state_current` is also stored in the job payload under the `grapheme:json:` prefix.

The other four ports stay in memory until they have SQLite adapters on this executor. Those in-memory types already exist on `main`:

`store_runtime` arrives with #29. The sketch below is the constructor to add then. On the Durable Object, `ids` is the `stasis_id_seq` generator (`SequenceIds` in `durable.rs`). A process-local `AtomicIdGenerator` repeats after eviction. `exec` is `JsSqlExecutor` in the Worker and `RusqliteExecutor` in the native tests.

```rust
use std::sync::Arc;

use stasis::application::runtime::in_memory_runtime::{
    InMemoryOutboxStore, InMemoryRecurringStore,
};
use stasis::application::runtime::store_runtime::StoreRuntime;
use stasis::infrastructure::runtime::in_memory_durable_wait_store::InMemoryDurableWaitStore;
use stasis::infrastructure::runtime::in_memory_job_continuation_store::InMemoryJobContinuationStore;
use stasis::infrastructure::sqlite::{
    ExecutorInboundTriggerStore, ExecutorJobAttemptStore, ExecutorJobStore,
    ExecutorResourceLeaseStore, SqlExecutor,
};
use stasis::ports::outbound::runtime::clock::Clock;
use stasis::ports::outbound::runtime::id_generator::IdGenerator;
use stasis::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use stasis::ports::outbound::runtime::runtime_tracing::RuntimeTracing;

fn runtime_from_executor(
    exec: Arc<dyn SqlExecutor>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    metrics: Arc<dyn RuntimeMetrics>,
    tracing: Arc<dyn RuntimeTracing>,
) -> StoreRuntime<
    ExecutorJobStore,
    InMemoryRecurringStore,
    InMemoryOutboxStore,
    ExecutorJobAttemptStore,
    InMemoryDurableWaitStore,
    InMemoryJobContinuationStore,
    ExecutorInboundTriggerStore,
    ExecutorResourceLeaseStore,
> {
    StoreRuntime::from_stores(
        ExecutorJobStore::new(exec.clone()),
        InMemoryRecurringStore::default(),
        InMemoryOutboxStore::default(),
        ExecutorJobAttemptStore::new(exec.clone()),
        InMemoryDurableWaitStore::default(),
        InMemoryJobContinuationStore::default(),
        ExecutorInboundTriggerStore::new(exec.clone()),
        ExecutorResourceLeaseStore::new(exec.clone()),
        clock,
        ids,
        metrics,
        tracing,
    )
}
```
