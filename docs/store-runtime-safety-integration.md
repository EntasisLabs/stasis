# StoreRuntime safety integration candidate

This is a **review candidate**, not deployment or safety acceptance. It starts at
`67226799252040f7a51e1f49b6eb8d11291adc1e`, and semantically ports the reviewed
native safety source `6f9372e6fbb2ec26946c656d76ec83d0678d2f9a` without importing its
historical generated evidence or restoring duplicated orchestration engines.

## Architecture and transaction contract

`StoreRuntime` is the single orchestration engine. Memory, Surreal and native
SQLite wrappers select stores and defaults only. Native stores implement granted
publication, fixed-outcome recovery, cancellation, replay, retained completion
receipts, cleanup observations/acknowledgements, stable incarnation wakeups and
terminal-cycle continuation isolation. The native/WASM async macro split remains:
native futures are Send; wasm store/handler futures can be ?Send.

The SQLite/DO safety adapters execute the same reference state transitions under
one SQL transaction. The canonical format-1 `runtime_safety_ledger` contains jobs,
attempts, staged children/events/continuations, completion/control receipts,
cleanup records, epochs/incarnations, waits and signals. A failed SQL write or
commit rolls back the whole transition. Completion checks the canonical
owner/epoch/version and trusted expiration, ignores mutable worker identity in
publication, and rechecks admission after the ledger write before commit.
Duplicate completions retain the original receipt rather than emitting effects
again. Grant-checked wait registration is a durable subscription operation before
parking (the reviewed signal/wakeup protocol); subsequent completion, recovery
and cancellation changes to that subscription participate in the job transaction.
It is not an arbitrary transaction around handler side effects.

The default SQLite and DO ID allocator persists its sequence in this ledger.
Callers propagate allocation errors; there is no fixed-string or volatile fallback.
Insertion is collision-rejecting, not an upsert. Explicit caller-provided IDs can
still collide and will be rejected rather than overwrite a job.

## Supported profiles and explicit restrictions

- **Memory:** coherent shared NativeState store views, intentionally volatile.
- **Surreal Mem/KV:** the reviewed native transaction/receipt contracts, driven by
  the shared engine; legacy identity reconciliation requirements remain.
- **Native SQLite:** synchronous reference transitions in a rusqlite transaction;
  files retain IDs and the safety ledger across close/reopen.
- **DO SqlExecutor:** durable job, attempt, outbox, recurring, wait/signal and
  continuation adapters. Inbound keys, resource leases and workflow definitions
  retain their existing durable SQL adapters. There are no volatile runtime
  auxiliary fillers in this composition.

This is a **correctness-first snapshot ledger**, not a high-throughput row-level
SQLite implementation. Every operation loads the ledger; mutations serialize it.
The executor/DO profile rejects snapshots larger than **1 MiB** before committing.
There is no fallback to memory. Unbounded histories, large-payload orchestration,
large tenants and sustained write throughput require a later row-level adapter
and performance validation; do not advertise this profile for those workloads.
Native SQLite does not enforce the DO size cap, but shares the linear-cost design.
Use retained receipts deliberately; do not truncate them to evade the cap.

The synchronous `DurableSqliteKernel` accepts immediately-ready handlers. A
handler that suspends returns an explicit unsupported error, not a successful
completion; the acquired lease needs recovery. Async callers can use its returned
shared runtime. Owner-name-only heartbeat is retired; use an execution grant.
The JS bridge requires actual synchronous transaction/rollback behavior. A missing
transaction callback or a commit exception is an error.

The WASM executor is pinned to the generation of the installed SQL bridge. Installing
another bridge invalidates older kernels: they fail closed instead of writing to
a different object's storage. Reconstruct after installation, or use distinct
WASM module instances for concurrently resident DO identities. This is an explicit
restriction of the current global bridge, not claimed multi-object isolation.

Workflow graph state persistence remains a separate feature: checkpoint storage
is durable, but arbitrary handler/workflow/external effects are not enrolled in
native job completion. The synchronous kernel does not execute Grapheme source.
Physical process-descendant cleanup, Linux execution and live Cloudflare deployment
are not verified or enabled by this integration.

## Migration

Migration 1 is unchanged; migration 2 adds the versioned safety ledger. Empty v1
runtime tables upgrade without inventing grants. **Populated v1 job/attempt/outbox/
wait/signal/continuation tables are rejected on every ledger operation**, including
when a ledger already exists. Unsupported ledger formats/corrupt payloads fail.
The old rows are not deleted, overwritten, or silently ignored.

An operator must preserve/export those rows and reconcile old leases, incarnation,
execution receipts and signal/continuation identities offline before using this
runtime. This candidate does not provide an automatic legacy-data reconciliation
utility. Back up the original database; do not delete rows merely to clear the
error. Raw SQL consumers of the old runtime tables must migrate to the store ports:
those tables are not mirrors of the new canonical ledger. Existing non-runtime
SQL stores are unaffected by this runtime ledger restriction.

## Regression sources and evidence

The archived audit probes originally asserted defects. Their committed regression
versions assert that A cannot overwrite B, cannot add an attempt/reset retries,
and file-backed SQLite restart cannot reuse or overwrite an inbound job ID.
A trusted test clock replaces spoofed caller timestamps in those race fixtures.
The retained native review fixture is now real reusable test source in
`tests/support/native_review_fixtures.rs`, injected with memory, SQLite or executor
backends. No historical logs/checksum files are part of incoming history.

Further tests cover SQL abort/after-write rollback, idempotent retries, durable
auxiliary reconstruction, retained receipts and cleanup, and fail-closed legacy
rows. WASM/JS tests cover reconstruction, installed storage identity and host
transaction failures. Host reconstruction is not a claim of live DO eviction or
power-loss testing. Exact commands/results, failures encountered, source hashes
and disk receipts belong to the retained **outside-Git** handoff evidence, not
this source tree. Independent review and integration remain with the owner.
