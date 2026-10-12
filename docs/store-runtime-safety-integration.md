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
one SQL transaction. Each operation loads only the indexed rows that transition
reads or writes, runs the in-memory function, and flushes the diff. Updates
compare-and-swap on the previously read payload (`UPDATE ... WHERE payload = ?
RETURNING`). A mismatch or a failed commit rolls the whole transition back.
Nothing is visible before commit. Handler side effects stay outside the
transaction.

Completion checks the canonical owner/epoch/version and trusted expiration,
ignores mutable worker identity in publication, and rechecks admission after the
row writes before commit. Duplicate completions return the original receipt
rather than emitting effects again, for as long as that receipt is retained.
Staged children, events and continuations commit with the completion that
publishes them. Grant-checked wait registration is a durable subscription
operation before parking; later completion, recovery and cancellation of that
subscription participate in the job transaction.

The default SQLite and DO ID allocator persists a single counter in
`runtime_id_alloc` and returns `{prefix}-{n}`. Callers propagate allocation
errors; there is no fixed-string or volatile fallback. A failed transaction does
not consume an id. Insertion is collision-rejecting, not an upsert. Explicit
caller-provided IDs can still collide and will be rejected rather than overwrite
a job.

Hot paths use indexes: next runnable job (`queue, state, scheduled_at, priority,
job_id`), expired leases (`state, lease_expires_at`), undelivered receipts
(`delivered`), and waits by signal key. Placement is still filtered in memory
after that index range, so a lease reads the due rows for one queue, not the
whole database. List-all recurring definitions remains a full read because that
is the API. Tie-breaks are `(scheduled_at, priority, job_id)` for leases,
`(next_attempt_at or occurred_at, event_id)` for pending outbox rows, and
`(created_at, signal_id)` for signal selection.

## Retention

`RetentionPolicy` defaults are 30 days for terminal jobs, completion/control
receipts, and cleanup records (`terminal_ttl_days`, `receipt_ttl_days`,
`cleanup_ttl_days`). `enforce_retention` prunes terminal jobs, attempts and
non-pending outbox rows on the job cutoff, then deletes receipts, cancellations
and cleanup rows whose `retained_at` is set and at or before their own cutoff.
A receipt with no `retained_at` is kept. Pruning a terminal job does not delete
its receipt. Inside the receipt window, a duplicate completion still returns
the original receipt after the job row is gone. After the receipt is pruned,
that duplicate is `Stale` and does not republish effects.

## Supported profiles and explicit restrictions

- **Memory:** coherent shared NativeState store views, intentionally volatile.
- **Surreal Mem/KV:** the reviewed native transaction/receipt contracts, driven by
  the shared engine. Opening a store runs incarnation backfill and then
  `reconcile_legacy_identity` (see Migration).
- **Native SQLite:** the same row transitions inside a rusqlite `BEGIN IMMEDIATE`
  transaction. Files retain rows and the id counter across close/reopen.
- **DO SqlExecutor:** the same row schema. Inbound keys, resource leases and
  workflow definitions retain their existing durable SQL adapters. There are no
  volatile runtime auxiliary fillers in this composition.

There is no total-state size cap. Each row payload is still refused above
**1 MiB** on every backend that shares this adapter, including native SQLite,
with no memory fallback. A queue with many simultaneously due jobs still loads
that due set on each lease so placement can be applied. A debug-build sample of
500 inserts was about 45 ms; 20 leases against a due set of up to 500 jobs was
about 830 ms. Leasing one queue does not read jobs that belong to another queue.
This is not a claim of production throughput, power-loss durability, or live
Cloudflare eviction.

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

Schema version 3 creates the `runtime_*` tables and, in the same transaction,
imports whichever single legacy source is present. The import is idempotent
because it runs only while moving to version 3; a failure rolls the version
stamp and the copied rows back. After a successful import the v1 runtime rows
and the ledger row are deleted. The old tables stay in the schema so a refused
database can still be inspected. Non-runtime tables (inbound keys, leases,
workflows, threads, identity) are not part of this import.

- **v1 SQLite** (`job`, `job_attempt`, `outbox_event`, `durable_wait`,
  `durable_signal`, `job_continuation`, `recurring_definition`): Leased and
  Running jobs are re-enqueued and their lease columns cleared. They are not
  trusted. Each job receives incarnation 1, terminal cycle 1, execution epoch 0
  and version 0. No receipts are invented. Terminal jobs stay terminal.
  Attempts, events, waits, signals and continuations are copied. A wait or
  continuation whose parent job is missing is refused. Unknown state, outcome,
  outbox status, event type, wait status, or continuation trigger/status is
  refused. Signals stay unbound (`target_incarnations` empty).
- **Format-1 `runtime_safety_ledger`:** the serialized native state is copied
  as stored, including an unexpired grant. Missing `retained_at` values are
  stamped with the upgrade time so the retention window starts then. A format
  other than 1, or JSON that does not decode, is refused and not imported.
- **Both sources populated:** refused. The import will not guess which source
  is canonical.
- **Id counter:** `runtime_id_alloc` is set to the maximum of the ledger
  sequence, the old `stasis_id_seq` counters, and any `{prefix}-{n}` suffix
  already present, so new ids do not reuse old ones.
- **Durable Objects** use the same import inside `apply_schema`.

Surreal does not use this SQLite migration. `initialize_execution_schema` still
backfills incarnation 0, then `reconcile_legacy_identity` re-enqueues epoch-zero
leased/running rows, sets a missing terminal cycle to 1 only when that job has
no execution receipt, and binds incarnation-zero waits and unbound continuation
floors to that parent. It refuses a cycle-zero job that already has a receipt,
a wait or continuation with no parent, and a non-zero edge that disagrees with
the parent. It does not invent receipts, rewrite signals, or trust an epoch-zero
lease. Quiesce old writers first; mixed writers are unsupported.

Back up the database before opening it with this runtime. Raw SQL consumers of
the v1 runtime tables must move to the store ports: those tables are no longer
the canonical rows.

## Regression sources and evidence

The archived audit probes originally asserted defects. Their committed regression
versions assert that A cannot overwrite B, cannot add an attempt/reset retries,
and file-backed SQLite restart cannot reuse or overwrite an inbound job ID.
A trusted test clock replaces spoofed caller timestamps in those race fixtures.
The retained native review fixture is now real reusable test source in
`tests/support/native_review_fixtures.rs`, injected with memory, SQLite or executor
backends. No historical logs/checksum files are part of incoming history.

Further tests cover SQL abort/after-write rollback, idempotent retries, durable
auxiliary reconstruction, retained receipts and cleanup, and upgrade refusal.
`tests/row_level_equivalence.rs` runs 32 seeds of 40 operations each against
memory, native SQLite and the executor path, including lease expiry, recovery,
cancel, replay-shaped duplicate completion, and injected rollback.
`src/infrastructure/sqlite/upgrade_tests.rs` opens real v1 files, format-1
ledger files, and executor databases and then runs a job. Surreal coverage is
`tests/surreal_legacy_reconcile.rs`. WASM/JS tests cover reconstruction,
installed storage identity, host transaction failures, and the per-row 1 MiB
refusal. Host reconstruction is not a claim of live DO eviction or power-loss
testing. Independent review remains with the owner.
