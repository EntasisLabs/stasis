# Correction evidence — rejected Stasis foundation `75eec078`

> Historical report. Raw logs and original manifests now live outside Git; see [archive location and verification](README.md#exact-preserved-audit-evidence). Evidence names below resolve in the archived `snapshot/foundation-evidence/` tree, with short final-run names in `review-75eec/`. Historical inventory checks describe the archived candidate, not the cleanup tree. The exact original report is preserved at the same archive-relative path.

Same isolated undertaking: `work-e2e8d519551b99d956eedbf656484a03`.
Original base: `a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2`.
Rejected pin: `75eec0783c0b6aa10d9faaac2edaa67b70a0d271`.
Fresh review pin: the exact isolated commit containing this report (`git rev-parse HEAD`), returned in the handback. **Sender retains independent review. Nothing is approved, merged or integrated.**

## Complete independent evidence retained byte-for-byte

All three supplied files were read, hash-checked and retained without source edits under `review-75eec/`:

| Retained file | SHA-256 |
|---|---|
| `report.md` | `190a171e22c7bfcd615681bc526a0a72cb8fe48a6b241dc8d5e01477466c02b4` |
| `reviewer_probe.rs` | `d75a3aad89946443f3eae334287c6b2ab0d339d3b68b7bffcfde864467264989` |
| `independent-probes.log` | `874bec59b64ad5424fe74fc9c7cee8e8a0224a79c6dde53625754eccf13056e0` |

The original `3d712afe` evidence/audit is retained unchanged. `review-75eec/previous-correction-report.md` preserves the preceding report verbatim, including its now-corrected signal wording. `previous-correction-source.sha256` and `previous-evidence.sha256` are historical manifests for the rejected 75eec tree, not current-tree verification substitutes.

## Native changes and regression mapping

### P1 — materialization cannot manufacture execution authority

`Job.normalize_new` validates a nonauthoritative new Enqueued specification: no running/terminal state, output, attempts, started/finished/error/progress publication. It clears caller epoch/version/incarnation/owner/lease/heartbeat/wakeup authority. Both stores apply it at insertion, staging and continuation child materialization. Native incarnation issuance occurs separately; native acquisition alone grants execution authority. Surreal also revalidates persisted staged children **inside** atomic publication and explicitly resets authority fields at canonical creation.

Both-store authority matrices exercise lower public native APIs directly: forged Running/terminal/output/attempt/progress specifications fail before canonical writes; new Enqueued forged epoch500/version7/caller leases are normalized at insertion, stage/commit and continuation settlement. The resulting child cannot commit before real native acquisition, and can commit after that acquisition. A real Mem transaction test models a malformed persisted staged row from the rejected writer: publication fails without parent disposition, child, outbox or receipt; a valid new specification subsequently materializes with normalized authority. Existing rollback/staging/cleanup-only suites remain in the full run.

### P1 — stable wait incarnation and delayed dispatch

`DurableWaitRecord.incarnation` and production wait identity include the stable native job incarnation, not epoch. Both native poll boundaries validate it. Cancellation/failure and delayed hook/wake operations target captured incarnation. Signals capture stable known recipients before publication in `target_incarnations`; identity/deduplication includes those recipients. A delayed signal completion/wakeup cannot act on a replacement. Previously consumed unbound buffered signals are excluded for retired incarnations; unbound events with no known recipient/history retain the documented buffered-event API.

Memory/actual Mem matrices retain Cancelled, failed, Signaled, TimedOut and Pending wait states, delete/recreate and require independent Pending/Deferred B. They inject delayed A-scoped signal/wait completion/wakeup, drain old controls, and require B unchanged and still waiting; a genuine B signal succeeds and duplicates are ignored. The original signal/lease matrices require same-incarnation Deferred/reacquisition to preserve the wait/signal and allow the new owner to consume it. Ambiguous legacy zero-incarnation waits fail closed; tests reconcile only using their original proved identity.

### P1 — continuation incarnation and settlement after deletion

`JobContinuation.parent_incarnation` is captured at registration, fenced at staging, filtered before delivery and verified at native settlement. Exact retained terminal receipt snapshots authorize settlement of deleted/replaced parents. B's outcome cannot dispose of A's edge; no surviving current row is required for committed A work. Child creation and edge disposition remain one atomic boundary. Failed native settlement does not acknowledge an unresolved delivery obligation; ambiguous legacy edges fail without acknowledgement.

Memory/actual Mem matrices test B success/failure × old/new receipt order, deletion of both parents before settlement, correct A/B edge disposition, normalized child authority and idempotent replay. The immutable independent probes use real ctx.continue_with/native commit/replay. A new **actual SurrealKV graceful close/reopen** test retains deleted A's progress/receipt/edge and old wait, recreates B, reopens, applies delayed A controls, fails/deletes B and requires A's child to materialize exactly once from retained evidence. Legacy edge tests require error and an undelivered receipt until explicitly proved reconciliation.

### P2 — effective native receipt snapshot

Surreal stores canonical `job_snapshot` in the same publication transaction, after preservation of current progress/effective wakeup schedule. Inspection and lifecycle delivery use that immutable effective snapshot. `receipt_json` retains the submitted-publication representation as historical fallback; the original submitted-outcome digest is not altered. Tests compare the complete effective snapshot with canonical success, require same-digest replay and changed-digest conflict, and retain progress/same-digest identity after deletion/KV reopen. The original effective Deferred schedule/lifecycle assertions still pass.

`tests/native_incarnation_corrections.rs` includes the byte-identical independent probe rather than modifying it. Its **40 native / 18 memory** test executions comprise the original 22/10 correction cases included by that probe, eight/four independent findings, and ten/four additional matrices. Those reused original cases are deliberately rerun, **not 40 new unique test scenarios**.

## Rejected-pin reproduction and fresh result

`review-75eec/reproduced-rejected.log`: compilation against unchanged exact rejected product sources succeeds, then **1 passes / 7 fail / 22 filtered**, executable exit **101**. This reproduces every reported both-store P1 and actual Mem P2. It is not a compile failure.

```sh
export PATH=/Users/theelevators/.cargo/bin:$PATH TMPDIR=/tmp
export CARGO_TARGET_DIR="$PWD/foundation-evidence/target"
old=$(mktemp -d /tmp/stasis-correction-rejected-75eec.XXXXXX)
git archive 75eec0783c0b6aa10d9faaac2edaa67b70a0d271 | tar -x -C "$old"
cp foundation-evidence/review-75eec/reviewer_probe.rs "$old/tests/reviewer_probe.rs"
cargo test --manifest-path "$old/Cargo.toml" --offline --locked --no-default-features --features surreal-native -p stasis-rs --test reviewer_probe review_ -- --nocapture
```

The recorded rerun used the supplied disposable `/tmp/stasis-independent-75eec-probe` with exact archived product and original manifests/lock. Fresh `review-75eec/fresh-independent.log` has **8 pass / 0 fail**, and the final supported suite below reruns the same probes alongside all matrices.

## Final supported locked/offline commands and results

Unchanged original `Cargo.toml`/`Cargo.lock`; cached Surreal 3.0.5; Mac Rust 1.98.0. `TMPDIR=/tmp` avoids the sandbox-denied default temp directory without permissions changes.

```sh
export PATH=/Users/theelevators/.cargo/bin:$PATH TMPDIR=/tmp
export CARGO_TARGET_DIR="$PWD/foundation-evidence/target"
cargo test --offline --locked --no-default-features --features surreal-native -p stasis-rs --lib --test native_execution_authority --test native_completion_atomicity --test native_commit_clock --test typed_job_framework --test typed_jobs_signals_leases --test job_lifecycle_recovery --test federated_job_contract --test inbound_job_triggers --test native_foundation_corrections --test native_incarnation_corrections
cargo test --offline --locked --no-default-features -p stasis-rs --lib --test native_execution_authority --test native_completion_atomicity --test native_foundation_corrections --test native_incarnation_corrections
cargo check --offline --locked --no-default-features --features surreal-native,llm-genai -p stasis-rs
rustfmt --edition 2024 --check $(git diff --name-only 3d712afe -- src tests | grep '\.rs$')
git diff --check 75eec0783c0b6aa10d9faaac2edaa67b70a0d271 -- src tests docs
git -c core.whitespace=-blank-at-eof diff --check 75eec0783c0b6aa10d9faaac2edaa67b70a0d271
git diff --exit-code a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2 HEAD -- Cargo.toml Cargo.lock
shasum -a 256 -c foundation-evidence/correction-source.sha256
shasum -a 256 -c foundation-evidence/evidence.sha256
```

- `review-75eec/full-native.log`: **253 passing executions**, including actual Mem transactions, original atomic rollback/authority/staging/cleanup-only tests and all actual KV close/reopen cases. Per target: 135,9,7,5,1,10,4,22,40,11,9.
- `review-75eec/full-memory.log`: **155 passing executions**: 121,4,2,10,18. Existing unused-import warning remains visible.
- `review-75eec/full-check.log`: supported native/LLM feature compilation only, no LLM execution.
- `review-75eec/final-fmt.log` is the preceding one-line claim, retained unchanged. Its blanket whitespace-success wording is not sufficient: the subsequent whole-tree strict check failed on terminal blank lines in preserved raw logs. Recovery verification distinguishes strict source/docs whitespace from the whole-tree per-command EOF exception; see `review-75eec/recovery-checks.log`. `correction-source.sha256` binds final source/tests/API docs and manifests; `evidence.sha256` binds textual evidence, including unchanged original audits.
- Raw Cargo/independent logs intentionally retain their terminal blank lines byte-for-byte. Standard source whitespace checks pass; the whole evidence diff uses a per-command `-blank-at-eof` exception rather than editing immutable logs.
- `review-75eec/intermediate-*` logs retain fixed development compile/assertion failures. They are not final results; the final full suites above pass.

## Recovery/finalization only

The prior executor terminated before committing. Its final CORRECTION.md whitespace clarification followed manifest generation: on recovery BOTH manifests failed only the report hash, while all listed source/tests/API/dependency and raw evidence files matched. The README's historical whitespace-normalization sentence was also clarified to exclude byte-exact current evidence. Recovery makes only these documentation/manifest finalization repairs, retains every earlier raw log (including the overly broad final-fmt claim), and does not repeat correction implementation. The recorded 253 native / 155 memory executions and eight passing probes are preceding executor results inspected in their raw logs, not fresh independent acceptance. Recovery checks compare all seven retained independent files directly with their supplied /tmp originals, preserve historical reports/manifests, verify unchanged dependency files and exact source/test bytes, and rerun scoped formatting/whitespace checks. Final staged-byte inspection also found fourteen historical raw logs listed in evidence.sha256 that existed only as ignored local files. Their existing manifest hashes all verified; recovery retains those exact bytes in Git so the committed evidence manifest is self-contained rather than dependent on this machine's ignored files. No historical log or product source is rewritten. Sender still owns fresh independent review.

## Proof wording and migration limits

The signal barrier pauses a **constructed commit task**, not a registered active handler/automatic typed consumer. It uses real ctx.wait_for, signal_event, heartbeat, native commit and receipt replay. Product regression comment and this current report use that precise wording; immutable independent/historical evidence is not edited. The old-3d712afe save/reload seam models historical save/unrestricted-reload semantics because the helper did not exist; it is **not black-box instrumentation of the old runtime**. Fresh reload tests call the exact production helper, and original automatic stale-A tests still run.

API and quiescent migration requirements are in `docs/native-execution-ownership.md`. New waits/edges carry explicit incarnation; absent legacy identity decodes as zero and fails closed. Reconcile only with provable retained history, or explicitly quarantine/archive ambiguity through the existing authorized operator route before resuming those IDs. No automatic name-based adoption or migration execution is claimed. Old receipts lacking an effective snapshot retain their historical fallback; absent historical progress is not invented. Keep receipts/generation tombstones while replay/continuation references require them. External hook effects require durable sink-side idempotency.

Inspect `git diff 75eec0783c0b6aa10d9faaac2edaa67b70a0d271 HEAD` for this correction and `git diff a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2 HEAD` for the full isolated proposal. Sender owns the fresh independent review decision.

No parent CI commands/changes, network/downloads, provisioning, credentials, permissions changes, deployment, publishing, self-approval or merge occurred. Frozen `/Users/theelevators/rust-ci-engine` `b87875eec9da2629df23c58505bdf024f4058fa4` was not accessed or altered by this correction. No MVP, Linux, remote Surreal, WASM, minimum-toolchain, power-loss or physical cleanup acceptance is claimed. Signal insertion/completion/wakeup are separate calls; graceful KV reopen is not crash/power-loss atomicity. Existing broader backend-parity/minimal-feature and newer uncached standalone dependency limitations remain unchanged.
