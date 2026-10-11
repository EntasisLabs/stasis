# Independent safety review: changes required

> Historical independent review of a rejected revision, not current acceptance. See [external audit archive](../README.md#exact-preserved-audit-evidence) for exact original report bytes and logs; resolve relative evidence names in archived `snapshot/foundation-evidence/review-75eec/`. Original reviewer hashes apply to those archived bytes, not this annotated copy.

Pin: 75eec0783c0b6aa10d9faaac2edaa67b70a0d271
Undertaking: work-e2e8d519551b99d956eedbf656484a03
Base: a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2
Rejected predecessor: 3d712afe05e79c8a92b96cab6df0c7363745c513

Recommendation: DO NOT ACCEPT/INTEGRATE this candidate. Three independently reproduced P1 defects remain in the complete foundation boundary; one additional P2 receipt inconsistency is reproduced. No product/source changes, approval, merge, parent-CI execution, network access, downloads, credentials, provisioning, deployment or publishing occurred. Original worktree is clean and still exactly at the audited pin. Writes were independent evidence/disposable test harnesses in /tmp and cached foundation target outputs.

## Fresh native seal observation

Unlike the stale seal described in the handoff, a final read-only work.project_review query now returns awaiting_review on the correct head. Sender evidently refreshed it during this audit. Coordinates:
- attempt att-18dd918d574e164836ccf45bd0d138ca
- evidence ev-18dd918d62137500a399aaf2ace7a6fb
- bundle/evidence digest 98b3364c9598e2f6728cf620f78d7e36416a881444d31b8e9dd2ea1e3cedadf8
- reviewed head 75eec0783c0b6aa10d9faaac2edaa67b70a0d271
No approval decision exists; audit is independently against Git pin, not an assertion of acceptance.

## Findings

### P1: staged child can mint caller-chosen execution authority

InMemoryJobStore::stage_child (in_memory_runtime.rs:1480 onward) only validates the parent's grant. Materialization at :1194-1196 calls issue_incarnation, which sets incarnation/wakeup_pending but not child state, epoch, version or lease owner. SurrealJobStore::stage_child (:456) similarly stages the caller Job unnormalized; COMMIT_EXECUTION (execution_transactions.surql.rs:18-24) CREATEs its full child_data and adjusts only incarnation/wakeup_pending.

Independent probe supplies a child with state Running, epoch 500, version 7, owner caller-picked-owner, future expiry under a valid parent grant. After valid parent completion, BOTH native stores materialize that unacquired child and accept its subsequent commit_execution as Committed. Native acquisition was never called for the child. This is a concrete public-native-API authority bypass, not an automatic ctx.enqueue regression. ctx.enqueue happens to build NewJob; the lower native boundary does not enforce that invariant.

Require new-enqueued/no-output child validation and native normalization of authority fields at every materialization path (staged and continuation), preferably a nonauthoritative child spec rather than arbitrary Job. Preserve normal enqueue behavior and test forged epoch/owner/version/terminal children directly at the boundary.

### P1: retained canceled wait contaminates recreated job incarnation

job_context.rs:326-343 creates wait identity from logical job id, signal type and correlation key, without incarnation. DurableWaitRecord also has no incarnation. InMemoryJobStore::poll_wait (:1606 onward) and SurrealJobStore::poll_wait (:500 onward) validate the new grant but then reuse any old wait by wait_id. Delete retains waits.

Independent production-path sequence on BOTH stores: A Running -> ctx.wait_for Pending/Deferred -> native cancellation -> terminal delete -> insert/reacquire B with strictly newer incarnation -> same ctx.wait_for. B immediately gets Cancelled inherited from A instead of its own Pending/Deferred wait. TypedJobHandler maps this to fatal failure. Existing old_hook_preserves_successor_wait starts A without a wait, so does not cover retained A state.

Bind wait identity/state to stable job incarnation (not simply execution epoch, because same-incarnation deferral/reacquisition must retain signals/waits). Test cancel/fail/signal/timeout retained states across delete/recreate and delayed signal/control operations, plus same-incarnation Deferred preservation.

### P1: retained continuation is disposed using successor outcome

job_continuation.rs:215-224 selects pending records solely by parent_job_id. JobContinuation has no parent incarnation. Both settle_continuation native implementations compare the supplied terminal parent's incarnation to current job but do not bind the continuation record to that incarnation (in_memory_runtime.rs:1557 onward; surreal_job_store.rs:486 onward).

Independent sequence on BOTH stores: A registers success continuation through real ctx.continue_with -> commits Success without draining lifecycle -> delete terminal A -> recreate B under same logical id -> fail B -> replay_lifecycle. A's success continuation becomes Discarded based on B's failure. Retained A receipt cannot settle against missing/replaced parent; replay can acknowledge it without settling the edge. This is lifecycle cross-incarnation contamination and permanent loss of committed work. Merely checking supplied parent incarnation against current row is insufficient.

Bind continuation to its actual parent incarnation/commit and prevent successor settlement. Preserve legitimate committed continuation replay across deletion, either by settlement against retained native receipts or by blocking deletion/pruning until dependent settlement is safe. Test both successor success/failure and replay order, in-memory, actual Mem and KV restart.

### P2: actual Surreal receipt snapshot drops committed progress

COMMIT_EXECUTION :31 copies current progress_json into canonical job after writing the caller-derived JobRecord. But receipt_json remains publication(snapshot, ...) from SurrealJobStore::commit_once (:969 onward); ReceiptRecord::decode only corrects effective Deferred schedule. Independent ctx.progress -> native Success -> compare canonical job with execution_receipt.job_json: actual Surreal job contains final progress while retained receipt contains None. Memory parity passes because memory copies current progress before publication (:1148).

Retain the effective native publication snapshot in the same transaction without changing original submitted outcome digest/replay identity. This finding does not establish wrong output provenance or success-authority theft, so is classified P2 rather than P1.

## Independent commands and outcomes

Working directory was the supplied isolated worktree. PATH needed /Users/theelevators/.cargo/bin prepended; default API shell PATH lacked cargo (first attempt exit 127, not a test result). The first runnable native suite stopped at the library KV constructor with 134 pass/1 failure because default TMPDIR /var/folders/... was denied by sandbox. Repeated unchanged command with TMPDIR=/tmp succeeded; no sandbox permissions were expanded.

All cargo commands used CARGO_TARGET_DIR="$PWD/foundation-evidence/target", --offline --locked, unchanged original manifests/lock, cached native dependencies.

1. cargo test --offline --locked --no-default-features --features surreal-native -p stasis-rs --lib --test native_execution_authority --test native_completion_atomicity --test native_commit_clock --test typed_job_framework --test typed_jobs_signals_leases --test job_lifecycle_recovery --test federated_job_contract --test inbound_job_triggers --test native_foundation_corrections
   Exit 0, 213 passed. Per target: 135,9,7,5,1,10,4,22,11,9. Both actual_surreal_kv_restart_retains_grants_receipts_cleanup_and_pending_hook and actual_kv_reopen_preserves_control_incarnations_and_signal_receipt passed.
2. cargo test --offline --locked --no-default-features -p stasis-rs --lib --test native_execution_authority --test native_completion_atomicity --test native_foundation_corrections
   Exit 0, 137 passed: 121,4,2,10. Existing unused-import warning is not suppressed.
3. cargo check --offline --locked --no-default-features --features surreal-native,llm-genai -p stasis-rs
   Exit 0; compilation only, no LLM execution.
4. rustfmt --edition 2024 --check $(git diff --name-only 3d712afe -- src tests | grep '\.rs$'); git diff --check 3d712afe
   Both exit 0.
5. git diff --exit-code a7814f4 HEAD -- Cargo.toml Cargo.lock
   Exit 0. Original manifests/lock unchanged.
6. shasum -a 256 -c foundation-evidence/correction-source.sha256; shasum -a 256 -c foundation-evidence/evidence.sha256
   Every listed source/evidence hash verifies from repository root. An initial evidence check from wrong subdirectory produced path errors; rerun from proper root cleared all 20 entries. Not an evidence-content mismatch.
7. Disposable git archive of exact 75eec pin at /tmp/stasis-independent-75eec-probe; copied original correction test as reviewer_probe.rs and appended ONLY independent test functions. Product sources unchanged. cargo test --manifest-path /tmp/stasis-independent-75eec-probe/Cargo.toml --offline --locked --no-default-features --features surreal-native -p stasis-rs --test reviewer_probe review_ -- --nocapture
   Successful compilation, executable exit 101: 1 passed, 7 failed, 22 original tests filtered. Three P1 cases fail for both memory and actual Surreal Mem; receipt consistency fails on actual Surreal Mem and passes memory. An initial probe compile required fixing only reviewer println Debug formatting; final failures are substantive assertions, not compilation.

## Regression honesty / original correction assessment

The eight save/reload cases call the exact shared production running_snapshot helper used by both concrete entry points, with native save then held reload and real recovery/reassignment. Real ordinary/typed handlers are invoked if the unsafe reload is accepted. No alternate fixed algorithm is substituted on fresh pin. Original automatic stale-A suites also execute and pass. Old-pin conditional seam duplicates the historical save/reload semantics because the helper did not exist; it is NOT black-box pause instrumentation inside the historical concrete runtime. I read the original review-audit.rs and verified its stored hash. Historical 14-failure old-pin log is hash-verified, not independently rerun by this audit.

The signal matrices use real JobContext.wait_for, production signal_event, heartbeat, native completion and retained receipt replay. However they pause a constructed commit task, not a registered active handler/automatic typed consumer. CORRECTION.md's wording about holding an active handler is too strong. The deterministic native interleaving is still meaningful: signal cannot revoke version, pre-Deferred wakeup is preserved, late wakeup works, effective schedule matches receipt/lifecycle while original-outcome digest replay stays idempotent. Additional KV test independently passed. This establishes scoped logical interleavings and graceful close/reopen, not crash atomicity between signal insertion, wait completion and wakeup calls.

Never-leased control journal/outbox/hook uniqueness, native incarnation backfill/idempotence, staged child incarnation immutability and missing-worker-grant fail-closed/producer continuation cases do use production operations and pass. But incarnation isolation is incomplete for durable waits/continuations, and child incarnation issuance does not itself prevent child grant fabrication.

## Inspection inventory and scope

Reviewed full proposed inventory, correction diff and safety-relevant changed hunks/native paths, not every unchanged line of the large runtime/SDK repository. This is a blocker-based safety review, not a claim of exhaustive formal verification or approval of untouched code.

Source paths inspected:
- src/application/runtime/{execution_commit,in_memory_runtime,job_context,job_continuation,job_lifecycle,runtime_job_dx,surreal_runtime,typed_job,stasis_runtime_builder,agent_turn_waitable_job_handler,coordinator_failover_job_handler,queue_ownership_rebalance_job_handler,mod}.rs
- src/domain/runtime/{execution,job,mod,job_continuation,durable_wait}.rs (last two unchanged definitions inspected for incarnation binding)
- src/infrastructure/runtime/{execution_transactions.surql,in_memory_durable_wait_store,in_memory_job_continuation_store,surreal_job_store,surreal_durable_wait_store,surreal_job_attempt_store,surreal_job_continuation_store,surreal_outbox_store}.rs
- src/ports/outbound/runtime/job_store.rs
- tests/{native_foundation_corrections,native_execution_authority,native_completion_atomicity,native_commit_clock,job_lifecycle_recovery}.rs; original acceptance tests inspected by key safety blocks; correction test's production paths/observation seam inspected in detail.
- docs/native-execution-ownership.md; foundation-evidence/{CORRECTION.md,README.md,review-audit.rs,correction-source.sha256,evidence.sha256}; historical logs hash-checked, not all independently reproduced.

No Linux, remote Surreal, WASM, minimum-toolchain, distributed CI/MVP, actual power-loss or physical descendant-cleanup acceptance is claimed. External hook effects require durable sink-side idempotency. Broad backend-parity/minimal-feature and newer standalone dependency limits remain documented, not concealed or fixed here.

## Retained independent evidence

/tmp/stasis-independent-75eec-probe/tests/reviewer_probe.rs SHA256 d75a3aad89946443f3eae334287c6b2ab0d339d3b68b7bffcfde864467264989
/tmp/stasis-independent-75eec-probes.log SHA256 874bec59b64ad5424fe74fc9c7cee8e8a0224a79c6dde53625754eccf13056e0
/tmp/stasis-independent-75eec-native.log SHA256 af0bd54502b1141f35fbd778cc999c65a41e0820191dee2cec42b6119dd01ef2
/tmp/stasis-independent-75eec-memory.log SHA256 4f7c1586d392d9ab292be11732d80cded465daae513a351684ca45e7f0ef5338
/tmp/stasis-independent-75eec-check.log
/tmp/stasis-independent-75eec-fmt.log

Sender retains review and must attach/retain these independent findings under the correct fresh evidence before requesting bounded corrections. This worker made no review decision mutation.
