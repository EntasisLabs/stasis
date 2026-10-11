# Independent safety review — changes required

> Historical independent review of a rejected revision, not current acceptance. See [external audit archive](../README.md#exact-preserved-audit-evidence) for exact original report bytes and logs; resolve relative evidence names in archived `snapshot/foundation-evidence/review-9cfb/`. Original reviewer hashes apply to those archived bytes, not this annotated copy.

Candidate: 9cfbcb86506aacfda3600e71f6052843a36b8fa5
Baseline: a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2
Rejected predecessor: 75eec0783c0b6aa10d9faaac2edaa67b70a0d271
Undertaking: work-e2e8d519551b99d956eedbf656484a03
Seal independently observed awaiting_review: att-18dd93f43f6b91b815c34669e03b80bc / ev-18dd93f44aeec230a0c705977d2921c8
Exact evidence digest: dd74012eddf2b7f926e7c50233f5e391f746259eefdfbea2e936b7674f81a1e5

Recommendation: DO NOT ACCEPT/INTEGRATE. Two additional P1 safety blockers independently reproduced on BOTH native in-memory and actual Surreal Mem. Four new tests compile and fail substantive safety assertions (0 passed / 4 failed), not compilation or sandbox failures. No review decision mutation, approval or integration made; sender retains that authority.

## P1 — signal dispatcher captures both retired and replacement incarnation

src/application/runtime/runtime_job_dx.rs:212-234 enumerates waits, checks each current job, captures A's identity, then performs another get(correlation_key) and unconditionally adds that current job's identity. With the normal correlation key equal to job ID, delete/recreate between these reads causes the same dispatch to capture A AND B. The new target_incarnations field is not an atomic recipient snapshot.

Reproduction: real ctx.wait_for creates A's Pending wait. Production signal_event runs through its first get and accepts A's incarnation. A transparent JobStore decorator pauses ONLY the second get, before reading the underlying store. During that pause, the real native store cancels/deletes A, inserts/acquires/runs B, and real B ctx.wait_for creates its own Pending wait. Resume the original signal_event. Its final direct-correlation lookup adds B, inserts a signal targeted to A+B, completes only old A's captured wait and calls real wake_incarnation(A). B's next real ctx.wait_for returns success by consuming that old dispatch's buffered signal instead of remaining Deferred. A incarnation 1, B incarnation 3 on both native stores. No replacement algorithm or synthetic signal is substituted.

This violates the claimed delayed-dispatch incarnation isolation even though wake_incarnation itself correctly fences A. The retained tests insert an already-A-scoped signal and therefore do not test production recipient capture across two reads. Require a coherent fail-closed native recipient capture/dispatch contract that cannot combine generations for the same logical job; test this real interleaving, custom correlation keys and buffered events. Merely fencing wakeup is insufficient.

## P1 — retained old terminal receipt can discard later success edge after dead-letter replay

src/application/runtime/job_continuation.rs:219-229 filters edges only by parent job ID and stable incarnation. Registration records incarnation but no execution/commit or terminal-cycle identity. In-memory settle_continuation at :1591-1614 and actual Surreal native settlement at surreal_job_store.rs:504 accept any matching retained old terminal receipt even if the current row is from a later replayed execution of the SAME incarnation.

Reproduction: acquire/run A, native commit FatalFailure (DeadLetter), retain undelivered A receipt. Call the real runtime replay_dead_letter API; it re-enqueues the same incarnation. Reacquire/run B (A epoch 2, B epoch 3), register a success continuation through real ctx.continue_with, and commit B Success. Call exact production settle_continuations with A's retained receipt first, then B's. A's old DeadLettered event permanently marks B's success edge Discarded; B's Success cannot restore or materialize it. Both native stores reproduce. There are no product edits, no forged receipt, and no arbitrary terminal-row mutation. The production settlement helper is invoked in a deterministic legitimate retained-delivery order, not a full automatic-handler scheduler test.

The delete/recreate correction works when incarnation changes, but incarnation alone cannot separate terminal/replay cycles. The new retained-receipt authorization expands this old-outcome reach. Require edge-to-appropriate-terminal-cycle/commit binding (with documented producer and same-incarnation retry/deferral semantics), or a native replay barrier that settles dependent obligations before reopening the terminal parent. Test delayed old failure/control receipts versus new success edges and both replay orders in memory/Mem/KV. Epoch alone must not accidentally invalidate intended continuations surviving ordinary nonterminal retries.

## Prior corrections verified, with limits

All eight immutable previous independent probes pass freshly: staged Running/forged lease authority rejected; canceled wait does not contaminate recreated incarnation; predecessor success continuation survives failed recreated successor; actual Surreal retained receipt now retains committed progress. New authority/state/order/migration matrices also pass. normalize_new runs on insertion, staging and continuation materialization, and actual Surreal publication revalidates persisted child specifications and resets authority inside the transaction. Receipt effective job_snapshot is created after canonical progress/wakeup adjustment without changing submitted-outcome digest. The exact prior delete/recreate P1 counterexamples and P2 are corrected; the two P1 findings above are additional cases, not claims that those exact eight probes still fail.

Native clock/grant publication, idempotent digest conflicts, transactional rollback, stale running reload fencing, same-owner/different-owner recovery, staging/cleanup-only barriers, control identities and retained receipt delivery were examined in the full proposal inventory and safety-relevant native source/test paths. Existing automatic stale-A tests and scoped native interleaving matrices freshly pass. This is a blocker-based code review, not exhaustive formal verification of every unchanged repository line.

Migration: native waits reject zero/mismatched incarnation and ambiguous legacy waits for the logical ID. Surreal row decoders map missing legacy wait/edge identity to zero. settle_continuations rejects ambiguous pending edges and does not acknowledge unresolved delivery. Fresh legacy tests pass. Documentation requires quiescent writers and explicit evidence-based reconciliation/quarantine, not adoption by current job name. Old receipt snapshots retain historical fallback; no retrospective repair/progress is fabricated. That fail-closed policy does not resolve the fresh same-incarnation replay blocker.

Evidence wording is now candid: signal matrices pause a constructed commit task, not an active automatic typed handler; old-pin running reload seam models old save/reload semantics, not black-box instrumentation. Close/reopen is graceful restart, not power-loss/crash atomicity; signal insertion/completion/wakeup remain separate calls. Native_incarnation_corrections includes the unchanged original probe plus reused original cases: 40 native / 18 memory are executions, not unique new scenarios.

## Independent commands and receipts

Only writes were disposable archive/cache/test/log files under /tmp/stasis-review-9cfb. Original source/worktree stayed clean at exact pin. Git archive HEAD created the isolated tree; cached target was APFS-cloned to /tmp so Cargo output never wrote in the original worktree. Cargo used unchanged archived Cargo.toml/Cargo.lock and --offline --locked. No network, dependency changes/downloads, credentials, provisioning or permission expansion.

All commands below ran from /tmp/stasis-review-9cfb with:
export PATH=/Users/theelevators/.cargo/bin:$PATH TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/stasis-review-9cfb/target

1. cargo test --offline --locked --no-default-features --features surreal-native -p stasis-rs --lib --test native_execution_authority --test native_completion_atomicity --test native_commit_clock --test typed_job_framework --test typed_jobs_signals_leases --test job_lifecycle_recovery --test federated_job_contract --test inbound_job_triggers --test native_foundation_corrections --test native_incarnation_corrections
Exit 0; 253 passed, zero failures. Per target 135,9,7,5,1,10,4,22,40,11,9. Log native-rerun.log. Four actual KV test executions pass: original native authority KV restart, original correction KV reopen, new retired-wait/continuation KV reopen, reused original correction KV reopen. Library KV constructor also passes. Initial native.log is incomplete (no test summary/terminal Cargo result); the asynchronous shell later reported exit 1 without a diagnostic. It is NOT counted as a completed passing suite or a substantive test failure. Identical full rerun was complete and successful. Initial cache-clone command also timed out at explicit 1s deadline; setup was completed before testing.

2. cargo test --offline --locked --no-default-features -p stasis-rs --lib --test native_execution_authority --test native_completion_atomicity --test native_foundation_corrections --test native_incarnation_corrections
Exit 0; 155 passed, zero failures: 121,4,2,10,18. Log memory.log. Existing unused-import warning retained.

3. cp foundation-evidence/review-75eec/reviewer_probe.rs tests/reviewer_probe.rs
cargo test --offline --locked --no-default-features --features surreal-native -p stasis-rs --test reviewer_probe review_ -- --nocapture
Exit 0; eight retained independent probes pass, 22 original cases filtered. Log probes.log; copied probe SHA256 matches original exactly.

4. cargo check --offline --locked --no-default-features --features surreal-native,llm-genai -p stasis-rs
Exit 0; check.log; compile only, no LLM execution.

5. cargo test --offline --locked --no-default-features --features surreal-native -p stasis-rs --test audit_probe audit_ -- --nocapture
Exit 101; 0 pass/4 failed/30 filtered, substantive safety assertions. Log new-probes.log. Earlier capture-only run (same command with audit_capture filter) also compiled then 0 pass/2 failed; capture-race.log. tests/audit_probe.rs includes the immutable original probe and transparent second-get barrier plus the two new both-store cases; product sources unchanged.

6. From original Git worktree: git diff --exit-code a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2 HEAD -- Cargo.toml Cargo.lock
Exit 0, dependencies unchanged.
shasum -a 256 -c foundation-evidence/correction-source.sha256
shasum -a 256 -c foundation-evidence/evidence.sha256
Both exit 0: 39/39 and 55/55 worktree entries. Independently computed SHA256 for every git show exact-pin:path blob: also 39/39 and 55/55. All seven retained prior independent files match supplied /tmp originals byte-for-byte (report, probe, probes/native/memory/check/fmt logs). The fourteen historical logs added in recovery are self-contained committed entries whose listed bytes all verify; historical failures are retained, not reclassified as fresh successes. integrity.log, source-hashes.log, evidence-hashes.log retain checks.

7. Documented scoped rustfmt --edition 2024 --check $(git diff --name-only 3d712afe HEAD -- src tests | grep '\.rs$')
Exit 0; fmt-scoped.log. Broader baseline changed-file rustfmt attempt exit 1 because changed module roots recursively reach unrelated existing chat_options_resolver, concurrent_tool_branch_memory and other unchanged runtime/outbox formatting; fmt.log preserves that output. It is not a correction source safety blocker or a blanket all-repository format pass.
git diff --check 75eec078 HEAD -- src tests docs
Exit 0; whitespace.log. Immutable evidence logs were not reformatted.

## Scope/authority

Fresh seal read-only query confirms exact head/evidence awaiting_review, no active executors and no decisions. No approval/request-changes/integration mutation made by worker. Sender must retain and independently assess these new blockers against this exact seal. Frozen parent rust-ci-engine b87875eec9da2629df23c58505bdf024f4058fa4 was not accessed or run. No Linux, remote Surreal, WASM, power-loss, physical descendant cleanup, deployment/publishing, distributed CI or MVP acceptance claimed.

## SHA256-pinned independent evidence
- /tmp/stasis-review-9cfb/native-rerun.log: d7ca5c6f7c20d568a57dba4d5b3be5f2b5e33e4ef3434fb0bf397536f95c0446
- /tmp/stasis-review-9cfb/memory.log: 97e740e2676a7f39d207987c65f7b77b94c5f9ec9162e1f6c07b666bab069b20
- /tmp/stasis-review-9cfb/probes.log: b1a6c5a4eede193320628daadf66d7d3445c5de03b50e6d029034d4a945468ad
- /tmp/stasis-review-9cfb/check.log: a8b5b6b8655c2d33c9a8d711ce8179a48a2b315aecc0a3d842f5199bb0c6550a
- /tmp/stasis-review-9cfb/new-probes.log: 4f1028dbef292fc92c1ea2d5f761d974e5141e56256e71b3eb268629c39b9dae
- /tmp/stasis-review-9cfb/capture-race.log: d19c21b14a497ee49a4f16a6ccfd4d289ddba70d23c0abcb2d66b3c0e8357bc7
- /tmp/stasis-review-9cfb/tests/audit_probe.rs: ad68428f3fdf4ed110d4d3c268d69927996109cc3c2493109b5269ad09ea0223
- /tmp/stasis-review-9cfb/tests/reviewer_probe.rs: d75a3aad89946443f3eae334287c6b2ab0d339d3b68b7bffcfde864467264989
- /tmp/stasis-review-9cfb/integrity.log: 99fd19b96422fceb0794dd80760b90f40c9196f3eaedc131115581b84ab6b46d
- /tmp/stasis-review-9cfb/source-hashes.log: 7d3228d3805a76ae8cd3eec1655b6d702b3840ee319d7d0de6722f46a60527de
- /tmp/stasis-review-9cfb/evidence-hashes.log: d7e949192187cac1567ee24474de132f408e6f16a1b6f58475da1beb66cee3ea
- /tmp/stasis-review-9cfb/fmt.log: b7536af6236f95f29bdb6610718f8ccd146b759da0b7cf80ccbad22d1b4b1308
- /tmp/stasis-review-9cfb/fmt-scoped.log: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
- /tmp/stasis-review-9cfb/whitespace.log: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
