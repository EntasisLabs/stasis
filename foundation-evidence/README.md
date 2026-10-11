# Stasis foundation reports and regression fixtures

Undertaking: `work-e2e8d519551b99d956eedbf656484a03`.
Original base: `a7814f40c5c3ee2522d93f4ac47c1e216cc9bcf2`.
Pre-cleanup candidate: `ef9c53a9c171379eb60576ad545d14fb130d634f`.
Review, acceptance and integration remain sender-owned. This cleanup does not implement CI or change product/test source.

## Exact preserved audit evidence

Before removing any generated file, the entire candidate was exported as `candidate.tar` and its binary-capable diff against the original base as `candidate.patch`. All tracked evidence and two loose audit files were copied and hash-verified outside Git and outside the worktree:

```text
/Users/theelevators/Library/Application Support/medousa/forge/evidence/work-e2e8d519551b99d956eedbf656484a03/cleanup-ef9c53a9c171379eb60576ad545d14fb130d634f
```

Call this directory `ARCHIVE`. Its `snapshot/` mirrors repository-relative evidence paths. Historical log/manifest names in the reports below resolve under `ARCHIVE/snapshot/foundation-evidence/`, NOT the current checkout. Short log names refer to the report's review directory (`review-75eec`, `review-9cfb`, or `review-b78f` for identity publication). Original report bytes also remain there. `loose/` preserves additional previously untracked audit outputs. These are local file evidence, not publicly hosted URLs; retain the archive through review/integration.

SHA-256 anchors:

| File relative to ARCHIVE | SHA-256 |
| --- | --- |
| `candidate.tar` | `7d974840dd3bd5f8c9cb92b2baeeda22234435a7293edd248a475265fa20085c` |
| `candidate.patch` | `f3308f8454965b74bcd8f78c67304d879b9d3628ef7892253df2fbea33db00df` |
| `archive.sha256` | `d6751727723686900fcc36c33689b452f81935d5b4d60ab57ca76fb4fbb615f9` |
| `snapshot/foundation-evidence/correction-source.sha256` | `86f1716fd26d5a3f893f6e99ac51806239c487adeeb4729ff5dba75f954845d1` |
| `snapshot/foundation-evidence/evidence.sha256` | `92a3dc7ed4bb6ce0def7e28cf9d47928d82502533cbd62f26aef1be380add910` |

The original 44 source and 107 evidence manifest entries matched both working files and committed Git blobs before cleanup. `archive.sha256` binds 115 exported files. Verify it from ARCHIVE with `shasum -a 256 -c archive.sha256`. To verify the original manifests, extract `candidate.tar` into a separate disposable evidence directory and run their checks from that extracted root. They describe ef9c53a9, not the cleanup revision. Do not regenerate or overwrite those original audit inventories.

Fresh cleanup checks and execution logs are stored separately under `ARCHIVE/verification/`; `verification.sha256` binds those receipts without changing the original archive inventory.

Cleanup verification freshly passed the supported offline/locked profiles: **301 native-profile executions across 12 suites**, **177 memory-profile executions across 6 suites**, and `cargo check` with `surreal-native,llm-genai`. All test totals have zero failed/ignored/measured/filtered cases. The native runs include actual SurrealKV close/reopen tests. This used the existing cached target, not a clean rebuild. Exact commands and exit codes are in `verification/commands.json`; logs are `native.log`, `memory.log`, and `check.log`. The prior memory unused-import warning remains, not a new failure.

All 361 protected source/test/dependency/contract files remained byte-identical to ef9c53a9 before and after those runs. Full base-to-tree whitespace, scoped rustfmt, archive integrity, Markdown link checks and positive/negative ignore checks also passed. Cleanup changes only `foundation-evidence/`: 98 generated files removed, seven maintained report/ignore files updated. Exact removed and changed path inventories are in `verification/removed-paths.txt` and `verification/cleanup-changed-paths.txt`.

## Retained maintained reports and source

- [Canonical identity publication](IDENTITY-PUBLICATION.md): latest foundation correction; host and independent prior runs each reported 301 native and 177 memory executions.
- [Recipient capture and replay cycles](RECIPIENT-REPLAY.md): earlier correction and migration limits.
- [Incarnation correction](CORRECTION.md): earlier safety corrections and historical results.
- [Independent review of 75eec](review-75eec/report.md) and [independent review of 9cfb](review-9cfb/report.md): historical rejected-candidate findings, not current acceptance.
- `baseline-race.rs`, `review-audit.rs`, and review probe Rust sources remain byte-identical. Tests include the retained `review-75eec/reviewer_probe.rs`; it is NOT generated output.
- [Native ownership contract](../docs/native-execution-ownership.md) remains unchanged.

Reports' historical commands, paths and checksum statements describe the archived candidate and its earlier revisions. For new runs, use the existing cached `foundation-evidence/target` if appropriate, but direct logs and temporary databases to an external evidence directory, not into Git. Scoped ignore rules reject `.log`, checksum snapshots, target products and generated test databases in this evidence directory; there is no repository-wide ignore of source or documentation.

## Integration warning

Cleanup is a new ordinary commit; no history rewrite or force push. Native review currently offers `fast_forward_only`, `preserve_branch`, and `export_patch`. Fast-forward or history-preserving integration retains the earlier log-containing commits even though the final tree is clean. If the owner requires clean incoming history as well as a clean final tree, review/export the full base-to-clean-head patch and apply it as a new clean commit using an authorized integration route. Export alone does not integrate anything. Fresh exact-head native sealing/review is required; the previous seal is not the cleanup revision.

No approval, merge, deployment, CI implementation or MVP claim is made here. Prior limits remain: cached local memory/Surreal Mem/KV coverage only; no Linux/remote/WASM, process-descendant cleanup, abrupt power-loss durability or distributed CI verification.
