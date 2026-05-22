# Medousa Grapheme STTP Compaction: Implementation Checklist

Date: 2026-05-21
Status: Active
Scope: Medousa only

## A) Checklist: Planning to Execution

- [x] Refinement plan captured in docs.
- [x] Lock decisions:
  - [x] STTP output is plain text.
  - [x] Schema-guided mapping instructions are supplied to model.
  - [x] Apply only to cognition_grapheme_run and cognition_grapheme_cli_run.
  - [x] Use active user-selected provider/model/base_url target.
  - [x] Start with generous timeout defaults.
  - [x] Pass compacted STTP output to main agent first.

## B) Phase A (Feature-Flagged Foundation)

- [x] Add Medousa helper module for oversized Grapheme output compaction.
- [x] Add feature flag gate: MEDOUSA_ENABLE_GRAPHEME_STTP_COMPACTION.
- [x] Add initial env tunables:
  - [x] MEDOUSA_GRAPHEME_COMPACTION_TRIGGER_BYTES
  - [x] MEDOUSA_GRAPHEME_COMPACTION_TARGET_CHUNK_CHARS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_OVERLAP_CHARS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_MAX_CHUNKS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_MAX_SUMMARY_CHARS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_MAX_STTP_CHARS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_CHUNK_TIMEOUT_MS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_COMPOSER_TIMEOUT_MS
  - [x] MEDOUSA_GRAPHEME_COMPACTION_TOTAL_TIMEOUT_MS
- [x] Persist oversized original output to artifact store as scratch pad.
- [x] Chunk oversized payloads and summarize per chunk.
- [x] Compose chunk summaries into one STTP plain-text node.
- [x] Return compacted bounded output contract to caller.
- [x] Keep fallback path when summarization/composer fail.

## C) Wiring Scope

- [x] cognition_grapheme_run uses compaction helper.
- [x] cognition_grapheme_cli_run uses compaction helper.
- [x] Runtime target passed from active TUI runtime build settings.
- [x] Session-aware artifact persistence from tool layer.

## D) Validation and Hardening (Next)

- [ ] cargo fmt
- [ ] cargo check -p medousa
- [ ] Run targeted medousa_tui tests
- [ ] Reproduce prior large markdown Grapheme failure and confirm no context overflow
- [ ] Tune default thresholds/timeouts from first production-like run

## E) Deferred (Future Revision)

- [ ] Optional direct memory-store path for generated STTP node.
- [ ] Rich telemetry panel in TUI for compaction runs.
- [ ] Optional model routing override specifically for summarizer/composer phases.
