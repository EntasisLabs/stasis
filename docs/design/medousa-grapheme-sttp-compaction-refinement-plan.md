# Medousa Grapheme Output Compaction to STTP: Refinement Plan

Date: 2026-05-21
Status: Refinement (pre-implementation)
Scope: Medousa only (no Stasis core changes)

## 1) Agreed Decisions (Locked)

1. STTP handoff format is plain text.
2. STTP is the protocol; we provide schema instructions to the model for mapping.
3. Compaction flow applies only to Grapheme run paths:
- cognition_grapheme_run
- cognition_grapheme_cli_run
4. Model target for subagent summarization/composition follows active user settings (provider/model/base URL). No hardcoded override.
5. Timeout strategy starts generous and will be tuned with telemetry after real usage.
6. Initial behavior: pass generated STTP result to main chat agent first.
7. Future revision path remains open for direct memory persistence.

## 2) Problem Statement

Large Grapheme outputs (especially markdown-heavy payloads) create context pressure and can cause context_length_exceeded failures. Truncation alone loses too much meaning. We need a compaction pipeline that preserves semantics and converts oversized output into a bounded, evidence-grounded STTP text node for downstream reasoning.

## 3) Target Flow (Conceptual)

1. Run Grapheme tool as normal.
2. Inspect tool output size.
3. If output is below threshold, return normal output unchanged.
4. If output exceeds threshold:
- Persist full output as artifact (scratch pad durability).
- Slice output into chunks.
- For each chunk, run summarization subagent with strict chunk-only scope + schema-aware constraints.
- Collect chunk summaries.
- Run composer subagent over collected summaries to produce one STTP plain-text node.
- Return compacted tool output containing STTP text + artifact references + compaction metadata.
5. Main chat agent receives compacted output and continues normal answer generation.

## 4) Integration Points (Medousa)

Primary insertion points:
- medousa/src/tools.rs
  - CognitionGraphemeRunTool::invoke
  - CognitionGraphemeCliRunTool::invoke

Supporting existing primitives (reuse, not reinvention):
- medousa/src/artifact_store.rs (artifact persistence)
- medousa/src/artifact_chunking.rs (chunk references)
- medousa/src/payload_receipt.rs (receipt and preview behavior)
- medousa/src/bin/medousa_tui/agent_runtime.rs (existing context-pack injection path remains compatible)

## 5) Proposed Data Contract for Compacted Tool Output

When compaction is triggered, tool output should include:

- status: compacted
- mode: sttp_compaction
- original_artifact_ref:
  - artifact_id
  - hash64
  - byte_size
- chunking:
  - chunk_count
  - target_chunk_chars
  - overlap_chars
- summarization:
  - summaries_count
  - summarizer_timeout_ms
  - elapsed_ms
  - failure_count
- sttp:
  - schema_version
  - text_node (plain text STTP)
- notes:
  - fallback indicators if partial completion happened

If compaction is not triggered, preserve current output shape.

## 6) Summarizer and Composer Prompting Rules

### 6.1 Chunk Summarizer Prompt (Subagent 1)

Requirements:
- Operate only on supplied chunk.
- No external assumptions.
- Preserve concrete facts, identifiers, errors, metrics, and decisions.
- Emit concise structured summary in plain text suitable for STTP composition.
- Include uncertainty markers for ambiguous content.

### 6.2 STTP Composer Prompt (Subagent 2)

Requirements:
- Input: all chunk summaries + provided STTP schema instructions.
- Output: one plain-text STTP node.
- No markdown wrappers or code fences.
- Must include explicit uncertainty when chunk summaries conflict.
- Must avoid introducing unsupported claims.

## 7) Timeout and Retry Policy (Initial)

Initial generous defaults (subject to tuning):
- Per chunk summarization timeout: 120s
- Composer timeout: 120s
- Total compaction wall clock budget: 10 min

Failure behavior:
- If some chunks fail summarization, continue with successful summaries and emit failure_count.
- If composer fails, return receipt-style compact fallback with chunk summary list and artifact reference.
- Never block entire tool result indefinitely.

## 8) Safety and Resource Controls

1. Trigger threshold based on serialized output bytes.
2. Hard cap on maximum chunks processed in one run.
3. Hard cap on per-summary chars forwarded to composer.
4. Hard cap on final STTP text length.
5. Graceful degradation path always returns bounded output.

## 9) Phased Rollout

Phase A: Internal implementation behind a feature flag
- Add compaction helper module and integrate in the two Grapheme run tools.
- Add observability counters/log lines.
- Keep default flag disabled.

Phase B: Enable by default for oversized outputs
- Turn on for cognition_grapheme_run and cognition_grapheme_cli_run only.
- Keep strict bounds and fallback behavior.

Phase C: Operational tuning
- Tune thresholds/timeouts/chunk sizes from observed workloads.
- Evaluate optional direct memory-store handoff in a separate RFC.

## 10) Test Strategy

Unit tests:
- Trigger threshold correctness.
- Chunking boundary behavior.
- Partial summarization failure handling.
- Composer failure fallback behavior.
- Output contract shape for compacted and non-compacted cases.

Integration tests:
- Large markdown Grapheme output does not overflow model context.
- Main agent receives bounded STTP output and completes response.
- Existing normal-sized Grapheme outputs remain unchanged.

Regression checks:
- cargo fmt
- cargo check -p medousa
- targeted medousa_tui tests

## 11) Non-Goals (Current Refinement)

1. No changes to Stasis tool loop internals.
2. No automatic memory persistence of generated STTP node yet.
3. No cross-tool expansion beyond the two Grapheme run tools.

## 12) Open Inputs for Implementation Kickoff

1. Final threshold values (bytes) for compaction trigger.
2. Final chunk target and overlap defaults.
3. Feature flag name and default behavior at launch.
4. Exact STTP schema instruction block to embed in composer prompt.

## 13) Implementation Readiness Summary

This plan is implementation-ready with Medousa-only touch points and preserves current runtime architecture. The pipeline is semantically stronger than truncation and bounded enough for operational safety.
