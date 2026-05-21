# Medousa Context Orchestration Phase Plan

## Document Metadata

- Document Type: Implementation Plan
- Audience: Engineer, Architect, Runtime Owner
- Stability: Evolving
- Last Updated: 2026-05-21
- Source Spec: docs/design/medousa-context-orchestration-spec.md

## Objective

Deliver context-safe, small-model-first orchestration for large workflow payloads by:

1. keeping the main model as intent orchestrator
2. offloading payload work to sub-agents
3. persisting chunk working memory as STTP nodes
4. using UI-managed provider/model role orchestration

## Pattern Constraints (Non-Negotiable)

1. Follow DDD + hexagonal boundaries.
2. Keep event-driven orchestration and durable outbox lineage.
3. Pass context by STTP references, not large raw blobs.
4. Use existing TUI draft/apply/revert settings pattern.
5. Preserve keyboard-first and tabbed overlay UX patterns.

## Delivery Strategy

This plan is phased to land value early and de-risk gradually:

1. first remove context blow-up risk
2. then add extraction/summarization value
3. then add trust (verification)
4. then add UI-controlled model-role orchestration

## Phase 0: Contracts and Scaffolding

Goal:

1. Define stable contracts before worker implementation.

Deliverables:

1. Stage event DTOs and envelope contracts.
2. Stage job contracts and idempotency key strategy.
3. STTP chunk node schema and working-memory reference schema.
4. Runtime builder extension points for orchestration workers.

Acceptance Criteria:

1. Contracts compile and are documented.
2. Event names and payloads are versioned.
3. No behavior changes to existing prompt flow yet.

Exit Artifacts:

1. Contract module set in application/domain layers.
2. ADR entry for orchestration contract versioning.

## Phase 1: Artifact Receipt and Safe Ingestion

Goal:

1. Prevent raw large payload injection into chat context.

Deliverables:

1. Payload artifact persistence and metadata capture.
2. Receipt object emitted to UI/main model instead of raw payload.
3. Events:
1. payload.artifact_ingested
2. payload.chunking_requested

Acceptance Criteria:

1. Large payload workflows produce receipts, not inline blobs.
2. Receipt includes payload_id, type, size, and lineage ids.
3. Existing runtime behavior remains backward-compatible.

Exit Artifacts:

1. Artifact store adapter and use case.
2. Basic ingestion observability counters.

## Phase 2: STTP Chunking and Memory Persistence

Goal:

1. Convert artifacts into semantically useful STTP chunk nodes.

Deliverables:

1. Semantic chunker worker with type-aware chunk policies.
2. STTP chunk node persistence for each chunk.
3. Working-memory reference set creation per payload/session.
4. Events:
1. payload.chunking_completed
2. payload.chunk_sttp_nodes_persisted
3. payload.extraction_requested

Acceptance Criteria:

1. Deterministic chunk ids for same input and policy.
2. Chunk nodes carry structural path and retrieval tags.
3. Main model can reference chunk nodes by id without hydration.

Exit Artifacts:

1. Chunking adapter and tests across JSON/markdown/tabular samples.
2. STTP node write/read parity across supported backends.

## Phase 3: Extraction and Layered Summarization

Goal:

1. Produce compact, useful context products from chunk nodes.

Deliverables:

1. Extractor worker for facts/entities/metrics/anomalies.
2. Summary worker for layer 0/1/2 outputs.
3. Claim-to-chunk reference mapping.
4. Events:
1. payload.extraction_completed
2. payload.summarization_completed
3. payload.verification_requested

Acceptance Criteria:

1. Layered summaries are generated without full raw hydration.
2. Claims carry evidence references.
3. Summaries are bounded and token-estimated.

Exit Artifacts:

1. Extracted facts model.
2. Layered summary model.

## Phase 4: Verification and Confidence

Goal:

1. Enforce trust and evidence-grounded output behavior.

Deliverables:

1. Verifier worker validating claim support against chunk nodes.
2. Confidence scoring model and unsupported-claim handling.
3. Events:
1. payload.verification_completed
2. context.pack_requested

Acceptance Criteria:

1. No high-confidence claim without evidence refs.
2. Unsupported claims excluded from layer 0 by default.
3. Verification verdicts visible in runtime diagnostics.

Exit Artifacts:

1. Verification record model.
2. Confidence policy controls.

## Phase 5: Context Packing and Reference-First Retrieval

Goal:

1. Build turn-ready packs within strict token budgets.

Deliverables:

1. Context pack builder with budget classes.
2. Reference-first retrieval strategy and selective hydration.
3. Fallback pack generation when budget exceeded.
4. Events:
1. context.pack_completed
2. orchestrator.response_ready

Acceptance Criteria:

1. Packer respects budget profile and priority ordering.
2. Orchestrator receives reference-rich compact context.
3. Low-latency mode can run in reference-only path.

Exit Artifacts:

1. Context pack model and policy tests.
2. Token budget utilization metrics.

## Phase 6: UI-Managed Multi-Provider Role Orchestration

Goal:

1. Move role assignment and routing control into TUI settings.

Deliverables:

1. New Settings tab section for orchestration profiles.
2. Role assignment grid (orchestrator/chunker/extractor/summarizer/verifier/packer).
3. Fallback chain editing for each role.
4. Routing policy controls and safety toggles.
5. Draft/apply/revert integration with existing settings flow.

Acceptance Criteria:

1. Operator can fully manage role-model assignments from UI.
2. Changes are validated before apply.
3. Persisted orchestration state survives restart.

Exit Artifacts:

1. Internal orchestration settings schema in defaults.
2. UI rendering and input handlers aligned with current settings patterns.

## Phase 7: UX Progressive Disclosure and Operator Controls

Goal:

1. Make output understandable while preserving speed.

Deliverables:

1. Provisional vs verified answer states in UI.
2. Confidence indicators and citation drill-down.
3. User controls for response depth and evidence mode.

Acceptance Criteria:

1. User can move from concise answer to evidence details on demand.
2. Low-confidence output is explicitly marked.
3. Interaction remains keyboard-first and consistent with existing overlays.

Exit Artifacts:

1. Updated TUI overlays and key maps.
2. Regression tests for mode transitions.

## Cross-Phase Testing and Quality Gates

For each phase:

1. Unit tests for new domain/application contracts.
2. Integration tests for orchestration event flow.
3. Backend parity checks where applicable.
4. Security checks for sensitive payload/log redaction.

Release Gate for moving to next phase:

1. acceptance criteria met
2. no architecture pattern violations
3. regression suite green

## Risks and Mitigations

1. Risk: Worker fanout increases operational complexity.
Mitigation: strict event contracts, idempotency, replay support.

2. Risk: Cost spikes from multi-model orchestration.
Mitigation: role routing policy with budget controls and fallback caps.

3. Risk: UI settings complexity drifts from current patterns.
Mitigation: reuse existing settings IA and transactional behavior.

4. Risk: Verification latency hurts UX.
Mitigation: provisional response path with later verified upgrade.

## Suggested Sprint Cadence

1. Sprint A: Phase 0 + Phase 1
2. Sprint B: Phase 2
3. Sprint C: Phase 3
4. Sprint D: Phase 4 + Phase 5
5. Sprint E: Phase 6
6. Sprint F: Phase 7 and hardening

## Immediate Start Checklist

1. Finalize event and job contract module layout.
2. Implement artifact receipt path first.
3. Implement STTP chunk node persistence next.
4. Wire minimal extraction plus layer 0 summary.
5. Add verification before enabling deep payload UX by default.

## Execution Log

- 2026-05-21: Sprint A kicked off with Phase 0 contract scaffolding in core layers (domain runtime context orchestration types plus application DTO request/response contracts).
