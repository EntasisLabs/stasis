# Medousa Context Orchestration Phase Plan

## Document Metadata

- Document Type: Implementation Plan
- Audience: Engineer, Architect, Runtime Owner
- Stability: Evolving
- Last Updated: 2026-05-21 (reassessed for Stasis-backbone-first delivery)
- Source Spec: docs/design/medousa-context-orchestration-spec.md

## Objective

Deliver payload-safe Medousa orchestration by composing on existing Stasis capabilities, not by extending Stasis core first.

Core outcome:

1. Medousa uses Stasis durable jobs, orchestration patterns, outbox, lineage, and memory ports as-is
2. Medousa implements stage flow, routing UX, and evidence-first user experience
3. only proven gaps trigger targeted Stasis delta work

## Baseline Inventory (Already Provided by Stasis)

These are treated as available prerequisites, not phase work:

1. durable job runtime with idempotency, retries, and leasing
2. orchestrator/sequential/concurrent/handoff execution patterns
3. outbox and correlation/causation/trace lineage envelope
4. memory recall/store contracts and runtime handler wiring
5. thread and lineage investigation capabilities
6. control-plane forward/failover/rebalance and diagnostics support

## Delivery Principle

Do the smallest Medousa-specific slice that unlocks end-user value while preserving architecture patterns.

## Stage-to-Stasis Mapping Table

This table is the implementation anchor for "compose on existing Stasis".

1. Artifact Ingest / Receipt
1. Stasis primitive used: durable job envelope and orchestration job builder
2. Current hooks:
1. src/application/orchestration/stasis_workflow_job_builder.rs
2. src/application/orchestration/agent_session_payload.rs
3. Medousa implementation delta:
1. define artifact-receipt payload contract in Medousa flow
2. dispatch as standard workflow job with lineage ids

2. Chunk Stage
1. Stasis primitive used: registered runtime handlers + memory writer port
2. Current hooks:
1. src/application/runtime/stasis_runtime_builder.rs
2. src/ports/outbound/memory/memory_context_writer.rs
3. Medousa implementation delta:
1. add chunk worker handler in Medousa runtime composition
2. persist each chunk as STTP node reference metadata

3. Extract Stage
1. Stasis primitive used: orchestration pattern jobs (sequential/concurrent/orchestrator)
2. Current hooks:
1. src/application/orchestration/stasis_workflow_job_builder.rs
2. src/application/runtime/stasis_runtime_builder.rs
3. Medousa implementation delta:
1. extraction handler contract and result schema
2. claim -> chunk reference persistence with diagnostics linkage

4. Summarize Stage
1. Stasis primitive used: prompt pipeline execution with policy_profile/model_hint routing
2. Current hooks:
1. src/application/orchestration/agent_session_payload.rs
2. src/application/runtime/prompt_chat_job_handler.rs
3. Medousa implementation delta:
1. layered summary DTOs and stage wrapper handler
2. stage-specific policy/model mapping from Medousa settings profile

5. Verify Stage
1. Stasis primitive used: same orchestration dispatch + lineage diagnostics envelope
2. Current hooks:
1. src/application/runtime/stasis_runtime_builder.rs
2. src/domain/runtime/outbox.rs
3. Medousa implementation delta:
1. verifier handler and confidence policy contract
2. provisional downgrade behavior when verification path is unavailable

6. Pack Stage
1. Stasis primitive used: memory recall and transform/aggregate operation handlers
2. Current hooks:
1. src/application/runtime/memory_recall_job_handler.rs
2. src/application/runtime/stasis_runtime_builder.rs
3. src/ports/outbound/memory/memory_models.rs
4. Medousa implementation delta:
1. context-pack builder policy layer (budget + selective hydration)
2. output pack contract consumed by final orchestrator response stage

7. Final Response Stage
1. Stasis primitive used: prompt chat handler with memory lineage diagnostics
2. Current hooks:
1. src/application/runtime/prompt_chat_job_handler.rs
3. Medousa implementation delta:
1. bind context-pack references into final answer prompt composition
2. carry citation/confidence summaries into UI-facing response model

8. Operator Routing and Profile Controls
1. Stasis primitive used: policy_profile/model_hint fields propagated in payloads and handlers
2. Current hooks:
1. src/application/orchestration/agent_session_payload.rs
2. src/application/runtime/prompt_chat_job_handler.rs
3. Medousa implementation delta:
1. settings UX profile editor and role mapping persistence
2. runtime mapper from role -> policy_profile/model_hint per stage dispatch

9. Investigability and Replay
1. Stasis primitive used: outbox lineage + thread store + lineage investigation use case
2. Current hooks:
1. src/domain/runtime/outbox.rs
2. src/domain/runtime/thread.rs
3. src/application/use_cases/investigate_runtime_lineage.rs
3. Medousa implementation delta:
1. add stage markers in diagnostics payload conventions
2. expose Medousa-oriented lineage drill-down in command center/TUI surfaces

## Phase 0: Medousa Composition Contracting

Goal:

1. Define Medousa stage contracts mapped onto current Stasis primitives.

Deliverables:

1. Medousa stage naming and payload contracts (artifact/chunk/extract/summarize/verify/pack).
2. Mapping table: stage contract -> existing Stasis runtime path.
3. Idempotency and lineage conventions for stage diagnostics.

Acceptance Criteria:

1. No new foundational Stasis runtime module is required for this phase.
2. Contract docs identify exact Stasis hooks for execution.
3. Team can implement stage handlers without runtime redesign.

## Phase 1: Artifact Receipt and Safe Ingestion

Goal:

1. Remove raw payload injection from primary chat flow.

Deliverables:

1. Payload artifact persistence path.
2. Lightweight receipt path in Medousa conversation flow.
3. Stage handoff into chunking pipeline using existing job dispatch.

Acceptance Criteria:

1. Large workflow outputs become receipts by default.
2. Receipts include payload id, type, size, and lineage ids.
3. Existing prompt flow remains backward-compatible.

## Phase 2: Chunking and STTP Node References

Goal:

1. Convert artifacts into referenceable STTP chunk nodes.

Deliverables:

1. Type-aware semantic chunking worker.
2. STTP chunk node writes through existing memory writer path.
3. Working-memory reference set creation logic.

Acceptance Criteria:

1. Chunk ids are deterministic for same content and policy.
2. Chunk refs include structural path and retrieval tags.
3. Downstream stages can operate by reference without full hydration.

## Phase 3: Extraction and Layered Summaries

Goal:

1. Build compact evidence products from chunk references.

Deliverables:

1. Extractor stage output model for claims/entities/metrics.
2. Layered summary outputs (L0/L1/L2).
3. Claim-to-chunk reference mapping persisted with lineage.

Acceptance Criteria:

1. Summaries are generated without broad raw payload reinjection.
2. Claims include evidence references.
3. Outputs are token-estimated and budget-ready.

## Phase 4: Verification and Confidence Gating

Goal:

1. Enforce evidence-grounded confidence behavior.

Deliverables:

1. Verifier stage for claim support checks.
2. Confidence scoring and unsupported-claim suppression policy.
3. Provisional fallback semantics when verification is unavailable.

Acceptance Criteria:

1. High-confidence claims require evidence refs.
2. Unsupported claims are excluded from default concise answers.
3. Verification outcomes appear in diagnostics and lineage views.

## Phase 5: Context Packing and Budget Enforcement

Goal:

1. Build turn-ready packs using reference-first retrieval.

Deliverables:

1. Context pack builder with budget classes.
2. Selective hydration policy and fallback pack logic.
3. Latency-optimized reference-only mode.

Acceptance Criteria:

1. Pack builder respects budget targets.
2. Orchestrator receives compact reference-rich context.
3. Budget compliance and latency metrics are emitted.

## Phase 6: Medousa Settings and Role Routing UX

Goal:

1. Expose role-based model orchestration in Medousa settings.

Deliverables:

1. Orchestration settings section with profile selector.
2. Role assignment matrix (orchestrator/chunker/extractor/summarizer/verifier/packer).
3. Per-role fallback chain and routing policy controls.
4. Draft/apply/revert integration with existing settings UX pattern.

Acceptance Criteria:

1. Operator can fully configure stage routing from UI.
2. Validation is explicit before apply.
3. Settings persist and survive restart.

## Phase 7: Progressive Disclosure UX

Goal:

1. Make confidence and evidence legible without sacrificing speed.

Deliverables:

1. Provisional vs verified answer states.
2. Confidence indicators and citation drill-down.
3. User depth controls (concise/standard/deep).

Acceptance Criteria:

1. User can progressively inspect evidence on demand.
2. Low-confidence outputs are clearly marked.
3. Keyboard-first overlay behavior remains consistent.

## Cross-Phase Quality Gates

For each phase:

1. unit tests for new Medousa contracts and handlers
2. orchestration integration tests for stage flow and retries
3. lineage traceability checks from answer to artifact
4. redaction and diagnostics safety checks

Release gate between phases:

1. phase acceptance criteria met
2. no architecture pattern regressions
3. regression suite green

## Risks and Mitigations

1. Risk: Stage fanout increases runtime load.
Mitigation: preserve existing Stasis idempotency/retry controls and batch limits.

2. Risk: Routing complexity increases operator burden.
Mitigation: opinionated defaults and profile presets in settings.

3. Risk: Verification latency degrades UX.
Mitigation: provisional responses with verified follow-up update.

4. Risk: Scope drift back into Stasis core changes.
Mitigation: maintain explicit "Stasis already provides" inventory and require gap proof for core deltas.

## Suggested Sprint Cadence

1. Sprint A: Phase 0 + Phase 1
2. Sprint B: Phase 2
3. Sprint C: Phase 3
4. Sprint D: Phase 4 + Phase 5
5. Sprint E: Phase 6
6. Sprint F: Phase 7 + hardening

## Immediate Next Checklist

1. finalize stage contract mapping table for Medousa implementation
2. implement artifact receipt path
3. implement chunking and STTP chunk-node writes
4. wire extraction + L0 summary path
5. add verification gate before deep evidence UX defaults

## Execution Log

- 2026-05-21: Plan reassessed to center Medousa composition on existing Stasis primitives and avoid premature Stasis core expansion.
