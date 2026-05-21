# Medousa Context Orchestration Spec

## Document Metadata

- Document Type: Architecture and Implementation Spec
- Audience: Engineer, Architect, Runtime Owner, Platform Operator
- Status: Updated after architecture reassessment
- Last Updated: 2026-05-21 (Stasis backbone alignment)
- Scope: Medousa orchestration composition on existing Stasis runtime primitives

## 1. Why We Reassessed

Medousa needs payload-safe orchestration for large workflow outputs, but the first planning pass felt off because it implied Stasis foundation gaps.

After inspection, most required primitives already exist in Stasis. The correct implementation direction is:

1. keep Stasis as the durable orchestration backbone
2. implement Medousa-specific flow composition and UX
3. avoid adding parallel core runtime abstractions unless a real gap is proven

## 2. Problem Statement (Still True)

Large payloads (search corpora, analytics bundles, diagnostic dumps) overload direct chat-context inlining.

Inlining raw payloads causes:

1. context saturation and quality drop
2. evidence truncation and fidelity loss
3. unstable latency
4. weaker citation grounding

## 3. Existing Stasis Backbone (Already Done)

The following are considered available platform capabilities that Medousa must use rather than re-implement.

### 3.1 Durable Runtime and Job Semantics

1. job identity, correlation, causation, trace, retries, and idempotency
2. durable state transitions and leasing
3. outbox-based runtime event publication

### 3.2 Orchestration Patterns

1. sequential pattern handling
2. concurrent branch pattern handling
3. orchestrator route pattern handling
4. handoff pattern handling

### 3.3 Memory and STTP Integration

1. memory recall/store contracts behind ports
2. memory handlers wired through runtime builder
3. lineage-ready diagnostics fields for memory query ids and output node ids

### 3.4 Lineage and Investigability

1. thread and thread-event tracking
2. lineage report query flow for runtime investigations
3. correlation-safe diagnostics surfaces for replay and debug

### 3.5 Control Plane Foundations

1. cluster forwarding and command outcomes
2. queue ownership and rebalance operations
3. endpoint diagnostics and health trend query support

## 4. Medousa Scope (What We Need To Build)

Medousa now focuses on composition and product behavior:

1. payload-to-artifact receipt path for chat safety
2. staged payload processing flow (chunk -> extract -> summarize -> verify -> pack)
3. STTP chunk-node reference strategy for reference-first retrieval
4. role-based model routing configured from Medousa settings UX
5. progressive disclosure UX (provisional, verified, evidence drill-down)

## 5. Non-Goals

1. no parallel runtime framework beside existing Stasis orchestration
2. no broad Stasis domain model expansion without concrete blocker
3. no duplicate memory stack abstraction that bypasses existing ports

## 6. Design Principle

Treat the main chat model as intent orchestrator, not raw payload processor.

1. Main model:
1. intent interpretation
2. depth selection
3. stage scheduling decisions
4. final response synthesis with citations

2. Stage workers:
1. payload chunking
2. fact extraction
3. layered summarization
4. claim verification
5. token-budget pack assembly

3. Stasis platform:
1. durable job execution
2. retries/idempotency
3. outbox and lineage diagnostics
4. cross-worker coordination

## 7. Target Runtime Flow (Medousa on Stasis)

1. workflow output is persisted as payload artifact
2. UI receives lightweight artifact receipt
3. chunk stage writes STTP chunk nodes
4. extract stage emits claims with chunk references
5. summarize stage emits layered summaries
6. verify stage marks support/confidence
7. pack stage builds turn-budget context pack by references first
8. orchestrator answers using compact pack and citations

## 8. Event Model Guidance

Use existing Stasis runtime event envelope and diagnostics surfaces.

Medousa logical stages are expressed through:

1. job_type and pattern payload contracts
2. diagnostics payload fields
3. lineage-linked thread events

Canonical logical stage names for Medousa flows:

1. payload.artifact_ingested
2. payload.chunking_completed
3. payload.extraction_completed
4. payload.summarization_completed
5. payload.verification_completed
6. context.pack_completed
7. orchestrator.response_ready

These are orchestration-level semantics, not a mandate to replace current runtime event type enums.

## 9. Data and Reference Strategy

### 9.1 Core Medousa Artifacts

1. PayloadArtifact:
1. payload_id
2. mime_type
3. byte_size
4. source_run_id
5. content_uri or storage key

2. STTPChunkNodeRef:
1. node_id
2. payload_id
3. chunk_id
4. sequence
5. token_estimate
6. structural_path
7. retrieval_tags
8. checksum

3. EvidenceClaim:
1. claim_id
2. statement
3. supporting_chunk_node_refs
4. support_strength

4. SummaryLayer:
1. layer_id
2. depth
3. summary_text
4. claim_refs

5. VerificationRecord:
1. claim_id
2. verdict
3. confidence_0_1
4. notes

6. ContextPack:
1. pack_id
2. budget_profile
3. selected_summary_layers
4. selected_claim_refs
5. selected_chunk_node_refs
6. total_token_estimate

### 9.2 Lineage Requirement

Every visible claim must resolve to source:

final answer -> summary -> claim -> chunk node ref -> STTP node -> payload artifact

## 10. Budget and Retrieval Policy

1. prioritize verified summary content before raw chunk text
2. use reference-first retrieval from STTP nodes
3. hydrate raw chunk text only when required by budget/intent
4. support low-latency reference-only path

Suggested budget split for turn budget B:

1. policy/system: 10-15%
2. continuity: 15-25%
3. evidence pack: 45-60%
4. guard/planning: remainder

## 11. UI-Managed Role Routing

Configuration remains UI-first and transactional (draft/apply/revert).

Roles:

1. orchestrator
2. chunker
3. extractor
4. summarizer
5. verifier
6. packer

Routing considers:

1. explicit role config
2. provider health and failures
3. latency/cost policy
4. stage criticality

Fallback policy:

1. fallback only within allowed model set
2. verifier is required by default for large/external payload classes
3. if verifier unavailable, response is marked provisional with explicit caveats

## 12. Reliability and Safety Requirements

1. stage handlers must remain idempotent and retry-safe
2. dead-letter paths must stay visible in command center
3. replay must be possible from stage boundaries
4. diagnostics/logs must follow redaction policy

## 13. Success Metrics

1. p95 provisional response latency
2. p95 verified response latency
3. layer-0 citation coverage
4. chunk-to-claim traceability completeness
5. context-pack budget compliance rate

## 14. Implementation Focus (Delta-Oriented)

Priority implementation sequence in Medousa:

1. artifact receipt and no-inline-blob ingestion
2. chunking and STTP chunk-node reference writes
3. extraction plus layered summary outputs
4. verification and confidence gating
5. context packing and reference-first retrieval
6. settings UI for role routing and profile control
7. progressive disclosure UX and operator controls

## 15. Open Questions

1. verifier strictness defaults by payload class threshold
2. when to parallelize extract and summarize for specific content types
3. cache strategy for repeated payload ids across short windows
4. cost policy as hard limit vs soft target
