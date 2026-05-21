# Medousa Context Orchestration Spec

## Document Metadata

- Document Type: Architecture and Implementation Spec
- Audience: Engineer, Architect, Runtime Owner, Platform Operator
- Status: Draft for implementation
- Last Updated: 2026-05-21 (UI config + STTP chunk memory update)
- Scope: Payload-heavy workflow handling for small-model-first Medousa runtime

## 1. Problem Statement

Medousa is increasingly running workflows that return very large payloads (web search corpora, analytics bundles, structured diagnostic dumps). For small models, practical effective context budgets often sit near 120k-250k tokens for full turn quality. Inlining raw payloads into the chat thread causes:

1. Context saturation and quality collapse.
2. Lossy truncation that removes evidence fidelity.
3. Slow response times and brittle UX.
4. Increased hallucination risk when summary claims are not citation-grounded.

## 1.1 Pattern Alignment Requirements

This spec must follow existing Stasis and Medousa patterns rather than introduce parallel architecture styles.

1. DDD + Hexagonal boundaries remain the default:
1. domain contracts and invariants stay in domain/application layers
2. provider/model adapters stay behind ports
3. orchestration workers are composed by runtime builder wiring

2. Event-driven runtime remains the orchestration backbone:
1. stage transitions happen through durable events and jobs
2. outbox and lineage metadata are preserved for diagnostics and replay
3. handlers are idempotent and retry-safe

3. Context by reference is mandatory:
1. STTP node references move across job boundaries
2. raw payload transfer is minimized and intentionally hydrated

4. UI settings behavior follows existing transactional pattern:
1. draft vs applied model
2. explicit validation summary before apply
3. revert to last applied configuration
4. no hidden side-effect writes while editing

5. UX surface patterns must stay consistent with current overlays:
1. keyboard-first controls
2. tabbed sections where scope is large
3. progressive disclosure for advanced detail

## 2. Core Design Principle

Treat the main chat model as an orchestrator of intent, not a raw payload processor.

1. Main model responsibilities:
1. interpret user intent
2. choose pipeline depth
3. schedule sub-agents
4. produce final user-facing answer using compressed evidence

2. Sub-agent responsibilities:
1. process payload artifacts
2. extract facts and produce citations
3. summarize and verify
4. generate budgeted context packs

3. Stasis responsibilities:
1. durable event-driven execution
2. stage isolation and retries
3. lineage and diagnostics
4. distributed worker orchestration

## 3. Target Architecture

### 3.1 Planes

1. Orchestration Plane:
1. main chat orchestrator
2. scheduling policy
3. budget policy

2. Processing Plane:
1. chunker worker
2. extractor worker
3. summarizer worker
4. verifier worker
5. context packer worker

3. Memory and Evidence Plane:
1. raw artifacts
2. STTP chunk memory nodes
3. evidence graph
4. layered summaries
5. confidence and verification records
6. working-memory reference sets

4. Delivery and UI Plane:
1. progressive UI updates
2. confidence indicators
3. citations and drill-down

### 3.2 Runtime Roles

1. Main Orchestrator Model:
1. low-latency intent routing
2. schedule decisions
3. response assembly

2. Chunker Model:
1. structure-aware segmentation
2. stable chunk ids

3. Extractor Model:
1. entities, metrics, claims, anomalies
2. evidence references

4. Summarizer Model:
1. hierarchical summaries
2. unresolved questions

5. Verifier Model:
1. claim-to-source validation
2. confidence scoring

6. Packer Model:
1. token-budgeted bundle creation
2. priority ordering

## 4. End-to-End Flow

1. Grapheme workflow completes and emits large payload.
2. Raw payload is persisted as artifact; chat thread receives only a receipt.
3. Chunker converts artifact to semantic chunks and stores each chunk as an STTP node.
4. Extractor generates structured facts and evidence mappings.
5. Summarizer creates layered summaries.
6. Verifier validates claims and annotates confidence.
7. Context packer builds turn-specific budgeted context package using reference-first retrieval.
8. Main orchestrator answers user using context pack and citations.
9. UI supports progressive disclosure from summary to raw evidence.

## 5. Event Contracts

All stages are event-driven and idempotent.

### 5.1 Canonical Events

1. payload.artifact_ingested
2. payload.chunking_requested
3. payload.chunking_completed
4. payload.chunk_sttp_nodes_persisted
5. payload.extraction_requested
6. payload.extraction_completed
7. payload.summarization_requested
8. payload.summarization_completed
9. payload.verification_requested
10. payload.verification_completed
11. context.pack_requested
12. context.pack_completed
13. orchestrator.response_ready

### 5.2 Shared Envelope Fields

1. event_id
2. causation_id
3. correlation_id
4. session_id
5. run_id
6. payload_id
7. stage
8. emitted_at_utc
9. attempt
10. idempotency_key

### 5.3 Failure Events

1. payload.stage_failed
2. payload.stage_dead_lettered
3. payload.retry_scheduled

Failure event payload includes:

1. stage
2. error_type
3. error_message_redacted
4. attempt
5. retry_at_utc
6. terminal_failure bool

## 6. Data Model and Lineage

### 6.1 Core Entities

1. PayloadArtifact:
1. payload_id
2. mime_type
3. byte_size
4. source_workflow_run_id
5. content_uri or embedded reference

2. STTPChunkNode:
1. node_id
2. payload_id
3. chunk_id
4. sequence
5. token_estimate
6. chunk_text
7. structural_path (json pointer, section path, table coordinates)
8. parent_node_id
9. retrieval_tags
10. checksum

3. EvidenceClaim:
1. claim_id
2. statement
3. supporting_chunk_ids
4. support_strength

4. SummaryLayer:
1. layer_id
2. depth (0 executive, 1 sectional, 2 detailed)
3. summary_text
4. claim_ids

5. VerificationRecord:
1. verification_id
2. claim_id
3. verdict (supported, weak, unsupported)
4. notes
5. confidence_0_1

6. ContextPack:
1. pack_id
2. budget_profile
3. selected_layers
4. selected_claims
5. selected_chunk_node_refs
6. total_token_estimate

7. WorkingMemoryRefSet:
1. ref_set_id
2. session_id
3. task_intent
4. node_refs ordered by priority
5. expiry_policy

### 6.2 Lineage Requirement

Every user-visible claim must be traceable:

final response -> summary sentence -> claim_id -> supporting_chunk_node_refs -> STTP chunk nodes -> raw payload artifact

## 7. Chunking Strategy

### 7.1 Principles

1. Chunk semantically, not by fixed size only.
2. Preserve structural boundaries first.
3. Maintain deterministic chunk ids for replayability.
4. Keep overlap minimal and targeted.
5. Persist each chunk as an STTP memory node with retrieval metadata.

### 7.2 Type-Specific Policy

1. JSON:
1. chunk by object boundaries and logical paths
2. preserve parent context keys in each chunk header

2. Markdown/HTML:
1. chunk by headings, then sub-sections
2. maintain heading path metadata

3. Tabular data:
1. chunk by row windows with schema header repeats
2. keep key column context

4. Logs/timelines:
1. chunk by time windows and event clusters
2. preserve ordering metadata

### 7.3 Chunk Sizing

1. target chunk size: 1.5k-3k tokens
2. hard maximum: 4k tokens
3. overlap budget: 5-12 percent when boundary risk exists

### 7.4 STTP Chunk Node Contract

Each produced chunk is written into memory as an STTP node with:

1. stable node id and chunk id
2. payload and run lineage metadata
3. retrieval tags by entity, metric, and topic
4. structural path and sequence ordering
5. checksum for deterministic replay checks

The orchestrator does not need raw chunk text in prompt context by default. It can retrieve working memory by node reference and hydrate only what is needed.

## 8. Summary and Verification Strategy

### 8.1 Layered Summary Contract

1. Layer 0:
1. direct answer summary
2. max 12 bullet-equivalent statements

2. Layer 1:
1. grouped by topical sections
2. includes key metrics and anomalies

3. Layer 2:
1. detailed claim-level notes
2. explicit caveats and unknowns

### 8.2 Verification Rules

1. No high-confidence claim without at least one supporting chunk.
2. Unsupported claims are excluded from Layer 0 by default.
3. Weak claims may appear only with confidence label.
4. Final response should surface confidence and caveats succinctly.

## 9. Context Budget Management

### 9.1 Budget Classes

For any turn budget B:

1. policy/system: 10-15 percent
2. conversation continuity: 15-25 percent
3. evidence bundle: 45-60 percent
4. planning/scratch and guard: remainder

### 9.2 Packing Policy

1. Prefer verified summaries over raw chunks.
2. Prefer STTP node references over inline raw chunk text.
3. Include raw chunks only for high-value or user-requested drill-down.
4. Prefer diversity of evidence coverage over redundant adjacent chunks.
5. Keep a fallback pack if primary pack exceeds budget post-tokenization.

### 9.3 Working-Memory Retrieval Policy

1. Build pack from reference sets first, not from full payload scans each turn.
2. Hydrate top-ranked STTP nodes just-in-time by intent.
3. Limit direct chunk text hydration to explicit budget slice.
4. Keep reference-only mode available for low-latency turns.

## 10. Multi-Provider and Multi-Model Orchestration in UI Config

This is required and first-class, and is configured from Medousa UI surfaces, not a separate user-managed config file.

### 10.1 Role-Based Model Assignment

Each stage role is assigned by config and can be switched without code changes.

Roles:

1. orchestrator
2. chunker
3. extractor
4. summarizer
5. verifier
6. packer

### 10.2 UI-First Configuration Surface

The configuration is managed in Settings under a dedicated Orchestration section with:

1. Profile selector (active profile per session or global default)
2. Role assignment grid (role -> provider/model)
3. Per-role fallback chain editor
4. Budget sliders and numeric controls
5. Routing policy selectors (latency, cost, balanced)
6. Safety toggles (verifier required, reference-only default)

Changes are draft-based and applied using the same transactional model as existing settings.

### 10.3 Persisted Internal Schema (Stored by UI)

UI writes orchestration settings to internal persisted defaults/state. Example shape:

```json
{
  "orchestration": {
    "default_profile": "small-model-safe",
    "profiles": {
      "small-model-safe": {
        "budgets": {
          "max_turn_tokens": 220000,
          "evidence_percent": 0.55,
          "reference_first": true
        },
        "roles": {
          "orchestrator": {
            "provider": "anthropic",
            "model": "claude-3-5-haiku",
            "fallback": [
              { "provider": "openai", "model": "gpt-4o-mini" }
            ]
          },
          "chunker": { "provider": "openai", "model": "gpt-4o-mini" },
          "extractor": { "provider": "openai", "model": "gpt-4.1-mini" },
          "summarizer": { "provider": "anthropic", "model": "claude-3-5-sonnet" },
          "verifier": { "provider": "openai", "model": "gpt-4.1" },
          "packer": { "provider": "openai", "model": "gpt-4o-mini" }
        },
        "routing": {
          "policy": "latency_cost_balanced",
          "max_stage_retries": 2,
          "health_weight_window_sec": 60
        }
      }
    }
  }
}
```

### 10.4 Routing Policy

At runtime, role routing considers:

1. explicit role config
2. provider health and recent failures
3. latency objective
4. cost objective
5. stage criticality

### 10.5 Fallback Rules

1. Role fallback only within allowed provider/model set.
2. Verifier role cannot be skipped for large payload classes by default.
3. If verifier unavailable, degrade response confidence and mark as provisional.

## 11. Scheduler Policy for Main UI Model

Main orchestrator chooses depth by intent and payload size.

1. quick-answer mode:
1. layer 0 summary only
2. verifier-required for external fact claims

2. analysis mode:
1. layer 0 plus selected layer 1
2. include evidence references

3. deep-audit mode:
1. layer 0/1/2
2. include direct chunk excerpts

Scheduling signals:

1. payload token estimate
2. user requested depth
3. latency budget
4. confidence target

## 12. UI and UX Contract

### 12.1 Progressive Experience

1. Stage 1: immediate receipt and progress state.
2. Stage 2: provisional summary appears quickly.
3. Stage 3: verified summary replaces provisional state.
4. Stage 4: optional evidence drill-down.

### 12.2 User Controls

1. response depth: concise, standard, deep
2. confidence threshold: permissive, balanced, strict
3. evidence mode: hidden, on-demand, always show citations

### 12.3 Safety Messaging

When confidence is low, UX must explicitly state:

1. what is uncertain
2. why uncertainty exists
3. how to fetch deeper evidence

## 13. Operational and Reliability Requirements

1. Every stage idempotent by idempotency_key.
2. Retries with bounded backoff.
3. Dead-letter visibility in command center.
4. Replay support from any stage boundary.
5. Full redaction policy for logs and diagnostics.

## 14. Metrics and SLOs

### 14.1 Core Metrics

1. payload size distribution
2. chunk count per payload
3. extraction latency
4. summarization latency
5. verification pass rate
6. claim citation coverage
7. context pack token utilization
8. user-visible answer latency p50, p95

### 14.2 Suggested SLOs

1. p95 provisional response under 3 seconds for moderate payload classes.
2. p95 verified response under 12 seconds for large payload classes.
3. citation coverage for layer 0 claims above 98 percent.

## 15. Implementation Phases

### Phase 1: Artifact and Receipts

1. persist payload artifacts
2. return lightweight receipts to chat thread
3. no raw payload in main context

### Phase 2: STTP Chunk, Extract, Summarize

1. semantic chunker worker with STTP node persistence
2. extractor worker
3. layered summarizer worker

### Phase 3: Verify and Confidence

1. verifier worker
2. confidence scoring and unsupported-claim suppression

### Phase 4: Context Packer and Budget Enforcement

1. budget class policy implementation
2. deterministic pack builder

### Phase 5: UI-Managed Multi-Provider Role Routing

1. role mapping from UI-managed config
2. fallback and health-aware routing
3. profile selection per session

### Phase 6: UI Progressive Disclosure

1. provisional vs verified states
2. confidence and citation UI
3. drill-down evidence interactions

## 16. Open Questions

1. Should verifier be mandatory for all external-data responses or only payload classes over threshold T?
2. Should extraction and summarization be parallel branches for certain content types?
3. How aggressive should recency caching be for repeated payload ids?
4. Should cost budgets be hard limits or soft targets with user override?

## 17. Immediate Next Engineering Tasks

1. Add orchestration UI section and internal schema validation.
2. Define event DTOs and stage job contracts.
3. Implement payload artifact ingestion and receipt emission.
4. Build chunker worker with deterministic chunk id generation and STTP chunk node persistence.
5. Add claim and citation storage model with STTP node references.
6. Implement working-memory reference retrieval in context pack builder.
7. Wire provisional and verified response states in TUI.
