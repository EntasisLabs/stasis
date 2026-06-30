# Locus Integration RFC And Delivery Plan

Status: Implemented (core delivery complete)
Date: 2026-05-14
Owner: Stasis Core
Depends on:
- stasis-framework-rfc.md
- stasis-framework-implementation-plan.md
- ../v1-runtime-draft.md

Execution update (2026-06-23):
- **0.7.0** delivers semantic tags/index, eviction policy, and graph workflows on Locus **0.4.1 / locus-sdk 0.2.1**.
- **0.7.1** bumps Locus to **0.4.2 / locus-sdk 0.2.2** for semantic `null` handling in parser and SurrealDB storage.
- `LocusMemoryStore` bundles node store + semantic index; ingest and find/recall/evict/transform paths wire `with_semantic_index()`.
- Memory operation workflows now include `find`, `evict`, and `graph` in addition to recall, aggregate, transform, rollup, and schema.

Execution update (2026-05-14):
- Phases L1 through L6 are delivered in code and validated in CI-style local runs.
- Locus-backed memory ports/adapters are wired through Stasis runtime handlers for prompt, tool-loop, agent-turn, and agent-session paths.
- Memory operation workflows (`workflow.stasis.memory.{recall,find,graph,aggregate,transform,rollup,schema,evict}`) are registered and exercised in backend parity coverage.
- Diagnostics contract keys from Section 7.1 are emitted in memory-enabled success paths, including RFC alias keys.
- Outbox lineage metadata from Section 7.2 is now projected when available: `input_memory_query_id`, `output_memory_node_id`, and `retrieval_path`.
- Remaining optional follow-up: deepen query lineage semantics beyond deterministic query-id generation if product analytics need richer provenance.

## 1. Purpose

Define the canonical approach for integrating Locus into Stasis as a first-class memory substrate for:
- pre-execution memory retrieval
- post-execution memory persistence
- deterministic STTP continuity across runtime jobs
- memory operations as governed runtime workflows

This RFC is both architecture decision record and phased implementation plan.

## 2. Problem Statement

Stasis already carries STTP continuity fields through runtime jobs:
- sttp_input_node_id
- sttp_output_node_id

However, current execution paths do not invoke Locus SDK services directly. This creates a semantic gap:
- job lineage captures STTP references, but memory retrieval and storage policy is not enforced in handlers
- recall, explainability, and migration operations are not available through Stasis runtime workflows
- Medousa and future consumers cannot rely on a single Stasis-owned memory orchestration surface

## 3. Scope

In scope:
- Add Stasis memory ports and adapters backed by locus-sdk and locus-core-rs.
- Integrate memory retrieval into prompt, tool-loop, agent-turn, and agent-session runtime handlers.
- Integrate memory persistence after successful executions.
- Introduce memory workflow jobs for maintenance and operability.
- Extend diagnostics and lineage with memory retrieval and storage metadata.

Out of scope:
- replacing existing runtime queue/store design
- changing Stasis job state machine semantics
- cross-region sync policy decisions beyond connector-ready primitives
- product UI implementation details

## 4. Design Principles

1. Framework-first ownership.
Memory orchestration belongs to Stasis runtime and orchestration layers, not product binaries.

2. Deterministic-by-default.
Recall policy, fallback behavior, strictness, scope, and limits are explicit in payloads and diagnostics.

3. Backward-compatible rollout.
Memory integration is additive with feature flags and safe defaults.

4. Auditability as contract.
Retrieval path, fallback trigger, selected node count, and output node id are always diagnosable.

5. Adapter neutrality.
Stasis ports define memory behavior; Locus is the default adapter implementation.

## 5. Current State Summary

Already in place:
- Runtime builders and handlers exist for prompt, tool-loop, agent-turn, and agent-session.
- STTP in/out references persist through in-memory and Surreal backends.
- Typed payload builders and runtime parity tests are established.
- Locus crates are already listed in root dependencies.

Missing today (historical — resolved by 0.7.0):
- ~~no concrete Locus service invocation in Stasis runtime handler path~~
- ~~no memory retrieval before model execution~~
- ~~no memory persistence after successful execution~~
- ~~no dedicated memory operations workflow jobs in runtime~~

## 6. Proposed Architecture

## 6.1 New Outbound Ports

Add new ports under src/ports/outbound/memory:
- memory_context_reader.rs
- memory_context_writer.rs
- memory_operations.rs

Port responsibilities:
- MemoryContextReader: recall, find, and graph retrieval operations
- MemoryContextWriter: persist STTP node content and sync semantic tag index on ingest
- MemoryOperations: aggregation, transform, rollup, schema, and evict operations

## 6.2 Locus Adapters

Add infrastructure adapters under src/infrastructure/memory:
- locus_context_reader.rs
- locus_context_writer.rs
- locus_memory_operations.rs
- locus_node_store_factory.rs

Behavior:
- use locus-sdk for recall, explain, composition, and transform workflows
- use locus-core-rs for node store bootstrap, validation, and store-context operations
- support in-memory and Surreal-backed Locus node store configuration

## 6.3 Runtime Injection

Extend runtime composition in src/application/runtime/stasis_runtime_builder.rs:
- allow optional memory reader/writer/operations injection
- provide sane default Locus implementations when enabled
- register memory-enabled variants of handlers

## 6.4 Handler Integration Points

Prompt path (src/application/runtime/prompt_chat_job_handler.rs):
- pre-step: retrieve memory context via MemoryContextReader
- execute prompt with injected context envelope
- post-step: store output STTP node via MemoryContextWriter

Tool-loop path (src/application/runtime/tool_loop_job_handler.rs):
- pre-step: recall context for tool planning
- loop execution: preserve retrieval diagnostics
- post-step: store summarized tool-loop result node

Agent-turn path (src/application/runtime/agent_turn_job_handler.rs):
- pre-step: recall context with turn and policy scope
- post-step: persist turn result node and update sttp_output_node_id

Agent-session path (src/application/runtime/agent_session_job_handler.rs):
- session-start: optional session-level recall bundle
- per-turn: propagate parent output node id as input
- session-end: persist session summary node and optional rollup trigger

## 6.5 Job Contracts And Payloads

Extend typed payload contracts in src/application/orchestration/agent_session_payload.rs and builder surface in src/application/orchestration/stasis_workflow_job_builder.rs to support:
- memory_scope
- memory_tiers
- memory_limit
- memory_scoring (alpha, beta, fallback_policy, strictness)
- memory_query_text (optional)
- memory_store_mode (disabled, summary_only, full)

Defaults:
- memory retrieval on for prompt and agent paths
- bounded limit with deterministic clamp
- fallback_policy set explicitly

## 6.6 Memory Operations As Runtime Workflows

Add workflow job classes:
- workflow.stasis.memory.recall
- workflow.stasis.memory.find
- workflow.stasis.memory.graph
- workflow.stasis.memory.aggregate
- workflow.stasis.memory.transform
- workflow.stasis.memory.rollup
- workflow.stasis.memory.schema
- workflow.stasis.memory.evict

Purpose:
- operational memory tasks run through existing retry, diagnostics, lineage, and outbox infrastructure

## 7. Data And Observability Contracts

## 7.1 Job Attempt Diagnostics

Extend diagnostics payload shape with:
- memory_retrieved_count
- memory_retrieval_path
- memory_fallback_triggered
- memory_fallback_reason
- memory_scope_hash
- memory_store_valid
- memory_store_node_id

## 7.2 Outbox Lineage

Include memory metadata in runtime lineage event payload where available:
- input_memory_query_id
- output_memory_node_id
- retrieval_path

## 7.3 STTP Continuity Rules

Continuity invariant:
- if job B is causally dependent on job A, then B.sttp_input_node_id should use A.sttp_output_node_id when available.

Persistence invariant:
- successful memory-enabled execution must produce sttp_output_node_id unless memory store mode is disabled by policy.

## 8. Security And Policy

1. Scope minimization.
Require explicit scope defaults: session-scoped by default, global access only by policy.

2. Secret hygiene.
No API keys or raw secret material in node content, job payload_ref, diagnostics, or outbox events.

3. Parse and validation hardening.
Use strict validation path for persisted STTP nodes.

4. Guardrail propagation.
Memory policy violations should map to existing runtime failure and diagnostics patterns.

## 9. Rollout Plan

## Phase L0: RFC Adoption And Contract Freeze

Actions:
- ratify this RFC
- freeze initial memory port contracts
- document temporary compatibility behavior

Acceptance:
- RFC linked in docs index
- port interfaces approved

## Phase L1: Ports And Adapters

Actions:
- introduce memory ports
- implement Locus-backed adapters
- add adapter tests using in-memory Locus store

Acceptance:
- adapter unit tests pass
- no runtime behavior change yet

## Phase L2: Prompt Path Integration

Actions:
- integrate retrieval and storage in prompt handler
- emit memory diagnostics in attempt records

Acceptance:
- prompt workflow produces memory metadata
- sttp_output_node_id set from memory persistence path

## Phase L3: Tool And Agent Turn Integration

Actions:
- integrate memory into tool-loop and agent-turn handlers
- enforce bounded retrieval defaults

Acceptance:
- parity tests validate both backends for tool and turn paths
- memory fallback path diagnostics visible

## Phase L4: Agent Session Integration

Actions:
- session-level memory orchestration hooks
- final session summary persistence

Acceptance:
- coordinated session path emits deterministic memory continuity

## Phase L5: Memory Operation Workflows

Actions:
- add runtime handlers and payload contracts for memory operation jobs
- wire into StasisWorkflowJobBuilder constructors

Acceptance:
- memory operation jobs run through runtime retry/lineage system

## Phase L6: Hardening And Drift Tests

Actions:
- architecture conformance checks to prevent direct Locus use in consumer binaries
- invariants for continuity and diagnostics

Acceptance:
- CI fails on boundary leakage or missing diagnostics fields

## Phase L7: Documentation And Consumer Migration

Actions:
- update README examples for memory-enabled prompt and ask flows
- migrate Medousa flows to use memory policy options through Stasis-only APIs

Acceptance:
- Medousa remains consumer-only
- end-to-end docs demonstrate deterministic memory orchestration

## 10. Testing Strategy

Unit tests:
- port contract behavior and clamp logic
- adapter mapping and error translation
- payload serialization with memory options

Integration tests:
- runtime backend parity for memory-enabled prompt/tool/turn/session workflows
- operation job classes and retry behavior
- continuity chain assertions using sttp_input_node_id and sttp_output_node_id

Conformance tests:
- application layer cannot construct Locus services directly in execution path
- memory diagnostics required for memory-enabled jobs

Failure-mode tests:
- empty retrieval fallback paths
- validation failure on store
- adapter unavailable and policy-driven degradation behavior

## 11. Backward Compatibility

Compatibility mode:
- if memory adapters are not configured, handlers preserve current behavior and still return functional results

Migration mode:
- optional runtime toggle enables memory operations gradually by workflow class

Deprecation:
- after full rollout, direct non-memory legacy prompt path behavior is retained only as compatibility fallback and may be deprecated later

## 12. Risks And Mitigations

Risk: latency increase from pre-recall calls.
Mitigation: bounded limits, optional explain path, and async operation budgets.

Risk: non-deterministic retrieval behavior from implicit defaults.
Mitigation: explicit scoring and fallback fields in payload contracts.

Risk: schema drift across handlers.
Mitigation: typed payloads and shared diagnostics contract tests.

Risk: architecture leakage into consumers.
Mitigation: architecture conformance tests and PR checklist enforcement.

## 13. Alternatives Considered

Alternative A: Keep Locus calls only in Medousa.
Rejected because it violates framework ownership and duplicates memory policy logic across consumers.

Alternative B: Add memory as a sidecar service only.
Rejected because it weakens runtime lineage coupling and retry/audit guarantees.

Alternative C: Persist memory only at session boundaries.
Rejected because turn/tool granularity is required for high-fidelity continuity and replay.

## 14. Open Questions

1. Should memory explain be always-on for prompt jobs or policy-controlled?
2. What is the default memory tier filter for consumer ask flows?
3. Should session completion automatically enqueue monthly rollup jobs?
4. Which diagnostics fields are mandatory vs optional for first rollout?

## 15. Done Criteria

This RFC is complete when:
- Locus-backed memory retrieval and storage are integrated into prompt/tool/turn/session handlers.
- Memory operation jobs are available through runtime workflow classes.
- Both runtime backends pass parity and continuity tests.
- Medousa uses Stasis memory orchestration APIs without direct Locus coupling.
- Architecture conformance tests enforce boundaries and diagnostics invariants.

## 16. Immediate Next Step

Ship Phase L1 as a dedicated PR that adds memory ports, Locus adapters, and adapter-level tests without changing runtime handler behavior.
