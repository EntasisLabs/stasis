# Stasis Framework RFC: Unified AI Orchestration Architecture

Status: Draft (alignment baseline)
Date: 2026-05-14
Owner: Stasis Core

## 1. Purpose

Define the canonical architecture for Stasis as a framework that unifies:
- AI interaction abstraction (provider-agnostic request/response handling)
- Tool and function routing
- Agent orchestration (single and multi-agent)
- Runtime job orchestration (queueing, retries, dead-letter, replay, lineage)

This RFC is the source of truth to prevent product-layer drift and boundary leakage.

## 2. Problem Statement

Recent work mixed product concerns (Medousa behavior) with framework concerns (Stasis contracts and execution pipeline). This created ambiguity about ownership of:
- Provider selection and AI client wiring
- Tool registration and invocation loops
- Agent orchestration semantics
- Runtime job lifecycle and observability

We need strict boundaries so applications build workflows while Stasis owns orchestration complexity.

## 3. Vision

Stasis should play the role analogous to an Extensions.AI + Agent Framework stack:
- Applications define business workflows and policies.
- Stasis executes, routes, retries, traces, and audits.
- Infrastructure adapters (for example genai) are hidden behind Stasis ports.

## 4. Core Principles

1. Framework-first ownership.
Stasis owns orchestration semantics and provider abstraction. Applications consume APIs.

2. Single pipeline for AI interactions.
All prompts, tool calls, agent turns, and model requests flow through a Stasis pipeline.

3. Explicit contracts over incidental behavior.
No application should depend on adapter-specific behavior or provider SDK types.

4. Observability by default.
Lineage, diagnostics, attempt history, and replay must be first-class and consistent.

5. Deterministic control points.
Policy, guardrails, retries, and routing decisions must be explicit and auditable.

## 5. Layer Model (Mandatory)

## 5.1 Application Layer (Consumers)

Examples: Medousa and future products.
Responsibilities:
- Declare workflows, tools, and agent intents.
- Submit Stasis requests and consume Stasis responses.
- Present product UX.

Must not:
- Instantiate provider SDK clients directly.
- Implement orchestration loops directly.
- Bypass Stasis job/runtime stores for execution state.

## 5.2 Stasis Framework Layer (Core)

Responsibilities:
- AI pipeline orchestration.
- Tool registration and invocation routing.
- Agent execution model (single/multi-agent).
- Runtime job lifecycle and scheduling.
- Replay, lineage, diagnostics, retention.

## 5.3 Adapter Layer (Infrastructure)

Responsibilities:
- Implement Stasis outbound ports.
- Map Stasis contracts to provider/library semantics.

Examples:
- genai-backed chat adapter
- Surreal-backed runtime stores

## 5.4 Provider Layer

External APIs/libraries (OpenAI, Anthropic, Ollama, etc.)

## 6. Boundary Rules (Non-Negotiable)

1. Product crates may only depend on Stasis public APIs for orchestration and AI operations.
2. Product crates must not import provider SDK types in core execution paths.
3. Tool registration must go through Stasis registries/contracts.
4. Agent-to-tool and agent-to-agent execution must go through Stasis runtime pipeline.
5. Job state, retries, replay, and lineage are authored by Stasis only.

## 7. Canonical Stasis Execution Flow

1. Application submits an orchestration request.
2. Stasis pipeline resolves execution context (model, policy, tool visibility, runtime options).
3. Stasis dispatches AI request through provider-agnostic port.
4. If tool calls are returned, Stasis invokes registered tools and appends outcomes.
5. Stasis loops until completion or policy termination.
6. Stasis persists attempts/events and emits lineage metadata.
7. Stasis returns final result object to application.

The same flow applies in synchronous and queued/background execution modes.

## 8. Stasis Contract Surface (Target)

This section defines intended API categories, not final signatures.

1. AI Contracts
- Chat request/response abstractions
- Tool call and tool response abstractions
- Model routing and policy options

2. Tooling Contracts
- Tool registration and discovery
- Validation schemas
- Invocation adapters

3. Agent Contracts
- Agent definition
- Thread/context model
- Group coordination strategy hooks

4. Runtime Contracts
- Job submission and processing
- Retry/dead-letter controls
- Recurring scheduling
- Replay and lineage query APIs

## 9. genai Integration Strategy

genai is an infrastructure capability provider, not the framework boundary.

Use genai for:
- provider/model routing
- auth/target resolver capabilities
- chat and tool-call execution primitives

Do not expose genai directly in application-layer APIs where this creates lock-in.

Stasis may internally reuse genai request/response structures where practical, but Stasis remains the owning abstraction boundary.

## 10. Observability and Governance Requirements

All orchestration paths must support:
- attempt-level diagnostics
- guardrail classification and policy reason
- execution id correlation
- outbox lineage events
- replay reports

PRs affecting execution paths must preserve these guarantees.

## 11. Migration Plan (From Current State)

## Phase A: Freeze and Align

- Pause net-new product features.
- Use this RFC as architecture gate for new changes.

## Phase B: Consolidate Stasis APIs

- Define and stabilize Stasis-first orchestration APIs.
- Move product-specific orchestration logic into Stasis where it is generic.

## Phase C: Convert Medousa to Consumer Mode

- Medousa uses only Stasis APIs for orchestration.
- Remove direct adapter-facing execution paths from product binaries.

## Phase D: Harden and Validate

- Add integration tests that enforce boundary rules.
- Add docs and examples for consumer-only usage patterns.

## 12. Drift Prevention Checklist (Required in PRs)

Every PR touching orchestration must answer:
1. Which layer does this change belong to?
2. Does any application code instantiate provider adapter logic directly?
3. Are tool/agent/runtime transitions flowing through Stasis APIs?
4. Are diagnostics/lineage/replay guarantees preserved?
5. Does this increase or reduce framework boundary leakage?

If any answer indicates leakage, redesign before merge.

## 13. Acceptance Criteria for RFC Adoption

This RFC is considered adopted when:
- It is linked from docs index and docs-book summary.
- New architecture-impacting PRs reference this RFC.
- Medousa roadmap and implementation plan align to consumer-only model.

## 14. Open Decisions

1. Final shape of Stasis public AI request/response contracts.
2. Degree of direct type reuse from genai vs Stasis-owned wrappers.
3. Standard middleware/pipeline extension points (policy, caching, telemetry).

## 15. Immediate Next Step

Create a short companion implementation plan that maps existing modules to target layers and identifies exact refactors needed to complete Phase B and Phase C.
