# Stasis Framework Implementation Plan

Status: Draft
Date: 2026-05-14
Depends on: stasis-framework-rfc.md

Execution note (2026-05-14):
- Sprint 1 started.
- Initial P-B artifact delivered: `PromptExecutionPipeline` and execution envelopes added in Stasis core.
- First routed path updated: Medousa `llm` command now executes through Stasis pipeline contract.
- Runtime path milestone delivered: `workflow.stasis.prompt` job handler now executes through the same Stasis prompt pipeline.
- Medousa `ask` is routed to `workflow.stasis.prompt` to validate queued orchestration through Stasis-owned contracts.
- P-C baseline delivered: Stasis tool registry contracts (`StasisTool`, `ToolRegistry`, `InMemoryToolRegistry`) and `ToolLoopPipeline` added.
- Runtime tool-loop path delivered: `workflow.stasis.tool_loop` handler executes prompt -> tool -> prompt through Stasis-owned orchestration.
- Medousa `ask` now routes through `workflow.stasis.tool_loop` using registered mock web-search tool.
- P-C hardening delivered: schema-enforced tool input validation (`required`, `type`, `enum`, `additionalProperties`) in Stasis tool registry.
- P-C control-mode delivered: tool-loop policy now supports `auto` and `strict` tool-call modes.
- P-C observability delivered: tool-loop diagnostics now include `tool_invocations`, invoked tool names, round count, and termination reason.
- P-C parity + roundtrip delivered: Surreal schema-violation parity test added, and model-emitted tool-call roundtrip path verified in runtime integration tests.
- P-D baseline delivered: Stasis `AgentSessionPipeline` introduced with agent identity/thread context and execution policy.
- P-D baseline delivered: `workflow.stasis.agent_turn` runtime handler routes single-agent turns through the shared Stasis tool-loop pipeline.
- P-D baseline delivered: strategy hook interfaces (`AgentSelectionStrategy`, `AgentTerminationStrategy`) added for upcoming multi-agent coordination work.
- P-D coordination baseline delivered: concrete round-robin selection and max-turn termination strategies implemented.
- P-D coordination baseline delivered: `AgentSessionCoordinator` added with multi-turn session execution over shared Stasis turn pipeline.
- P-D runtime milestone delivered: `workflow.stasis.agent_session` job handler added to execute coordinated multi-turn sessions through Stasis runtime pipeline.
- P-E baseline progress delivered: Medousa `ask` now routes through `workflow.stasis.agent_session` instead of direct tool-loop job submission.
- P-D parity delivered: Surreal runtime parity test added for `workflow.stasis.agent_session` coordinated session path.
- P-E baseline progress delivered: shared `AgentSessionJobPayload` contract and serializer introduced; Medousa now uses typed payload construction instead of manual JSON string assembly.
- P-E baseline progress delivered: comprehensive `StasisRuntimeBuilder` introduced to centralize runtime composition (chat client, tool registration, default handlers, and optional customization); Medousa runtime wiring migrated to builder usage.
- P-E baseline progress delivered: shared typed workflow payload contracts (`AgentTurnJobPayload`, `ToolLoopJobPayload`) and fluent `StasisWorkflowJobBuilder` added to standardize `workflow.stasis.*` job submission; Medousa `ask` migrated off manual `NewJob` assembly.
- P-E baseline hardening delivered: runtime backend parity tests now construct all `workflow.stasis.{tool_loop,agent_turn,agent_session}` jobs via typed payloads + `StasisWorkflowJobBuilder` helpers, eliminating manual `job_type` string assembly in those scenarios.
- P-E migration delivered: Medousa `llm` path now submits `workflow.stasis.prompt` jobs through `StasisWorkflowJobBuilder::for_prompt` and runtime processing, removing direct adapter/pipeline construction from CLI execution flow.
- P-A guardrail artifact delivered: repository PR template added with mandatory RFC/implementation-plan reference and boundary checklist.
- P-F baseline hardening delivered: architecture conformance tests added to enforce Medousa runtime-path usage and prevent direct adapter construction drift in CLI code.

## 1. Goal

Translate the architecture RFC into an execution plan with:
- exact module ownership by layer
- ordered refactor phases
- acceptance checks per phase
- rollback-safe increments

## 2. Current-to-Target Module Mapping

## 2.1 Application Layer (Consumers)

Current modules:
- medousa/src/bin/medousa_cli.rs
- medousa/src/bin/medousa_daemon.rs
- medousa/src/lib.rs

Target ownership:
- Keep only workflow declarations, command UX, and application configuration.
- Remove direct orchestration internals from binaries over time.

## 2.2 Stasis Framework Layer (Core)

Current modules:
- src/application/runtime/in_memory_runtime.rs
- src/application/runtime/surreal_runtime.rs
- src/application/runtime/grapheme_job_handler.rs
- src/application/runtime/grapheme_healthcheck_job_handler.rs
- src/application/runtime/grapheme_echo_job_handler.rs
- src/application/runtime/grapheme_textops_job_handler.rs
- src/application/use_cases/investigate_runtime_lineage.rs

Target ownership:
- Unified orchestration pipeline for prompt, tool, and job execution.
- Stasis-owned tool registry and invocation lifecycle.
- Stasis-owned agent turn loop (single and multi-agent readiness).

## 2.3 Adapter Layer (Infrastructure)

Current modules:
- src/infrastructure/llm/genai_chat_client.rs
- src/infrastructure/llm/genai_gateway.rs
- src/infrastructure/runtime/surreal_* stores
- src/infrastructure/runtime/*_metrics.rs

Target ownership:
- Keep provider/storage mappings only.
- No orchestration policy in adapters.

## 2.4 Port Layer (Contracts)

Current modules:
- src/ports/outbound/ai_chat_client.rs
- src/ports/outbound/llm_gateway.rs
- src/ports/outbound/runtime/*

Target ownership:
- Canonical orchestration-facing contracts.
- Backward compatibility shims only when migration requires.

## 3. Refactor Phases

## Phase P-A: Freeze and Guardrails

Actions:
- Declare architecture-impacting PR rule: RFC reference required.
- Mark legacy/bridge interfaces explicitly as compatibility adapters.
- Add boundary checks to review checklist.

Acceptance:
- New PR template or checklist references the RFC and this plan.

## Phase P-B: Canonical AI Pipeline Contract

Actions:
- Introduce Stasis-level orchestration contract modules (new package area):
  - prompt request/response envelope
  - tool call envelope
  - execution context envelope (policy, model route, trace ids)
- Ensure all runtime AI execution paths go through this contract.

Acceptance:
- A single Stasis API path can execute a model request with policy + diagnostics.
- Existing behavior remains green in tests.

## Phase P-C: Tool Registration Unification

Actions:
- Implement Stasis tool registry abstraction.
- Route function/tool invocation through Stasis runtime pipeline.
- Ensure tool execution emits attempt + lineage diagnostics.

Acceptance:
- Tool invocation loop is owned by Stasis, not app crates.
- At least one integration test verifies tool-call roundtrip through runtime.

## Phase P-D: Agent Flow Unification

Actions:
- Add Stasis agent session abstraction:
  - agent identity
  - thread/context
  - turn execution policy
- Add single-agent execution path through pipeline.
- Add initial multi-agent coordination hooks (selection and termination strategy interfaces).

Acceptance:
- Single-agent and tool-using turns run through the same runtime event/attempt pipeline.
- Trace lineage connects request -> tool calls -> final response.

## Phase P-E: Consumer Mode Migration (Medousa)

Actions:
- Move Medousa binaries to call only Stasis public orchestration APIs.
- Remove direct adapter-facing code paths from Medousa execution flow.
- Keep Medousa focused on workflow intent and output formatting.

Acceptance:
- Medousa compiles without relying on infrastructure adapter specifics in execution path.
- End-to-end ask flow still works.

## Phase P-F: Hardening and Drift Tests

Actions:
- Add architecture conformance tests:
  - boundary checks for app crates
  - lineage/diagnostics invariants
- Add docs examples showing consumer-only usage.

Acceptance:
- CI catches layer violations and missing diagnostics metadata.

## 4. Sequenced Work Items (First 3 Sprints)

## Sprint 1: Contracts and Path Consolidation

Deliverables:
- P-A complete
- P-B partial (core request/response envelopes + one execution path)

Verification:
- cargo test -p stasis passes
- no regressions in runtime parity tests

## Sprint 2: Tool Pipeline Ownership

Deliverables:
- P-B complete
- P-C complete

Verification:
- integration test for tool-call loop via runtime
- lineage diagnostics include tool steps

## Sprint 3: Agent Session + Medousa Consumer Shift

Deliverables:
- P-D baseline
- P-E baseline

Verification:
- medousa ask flow uses Stasis-only orchestration API
- no direct adapter usage in Medousa execution path

## 5. Compatibility Strategy

- Keep llm_gateway as compatibility adapter until P-E completes.
- Keep existing Grapheme handlers operational while introducing unified pipeline routing.
- Prefer additive API introduction, then deprecate old entrypoints with migration notes.

## 6. Done Criteria

This plan is complete when:
- Medousa is fully consumer-mode.
- Stasis owns tool + agent + runtime orchestration through one canonical pipeline.
- Adapter libraries remain hidden behind Stasis ports.
- Drift-prevention checks are part of normal PR workflow.

## 7. Immediate Next Step

Start Sprint 1 with a dedicated “P-B contracts” PR that introduces canonical execution envelopes and routes one current path through them without behavior change.
