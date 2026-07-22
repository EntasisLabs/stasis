# ADR-0007 Agent Platform Runtime Contracts

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-07-22
- Verified Against:
  - docs/design/agent-platform-runtime-contracts-plan.md
  - src/ports/outbound/runtime/event_publisher.rs
  - src/ports/outbound/runtime/endpoint_transport_publisher.rs
  - src/application/orchestration/tool_registry.rs
  - src/domain/runtime/outbox.rs
  - src/application/runtime/stasis_runtime_builder.rs

## Status

Proposed

## Date

2026-07-22

## Context

Stasis is a durable runtime for long-running AI systems (leases, retries, outbox, threads, orchestration), analogous to MassTransit/Hangfire for agent workloads.

The ecosystem is shifting from “call a model provider” to “coordinate heterogeneous agent platforms.” Platform builders need Stasis to provide durable work and contracts, not vendor integrations for Cursor, Codex, Claude Code, or similar products.

Today’s gaps relative to that role:

1. Eventing is primarily outbound job-lifecycle outbox fanout (`JobSucceeded`, retry, dead-letter).
2. There is no canonical agent message model or format translation port.
3. Tools are local `StasisTool` registrations only; there is no injectable MCP provider/export contract.
4. Putting product-specific agent adapters in-repo would couple the runtime to vendors and fight hexagonal boundaries.

## Decision

Stasis will remain a **runtime kernel** and expose three vendor-neutral contracts that external gateways implement:

### 1) Comms contract

Expand the existing delivery/outbox plane into a bidirectional, durable agent communication plane:

- Outbound publish (already present) plus **ingress** for replies, progress, cancel, and heartbeat.
- Transport ports stay protocol-oriented (HTTP webhook, TCP, Kafka, RabbitMQ, and future agent-common channels such as WebSocket/SSE/stdio), never product-named.
- Correlation remains first-class via `thread_id`, `job_id`, `correlation_id`, `causation_id`, and turn identifiers.

### 2) Translation contract

Introduce a Stasis-canonical agent envelope and pure codec ports:

```text
canonical Stasis agent events/messages  ⇄  ACP / JSON / XML / other wire formats
```

- Runtime orchestration always speaks the canonical model.
- Codecs are side-effect free encode/decode (+ schema version).
- Concrete vendor/gateway codecs live outside the core repo unless shipped as generic reference codecs (for example canonical JSON).

### 3) MCP tool bridge contract

Treat MCP as an injectable tool source/sink around `StasisTool` / `ToolRegistry`:

- `McpToolProvider` — list/invoke remote MCP tools as `StasisTool`s inside Stasis tool loops.
- `McpToolExporter` — project selected Stasis tools/workflows as MCP tools for external agents.
- Composition root wires both directions (DI), similar to MassTransit consumers/publishers. Core Stasis never depends on a concrete gateway crate.

## Non-Goals

1. No in-repo adapters named for Cursor, Codex, Claude Code, Grok, Hermes, OpenClaw, or other agent products.
2. No replacement of `AiChatClient` / `genai` provider integration; direct model calls remain a separate capability.
3. No requirement that every transport support every agent event in the first slice.
4. No full MCP server/client implementation mandated in-core beyond ports, test doubles, and optional thin reference adapters.

## Consequences

### Positive

1. Platform builders can ship gateways without forking Stasis.
2. Durable job/outbox/thread semantics stay the coordination backbone.
3. Clear DI seams (`with_mcp_provider`, codec registration, ingress handlers) match existing builder patterns.
4. Avoids vendor lock-in and keeps architecture conformance simple.

### Tradeoffs

1. Requires a frozen canonical agent event/tool model and versioning discipline.
2. Ingress + waitable turns add runtime state beyond fire-and-forget outbox publish.
3. MCP dual-facing bridge needs careful recursion/allowlist guards (Stasis tool → MCP → Stasis tool).

## Guardrails

1. Core modules may depend only on ports and the canonical model — never on gateway crates.
2. Comms, translation, and MCP remain separate ports (transport ≠ codec ≠ tool bridge).
3. New agentic runtime events must be durable via outbox (or an explicit documented fast-path exception for stream deltas).
4. Every phase ships with in-memory fakes and parity tests before optional network adapters.

## Plan

See [agent-platform-runtime-contracts-plan.md](../design/agent-platform-runtime-contracts-plan.md).
