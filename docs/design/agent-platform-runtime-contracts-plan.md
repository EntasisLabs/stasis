# Agent Platform Runtime Contracts — RFC and Delivery Plan

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-07-22
- Verified Against:
  - docs/adr/ADR-0007-agent-platform-runtime-contracts.md
  - src/ports/outbound/runtime/event_publisher.rs
  - src/ports/outbound/runtime/endpoint_transport_publisher.rs
  - src/domain/runtime/outbox.rs
  - src/domain/runtime/delivery_endpoint.rs
  - src/application/orchestration/tool_registry.rs
  - src/application/orchestration/agent_session_pipeline.rs
  - src/application/runtime/stasis_runtime_builder.rs
  - docs-book/src/extension-points.md
  - docs-book/src/control-plane-endpoint-routing.md

Status: **Proposed Contract — Ready for Phased Implementation**  
Date: 2026-07-22  
Owner: Stasis Core  
ADR: [ADR-0007-agent-platform-runtime-contracts.md](../adr/ADR-0007-agent-platform-runtime-contracts.md)

Depends on:

- [ADR-0001](../adr/README.md) Durable Job Runtime
- [ADR-0002](../adr/README.md) Event-Driven Orchestration with Local CoR
- [ADR-0003](../adr/README.md) STTP Reference-Only Job Context
- [ADR-0007](../adr/ADR-0007-agent-platform-runtime-contracts.md) Agent Platform Runtime Contracts
- Existing outbox, endpoint routing, tool registry, and runtime builder surfaces

## 1. Purpose

Define how Stasis becomes the **durable work runtime for agent platforms** — the MassTransit/Hangfire layer — without embedding vendor agent products in-repo.

This plan freezes three contracts and a phased delivery path:

1. **Comms** — bidirectional durable messaging over transport ports
2. **Translation** — canonical Stasis agent model ↔ external wire formats
3. **MCP tool bridge** — injectable provider/exporter around `StasisTool`

Platform builders implement gateways against these ports. Stasis owns durability, leases, retries, correlation, and tool/job orchestration.

## 2. Problem Statement

Today Stasis can:

- run durable jobs with leases/retries/DLQ
- publish outbound job-lifecycle outbox events
- coordinate local multi-agent sessions through `AiChatClient` + `StasisTool`

It cannot yet:

- accept agentic replies/progress as first-class ingress
- speak a stable agent envelope independent of JSON shape of the day
- inject an MCP gateway as both a tool source and an outward MCP projection

Without contracts, every platform integration risks becoming a one-off vendor adapter inside core.

## 3. Product Positioning

```text
┌──────────────────────────────────────────────────────────────┐
│                 Agent platforms (outside Stasis)             │
│   IDE agents, CLI agents, custom runtimes, MCP gateways      │
└───────────────────────────┬──────────────────────────────────┘
                            │ implement ports
                            ▼
┌──────────────────────────────────────────────────────────────┐
│                      Stasis runtime kernel                   │
│  jobs · leases · retries · threads · outbox · tool loops     │
│  Comms ports · Translation ports · MCP bridge ports          │
└──────────────────────────────────────────────────────────────┘
```

Stasis is not “the agent.” Stasis is the durable coordination substrate people use to build agents and agent platforms.

## 4. Scope

### In scope

1. Canonical agent envelope + event vocabulary (versioned).
2. Comms ports: outbound publish enrichment + ingress + correlation.
3. Translation ports: encode/decode codecs; one reference JSON codec.
4. MCP ports: `McpToolProvider`, `McpToolExporter`, registry merge, recursion guards.
5. Builder DI wiring and extension-point docs.
6. Waitable/external turn job pattern using existing job + outbox machinery.
7. In-memory fakes, conformance tests, cookbook example of a **fake gateway** (not a vendor).

### Out of scope

1. Vendor-named integrations (Cursor, Codex, Claude Code, Grok, Hermes, OpenClaw, …).
2. Replacing `genai` / `AiChatClient` provider calls.
3. Full production MCP SDK embedding as a required dependency (optional feature only if needed later).
4. Dashboard UX for live multi-platform sessions (follow-on after contracts stabilize).
5. Exactly-once cross-process messaging guarantees beyond current at-least-once outbox semantics.

## 5. Architecture

### 5.1 Three planes (must stay separate)

```text
                ┌────────────────────┐
                │  Translation       │
                │  AgentMessageCodec │
                └─────────┬──────────┘
                          │ canonical envelopes
          ┌───────────────┴────────────────┐
          ▼                                ▼
 ┌─────────────────┐              ┌────────────────────┐
 │ Comms           │              │ MCP tool bridge    │
 │ Transport+Ingress│              │ Provider/Exporter  │
 └────────┬────────┘              └─────────┬──────────┘
          │                                 │
          └──────────────┬──────────────────┘
                         ▼
              Stasis runtime (jobs/tools/threads/outbox)
```

| Plane | Responsibility | Must not own |
| --- | --- | --- |
| Comms | deliver/receive bytes + metadata; durability; retries | format semantics, MCP tool schemas |
| Translation | map canonical ↔ wire format | sockets, tool invocation |
| MCP bridge | project tools in/out of `ToolRegistry` | transport protocol details |

### 5.2 Canonical model (hub)

All runtime orchestration uses a Stasis-owned model, for example:

```rust
// Illustrative frozen names — finalize in Phase 0
pub enum AgentEnvelopeKind {
    SessionStarted,
    TurnGranted,
    TurnAccepted,
    MessageAppended,
    ToolCallRequested,
    ToolCallCompleted,
    TurnCompleted,
    Progress,
    Heartbeat,
    CancelRequested,
    Cancelled,
    Failed,
    SessionTerminated,
}

pub struct AgentEnvelope {
    pub schema_version: u32,
    pub kind: AgentEnvelopeKind,
    pub session_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub job_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: String,
    pub participant_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value, // kind-specific structured payload
}
```

Rules:

1. Runtime persists/publishes canonical envelopes (or STTP refs to them).
2. Codecs convert only at comms edges.
3. `schema_version` is mandatory; unknown versions fail closed with a typed error.

### 5.3 Port sketches (target API)

```rust
#[async_trait]
pub trait AgentMessageCodec: Send + Sync {
    fn content_type(&self) -> &'static str;
    fn schema_name(&self) -> &'static str;
    fn encode(&self, envelope: &AgentEnvelope) -> Result<EncodedAgentMessage>;
    fn decode(&self, message: &EncodedAgentMessage) -> Result<AgentEnvelope>;
}

#[async_trait]
pub trait AgentEventIngress: Send + Sync {
    async fn accept(&self, envelope: AgentEnvelope) -> Result<IngressAck>;
}

#[async_trait]
pub trait AgentTransport: Send + Sync {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool;
    async fn publish(
        &self,
        endpoint: &DeliveryEndpoint,
        message: &EncodedAgentMessage,
    ) -> Result<()>;
}

#[async_trait]
pub trait McpToolProvider: Send + Sync {
    async fn list_tools(&self) -> Result<Vec<McpToolDescriptor>>;
    async fn invoke(&self, tool_name: &str, input: Value) -> Result<Value>;
}

#[async_trait]
pub trait McpToolExporter: Send + Sync {
    async fn exported_tools(&self) -> Result<Vec<McpToolDescriptor>>;
    async fn invoke_exported(&self, tool_name: &str, input: Value) -> Result<Value>;
}
```

Builder surface (illustrative):

```rust
StasisRuntimeBuilder::new(backend)
    .with_agent_codec(Arc::new(JsonAgentMessageCodec::v1()))?
    .with_agent_event_ingress(/* default store-backed ingress */)?
    .with_mcp_tool_provider(gateway_provider)?
    .with_mcp_tool_exporter(gateway_exporter)?
```

### 5.4 DI / “circular” bridge (composition only)

```text
External agent  --MCP-->  Gateway (outside repo)
                              │ implements McpToolExporter invoke → Stasis tools/jobs
                              │ implements McpToolProvider ← remote MCP tools
                              ▼
                         Stasis ToolRegistry / job enqueue
```

Crate dependency rule:

- `stasis` → ports + canonical model only
- `my-gateway` → `stasis` ports/SDK
- app binary → wires gateway into builder both ways

Recursion guardrail: invocations that re-enter Stasis via exported MCP tools must carry a depth/budget header or registry allowlist so tool loops cannot infinite-bounce.

### 5.5 Waitable turns via existing durable work

Do not invent a second orchestrator. External participation is a job pattern:

1. Session/coordinator grants a turn → enqueue `workflow.stasis.agent_turn.waitable` (name TBD).
2. Handler publishes `TurnGranted` through comms (codec → transport).
3. Job waits by durable state (lease heartbeat / parked wait record), not by holding a thread forever.
4. Ingress accepts `TurnCompleted` / `Failed` / `Cancel*` → correlates → completes wait → next selection strategy runs.

This reuses leases, retries, DLQ, and lineage instead of in-process `run_session` busy loops for external participants.

## 6. Delivery Phases

### Phase 0 — Contract freeze and module layout

**Goals**

- Lock names, schema versioning, and module boundaries before behavior expands.

**Deliverables**

1. Domain types for `AgentEnvelope` + kind enum + `EncodedAgentMessage`.
2. Port traits under `src/ports/outbound/agent/` (or equivalent).
3. ADR-0007 accepted; this plan marked Ready for Implementation.
4. Architecture conformance allowlist updates for new modules.
5. Docs-book stub page + extension-points section draft.

**Acceptance**

1. Types compile; no runtime behavior change yet.
2. Conformance tests pass.
3. No vendor strings in core.

### Phase 1 — Comms expansion (ingress + richer events)

**Goals**

- Make the bus bidirectional for agent envelopes while preserving current job outbox behavior.

**Deliverables**

1. Extend outbox/event vocabulary for agent envelope kinds (or parallel agent-outbox store if cleaner — prefer one durable publish path).
2. `AgentEventIngress` with idempotent accept by `(correlation_id, envelope_id/kind)`.
3. Correlation to `job_id` / `thread_id` / `turn_id`.
4. Control-plane registration remains protocol-based; add only generic protocols if required (for example `WebSocket`), still vendor-neutral.
5. Publish path: canonical envelope → codec → `AgentTransport` / existing endpoint publishers.
6. Metrics counters for ingress accept/reject and publish success/fail.

**Acceptance**

1. Round-trip test: publish `TurnGranted`, ingress `TurnCompleted`, job unblocks.
2. Duplicate ingress is idempotent.
3. Existing job lifecycle outbox tests remain green.

### Phase 2 — Translation layer

**Goals**

- Stabilize the edge codec model.

**Deliverables**

1. `AgentMessageCodec` port.
2. Reference `JsonAgentMessageCodec` (schema v1) in-repo.
3. Codec registry keyed by content type / schema name.
4. Negative tests: unknown version, malformed payload, kind/payload mismatch.
5. Cookbook: “bring your own codec” implementing a toy non-JSON format (for example length-prefixed JSON lines) **without** naming a vendor.

**Acceptance**

1. Comms path never inspects vendor formats — only codec output.
2. Golden files for JSON v1 encode/decode.

### Phase 3 — MCP tool bridge

**Goals**

- Injectable MCP provider/exporter around `ToolRegistry`.

**Deliverables**

1. `McpToolProvider` + descriptor types (`name`, `description`, `input_schema`).
2. Registry merge: provider tools appear beside local `StasisTool`s for tool loops.
3. `McpToolExporter` backed by selected local tools (explicit export list; default export nothing).
4. Recursion/depth guard and export allowlist.
5. In-memory fake provider/exporter tests.
6. Builder methods + docs-book extension points.

**Acceptance**

1. Tool loop can invoke a provider-backed tool.
2. Exporter invokes a local `#[stasis_tool]` tool by MCP descriptor name.
3. Recursive export→provider bounce fails with typed budget error.
4. Still zero vendor gateways in-repo.

### Phase 4 — Waitable external participant pattern

**Goals**

- Let orchestration treat external gateways as participants without in-process LLM assumption.

**Deliverables**

1. Waitable turn job handler + payload contracts.
2. `AgentParticipant` gains a participant kind: `LocalToolLoop` | `ExternalComms` (names TBD) — still no vendor fields.
3. Selection/termination strategies work across mixed participants.
4. Timeout → retry/DLQ policy for missing ingress.
5. Example: fake external gateway process/thread using JSON codec + HTTP webhook ingress.

**Acceptance**

1. Mixed session: local tool-loop participant + fake external participant completes deterministically.
2. Timeout path dead-letters or fails with diagnostics + lineage IDs.
3. Memory/lineage fields continue to populate on outbox metadata where applicable.

### Phase 5 — Hardening and operator readiness

**Goals**

- Production seams without building a full command-center redesign.

**Deliverables**

1. Authn hooks on ingress (signature/shared-secret port; implementation optional).
2. Endpoint delivery status for agent envelope publishes.
3. Runbook: how to build a gateway against the three contracts.
4. Feature flags if MCP/reference network adapters need optional deps.
5. Compatibility matrix: which envelope kinds are required vs optional per phase.

**Acceptance**

1. Unauthorized ingress rejected with no state mutation.
2. Docs-book + cookbook complete enough for an external team to implement a gateway.
3. Parity tests across in-memory and Surreal backends for ingress/wait records.

## 7. Mapping to existing code

| Existing surface | Reuse strategy |
| --- | --- |
| `EventPublisher` / outbox | Extend or wrap for agent envelopes; keep at-least-once publish policy |
| `EndpointTransportPublisher` | Basis for `AgentTransport`; avoid parallel unmanaged publishers |
| `DeliveryEndpoint` / control plane | Register gateway endpoints by protocol/target metadata |
| `ToolRegistry` / `StasisTool` | Merge MCP provider tools; export selected tools outward |
| `AgentSessionCoordinator` | Keep strategies; add external waitable participant path |
| `StasisRuntimeBuilder` | Single composition root for codecs/providers/ingress |
| Threads + correlation IDs | Session/turn lineage spine |

## 8. Testing strategy

1. **Contract tests** for codec golden files and envelope schema_version.
2. **Ingress idempotency** and correlation tests.
3. **Tool registry merge** tests (local + provider name collision policy — decide in Phase 3: reject vs alias).
4. **Recursion budget** tests for MCP export/provider loops.
5. **Runtime backend parity** for waitable turns + ingress completion.
6. **Architecture conformance** forbidding gateway crates / vendor modules under `src/`.

Name collision policy recommendation (Phase 3 decision gate):

- Default: **reject duplicate names** at registration time.
- Optional: explicit alias map on builder for advanced composition.

## 9. Documentation deliverables

1. ADR-0007 (decision).
2. This plan (phased delivery).
3. docs-book pages:
   - Agent platform contracts overview
   - Extension points updates (codec, ingress, MCP provider/exporter)
   - Cookbook: fake gateway + JSON codec
4. `docs/README.md` index entry.

## 10. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Envelope vocabulary churn | Phase 0 freeze + schema_version; additive kinds only after v1 |
| Comms/MCP responsibilities blur | Explicit plane table; PR checklist in conformance docs |
| Waitable turns block workers | Parked wait records + lease heartbeats; never infinite in-handler sleep |
| MCP recursion | Depth budget + export allowlist |
| Scope creep into vendor adapters | Hard non-goal; examples stay fake/reference only |

## 11. Suggested implementation order (engineering slices)

Work proceeds by dependency, not by calendar:

1. Phase 0 types/ports
2. Phase 2 codec (can start in parallel with Phase 1 once envelope types exist)
3. Phase 1 ingress/publish wiring using JSON codec
4. Phase 3 MCP bridge (independent of waitable turns once registry merge exists)
5. Phase 4 waitable external participants (needs Phase 1)
6. Phase 5 hardening/docs polish

Critical path: **Phase 0 → Phase 1 → Phase 4**.  
Parallel track: **Phase 2** early, **Phase 3** after registry seams are clear.

## 12. Exit criteria for “contracts v1 complete”

1. External team can implement a gateway using only public ports/docs.
2. Mixed local + external fake participant session runs durably with retry/DLQ.
3. MCP provider tools run inside tool loops; exported Stasis tools invoke correctly.
4. No vendor-oriented modules in `src/`.
5. ADR-0007 moved from Proposed → Accepted with verification against implemented ports.

## 13. Next immediate actions

1. Review/accept ADR-0007 and freeze envelope kind list for v1.
2. Land Phase 0 module scaffolding behind stable port paths.
3. Implement JSON codec + ingress idempotency store as the first vertical slice.
4. Publish cookbook “Fake Gateway” before any optional network MCP feature work.
