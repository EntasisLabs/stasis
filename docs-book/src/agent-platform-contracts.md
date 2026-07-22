# Agent Platform Runtime Contracts

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-07-22
- Verified Against:
  - docs/adr/ADR-0007-agent-platform-runtime-contracts.md
  - docs/design/agent-platform-runtime-contracts-plan.md
  - docs-book/src/extension-points.md
  - src/application/orchestration/tool_registry.rs
  - src/ports/outbound/runtime/event_publisher.rs

## Purpose

Describe Stasis as a durable runtime kernel for people building agent platforms — analogous to MassTransit/Hangfire for agentic work — and point to the three vendor-neutral contracts that enable that role.

## Positioning

Stasis provides durable jobs, leases, retries, threads, outbox delivery, and tool/job orchestration.

Stasis does **not** ship product adapters for IDE/CLI agent vendors. Platform builders implement gateways against public ports and inject them at the composition root.

## Three Contracts

1. **Comms** — bidirectional durable messaging (outbound publish + ingress) over transport ports.
2. **Translation** — canonical Stasis agent envelopes encoded/decoded by pluggable codecs (ACP, JSON, XML, etc.).
3. **MCP tool bridge** — injectable `McpToolProvider` / `McpToolExporter` around `StasisTool` / `ToolRegistry`.

```text
External agent platforms / MCP gateways (outside repo)
        │ implement ports
        ▼
Stasis runtime kernel
  jobs · leases · retries · threads · outbox · tool loops
  Comms · Translation · MCP bridge contracts
```

## DI Shape

```rust
// Illustrative target builder surface from the delivery plan
StasisRuntimeBuilder::new(backend)
    .with_agent_codec(codec)?
    .with_agent_event_ingress(ingress)?
    .with_mcp_tool_provider(provider)?
    .with_mcp_tool_exporter(exporter)?;
```

The gateway crate depends on Stasis ports/SDK. Core Stasis never depends on a concrete gateway.

## Status

Contracts are proposed. Implementation is phased in the delivery plan.

## Related deployable

[`stasisd`](./stasisd.md) (ADR-0008) is the declarative YAML/TOML engine that reconciles desired state into durable schedules on top of this runtime.

## References

- ADR: [ADR-0007 Agent Platform Runtime Contracts](../../docs/adr/ADR-0007-agent-platform-runtime-contracts.md)
- Delivery plan: [agent-platform-runtime-contracts-plan.md](../../docs/design/agent-platform-runtime-contracts-plan.md)
- Related: [Extension Points](./extension-points.md), [Control Plane and Endpoint Routing](./control-plane-endpoint-routing.md), [Agent Coordination](./agent-coordination.md), [`stasisd`](./stasisd.md)
