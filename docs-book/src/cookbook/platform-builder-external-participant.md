# Platform Builder: Mixed Local + External Participants

## Document Metadata

- Document Type: Cookbook
- Audience: Engineer, Platform Owner
- Stability: Evolving
- Last Verified: 2026-07-22

## Goal

Run a durable mixed workload from YAML/TOML only: one local job plus one external waitable turn, with zero vendor adapters in-repo.

## Config

```toml
api_version = "stasisd/v1"

[[endpoint]]
id = "fake-external"
name = "Fake external participant"
protocol = "http_webhook"
target = "http://127.0.0.1:39001/agent"

[[mcp_gateway]]
id = "local-mcp"
transport = "command"
command = "fake-mcp-gateway"
args = ["--stdio"]
export_allowlist = ["summarize"]

[[schedule]]
id = "local-step"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0/30 * * * * * *"
payload = { user_prompt = "do local work" }

[[schedule]]
id = "external-step"
queue = "agents"
job_type = "workflow.stasis.agent_turn.waitable"
cron = "0/30 * * * * * *"
payload = {
  agent_id = "external-reviewer",
  session_id = "sess-1",
  turn_id = "turn-1",
  user_prompt = "review the plan",
  endpoint_ref = "fake-external",
  mcp_gateway_ref = "local-mcp",
  timeout_seconds = 30,
  poll_interval_seconds = 1
}
```

Agent session payloads may also declare:

```toml
[[schedule.payload.participants]]
agent_id = "external-reviewer"
kind = "external"
endpoint_ref = "fake-external"
```

`stasisd` validates those refs. Durable external turns execute via `workflow.stasis.agent_turn.waitable` (leases, `Deferred` park, retry/DLQ).

## Run

```bash
mkdir -p /tmp/stasis-join.d
# write the TOML above into /tmp/stasis-join.d/mixed.toml
cargo run -p stasisd -- --config /tmp/stasis-join.d --once --strict
```

## Gateway contract (outside this repo)

1. Receive encoded `TurnGranted` on the endpoint transport.
2. Do external work.
3. POST/accept `TurnCompleted` / `Failed` / `Cancelled` through `AgentEventIngress` (JSON codec v1).

Core Stasis never imports a vendor SDK. Compose gateways at your process root with:

```rust
StasisRuntimeBuilder::new(backend)
    .with_delivery_endpoint_store(endpoints)
    .with_turn_wait_store(waits)
    .with_agent_message_codec(codec)
    .with_agent_event_ingress(ingress)
    .with_agent_transport(transport)
    .with_mcp_tool_provider(provider)
```

## Related

- [`stasisd`](../stasisd.md)
- [Agent Platform Runtime Contracts](../agent-platform-contracts.md)
- Epic: [agent-platform-and-stasisd-epic.md](../../../docs/design/agent-platform-and-stasisd-epic.md)
