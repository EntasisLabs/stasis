# Inbound Job Triggers

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-09-22
- Verified Against:
  - docs/adr/ADR-0012-inbound-job-triggers.md
  - src/domain/runtime/inbound_trigger.rs
  - src/application/runtime/inbound_trigger.rs
  - src/sdk/runtime_sdk.rs
  - tests/inbound_job_triggers.rs

## Purpose

Say what already exists for outbound delivery, what agent ingress is for, and how an external source starts a job.

## What exists

| Direction | Surface | Effect |
| --- | --- | --- |
| Outbound | Outbox + HTTP webhook, TCP, Kafka, and RabbitMQ publishers | Push a job lifecycle event to another system |
| Inbound, agent | `AgentEventIngress` | Store an agent envelope and, for waitable turns, complete or fail the parked job |
| Inbound, job start | `RuntimeSdk::accept_inbound_json` / `accept_inbound_job` | Enqueue a new durable job |

## Document

```json
{
  "schema_version": 1,
  "idempotency_key": "stripe:evt_123",
  "job_type": "billing.invoice_paid",
  "payload": { "invoice_id": "inv-9" },
  "queue": "billing",
  "correlation_id": "stripe:evt_123"
}
```

`payload_encoding` defaults to `typed`, so the stored `payload_ref` is `{"version":1,"payload":...}`. Set `"payload_encoding":"raw"` when the handler reads `payload_ref` as plain JSON.

## Host edges

The kernel does not open these listeners. Each one calls the same method.

```rust
runtime
    .accept_inbound_json(InboundProtocol::HttpWebhook, &request_body)
    .await?;

runtime
    .accept_inbound_json(InboundProtocol::Tcp, &line)
    .await?;

runtime
    .accept_inbound_json(InboundProtocol::Kafka, &record.value)
    .await?;

runtime
    .accept_inbound_json(InboundProtocol::RabbitMq, &delivery.body)
    .await?;
```

`InboundProtocol::RabbitMq` is the queue source. It matches the outbound RabbitMQ publisher.

A typed call skips the document and uses the job declaration:

```rust
runtime
    .accept_inbound_job(InboundProtocol::HttpWebhook, "stripe:evt_123", InvoicePaid { invoice_id })
    .await?;
```

Duplicate keys return `InboundDisposition::Duplicate` and the original job id.

## Later

1. Optional shared-secret or signature check in front of accept, still implemented by the host or a small port.
2. Reference listener binaries, still outside the kernel, if operators want a process that only binds webhook, TCP, Kafka, or Rabbit and calls accept.
3. Crash recovery for a receipt that was written and a job that was not.
