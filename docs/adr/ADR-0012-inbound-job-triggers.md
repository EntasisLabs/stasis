# ADR-0012 Inbound Job Triggers

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-09-22
- Verified Against:
  - src/domain/runtime/inbound_trigger.rs
  - src/application/runtime/inbound_trigger.rs
  - src/ports/outbound/runtime/inbound_trigger_store.rs
  - src/sdk/runtime_sdk.rs
  - docs/design/inbound-job-triggers.md
  - tests/inbound_job_triggers.rs

## Status

Accepted

## Date

2026-09-22

## Context

Stasis can push job lifecycle events out over HTTP webhook, TCP, Kafka, and RabbitMQ. That path is the outbox plus `EndpointTransportPublisher`.

Inbound is a different problem. `AgentEventIngress` accepts an agent envelope so a parked external turn can complete. It does not start a job. There is no shared way for a webhook, a TCP line, a Kafka record, or a queue delivery to enqueue work. Each host would otherwise invent its own JSON shape, idempotency rule, and `NewJob` construction.

The listeners themselves (HTTP server, Kafka consumer, TCP accept loop, Rabbit consumer) stay outside the kernel. ADR-0007 already keeps vendor gateways out of core. The missing piece is the contract those listeners call.

## Decision

Stasis accepts one canonical inbound job trigger. The protocol is an argument from the listener. The body is the same document for every transport.

### 1) One document, four protocols

`InboundProtocol` is `HttpWebhook`, `Tcp`, `Kafka`, or `RabbitMq` (the queue transport already used outbound).

`decode_inbound_json` reads schema version 1:

- `idempotency_key` (required)
- `job_type` (required)
- `payload` (required)
- `queue`, `correlation_id`, `priority`, `max_attempts` (optional)
- `payload_encoding`: `typed` (default) wraps `payload` in a typed job envelope; `raw` stores the JSON value as `payload_ref`

`RuntimeSdk::accept_inbound_json(protocol, body)` decodes and enqueues. `accept_inbound_job` does the same for a `StasisJob`, using that type's declaration for queue, priority, and retry.

The job's `causation_id` is `inbound:<protocol>`. Correlation defaults to the idempotency key.

### 2) Idempotent accept

The first accept inserts a receipt keyed by `idempotency_key`, then inserts the job. A second accept with the same key returns `Duplicate` and the original job id, including when the second delivery arrives on a different protocol. If job insert fails, the receipt is removed so a retry can accept again.

### 3) Listeners stay at the edge

The kernel does not bind a port or join a consumer group. A host does:

```text
HTTP route / TCP read / Kafka record / queue delivery
        → RuntimeSdk::accept_inbound_json(protocol, bytes)
        → durable job
```

Agent ingress remains the path for turn replies. Outbound publishers remain the path for lifecycle fanout.

## Non-Goals

1. No in-repo HTTP server, Kafka consumer, TCP listener, or Rabbit consumer.
2. No replacement of `AgentEventIngress` or waitable turns.
3. No signature verification in this slice. Hosts authenticate before calling accept.
4. No per-protocol idempotency namespace. Callers prefix keys when sources can collide.

## Consequences

### Positive

1. Webhook, TCP, Kafka, and queue deliveries share one enqueue path and one idempotency rule.
2. Typed jobs started from outside use the same declaration as in-process `enqueue_job`.
3. Core stays free of broker client loops.

### Tradeoffs

1. Hosts still write the socket or consumer loop.
2. Receipt and job insert are two steps. A crash between them can leave a receipt without a job until an operator deletes the receipt.
3. Surreal stores receipts in schemaless `inbound_trigger`, created on first write.

## Guardrails

1. Protocol is chosen by the listener, not by a field in the body.
2. Empty idempotency key or job type is rejected and does not enqueue.
3. Duplicate accepts must not insert a second job.
4. Typed inbound payloads use the existing typed envelope so `JobConsumer` can run them unchanged.
