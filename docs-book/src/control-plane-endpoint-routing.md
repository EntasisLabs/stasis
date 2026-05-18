# Control Plane and Endpoint Routing

## Document Metadata

- Document Type: Architecture and Delivery Reference
- Audience: Engineer, Platform, SRE, Architect
- Stability: Evolving
- Last Verified: 2026-05-17
- Verified Against:
  - src/ports/inbound/control_plane_commands.rs
  - src/application/use_cases/manage_delivery_endpoints.rs
  - src/ports/outbound/runtime/delivery_endpoint_store.rs
  - src/sdk/control_plane_sdk.rs
  - src/infrastructure/runtime/in_memory_delivery_endpoint_store.rs
  - src/infrastructure/runtime/surreal_delivery_endpoint_store.rs
  - src/infrastructure/runtime/http_webhook_event_publisher.rs
   - src/infrastructure/runtime/http_cluster_command_forwarder.rs
   - src/infrastructure/runtime/surreal_cluster_forward_outcome_store.rs

## Purpose

Document the command-center foundation for Stasis runtime operations and outbound delivery routing.

This reference covers:

1. Control-plane command contracts.
2. Delivery endpoint registry model.
3. Endpoint persistence ports and built-in adapters.
4. Event-publisher routing direction and phased rollout.

## Scope

In scope:

- endpoint registration and lifecycle toggling.
- adapter boundaries for endpoint persistence and event delivery.
- distributed-ready delivery topology direction.

Out of scope:

- full dashboard UI implementation.
- complete transport matrix (Kafka, RabbitMQ, TCP runtime adapters are planned).
- multi-cluster membership and scheduler federation internals.

## Forwarded Command Durability and Guardrails

Forwarded control-plane commands now support:

1. Durable outcome storage through `SurrealClusterForwardOutcomeStore`.
2. In-memory development outcome storage through `InMemoryClusterForwardOutcomeStore`.
3. Correlation-ID dedupe in `HttpClusterCommandForwarder` to prevent accidental duplicate dispatch on repeated operator retries.

Runtime metric additions:

1. `cluster_forward_idempotent_hits_total` for dedupe cache hits.

## Control Plane Contract

The primary inbound contract is `ControlPlaneCommands`.

Current operations:

1. register delivery endpoint.
2. enable/disable delivery endpoint.
3. list registered endpoints.

Built-in implementation:

- `ControlPlaneSdk` orchestrates endpoint management use cases over a `DeliveryEndpointStore`.

## Delivery Endpoint Model

`DeliveryEndpoint` defines protocol-aware delivery targets.

Key fields:

1. `endpoint_id` and `name`.
2. `protocol` (`HttpWebhook`, `Tcp`, `Kafka`, `RabbitMq`).
3. `target` plus optional metadata.
4. `enabled` lifecycle flag.
5. immutable `created_at` and mutable `updated_at` timestamps.

## Persistence Port

`DeliveryEndpointStore` abstracts endpoint persistence.

Operations:

1. insert endpoint.
2. get endpoint by ID.
3. list endpoints.
4. set enabled state.

Built-in adapters:

1. `InMemoryDeliveryEndpointStore` for local and test flows.
2. `SurrealDeliveryEndpointStore` for durable runtime deployments.

## Event Delivery Adapter Direction

Outbox publication remains the reliability boundary for external delivery.

Current built-in adapter:

1. `HttpWebhookEventPublisher` posts normalized runtime event payloads to configured webhook URLs.

Planned adapter set:

1. TCP publisher.
2. Kafka publisher.
3. RabbitMQ publisher.

## Delivery Flow (Current and Next)

Current flow:

1. Runtime commits durable outbox events.
2. Event publisher sends to configured external subscriber implementation.

Target flow:

1. Control plane manages protocol endpoints in the endpoint registry.
2. Routing policy selects endpoint sets by event/job criteria.
3. Publisher adapters fan out events with retry-aware outbox coordination.

## Rollout Plan

### Phase 1 (Implemented)

1. Endpoint model and store port.
2. Inbound control-plane contract.
3. Endpoint management use cases.
4. In-memory and Surreal endpoint store adapters.
5. Control-plane SDK wiring.

### Phase 2 (Next)

1. Routing policy and dispatcher integration.
2. Protocol adapter expansion (TCP, Kafka, RabbitMQ).
3. Delivery diagnostics enrichment per endpoint attempt.

### Phase 3

1. Distributed cluster coordination for control-plane operations.
2. Regional/edge execution with upstream summary sync.
3. Command-center API and dashboard surfaces.

## Operational Guidance

1. Keep endpoint credentials in dedicated secret stores, not endpoint metadata.
2. Enforce role-based access on control-plane mutation operations.
3. Use deterministic correlation and causation IDs for all external fanout events.
4. Start with webhook integration to validate end-to-end delivery and observability before enabling additional transport adapters.