# Stasis Distributed Command Center Phase Plan

## Document Metadata

- Document Type: Implementation Plan
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-05-17
- Verified Against:
  - src/application/runtime/surreal_runtime.rs
  - src/application/runtime/in_memory_runtime.rs
  - src/application/orchestration/stasis_workflow_job_builder.rs
  - src/application/use_cases/investigate_runtime_lineage.rs
  - src/ports/outbound/runtime/event_publisher.rs
  - docs-book/src/introduction.md

## Context

Stasis already has durable execution primitives:
- leased job processing
- retries and dead-letter handling
- recurring job materialization
- outbox-based event publication
- lineage investigation and diagnostics

What is not yet implemented is a first-class distributed command center with dynamic endpoint routing, cluster coordination, and operator workflows.

## Target Architecture

### Planes

1. Control Plane
- operator-facing commands for scheduling, replay, pausing, endpoint registration, and workflow deployment.

2. Execution Plane
- worker and scheduler nodes that run jobs with lease safety and publish runtime events.

3. Memory Plane
- STTP-based capture, summary, and rollup for rapid context rehydration.

### Cluster Topology

1. Global coordinator cluster
2. Regional or edge orchestration clusters
3. Worker pools per queue and capability

Edge clusters should continue local processing during upstream degradation and sync state/events when available.

## Delivery Phases

### Phase 1: Control Plane Foundation (current)

Goals:
- establish command-center control contracts without breaking runtime behavior.
- create endpoint registration primitives for future transport fanout.

Deliverables:
1. Domain model for delivery endpoints.
2. Outbound store port for endpoint persistence.
3. Inbound control-plane command interface for endpoint operations.
4. Application use case for endpoint registration and enable/disable control.
5. In-memory adapter for local development and tests.

Acceptance criteria:
1. endpoint model supports protocol and target metadata.
2. duplicate endpoint IDs are rejected.
3. endpoint enable/disable operation is explicitly modeled.
4. no runtime execution semantics are changed.

### Phase 2: Transport Adapter Pack

Goals:
- emit outbox events and final payloads to real destinations.

Deliverables:
1. HTTP webhook publisher adapter.
2. TCP publisher adapter.
3. Kafka publisher adapter.
4. RabbitMQ publisher adapter.
5. routing policy that maps event types and workflow outputs to endpoint sets.

### Phase 3: Distributed Cluster Control

Goals:
- add true multi-node orchestration control.

Deliverables:
1. cluster node identity + heartbeat registration.
2. queue ownership and capability tags.
3. scheduler and worker health/state views.
4. cross-cluster command forwarding model.

### Phase 4: Command Center Dashboard

Goals:
- operator-grade Hangfire-style experience.

Deliverables:
1. jobs board (queued, running, retrying, dead-lettered).
2. recurring schedules manager.
3. endpoint registry and delivery health.
4. workflow graph view and run inspector.
5. lineage explorer with thread and causation tracing.

## Current Implementation Status Snapshot (2026-05-17)

### Phase 1: Control Plane Foundation

Status: Complete

Delivered:
1. Delivery endpoint domain model and persistence contracts.
2. Control-plane inbound commands for endpoint registration and enable/disable operations.
3. Application-layer endpoint registration and policy handling.
4. In-memory adapter support and targeted tests.

### Phase 2: Transport Adapter Pack

Status: Complete

Delivered:
1. HTTP webhook adapter.
2. TCP socket adapter.
3. Kafka adapter (feature-gated via `transport-kafka`).
4. RabbitMQ adapter (feature-gated via `transport-rabbitmq`).
5. Kafka WASM placeholder path (feature-gated via `transport-kafka-wasm`).
6. Endpoint routing and delivery diagnostics read-models.

### Phase 3: Distributed Cluster Control

Status: In Progress (Phase 3A complete, Phase 3B delivered, Phase 3C started)

Delivered in Phase 3A:
1. Cluster node registration and heartbeat lifecycle.
2. Queue ownership and capability visibility in health views.
3. Cluster health listings and stale-node pruning.
4. Queue ownership conflict policy (`SingleOwner` and `MultiOwner`).
5. Heartbeat sweep command with control-plane event emission.

Outstanding for later Phase 3 slices:
1. Coordinator failover workflows and ownership rebalancing strategy.
2. Forwarding durability and replay policy beyond in-memory outcomes.

Phase 3B initial slice delivered:
1. Forward command contract in control-plane DTO and inbound command API.
2. Outbound `ClusterCommandForwarder` adapter boundary.
3. In-memory and noop forwarder adapters.
4. ControlPlaneSdk forwarding method with configuration guardrails.
5. HTTP-backed forwarder adapter with retry/backoff policy.
6. Forwarding observability counters and duration metrics.
7. Integration-style retry coverage via local HTTP server tests.
8. Coordinator handoff command contract and forwarding use case.
9. Forwarded command outcome store and control-plane query read-model.

Phase 3C kickoff delivered:
1. Coordinator failover intent command contract and SDK operation.
2. Queue ownership rebalance command contract and SDK operation.
3. Validation coverage for both command families through control-plane tests.
4. Execution-side runtime handlers for coordinator failover and queue ownership rebalance.
5. Runtime builder defaults now register cluster-control workflow handlers.
6. Durable SurrealDB adapter for forwarded-command outcomes.
7. HTTP forwarder idempotency guardrail using correlation-ID dedupe.

### Phase 4: Command Center Dashboard

Status: Not Started

## Next Best Value Target

1. Replay tooling for failed forwarded commands driven by durable outcomes.
2. Conflict-resolution policy tracing for ownership handoff/failover/rebalance.

## Phase 1 Notes

Phase 1 intentionally establishes interfaces and foundational behavior only. It avoids premature binding to any single transport stack and keeps compatibility with the existing DDD and hexagonal module boundaries.