# ADR-0010 Runtime-Neutral Federated Job Contract

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-08-28
- Verified Against:
  - src/domain/runtime/provenance.rs
  - src/domain/runtime/placement.rs
  - src/domain/runtime/remote_job_envelope.rs
  - src/domain/runtime/ownership_handoff.rs
  - src/domain/runtime/federation.rs
  - src/domain/runtime/blob_descriptor.rs
  - src/ports/outbound/runtime/blob_transfer.rs
  - src/ports/outbound/runtime/federated_delivery.rs
  - src/ports/outbound/runtime/ownership_handoff_store.rs
  - tests/federated_job_contract.rs

## Status

Accepted

## Date

2026-08-28

## Context

Stasis jobs historically required STTP node IDs for input/output lineage and assumed a single shared durable store. Multi-cluster and multi-runtime deployments need:

1. Runtime-neutral lineage that is not STTP-mandatory.
2. A signed remote job envelope that can travel between independent Stasis runtimes.
3. Placement constraints enforced atomically at lease time.
4. Fenced ownership handoff so two nodes cannot execute the same resource generation.
5. Content-addressed artifacts moved through pluggable blob ports (not inlined in job rows).
6. Durable signals and terminal results that cross runtimes without a shared database.

## Decision

Stasis adopts a **runtime-neutral federated job contract**:

### 1) Optional provenance references

`Job` / `RuntimeEvent` carry `input_provenance` / `output_provenance` as optional `ProvenanceRef` values (`sttp`, `cas`, `uri`, `opaque`). STTP remains available through `SttpProvenanceAdapter`, including Surreal compat columns.

### 2) Signed remote job envelope

`RemoteJobEnvelope` (schema v1) carries job type, content-addressed payload descriptor, idempotency key, correlation/causation, deadline, origin authority, terminal-delivery endpoint, placement requirements, and an HMAC-SHA256 signature over canonical bytes.

### 3) Placement-aware leasing

`PlacementConstraints` (capabilities, platform, architecture, region, optional target node) are stored on each job. `JobStore::lease_due` takes `WorkerCapabilities` and matches atomically during lease acquisition.

### 4) Fenced ownership handoff

`OwnershipHandoffStore` implements reserve → transfer generation → commit|abort. An active reservation blocks concurrent handoffs for the same generation; commit advances the fencing token so stale executors fail fence validation.

### 5) Content-addressed blobs

`BlobDescriptor` + `BlobTransferPort` keep large payloads/outputs out of job records. In-memory reference adapter ships for tests.

### 6) Cross-runtime delivery without shared DB

`FederatedDeliveryPort` / `FederatedIngressPort` exchange signed remote jobs, signals, and terminal results between independent runtime inboxes (in-memory bus for tests; network adapters later).

## Non-Goals

1. No requirement that every deployment enable federation.
2. No replacement of local durable waits / outbox for single-runtime installs.
3. No mandatory ed25519 PKI in v1 (HMAC reference signer; pluggable later).
4. No shared global job table across runtimes.

## Consequences

### Positive

1. STTP is optional; other provenance schemes work.
2. Federated execution can span isolated Stasis runtimes safely.
3. Placement and fencing harden multi-node execution.

### Tradeoffs

1. Surreal rows keep STTP compat columns plus JSON provenance/placement fields.
2. Call sites constructing `NewJob` must set optional provenance and placement.
3. Workers must declare capabilities for constrained queues.

## Guardrails

1. Domain types stay persistence-agnostic.
2. Blob bytes never become required inline job fields.
3. Handoff commit must re-check generation before transfer.
4. Envelope signature verification is mandatory before accepting remote work.
