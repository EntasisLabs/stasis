# Federated Job Contract

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-08-28
- Verified Against:
  - docs/adr/ADR-0010-federated-job-contract.md
  - src/domain/runtime/provenance.rs
  - src/domain/runtime/remote_job_envelope.rs
  - src/domain/runtime/placement.rs
  - src/domain/runtime/ownership_handoff.rs
  - src/domain/runtime/federation.rs
  - tests/federated_job_contract.rs

## Purpose

Define the runtime-neutral contract that lets independent Stasis runtimes exchange work, signals, and terminal results without sharing a database, while keeping STTP as an optional adapter.

## Provenance

Jobs and runtime events use optional `ProvenanceRef`:

| Scheme | Meaning |
| --- | --- |
| `sttp` | Locus/STTP node id (via `SttpProvenanceAdapter`) |
| `cas` | Content-addressed digest |
| `uri` | External locator |
| `opaque` | Integrator-defined |

Mandatory STTP input/output fields are removed from the domain model; Surreal persistence retains STTP string columns for backward compatibility and fills them from the adapter when scheme is `sttp`.

## Remote job envelope

`RemoteJobEnvelope` (schema version 1) fields:

- `job_type`
- `payload` (`BlobDescriptor`)
- `idempotency_key`
- `correlation_id` / `causation_id`
- `deadline`
- `origin_authority`
- `terminal_delivery`
- `placement`
- `signature` (HMAC-SHA256 over canonical JSON excluding the signature field)

## Placement

`PlacementConstraints` on each job:

- required capabilities
- platform
- architecture
- region
- optional target node

`JobStore::lease_due(..., worker: &WorkerCapabilities)` matches constraints atomically with lease CAS.

Runtime workers use `RuntimeSdk::process_once_with_capabilities`. The compatibility `process_once` entry point declares no capabilities and therefore leases only unrestricted jobs.

## Ownership handoff

`OwnershipHandoffStore`:

1. `reserve` — fence-check current owner/generation; mark exclusive reservation
2. `commit` — re-validate generation, transfer lease (new fencing token)
3. `abort` — release reservation without transfer

Prevents two nodes from executing the same resource generation.

## Blob transfer

`BlobTransferPort::{put,get,exists,delete}` moves bytes. Job rows and envelopes store descriptors only.

## Cross-runtime signals and results

`FederatedDeliveryPort` / `FederatedIngressPort` deliver:

- remote job envelopes
- `FederatedSignalEnvelope`
- `FederatedTerminalResult`

Independent runtimes bind delivery to destination/origin runtime ids. No shared job/outbox database is required.

All three envelope types are signed over canonical bytes. The reference in-memory bus requires a verification key registered for the envelope `key_id`, rejects invalid signatures and expired remote jobs, and treats repeated envelope/signal/result identities idempotently.

## Successful output

`JobExecutionOutcome::Success` carries optional `output_provenance`. Generic typed jobs emit CAS provenance for their serialized result; STTP-aware handlers explicitly use `ProvenanceRef::sttp`. Jobs, attempts, and outbox events preserve the structured reference through both in-memory and Surreal backends.

## Test coverage

See `tests/federated_job_contract.rs` for provenance adapter, placement leasing, fenced handoff, blob transfer, and cross-runtime bus behavior.
