# Runtime Phase Plan And Grapheme Track

## Purpose

Capture current implementation status, preserve the original P0-P3 delivery plan, and add a first-class Grapheme integration track without losing project scope.

## Current Status Snapshot

Completed:
- P0 runtime safety and correctness baseline.
- Surreal lease hardening and contention/recovery coverage.
- Outbox persistence and publish retry flow.
- Dead-letter replay API.
- Runtime factory composition.
- Backend parity integration tests.
- P1 recurring model migrated to cron_expr + timezone.
- P1 continuation demonstrated end-to-end in integration tests.

Open:
- P2 forensics and replay diagnostics.
- P3 operational readiness (clock/id generation and runtime metrics).

In progress:
- G2 observability and diagnostics.

## Core Phase Roadmap (Preserved)

### P0: Runtime Safety And Correctness

Scope:
- Atomic/CAS-safe leasing for workers.
- Lease-expiry recovery.
- Retry/dead-letter correctness under contention.

Acceptance:
- Multi-worker lease contention tests pass.
- Lease-expiry recovery tests pass.
- No job duplication from lease race in validated scenarios.

Status:
- Complete.

### P1: Spec Fidelity

Scope:
- Recurring definitions use cron_expr + timezone.
- Continuation flow validated from upstream output to downstream input.

Acceptance:
- Recurring materialization follows cron/timezone semantics.
- Continuation lineage (correlation/causation/trace + STTP references) is validated by tests.

Status:
- Complete for runtime behavior.

### P2: Forensics And Replay Operability

Scope:
- Persist job_attempt records with timing/outcome diagnostics.
- Preserve replay lineage metadata for auditability.

Acceptance:
- Attempt history is queryable per job.
- Replay retains causation and diagnostics across attempts.

Status:
- Not started.

### P3: Operational Readiness

Scope:
- Clock and IdGenerator ports.
- Runtime metrics (queue depth, retries, dead letters, lease recoveries, latency).
- Retention strategy for terminal records.

Acceptance:
- Runtime surfaces deterministic time/id abstractions.
- Metrics emitted for key reliability dimensions.
- Retention behavior documented and test-covered.

Status:
- Not started.

## Grapheme First-Class Integration Track (New)

Design principle:
- Integrate Grapheme as a constrained execution substrate in the orchestrator, not as unrestricted scripting.

### G0: Minimal First-Class Integration

Scope:
- Add a workflow execution port backed by grapheme-sdk.
- Add a job handler for Grapheme workflow execution.
- Keep payload_ref and STTP in/out contracts unchanged.

Acceptance:
- A grapheme workflow job executes through SDK path.
- sttp_output_node_id is persisted on success.

### G1: Guardrails And Policy

Scope:
- Allowlist modules/ops (source import allowlist + runtime policy checks).
- Execution timeout and resource bounds (timeout, max_steps, max_call_depth, source-size cap).
- Version pinning for workflow artifacts via exact import/module allowlist.

Acceptance:
- Policy violations fail safely and are observable.
- Wasm module support is gated by explicit policy.

Status:
- Complete for initial guardrail baseline.

### G2: Observability And Diagnostics

Scope:
- Attempt-level Grapheme diagnostics in job_attempt records.
- Outbox events include execution identifiers for traceability.

Implemented baseline:
- Runtime job_attempt persistence port and adapters (in-memory and Surreal).
- Attempt records emitted on success/retry/fatal paths with worker/attempt metadata.
- Grapheme execution_id propagated into runtime outbox lineage.

Acceptance:
- Failures are diagnosable without payload sprawl.
- Replay behavior can be audited end-to-end.

### G3: Expansion

Scope:
- Introduce more orchestrator capabilities via Grapheme workflows after guardrails stabilize.

Acceptance:
- Reliability SLOs hold under production-like workloads.
- Additional workflow classes can be introduced without changing core runtime semantics.

## Sequencing

Primary sequence:
1. Finish P2.
2. Start G0 and G1 in parallel with early P3 abstractions.
3. Complete P3 and G2.
4. Expand via G3.

Rationale:
- P2 provides forensic safety before broadening execution surface.
- G1 policy controls are mandatory before using Grapheme as a general capability layer.
