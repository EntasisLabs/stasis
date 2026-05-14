# Runtime Phase Plan And Grapheme Track

## Purpose

This chapter preserves the original P0-P3 runtime roadmap and adds a first-class Grapheme integration track.

## Status Snapshot

Completed:
- P0 runtime safety and correctness baseline.
- Surreal lease hardening with contention/recovery tests.
- Outbox publish retry flow and dead-letter replay.
- P1 recurring model migrated to cron_expr + timezone.
- P1 continuation validated end-to-end.

Open:
- G2 diagnostics and execution-level lineage.

In progress:
- G2 diagnostics and execution-level lineage.

## Core Phases (Preserved)

### P0: Runtime Safety And Correctness

Status:
- Complete.

### P1: Spec Fidelity

Status:
- Complete for runtime behavior.

### P2: Forensics And Replay Operability

Status:
- Complete for replay and diagnostics baseline.

Scope:
- Persist job_attempt records.
- Keep replay lineage auditable.

Implemented baseline:
- Job attempt persistence/query APIs for in-memory and Surreal.
- Replay report APIs return attempt history and lineage events.
- Parity test coverage validates replay lineage continuity.

### P3: Operational Readiness

Status:
- Complete for operational-readiness baseline.

Scope:
- Clock and IdGenerator ports.
- Runtime metrics.
- Retention behavior.

Implemented baseline:
- Clock and IdGenerator runtime ports with default adapters.
- In-memory and Surreal runtimes support dependency-injected clock/id.
- Runtime now() convenience APIs added for deterministic orchestration.
- Runtime metrics port with no-op and in-memory collector adapters.
- Runtime emits outcome/outbox counters and processing duration histograms.
- Retention policy/prune APIs implemented for terminal jobs, attempts, and outbox records.
- Parity tests validate retention pruning in in-memory and Surreal runtimes.

## Grapheme First-Class Track

Design principle:
- Use Grapheme as a constrained execution substrate, not unrestricted scripting.

### G0: SDK Integration
- Add workflow execution port backed by grapheme-sdk.
- Add Grapheme workflow job handler.

### G1: Policy Guardrails
- Status: Complete for initial baseline.
- Allowlist ops/modules.
- Timeout and resource limits.
- Version pinning via exact module import allowlist.

### G2: Diagnostics
- Attempt-level Grapheme diagnostics.
- Traceable execution identifiers in outbox lineage.
- Baseline implemented: job_attempt persistence + execution_id propagation in runtime events.
- Structured diagnostics payload includes guardrail_code, policy_reason, and duration_ms.
- Indexed diagnostics fields persisted on attempts for queryability.
- Query APIs support attempts by guardrail_code and attempts/lineage by execution_id.
- Lineage investigator use case provides a single composed query/report surface.

### G3: Expansion
- Broaden workflow coverage only after reliability metrics are stable.
- Kickoff implemented with first expanded workflow class: `workflow.grapheme.healthcheck`.
- Healthcheck path delegates to the existing guarded Grapheme handler for policy parity.
- Parity tests now include healthcheck execution for in-memory and Surreal runtimes.
- Second workflow class added: `workflow.grapheme.echo` with typed JSON payload validation.
- Invalid echo payloads are rejected as policy violations before Grapheme execution.
- Third workflow class added: `workflow.grapheme.textops` with enum-mode payloads (`summarize`, `extract_keywords`).
- Invalid textops payloads are rejected as policy violations before Grapheme execution.

## Sequencing

1. Finish P2.
2. Run G0 and G1 in parallel with early P3 abstractions.
3. Complete P3 and G2.
4. Expand via G3.
