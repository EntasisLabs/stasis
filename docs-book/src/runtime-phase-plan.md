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
- P2 forensics and replay diagnostics.
- P3 operational readiness and metrics.
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
- Not started.

Scope:
- Persist job_attempt records.
- Keep replay lineage auditable.

### P3: Operational Readiness

Status:
- Not started.

Scope:
- Clock and IdGenerator ports.
- Runtime metrics.
- Retention behavior.

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

### G3: Expansion
- Broaden workflow coverage only after reliability metrics are stable.

## Sequencing

1. Finish P2.
2. Run G0 and G1 in parallel with early P3 abstractions.
3. Complete P3 and G2.
4. Expand via G3.
