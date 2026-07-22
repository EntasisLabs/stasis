# ADR-0008 `stasisd` Declarative Agent Orchestration Engine

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner, SRE
- Stability: Evolving
- Last Verified: 2026-07-22
- Verified Against:
  - docs/design/stasisd-declarative-engine-plan.md
  - docs/adr/ADR-0007-agent-platform-runtime-contracts.md
  - src/domain/runtime/recurring.rs
  - src/ports/outbound/runtime/recurring_store.rs
  - src/sdk/runtime_sdk.rs
  - docs-book/src/recurring-jobs.md

## Status

Proposed

## Date

2026-07-22

## Context

Stasis already provides the durable substrate for agent work: jobs, leases, retries, dead letters, recurring definitions, outbox delivery, and (per ADR-0007) vendor-neutral agent platform contracts.

What operators still lack is a small, obvious deployable that turns **declarative files** into live orchestration — the “nginx of AI orchestration”:

- drop YAML/TOML into a config directory
- engine reconciles desired state into runtime objects
- recurring jobs materialize on schedule
- remove a file and the corresponding schedules stop (with explicit drain policy for in-flight work)

Today, recurring definitions are registered programmatically via SDK/runtime APIs. There is no first-class file watch + reconcile loop packaged as a deployable engine.

## Decision

Introduce **`stasisd`** as a thin deployable engine (binary/crate) that:

1. Reads YAML and/or TOML from a configured config path (file or directory).
2. Treats those files as **desired state** for agents, schedules, workflows, endpoints, and tool wiring.
3. **Reconciles** desired state into Stasis runtime objects — primarily `RecurringDefinition`s and related registrations — using existing runtime APIs.
4. Watches for create/update/delete and applies idempotent diffs.
5. On file removal, removes or disables managed definitions; in-flight jobs follow an explicit drain policy.
6. Remains a **composition/packaging layer** on top of the Stasis runtime kernel — not a second orchestrator and not a place for vendor agent adapters.

`stasis` library/runtime stays the durable engine. `stasisd` is the operator UX and process host (watch → reconcile → tick workers/scheduler/outbox).

## Non-Goals

1. No vendor-named agent integrations inside `stasisd` (same guardrail as ADR-0007).
2. No replacement of Surreal-backed durability with “config file is source of truth at runtime” — files are desired state; runtime store remains execution truth.
3. No full Kubernetes operator in v1 (directory watch + periodic reconcile is enough).
4. No requirement that every Stasis capability be expressible in v1 config schema.

## Consequences

### Positive

1. Clear product story: `stasisd -c /etc/stasis/agents.d/`.
2. GitOps-friendly agent schedules without custom app code.
3. Reuses existing recurring materialization, leases, and job handlers.
4. Separates kernel evolution from deployable UX.

### Tradeoffs

1. Need a versioned config schema and strict validation (fail closed on bad files).
2. Ownership model required so `stasisd`-managed defs don’t fight SDK-registered defs.
3. Delete/drain semantics must be explicit to avoid surprising cancellations.

## Guardrails

1. Every managed object carries provenance metadata (for example `managed_by=stasisd`, `config_source=<path>`, `config_hash`).
2. Reconcile is idempotent: re-reading unchanged files is a no-op.
3. Invalid files never partially apply; per-file atomic apply or reject.
4. `stasisd` depends on public Stasis SDK/runtime ports — no bypass of hexagonal boundaries.
5. Config schema references job types and ADR-0007 participant/comms kinds — never product vendors.

## Plan

See [stasisd-declarative-engine-plan.md](../design/stasisd-declarative-engine-plan.md).
