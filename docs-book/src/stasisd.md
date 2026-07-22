# `stasisd` Declarative Engine

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, SRE, Platform Owner
- Stability: Evolving
- Last Verified: 2026-07-22
- Verified Against:
  - docs/adr/ADR-0008-stasisd-declarative-engine.md
  - docs/design/stasisd-declarative-engine-plan.md
  - docs-book/src/recurring-jobs.md
  - docs-book/src/agent-platform-contracts.md

## Purpose

Introduce `stasisd` — a small deployable engine that turns YAML/TOML desired state into durable Stasis schedules and jobs. Think “nginx for AI orchestration.”

## Idea

```bash
stasisd --config /etc/stasis/agents.d/
```

- Add a file → recurring agent schedules appear  
- Edit a file → definitions update  
- Remove a file → managed schedules stop; in-flight jobs follow a drain policy  

## Layering

| Layer | Role |
| --- | --- |
| Config files | Desired state |
| `stasisd` | Watch, validate, reconcile, run ticks |
| Stasis runtime | Durable execution (jobs, leases, outbox) |

`stasisd` is packaging and operator UX. It is not a second orchestrator and does not embed vendor agent adapters.

## Relationship to platform contracts

[Agent Platform Runtime Contracts](./agent-platform-contracts.md) (ADR-0007) define comms, translation, and MCP bridge ports.

`stasisd` (ADR-0008) is the declarative control surface that can later reference those ports — still without vendor code in-repo.

## Status

ADR-0008 is **Accepted**. Phase 1 loads YAML/TOML `stasisd/v1` schedules, validates (cron/timezone/job_type/duplicates/payload limits), and reconciles managed `stasisd:<id>` recurring definitions (`drain` / `orphan` / `cancel` policies).

Phase 2 adds the long-running host: filesystem watch + debounce, periodic reconcile, and a materialize → process → publish tick loop. Backend selection via `STASIS_STASISD_RUNTIME_BACKEND` (`in-memory` default, `surreal-mem` / `surreal-ws` / `surreal-kv`).

Phase 3 hardens operators: `--strict` quarantine semantics, id-prefix provenance (`stasisd:`), Ctrl-C/SIGTERM shutdown, optional `--healthz-addr` (`/healthz`, `/readyz`), and a systemd/runbook path.

Phased epic board (with ADR-0007 contracts): [agent-platform-and-stasisd-epic.md](../../docs/design/agent-platform-and-stasisd-epic.md).

Cron dialect matches `RecurringDefinition` / `cron` 0.12 (**7 fields**: sec min hour dom month dow year).

```bash
mkdir -p /tmp/stasis-agents.d
cat > /tmp/stasis-agents.d/nightly.toml <<'EOF'
api_version = "stasisd/v1"
[[schedule]]
id = "nightly"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 2 * * * *"
payload = { user_prompt = "review open work" }
EOF

# One-shot reconcile + tick
cargo run -p stasisd -- --config /tmp/stasis-agents.d --once

# Long-running host (watch + tick). Remove the TOML to drain the schedule.
cargo run -p stasisd -- --config /tmp/stasis-agents.d \
  --tick-interval 1s --reconcile-interval 30s --run-for 10s
```

## References

- ADR: [ADR-0008](../../docs/adr/ADR-0008-stasisd-declarative-engine.md)
- Plan: [stasisd-declarative-engine-plan.md](../../docs/design/stasisd-declarative-engine-plan.md)
- Runbook: [`stasisd` Operator Runbook](./stasisd-runbook.md)
- systemd: [`docs/deploy/stasisd.service`](../../docs/deploy/stasisd.service)
- Related: [Recurring Jobs](./recurring-jobs.md), [Agent Platform Runtime Contracts](./agent-platform-contracts.md)
