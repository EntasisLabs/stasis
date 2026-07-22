# `stasisd` Operator Runbook

## Document Metadata

- Document Type: Operations Guide
- Audience: SRE, Platform Owner
- Stability: Evolving
- Last Verified: 2026-07-22

## Purpose

Operate `stasisd` safely in local and systemd-hosted environments.

## Ownership model

Managed recurring definitions use the frozen id prefix `stasisd:<schedule-id>`.

- Reconcile **only** touches ids with that prefix.
- Unmanaged definitions (any other id) are left alone.
- `managed_by` is the constant `stasisd` (id-prefix ownership in Phase 3).

## Quarantine and `--strict`

- Bad sibling files are quarantined into diagnostics; valid siblings still load.
- Default (non-strict): warnings to stderr; good schedules still reconcile.
- `--strict`: any diagnostic fails the reconcile/process (exit code 2).

Chaos expectation: an invalid file must not break good schedules unless `--strict` is set.

## Host loop

```bash
stasisd --config /etc/stasis/agents.d \
  --strict \
  --tick-interval 1s \
  --reconcile-interval 30s \
  --healthz-addr 127.0.0.1:8081
```

- Filesystem watch + debounce triggers reconcile.
- Periodic reconcile catches missed events.
- Tick: materialize due recurring jobs → process queues → publish outbox.
- Graceful shutdown on Ctrl-C / SIGTERM.

## Health endpoints

When `--healthz-addr` is set:

| Path | Meaning |
| --- | --- |
| `/healthz` | Process alive |
| `/readyz` | Last reconcile succeeded |

## Backend selection

`STASIS_STASISD_RUNTIME_BACKEND`:

- `in-memory` (default)
- `surreal-mem` / `surreal-ws` / `surreal-kv`

HA note: run **one active reconciler** per durable store. Multiple `stasisd` processes against the same Surreal database can race on managed def updates.

## systemd

Example unit: [`docs/deploy/stasisd.service`](../../docs/deploy/stasisd.service).

```bash
sudo install -d /etc/stasis/agents.d
sudo systemctl enable --now stasisd
curl -fsS http://127.0.0.1:8081/healthz
```

## Related

- [`stasisd` overview](./stasisd.md)
- [ADR-0008](../../docs/adr/ADR-0008-stasisd-declarative-engine.md)
- Epic board: [agent-platform-and-stasisd-epic.md](../../docs/design/agent-platform-and-stasisd-epic.md)
