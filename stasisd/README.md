# stasisd

Declarative Stasis engine (ADR-0008): YAML/TOML desired state → durable recurring agent schedules.

## Phase 0

Skeleton CLI only — discovers config files, does not yet parse schedule bodies or reconcile.

```bash
cargo run -p stasisd -- --config ./agents.d --once
```

Managed recurring ids use the frozen prefix `stasisd:<id>`.

See [docs/design/stasisd-declarative-engine-plan.md](../docs/design/stasisd-declarative-engine-plan.md).
