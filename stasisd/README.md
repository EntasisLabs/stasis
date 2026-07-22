# stasisd

Declarative Stasis engine (ADR-0008): YAML/TOML desired state → durable recurring agent schedules.

## Phase 1

- Discover `*.toml` / `*.yaml` / `*.yml`
- Parse `stasisd/v1` schedule documents
- Validate cron (7-field), timezone, known job types, duplicates, payload size
- Reconcile managed recurring ids (`stasisd:<id>`) against an in-memory runtime
- Removal policies: `drain` (default), `orphan`, `cancel` (disables + records skip)

```bash
cargo run -p stasisd -- --config ./agents.d --once
cargo test -p stasisd
```

See [docs/design/stasisd-declarative-engine-plan.md](../docs/design/stasisd-declarative-engine-plan.md).
