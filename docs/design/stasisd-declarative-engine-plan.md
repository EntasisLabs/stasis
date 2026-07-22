# `stasisd` Declarative Engine — RFC and Delivery Plan

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner, SRE
- Stability: Evolving
- Last Verified: 2026-07-22
- Verified Against:
  - docs/adr/ADR-0008-stasisd-declarative-engine.md
  - docs/adr/ADR-0007-agent-platform-runtime-contracts.md
  - docs/design/agent-platform-runtime-contracts-plan.md
  - src/domain/runtime/recurring.rs
  - src/ports/outbound/runtime/recurring_store.rs
  - src/sdk/runtime_sdk.rs
  - src/application/runtime/stasis_runtime_builder.rs
  - docs-book/src/recurring-jobs.md

Status: **Proposed — Ready for Phased Implementation**  
Date: 2026-07-22  
Owner: Stasis Core  
ADR: [ADR-0008-stasisd-declarative-engine.md](../adr/ADR-0008-stasisd-declarative-engine.md)  
Epic sequencing: [agent-platform-and-stasisd-epic.md](agent-platform-and-stasisd-epic.md)

Depends on:

- Existing `RecurringDefinition` materialization (ADR-0004)
- Runtime SDK enqueue / process / publish loops
- [ADR-0007](../adr/ADR-0007-agent-platform-runtime-contracts.md) agent platform contracts (for external participant kinds; not required for local-only v1)
- [agent-platform-runtime-contracts-plan.md](agent-platform-runtime-contracts-plan.md)

## 1. Purpose

Define **`stasisd`**: a small deployable engine that reads YAML/TOML desired state and reconciles it into durable Stasis runtime objects — the “nginx of AI orchestration.”

Operators should be able to:

```bash
stasisd --config /etc/stasis/agents.d/
```

- add a file → schedules/agents appear  
- edit a file → definitions update  
- remove a file → managed schedules drop; in-flight work follows drain policy  

## 2. Problem Statement

Stasis can already register recurring definitions and materialize jobs, but only via code/SDK. There is no:

1. declarative config schema for agent schedules
2. file watch + reconcile loop
3. ownership/provenance model for config-managed objects
4. packaged process that runs scheduler + workers from config alone

Without this, every deployment reinvents “load YAML and call `register_recurring`.”

## 3. Positioning

```text
Git / config files (YAML|TOML)
        │ desired state
        ▼
┌─────────────────────┐
│       stasisd       │  watch · validate · reconcile · run ticks
└──────────┬──────────┘
           │ RuntimeSdk / builder
           ▼
┌─────────────────────┐
│   Stasis runtime    │  jobs · leases · recurring · outbox · tools
└─────────────────────┘
```

| Component | Owns |
| --- | --- |
| Config files | Desired schedules/agents/wiring |
| `stasisd` | Parse, validate, reconcile, process host |
| Stasis runtime | Durable execution truth |

Files are **not** the runtime database. Surreal/in-memory stores remain execution truth; files are the desired-state input.

## 4. Scope

### In scope (v1)

1. Config directory (and single-file) loading for YAML + TOML.
2. Schema for recurring agent/job schedules mapping to existing job types.
3. Reconcile loop: create/update/disable-or-delete managed `RecurringDefinition`s.
4. File watch + periodic full reconcile (safety net).
5. Provenance metadata on managed objects.
6. Delete/drain policy for removed files.
7. Process host: materialize recurring, `process_once` workers, publish outbox.
8. `stasisd` binary (workspace crate or `src/bin`) + cookbook.

### Out of scope (v1)

1. Vendor agent adapters.
2. Full Kubernetes CRD/operator.
3. Hot-reload of arbitrary Rust tools/plugins (tools remain compiled/injected).
4. Multi-tenant config isolation beyond provenance labels.
5. Replacing dashboard/control-plane APIs.

### Follow-on (v1.x / v2)

1. Declarative endpoint registration (comms targets from ADR-0007).
2. Declarative MCP provider/exporter wiring references (gateway sockets/paths, not vendor code).
3. One-shot (non-recurring) job templates and workflow revision binds.
4. Optional HTTP admin: `/healthz`, `/readyz`, reload trigger.

## 5. Config model (draft)

### 5.1 File discovery

- `--config <path>` accepts a file or directory.
- Directory loads `*.yaml`, `*.yml`, `*.toml` (non-recursive v1, or recursive with explicit `--recursive`).
- Each file may define one document or a list of resources.
- Resource identity: stable `id` field (required). Collision across files = validation error.

### 5.2 Resource kinds (v1)

Minimal v1 kinds:

```text
Schedule   → RecurringDefinition (+ payload template)
AgentSet   → optional named participant bundle referenced by Schedule
```

Illustrative TOML:

```toml
api_version = "stasisd/v1"

[[schedule]]
id = "nightly-review"
enabled = true
queue = "agents"
job_type = "workflow.stasis.agent_session"
cron = "0 2 * * *"
timezone = "UTC"
jitter_seconds = 0
max_attempts = 3
on_remove = "drain"   # drain | cancel | orphan

[schedule.payload]
thread_id = "nightly-review"
initial_user_prompt = "Review open changes and summarize risk."
max_turns = 4
model_hint = "openai::gpt-4o-mini"

[[schedule.payload.participants]]
agent_id = "planner"
tool_name = "noop"
system_prompt = "You plan the review."

[[schedule.payload.participants]]
agent_id = "reviewer"
tool_name = "noop"
system_prompt = "You critique the plan."
```

Illustrative YAML equivalent should parse to the same canonical internal model.

### 5.3 Canonical internal model

Parser output is not “YAML Value forever.” Normalize to:

```rust
pub struct StasisdDocument {
    pub api_version: String,
    pub source_path: PathBuf,
    pub content_hash: String,
    pub schedules: Vec<StasisdSchedule>,
}

pub struct StasisdSchedule {
    pub id: String,
    pub enabled: bool,
    pub queue: String,
    pub job_type: String,
    pub cron: String,
    pub timezone: String,
    pub jitter_seconds: i64,
    pub max_attempts: u32,
    pub on_remove: OnRemovePolicy,
    pub payload: Value, // validated against job_type schema when available
}
```

`api_version` fails closed if unsupported.

### 5.4 Mapping to runtime

| Config field | Runtime target |
| --- | --- |
| `schedule.id` | `RecurringDefinition.id` (optionally prefixed `stasisd:` — decide in Phase 0) |
| `queue` / `job_type` / cron / tz / jitter / max_attempts | `RecurringDefinition` fields |
| `payload` | serialized to `payload_template_ref` or STTP-backed payload artifact |
| provenance | stored alongside definition (extension fields or parallel manage-index) |

**Ownership:** only definitions with `managed_by=stasisd` are created/updated/deleted by reconcile. SDK-created defs are untouched.

## 6. Reconcile semantics

### 6.1 Loop

```text
on startup:
  load all files → validate → compute desired set D
  list managed runtime defs M
  apply diff(D, M)

on fs event or tick:
  reload changed/removed files
  recompute D
  apply diff(D, M)
```

Diff operations:

| Situation | Action |
| --- | --- |
| id in D, not in M | insert recurring def |
| id in both, hash differs | update def (preserve lease fields carefully) |
| id in both, hash same | no-op |
| id in M, not in D | apply `on_remove` policy |

### 6.2 `on_remove` policy

| Policy | Behavior |
| --- | --- |
| `drain` (default) | disable/delete definition so no new materializations; leave in-flight jobs to finish |
| `cancel` | remove definition and request cancel for non-terminal jobs materialized from that definition (best-effort; needs job attribution) |
| `orphan` | stop managing definition but leave it enabled in runtime (escape hatch) |

v1 minimum: implement `drain` + `orphan`; `cancel` when job attribution (`recurring_id` / provenance on jobs) is available.

### 6.3 Validation rules

1. Unknown `api_version` → reject file.
2. Duplicate `id` across loaded files → reject conflicting files (neither applied if atomic per-reconcile snapshot fails).
3. Invalid cron/timezone → reject schedule.
4. Unknown `job_type` (not registered on runtime) → reject schedule.
5. Payload schema validation where job payload types exist; otherwise JSON structure checks only.
6. Fail closed: a bad file must not tear down already-applied good files from other paths (per-file quarantine with error log + non-zero readiness if `--strict`).

### 6.4 Concurrency / HA

- Runtime recurring leases already prevent double materialization across schedulers.
- Multiple `stasisd` instances may run against one Surreal backend **if** they share identical desired state (GitOps) or only one instance reconciles (`--reconcile=leader` later).
- v1 recommendation: one reconciler active per environment; N workers may still process jobs.

## 7. Process host responsibilities

`stasisd` runs the operational loop operators expect from nginx/systemd units:

1. Build runtime from env (`STASIS_*`, backend selection).
2. Register built-in job handlers (same defaults as `StasisRuntimeBuilder`).
3. Optional: load dynamic tool/gateway plugins later — **not v1**.
4. Reconcile configs.
5. Tick forever:
   - `materialize_recurring_now`
   - `process_once` (or bounded batch) on configured queues
   - `publish_pending_events`
6. Emit logs/metrics for reconcile diffs and tick counts.
7. Graceful shutdown: stop reconcile/ticks; do not corrupt leases.

## 8. Architecture sketch

```text
┌──────────────────────────────────────────────────────────┐
│                        stasisd                           │
│  ConfigLoader(YAML|TOML) → Validator → DesiredState      │
│           │                                              │
│           ▼                                              │
│     Reconciler ──► RecurringStore / RuntimeSdk           │
│           │                                              │
│           ▼                                              │
│     TickLoop (materialize / process / publish)           │
│                                                          │
│  FsWatcher ──► debounce ──► reconcile                    │
└──────────────────────────────────────────────────────────┘
```

Suggested module layout (implementation detail):

- `stasisd` binary crate **or** `src/bin/stasisd.rs` + `src/application/stasisd/*`
- Prefer a small workspace crate `stasisd` that depends on `stasis` public SDK to keep kernel clean — decide in Phase 0.

Recommendation: **workspace crate `stasisd`** so deployable packaging, CLI flags, and fs-watch deps do not weigh down the library.

## 9. Delivery phases

### Phase 0 — Schema freeze and crate skeleton

**Deliverables**

1. ADR-0008 accepted; this plan Ready for Implementation.
2. `stasisd/v1` schema draft frozen (schedule fields above).
3. Workspace crate skeleton + CLI (`--config`, `--once`, `--strict`).
4. Provenance field convention documented.
5. Decide ID strategy (`raw id` vs `stasisd:<id>`).

**Acceptance**

- Binary starts, loads empty dir, exits cleanly with `--once`.

### Phase 1 — Load + validate + apply schedules

**Deliverables**

1. YAML + TOML parsers → `StasisdDocument`.
2. Validation (cron, timezone, duplicate ids, api_version).
3. Reconcile apply for create/update/`drain` remove against in-memory runtime.
4. `payload` serialization into recurring `payload_template_ref` (document size limits).
5. Unit tests with golden config files.

**Acceptance**

- Add file → recurring def exists and materializes a job.
- Edit cron → next_run computation uses new expression after reconcile.
- Remove file with `drain` → def gone/disabled; no new jobs; in-flight untouched.

### Phase 2 — Watch loop + process host

**Deliverables**

1. Filesystem watch with debounce.
2. Periodic full reconcile safety net.
3. Long-running tick loop (materialize/process/publish).
4. Surreal backend support via existing env/runtime composition.
5. Structured logging for reconcile diffs.

**Acceptance**

- Live directory edits reflected without restart.
- One end-to-end cookbook: drop TOML, see job succeed, remove TOML, schedule stops.

### Phase 3 — Hardening

**Deliverables**

1. `--strict` readiness behavior for bad files.
2. `on_remove=cancel` if job attribution exists; otherwise document limitation.
3. Config hash / drift detection in logs.
4. Graceful shutdown + basic `/healthz` (optional feature).
5. Docs-book runbook + systemd unit example.

**Acceptance**

- Chaos test: invalid file quarantined; valid siblings still reconcile.
- HA note documented (single reconciler recommendation).

### Phase 4 — ADR-0007 alignment (after contracts land)

**Deliverables**

1. Config fields for external participant kind + endpoint refs (vendor-neutral).
2. Optional declarative comms endpoint resources.
3. Optional MCP gateway socket/command references (inject paths, not vendor SDKs).

**Acceptance**

- Fake external gateway example driven purely from YAML/TOML + contracts.

## 10. CLI sketch

```bash
stasisd --config /etc/stasis/agents.d/
stasisd --config ./agents.toml --once --strict
stasisd --config ./agents.d --reconcile-interval 5s --watch
```

Exit codes:

| Code | Meaning |
| --- | --- |
| 0 | success / clean shutdown |
| 2 | validation errors (`--strict` or `--once`) |
| 3 | runtime/backend failure |

## 11. Testing strategy

1. Golden YAML/TOML → canonical document fixtures.
2. Reconcile diff tests (add/update/drain/orphan).
3. Runtime integration: materialize after reconcile.
4. Watch debounce test with temp dirs.
5. Backend parity for managed def provenance on Surreal when Phase 2 lands.
6. Architecture check: no vendor modules under `stasisd/`.

## 12. Documentation deliverables

1. ADR-0008
2. This plan
3. docs-book page: `stasisd` overview + runbook
4. Cookbook: “Declarative nightly agent from a TOML file”
5. Index updates in `docs/README.md` and ADR index

## 13. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Large payloads in `payload_template_ref` | size limit + STTP artifact refs (ADR-0003) |
| Reconcile fights SDK-created defs | provenance filter `managed_by=stasisd` |
| Split-brain multi-stasisd writers | v1 single reconciler; later leader election |
| Delete cancels wanted work | default `drain`; explicit `cancel` |
| Schema churn | `api_version`; additive fields only within v1 |

## 14. Relationship to ADR-0007

| ADR-0007 | `stasisd` |
| --- | --- |
| Comms / translation / MCP contracts | Consumes those ports when present |
| Gateways outside core | Config only references gateway endpoints/sockets |
| Runtime kernel stays vendor-neutral | Deployable stays vendor-neutral |

`stasisd` is the **declarative control surface**; ADR-0007 is the **interop substrate**. Together they are the product story: files in → durable agent work out → optional external agents via injected gateways.

## 15. Exit criteria for v1

1. Operator can run agents from YAML/TOML only (local tool-loop / agent_session job types).
2. Add/edit/remove file reconciles schedules correctly with `drain` default.
3. Process host materializes and processes jobs without a custom Rust main beyond config/env.
4. Docs + cookbook published; ADR-0008 Accepted.

## 16. Next immediate actions

1. Review/accept ADR-0008 and freeze `stasisd/v1` schedule schema.
2. Scaffold workspace `stasisd` crate with `--once` load path.
3. Implement Phase 1 reconcile against in-memory runtime + golden configs.
4. Land cookbook before watch/HA complexity.
