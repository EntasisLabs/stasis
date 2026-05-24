# Stasis API and SDK Layer Design

## Document Metadata

- Document Type: Architecture and API Design
- Audience: Engineer, Architect
- Stability: Draft
- Last Updated: 2026-05-23
- Related:
  - docs/design/stasis-framework-rfc.md
  - docs/design/medousa-engine-strategy.md

## Purpose

Define a stable, consumer-oriented API/SDK boundary for Stasis so product crates (starting with Medousa) can depend on framework contracts without importing internal application/infrastructure modules.

This document is the concrete companion to RFC migration phases B/C.

## Design Goals

1. Provide a stable consumer surface for runtime composition, job submission, and runtime operations.
2. Keep provider and storage adapters behind Stasis-owned contracts.
3. Minimize Medousa imports of `stasis::application::*`, `stasis::infrastructure::*`, and `stasis::ports::*`.
4. Preserve current behavior while enabling independent repo/versioning.
5. Keep migration incremental and compatibility-safe.

## Non-Goals

1. Rewriting runtime internals.
2. Changing orchestration job types or payload schemas in this phase.
3. Immediate removal of all existing prelude exports.

## SDK Surface Model

### 1. Core SDK namespace

`stasis::sdk` hosts explicit consumer facades:

1. `StasisSdk`: agent registration/invocation use cases.
2. `ControlPlaneSdk`: endpoint/cluster control-plane use cases.
3. `RuntimeSdk`: backend-agnostic runtime operations facade.

### 2. Runtime SDK (new facade)

`RuntimeSdk` is the canonical consumer entry point for operational runtime actions:

1. `enqueue(NewJob)`
2. `register_recurring(RecurringDefinition)`
3. `process_once(queue, worker_id)`
4. `publish_pending_events(limit)`
5. `materialize_recurring_now(scheduler_id)`
6. `stats_snapshot(pending_limit)`

`RuntimeStatsSnapshot` is the neutral read model for daemon/control-plane stats queries.

## API Stability Tiers

### Tier 1: Stable Consumer API

Allowed for product crates and external repos:

1. `stasis::prelude::*` consumer-safe exports.
2. `stasis::sdk::*` facades.
3. DTO/payload contracts used by runtime job builders.

### Tier 2: Advanced but Cautionary

Allowed for extension-heavy consumers, may evolve with minor versioning:

1. Tool traits and middleware extension points.
2. Runtime builder option toggles.

### Tier 3: Internal (not for product import)

Not allowed for Medousa core paths after split:

1. `stasis::application::*`
2. `stasis::infrastructure::*`
3. `stasis::ports::*`
4. direct runtime store field usage (`job_store`, `outbox_store`, `recurring_store`).

## Current Gap Inventory (Medousa)

### Good (already aligned)

1. Runtime job creation uses `RuntimeWorkflowJobBuilder`.
2. Payload contracts are moving to neutral orchestration module names.

### Remaining gaps to close

1. Some product flows still instantiate infrastructure adapters directly.
2. Medousa still depends on local path wiring to Stasis (not yet published/git dependency mode).
3. Standalone split CI/release workflow is not yet formalized.

## Migration Plan

### Stage A: SDK Facade Introduction (completed in this slice)

1. Add `RuntimeSdk` facade and export via prelude.
2. Define `RuntimeStatsSnapshot` to replace direct store counting logic.

### Stage B: Medousa Consumer Refactor

1. Completed: replaced direct `RuntimeComposition` operational calls in Medousa daemon with `RuntimeSdk`.
2. Completed: replaced direct store trait imports (`JobStore`, `OutboxStore`, `RecurringStore`) with `RuntimeSdk` methods.
3. Completed: behavior parity validated with conformance/parity suites.

### Stage C: Internal Import Elimination

1. Completed: replaced Medousa imports from `stasis::application::*` with prelude/sdk exports.
2. Completed: replaced Medousa imports from `stasis::infrastructure::*` with builder/sdk abstractions where covered by current runtime/tool-loop paths.
3. Completed: added architecture conformance checks for forbidden Medousa import prefixes/usages.

### Stage D: Repo Split Readiness

1. Completed: removed Medousa from Stasis workspace membership and detached Medousa manifest with local `[workspace]` for independent checks.
2. In progress: switch Medousa dependency to published/git Stasis.
3. In progress: validate standalone CI and release cadence.
4. Completed hardening: pinned `locus-core-rs` and `locus-sdk` to exact versions in Stasis to prevent independent dependency-resolution drift.

## Versioning and Compatibility

1. `stasis::sdk::*` and designated prelude exports are SemVer-governed API.
2. Internal modules are not SemVer-stable for consumers.
3. Deprecations must include migration path and minimum one release overlap.

## Split Readiness Exit Criteria

1. No Medousa imports from `stasis::application::*`, `stasis::infrastructure::*`, or `stasis::ports::*` in core execution paths.
2. Medousa runtime operations are performed through `RuntimeSdk`/stable surfaces.
3. Medousa builds and tests pass with Stasis as external dependency.
4. Architecture conformance tests enforce boundary rules automatically.

## Immediate Next Implementation Slice

1. Completed: migrated `medousa/src/bin/medousa_daemon.rs` stats and scheduler operations to `RuntimeSdk`.
2. Completed: removed direct runtime store trait imports from daemon (`JobStore`, `OutboxStore`, `RecurringStore`).
3. Completed: added a boundary conformance check for forbidden Medousa import prefixes and migrated remaining offending imports to prelude-safe surfaces.