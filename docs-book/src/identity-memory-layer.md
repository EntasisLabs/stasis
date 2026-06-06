# Identity Memory Layer

## Document Metadata

- Document Type: Architecture and Operations Guide
- Audience: Engineer, Runtime Owner, Platform Team
- Stability: Stable
- Last Verified: 2026-06-05
- Verified Against:
  - src/ports/outbound/memory/identity_memory_store.rs
  - src/ports/outbound/memory/identity_memory_models.rs
  - src/application/use_cases/identity_memory_service.rs
  - src/application/runtime/identity_context_compiler.rs
  - src/application/runtime/prompt_chat_job_handler.rs
  - src/application/runtime/stasis_runtime_builder.rs
  - src/infrastructure/memory/in_memory_identity_memory_store.rs
  - src/infrastructure/memory/surreal_identity_memory_store.rs
  - src/infrastructure/memory/identity_context_filter.rs
  - docs/design/identity-model-0.4.0-roadmap.md

## Purpose

The identity memory layer gives Stasis durable, policy-aware identity context for runtime decisions. It complements the Locus memory plane:

1. Locus memory handles conversational and resonance retrieval.
2. Identity memory handles relationship, policy, trust, and governance state.

Use this layer when actor identity, trust state, approval workflows, or relationship continuity must influence execution behavior.

## Scope and Responsibilities

Identity memory in Stasis currently provides:

1. Identity context retrieval via `get_identity_context` with optional **mode filtering** (0.4.0).
2. Governed mutation workflow via `propose_entity_update` and `commit_entity_update`.
3. Version and transition history for audit and rollback.
4. Relationship continuity constraints (for revoked and replacement links).
5. Bridge node generation for material relationship transitions.
6. **User preferences** and **contact graph** entities without overloading relationship edges (0.4.0).

## Core Port Contract

`IdentityMemoryStore` defines five operations:

1. `get_identity_context`
2. `propose_entity_update`
3. `commit_entity_update`
4. `list_entity_history`
5. `rollback_entity_version`

Reference: `src/ports/outbound/memory/identity_memory_store.rs`

## Data Model Overview

Primary model families are defined in `src/ports/outbound/memory/identity_memory_models.rs`.

| Family | Examples | Notes |
|---|---|---|
| Identity entities | PersonaEntity, UserEntity, **ContactEntity**, ChannelProfileEntity, PolicyProfileEntity, RelationshipEntity | Core state read by `get_identity_context` |
| Typed relationships | **`RelationshipKind`** enum | Structural (`assistant_user`, `user_channel`) and social (`knows`, `prefers`, `delegation`, `colleague`) |
| Context modes | **`IdentityContextMode`** | `full`, `policy`, `cognitive` — see below |
| Governance enums | UpdateTier, ProposalState, CommitOutcomeCode, RelationshipStatus, UpdateSource | Used for policy and approval semantics |
| Workflow records | EntityUpdateProposalRecord, RelationshipTransitionEvent | Persistent audit trail for proposal and state change lifecycle |
| IO contracts | GetIdentityContextRequest/Response, ProposeEntityUpdateRequest/Response, CommitEntityUpdateRequest/Response | Boundary contracts used by services and adapters |

### User preferences (0.4.0)

`UserEntity.preferences` is a `BTreeMap<String, Value>` for lightweight scalar settings (theme, default model, digest hour) that do not require graph edges.

Store via `upsert_user` on the identity adapter. Patch tier: `preferences.*` → **AutoCommit** (reserved: `preferences.policy.*` → ConfirmRequired).

### ContactEntity (0.4.0)

First-class people with `contact_id`, `display_name`, and `aliases`. Link to users or personas via social `RelationshipKind` edges (`knows`, `prefers`, `delegation`, `colleague`).

Contacts appear in `GetIdentityContextResponse.contacts` when reachable through active relationships in the current context query.

### RelationshipKind (0.4.0)

Typed enum replacing free-form strings. Serializes as snake_case (`assistant_user`, `knows`, …). Unknown persisted values deserialize to `Legacy(String)` for backward compatibility.

| Kind | Role |
|---|---|
| `assistant_user`, `user_channel` | Runtime structural wiring (0.3.0 compat) |
| `knows`, `prefers`, `delegation`, `colleague` | Social contact graph |

Helpers: `is_structural()`, `is_social()`.

## Identity Context Modes (0.4.0)

`GetIdentityContextRequest.mode` selects which slice of identity state callers receive. Default: **`Full`** (backward compatible).

| Field | `Full` | `Policy` | `Cognitive` |
|---|---|---|---|
| persona, user, channel | ✓ | anchor only | ✓ |
| `user.preferences` | ✓ | stripped | ✓ |
| `contacts` | ✓ | empty | ✓ |
| structural relationships | ✓ | ✓ | excluded |
| social relationships | ✓ | excluded | ✓ |
| `policy_profiles` | ✓ | ✓ | empty |
| `flattened_claims` | ✓ | ✓ | empty |

Filtering is implemented in `src/infrastructure/memory/identity_context_filter.rs` and applied by both store adapters after assembling the full graph.

**Runtime default:** prompt handlers request **`Cognitive`** mode via `identity_context_compiler` so policy enforcement data is not mixed into personalization snapshots.

```rust
use stasis::ports::outbound::memory::identity_memory_models::{
    GetIdentityContextRequest, IdentityContextMode,
};

let policy_context = store
    .get_identity_context(&GetIdentityContextRequest {
        user_id: "user-123".to_string(),
        persona_id: "persona:default".to_string(),
        channel_id: "channel:default".to_string(),
        relationship_limit: 8,
        mode: IdentityContextMode::Policy,
    })
    .await?;
```

## Governance Model

Patch fields are classified into policy tiers before proposal records are created.

| Tier | Behavior | Representative fields |
|---|---|---|
| AutoCommit | Can be committed automatically | recency_score, confidence, strength_score, **preferences.*** |
| ConfirmRequired | Requires explicit confirm flow | interruption_policy.quiet_hours, escalation_policy.fallback, policy_tags, relationship_kind |
| ApprovalRequired | Requires explicit approver | status, trust_level, autonomy_scope.allow, autonomy_scope.deny, approval_profile_id |

Important behavior:

1. Mixed-tier patches are split into multiple proposals.
2. Propose response reports `split_patch` and per-proposal tiers.
3. ApprovalRequired commits are rejected without approver.

Reference: `src/infrastructure/memory/in_memory_identity_memory_store.rs`

## Commit and Rollback Semantics

Current implementation status:

1. `RelationshipEntity` has a full commit path.
2. Non-relationship entity commit currently returns `InvalidPatch` rationale (includes `ContactEntity` and `UserEntity` in 0.4.0 — use adapter `upsert_*` helpers for direct writes).
3. Relationship rollback is supported by version snapshot restore.
4. Non-relationship rollback currently returns not implemented rationale.

Commit outcomes include:

- Ok
- StaleState
- ApprovalRequired
- PolicyDenied
- InvalidPatch
- ExpiredProposal
- NotFound

## Runtime Integration Path

When an identity store is wired into runtime handlers:

1. Runtime loads identity summary before prompt execution using **`IdentityContextMode::Cognitive`**.
2. Summary is prepended to user prompt as an identity snapshot header.
3. Diagnostics include identity_context attempted, summary, and error information.
4. Snapshot counts include **`contacts=`** and **`preferences=`** (0.4.0).

Defaults and resolution rules:

1. `user_id` is sourced from `correlation_id`.
2. `persona_id` defaults to `STASIS_DEFAULT_PERSONA_ID` or `persona:default`.
3. `channel_id` defaults to `STASIS_DEFAULT_CHANNEL_ID` or `channel:default`.
4. If `policy_profile` exists, `channel_id` is derived as `channel:{policy_profile}`.

Reference:

- `src/application/runtime/identity_context_compiler.rs`
- `src/application/runtime/prompt_chat_job_handler.rs`

## Wiring Patterns

### In-memory runtime with identity store

```rust
use std::sync::Arc;

use stasis::application::runtime::runtime_factory::RuntimeBackend;
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;

#[tokio::main]
async fn main() -> stasis::domain::errors::Result<()> {
    let identity_store = Arc::new(InMemoryIdentityMemoryStore::default());

    let _composition = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_locus_memory()
        .with_identity_memory_store(identity_store)
        .build()
        .await?;

    Ok(())
}
```

### Surreal-backed identity store

```rust
use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use stasis::infrastructure::memory::surreal_identity_memory_store::SurrealIdentityMemoryStore;

#[tokio::main]
async fn main() -> stasis::domain::errors::Result<()> {
    let db = Surreal::<Any>::init();
    db.connect("mem://").await?;
    db.use_ns("stasis").use_db("runtime").await?;

    let identity_store = SurrealIdentityMemoryStore::new(db.clone());
    identity_store.ensure_schema().await?;

    let _identity_store_arc = Arc::new(identity_store);
    Ok(())
}
```

Surreal schema for identity tables (including `identity_contact` and `identity_user.preferences`) is documented in [SurrealDB Schema](./surrealdb-schema.md#identity-memory-tables) and `docs/architecture/surrealdb-schema.md`.

## Production Checklist

1. Decide ownership model for approver identities and receipt IDs.
2. Enforce `expected_version` checks in all write paths.
3. Keep `relationship_limit` bounded for predictable latency.
4. Choose `IdentityContextMode` explicitly for each consumer (`Cognitive` for prompts, `Policy` for guardrails).
5. Track `CommitOutcomeCode` rates and stale-state frequency.
6. Validate transition and proposal history retention policies.
7. Alert on repeated `PolicyDenied` or `ApprovalRequired` failures.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| commit returns StaleState | expected_version mismatch | Reload current entity version and retry with updated expected_version |
| commit returns ApprovalRequired | proposal tier needs approver | Re-submit commit with approver set |
| commit returns PolicyDenied | guardrail violation | Inspect rationale, patch fields, and relationship continuity constraints |
| identity context missing in diagnostics | store not wired | Ensure `with_identity_memory_store` is used in StasisRuntimeBuilder |
| summary not prepended to prompt | identity read failure | Check identity store health and identity_context diagnostics section |
| contacts empty in Cognitive mode | no active social edge to ContactEntity | Seed contact + `RelationshipKind::Knows` edge from user/persona |
| policy profiles in prompt snapshot | wrong mode | Use `IdentityContextMode::Cognitive`; compiler sets this by default |

## Related Documents

- [Identity Memory Change Control](./cookbook/identity-memory-change-control.md) — governed relationship updates and contact graph seeding
- [Identity Model 0.4.0 Roadmap](../../docs/design/identity-model-0.4.0-roadmap.md) — internal delivery plan
