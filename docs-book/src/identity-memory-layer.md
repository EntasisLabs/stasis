# Identity Memory Layer

## Document Metadata

- Document Type: Architecture and Operations Guide
- Audience: Engineer, Runtime Owner, Platform Team
- Stability: Active
- Last Verified: 2026-05-27
- Verified Against:
  - src/ports/outbound/memory/identity_memory_store.rs
  - src/ports/outbound/memory/identity_memory_models.rs
  - src/application/use_cases/identity_memory_service.rs
  - src/application/runtime/identity_context_compiler.rs
  - src/application/runtime/prompt_chat_job_handler.rs
  - src/application/runtime/stasis_runtime_builder.rs
  - src/infrastructure/memory/in_memory_identity_memory_store.rs
  - src/infrastructure/memory/surreal_identity_memory_store.rs

## Purpose

The identity memory layer gives Stasis durable, policy-aware identity context for runtime decisions. It complements the Locus memory plane:

1. Locus memory handles conversational and resonance retrieval.
2. Identity memory handles relationship, policy, trust, and governance state.

Use this layer when actor identity, trust state, approval workflows, or relationship continuity must influence execution behavior.

## Scope and Responsibilities

Identity memory in Stasis currently provides:

1. Identity context retrieval via get_identity_context.
2. Governed mutation workflow via propose_entity_update and commit_entity_update.
3. Version and transition history for audit and rollback.
4. Relationship continuity constraints (for revoked and replacement links).
5. Bridge node generation for material relationship transitions.

## Core Port Contract

`IdentityMemoryStore` defines five operations:

1. get_identity_context
2. propose_entity_update
3. commit_entity_update
4. list_entity_history
5. rollback_entity_version

Reference: src/ports/outbound/memory/identity_memory_store.rs

## Data Model Overview

Primary model families are defined in src/ports/outbound/memory/identity_memory_models.rs.

| Family | Examples | Notes |
|---|---|---|
| Identity entities | PersonaEntity, UserEntity, ChannelProfileEntity, PolicyProfileEntity, RelationshipEntity | Core state read by get_identity_context |
| Governance enums | UpdateTier, ProposalState, CommitOutcomeCode, RelationshipStatus, UpdateSource | Used for policy and approval semantics |
| Workflow records | EntityUpdateProposalRecord, RelationshipTransitionEvent | Persistent audit trail for proposal and state change lifecycle |
| IO contracts | GetIdentityContextRequest/Response, ProposeEntityUpdateRequest/Response, CommitEntityUpdateRequest/Response | Boundary contracts used by services and adapters |

## Governance Model

Patch fields are classified into policy tiers before proposal records are created.

| Tier | Behavior | Representative fields |
|---|---|---|
| AutoCommit | Can be committed automatically | recency_score, confidence, strength_score |
| ConfirmRequired | Requires explicit confirm flow | interruption_policy.quiet_hours, escalation_policy.fallback, policy_tags |
| ApprovalRequired | Requires explicit approver | status, trust_level, autonomy_scope.allow, autonomy_scope.deny, approval_profile_id |

Important behavior:

1. Mixed-tier patches are split into multiple proposals.
2. Propose response reports split_patch and per-proposal tiers.
3. ApprovalRequired commits are rejected without approver.

Reference: src/infrastructure/memory/in_memory_identity_memory_store.rs

## Commit and Rollback Semantics

Current implementation status:

1. RelationshipEntity has a full commit path.
2. Non-relationship entity commit currently returns InvalidPatch rationale.
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

1. Runtime loads identity summary before prompt execution.
2. Summary is prepended to user prompt as an identity snapshot header.
3. Diagnostics include identity_context attempted, summary, and error information.

Defaults and resolution rules:

1. user_id is sourced from correlation_id.
2. persona_id defaults to STASIS_DEFAULT_PERSONA_ID or persona:default.
3. channel_id defaults to STASIS_DEFAULT_CHANNEL_ID or channel:default.
4. If policy_profile exists, channel_id is derived as channel:{policy_profile}.

Reference:

- src/application/runtime/identity_context_compiler.rs
- src/application/runtime/prompt_chat_job_handler.rs

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

## Production Checklist

1. Decide ownership model for approver identities and receipt IDs.
2. Enforce expected_version checks in all write paths.
3. Keep relationship_limit bounded for predictable latency.
4. Track CommitOutcomeCode rates and stale-state frequency.
5. Validate transition and proposal history retention policies.
6. Alert on repeated PolicyDenied or ApprovalRequired failures.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| commit returns StaleState | expected_version mismatch | Reload current entity version and retry with updated expected_version |
| commit returns ApprovalRequired | proposal tier needs approver | Re-submit commit with approver set |
| commit returns PolicyDenied | guardrail violation | Inspect rationale, patch fields, and relationship continuity constraints |
| identity context missing in diagnostics | store not wired | Ensure with_identity_memory_store is used in StasisRuntimeBuilder |
| summary not prepended to prompt | identity read failure | Check identity store health and identity_context diagnostics section |
