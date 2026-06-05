# Identity Memory Change Control

## Document Metadata

- Document Type: Cookbook Recipe
- Audience: Engineer
- Stability: Stable
- Last Verified: 2026-06-04
- Verified Against:
  - src/application/use_cases/

## Outcome

Apply governed identity relationship updates with tier-aware proposal splitting, explicit approvals, and version-safe commits.

## Recipe

### 1. Seed baseline relationship state

```rust
use chrono::Utc;
use std::sync::Arc;

use stasis::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;
use stasis::ports::outbound::memory::identity_memory_models::{
    EntityRef, RelationshipEntity, RelationshipStatus, UpdateSource,
};

fn seed_relationship(store: &InMemoryIdentityMemoryStore) -> stasis::domain::errors::Result<()> {
    store.upsert_relationship(RelationshipEntity {
        relationship_id: "rel-1".to_string(),
        source_entity_ref: EntityRef {
            entity_type: "PersonaEntity".to_string(),
            entity_id: "persona:default".to_string(),
        },
        target_entity_ref: EntityRef {
            entity_type: "UserEntity".to_string(),
            entity_id: "user-123".to_string(),
        },
        relationship_kind: "assistant_user".to_string(),
        status: RelationshipStatus::Active,
        trust_level: 0.50,
        confidence: 0.80,
        strength_score: 0.80,
        recency_score: 0.60,
        autonomy_scope: Default::default(),
        approval_profile_id: None,
        interruption_policy: Default::default(),
        escalation_policy: Default::default(),
        policy_tags: vec![],
        provenance: UpdateSource::UserDirect,
        parent_relationship_id: None,
        governing_relationship_ids: vec![],
        derived_from_relationship_id: None,
        last_transition_reason: None,
        transition_receipt_id: None,
        version: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    })
}
```

### 2. Initialize store and service

```rust
use stasis::application::use_cases::identity_memory_service::IdentityMemoryService;

let store = Arc::new(InMemoryIdentityMemoryStore::default());
seed_relationship(store.as_ref())?;

let service = IdentityMemoryService::new(store);
```

### 3. Propose mixed-tier patch

```rust
use serde_json::json;

use stasis::ports::outbound::memory::identity_memory_models::{
    IdentityEntityType, ProposeEntityUpdateRequest, UpdateSource,
};

let proposed = service
    .propose_entity_update(&ProposeEntityUpdateRequest {
        entity_type: IdentityEntityType::RelationshipEntity,
        entity_id: "rel-1".to_string(),
        patch: json!({
            "recency_score": 0.90,
            "autonomy_scope.allow": ["external_posting"]
        }),
        source: UpdateSource::ModelInferred,
        confidence: 0.82,
        reason: "post interaction update".to_string(),
        actor: "model".to_string(),
        receipt_id: None,
        expires_at: None,
    })
    .await?;

println!("split_patch={}", proposed.split_patch);
println!("tiers={:?}", proposed.tiers);
```

Expected behavior:

1. Patch is split into multiple proposals because tiers differ.
2. recency_score proposal is AutoCommit tier.
3. autonomy_scope.allow proposal is ApprovalRequired tier.

### 4. Auto-commit safe tiers

```rust
use stasis::application::use_cases::identity_memory_service::{
    IdentityMemoryService, ProposeAndCommitRequest,
};

let auto = service
    .propose_and_commit_autocommit(&ProposeAndCommitRequest {
        proposal_request: ProposeEntityUpdateRequest {
            entity_type: IdentityEntityType::RelationshipEntity,
            entity_id: "rel-1".to_string(),
            patch: json!({ "recency_score": 0.88 }),
            source: UpdateSource::ModelInferred,
            confidence: 0.80,
            reason: "freshness update".to_string(),
            actor: "model".to_string(),
            receipt_id: None,
            expires_at: None,
        },
        expected_version: 1,
        approver: Some("owner".to_string()),
    })
    .await?;

println!("commits={}", auto.commits.len());
```

### 5. Commit approval-required proposal

```rust
use stasis::ports::outbound::memory::identity_memory_models::CommitEntityUpdateRequest;

let approval_proposal_id = proposed.proposal_ids[1].clone();

let commit = service
    .commit_entity_update(&CommitEntityUpdateRequest {
        proposal_id: approval_proposal_id,
        expected_version: 2,
        approver: Some("owner".to_string()),
    })
    .await?;

println!("committed={}", commit.committed);
println!("code={:?}", commit.code);
println!("new_version={:?}", commit.new_version);
```

### 6. Inspect history and rollback if needed

```rust
use stasis::ports::outbound::memory::identity_memory_models::{
    ListEntityHistoryRequest, RollbackEntityVersionRequest,
};

let history = service
    .list_entity_history(&ListEntityHistoryRequest {
        entity_type: IdentityEntityType::RelationshipEntity,
        entity_id: "rel-1".to_string(),
        limit: 50,
    })
    .await?;

println!("proposal_count={}", history.proposals.len());
println!("transition_count={}", history.transitions.len());

let rollback = service
    .rollback_entity_version(&RollbackEntityVersionRequest {
        entity_type: IdentityEntityType::RelationshipEntity,
        entity_id: "rel-1".to_string(),
        target_version: 2,
        reason: "operator rollback".to_string(),
        approver: "owner".to_string(),
    })
    .await?;

println!("rolled_back={}", rollback.rolled_back);
```

## Operational Notes

1. Always pass expected_version from latest read to avoid stale writes.
2. Treat ApprovalRequired proposals as explicit human checkpoints.
3. Persist proposal and transition history to support audits.
4. Watch for PolicyDenied outcomes and adjust patch strategy accordingly.
