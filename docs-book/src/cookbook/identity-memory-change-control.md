# Identity Memory Change Control

## Document Metadata

- Document Type: Cookbook Recipe
- Audience: Engineer
- Stability: Stable
- Last Verified: 2026-06-05
- Verified Against:
  - src/application/use_cases/identity_memory_service.rs
  - src/infrastructure/memory/in_memory_identity_memory_store.rs
  - src/infrastructure/memory/identity_context_filter.rs
  - src/ports/outbound/memory/identity_memory_models.rs

## Outcome

Apply governed identity relationship updates with tier-aware proposal splitting, explicit approvals, and version-safe commits. Seed **contacts**, **user preferences**, and query **cognitive vs policy** context slices (0.4.0).

## Recipe

### 1. Seed baseline relationship state

```rust
use chrono::Utc;
use std::sync::Arc;

use stasis::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;
use stasis::ports::outbound::memory::identity_memory_models::{
    EntityRef, RelationshipEntity, RelationshipKind, RelationshipStatus, UpdateSource,
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
        relationship_kind: RelationshipKind::AssistantUser,
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

let service = IdentityMemoryService::new(store.clone());
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
2. `recency_score` proposal is AutoCommit tier.
3. `autonomy_scope.allow` proposal is ApprovalRequired tier.

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

### 7. Seed contacts, preferences, and query by mode (0.4.0)

Store scalar user prefs and first-class contacts without relationship-edge overhead for every setting:

```rust
use std::collections::BTreeMap;

use stasis::ports::outbound::memory::identity_memory_models::{
    ContactEntity, GetIdentityContextRequest, IdentityContextMode, UserEntity,
};
use stasis::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

store.upsert_user(UserEntity {
    user_id: "user-123".to_string(),
    timezone: "America/New_York".to_string(),
    language_variant: None,
    preferences: BTreeMap::from([("theme".to_string(), json!("dark"))]),
    status: "active".to_string(),
    version: 1,
    updated_at: Utc::now(),
    ..Default::default()
})?;

store.upsert_contact(ContactEntity {
    contact_id: "contact-alex".to_string(),
    display_name: "Alex Rivera".to_string(),
    aliases: vec!["alex@example.com".to_string(), "Alex R.".to_string()],
    status: "active".to_string(),
    version: 1,
    updated_at: Utc::now(),
})?;

store.upsert_relationship(RelationshipEntity {
    relationship_id: "rel-knows-alex".to_string(),
    source_entity_ref: EntityRef {
        entity_type: "UserEntity".to_string(),
        entity_id: "user-123".to_string(),
    },
    target_entity_ref: EntityRef {
        entity_type: "ContactEntity".to_string(),
        entity_id: "contact-alex".to_string(),
    },
    relationship_kind: RelationshipKind::Knows,
    status: RelationshipStatus::Active,
    trust_level: 0.5,
    confidence: 0.85,
    strength_score: 0.8,
    recency_score: 0.7,
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
})?;

let cognitive = store
    .get_identity_context(&GetIdentityContextRequest {
        user_id: "user-123".to_string(),
        persona_id: "persona:default".to_string(),
        channel_id: "channel:default".to_string(),
        relationship_limit: 8,
        mode: IdentityContextMode::Cognitive,
    })
    .await?;

assert_eq!(cognitive.contacts.len(), 1);
assert!(cognitive.policy_profiles.is_empty());

let policy = store
    .get_identity_context(&GetIdentityContextRequest {
        user_id: "user-123".to_string(),
        persona_id: "persona:default".to_string(),
        channel_id: "channel:default".to_string(),
        relationship_limit: 8,
        mode: IdentityContextMode::Policy,
    })
    .await?;

assert!(policy.contacts.is_empty());
assert!(policy.user.expect("user").preferences.is_empty());
```

## Operational Notes

1. Always pass `expected_version` from latest read to avoid stale writes.
2. Treat ApprovalRequired proposals as explicit human checkpoints.
3. Persist proposal and transition history to support audits.
4. Watch for `PolicyDenied` outcomes and adjust patch strategy accordingly.
5. Use **`IdentityContextMode::Cognitive`** for prompt personalization; use **`Policy`** for guardrail consumers.
6. Prefer `UserEntity.preferences` for scalar settings; use `RelationshipKind::Knows` (or `prefers`, `delegation`, `colleague`) for people graph edges.

## Related Documents

- [Identity Memory Layer](../identity-memory-layer.md)
- [SurrealDB Schema — Identity Memory Tables](../surrealdb-schema.md#identity-memory-tables)
