//! One identity scenario run against every store implementation.

use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::json;

use crate::domain::errors::Result;
use crate::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;
use crate::ports::outbound::memory::identity_memory_models::{
    CommitEntityUpdateRequest, CommitOutcomeCode, EntityRef, GetIdentityContextRequest,
    IdentityEntityType, PersonaEntity, ProposeEntityUpdateRequest, RelationshipEntity,
    RelationshipKind, RelationshipStatus, UpdateSource, UserEntity,
};
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

#[allow(clippy::double_must_use)]
#[async_trait]
pub(crate) trait IdentitySeed: IdentityMemoryStore {
    async fn seed_persona(&self, persona: PersonaEntity) -> Result<()>;
    async fn seed_user(&self, user: UserEntity) -> Result<()>;
    async fn seed_relationship(&self, relationship: RelationshipEntity) -> Result<()>;
}

#[async_trait]
impl IdentitySeed for InMemoryIdentityMemoryStore {
    async fn seed_persona(&self, persona: PersonaEntity) -> Result<()> {
        self.upsert_persona(persona)
    }
    async fn seed_user(&self, user: UserEntity) -> Result<()> {
        self.upsert_user(user)
    }
    async fn seed_relationship(&self, relationship: RelationshipEntity) -> Result<()> {
        self.upsert_relationship(relationship)
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl IdentitySeed
    for crate::infrastructure::sqlite::identity_memory_store::SqliteIdentityMemoryStore
{
    async fn seed_persona(&self, persona: PersonaEntity) -> Result<()> {
        self.upsert_persona(persona)
    }
    async fn seed_user(&self, user: UserEntity) -> Result<()> {
        self.upsert_user(user)
    }
    async fn seed_relationship(&self, relationship: RelationshipEntity) -> Result<()> {
        self.upsert_relationship(relationship)
    }
}

#[cfg(feature = "surreal")]
#[async_trait]
impl IdentitySeed
    for crate::infrastructure::memory::surreal_identity_memory_store::SurrealIdentityMemoryStore
{
    async fn seed_persona(&self, persona: PersonaEntity) -> Result<()> {
        self.upsert_persona(persona).await
    }
    async fn seed_user(&self, user: UserEntity) -> Result<()> {
        self.upsert_user(user).await
    }
    async fn seed_relationship(&self, relationship: RelationshipEntity) -> Result<()> {
        self.upsert_relationship(relationship).await
    }
}

fn relationship(id: &str, status: RelationshipStatus) -> RelationshipEntity {
    RelationshipEntity {
        relationship_id: id.to_string(),
        source_entity_ref: EntityRef {
            entity_type: "PersonaEntity".to_string(),
            entity_id: "p1".to_string(),
        },
        target_entity_ref: EntityRef {
            entity_type: "UserEntity".to_string(),
            entity_id: "u1".to_string(),
        },
        relationship_kind: RelationshipKind::AssistantUser,
        status,
        trust_level: 0.5,
        confidence: 0.8,
        strength_score: 0.8,
        recency_score: 0.8,
        version: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        provenance: UpdateSource::UserDirect,
        ..Default::default()
    }
}

fn persona() -> PersonaEntity {
    PersonaEntity {
        persona_id: "p1".to_string(),
        display_name: "Medousa".to_string(),
        status: "active".to_string(),
        version: 1,
        updated_at: Utc::now(),
    }
}

fn user() -> UserEntity {
    UserEntity {
        user_id: "u1".to_string(),
        timezone: "UTC".to_string(),
        status: "active".to_string(),
        version: 1,
        updated_at: Utc::now(),
        ..Default::default()
    }
}

/// Split proposals, commit guards, and replacement continuity.
pub(crate) async fn run_identity_contract(store: &impl IdentitySeed) {
    store.seed_persona(persona()).await.expect("persona");
    store.seed_user(user()).await.expect("user");
    store
        .seed_relationship(relationship("rel-1", RelationshipStatus::Active))
        .await
        .expect("relationship");

    let proposal = store
        .propose_entity_update(&ProposeEntityUpdateRequest {
            entity_type: IdentityEntityType::RelationshipEntity,
            entity_id: "rel-1".to_string(),
            patch: json!({
                "recency_score": 0.92,
                "autonomy_scope.allow": ["external_posting"]
            }),
            source: UpdateSource::ModelInferred,
            confidence: 0.91,
            reason: "learned preference".to_string(),
            actor: "model".to_string(),
            receipt_id: None,
            expires_at: None,
        })
        .await
        .expect("split proposal");
    assert!(proposal.split_patch);
    assert_eq!(proposal.proposal_ids.len(), 2);

    let first = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: proposal.proposal_ids[0].clone(),
            expected_version: 1,
            approver: None,
        })
        .await
        .expect("first commit");
    assert!(first.committed);

    let second = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: proposal.proposal_ids[1].clone(),
            expected_version: 2,
            approver: Some("owner".to_string()),
        })
        .await
        .expect("second commit");
    assert!(second.committed);

    let context = store
        .get_identity_context(&GetIdentityContextRequest {
            user_id: "u1".to_string(),
            persona_id: "p1".to_string(),
            channel_id: "none".to_string(),
            relationship_limit: 10,
            ..Default::default()
        })
        .await
        .expect("context");
    assert_eq!(context.relationships.len(), 1);
    assert_eq!(context.relationships[0].version, 3);
    assert!(context.user.is_some());
    assert!(context.persona.is_some());

    let stale_proposal = store
        .propose_entity_update(&ProposeEntityUpdateRequest {
            entity_type: IdentityEntityType::RelationshipEntity,
            entity_id: "rel-1".to_string(),
            patch: json!({"status": "suspended"}),
            source: UpdateSource::UserDirect,
            confidence: 1.0,
            reason: "pause".to_string(),
            actor: "owner".to_string(),
            receipt_id: None,
            expires_at: None,
        })
        .await
        .expect("stale proposal");
    let stale = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: stale_proposal.proposal_ids[0].clone(),
            expected_version: 99,
            approver: Some("owner".to_string()),
        })
        .await
        .expect("stale commit");
    assert_eq!(stale.code, Some(CommitOutcomeCode::StaleState));

    let missing = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: "missing".to_string(),
            expected_version: 1,
            approver: None,
        })
        .await
        .expect("missing proposal");
    assert_eq!(missing.code, Some(CommitOutcomeCode::NotFound));

    let needs_approval = store
        .propose_entity_update(&ProposeEntityUpdateRequest {
            entity_type: IdentityEntityType::RelationshipEntity,
            entity_id: "rel-1".to_string(),
            patch: json!({"status": "suspended"}),
            source: UpdateSource::UserDirect,
            confidence: 1.0,
            reason: "pause".to_string(),
            actor: "owner".to_string(),
            receipt_id: None,
            expires_at: None,
        })
        .await
        .expect("approval proposal");
    let denied = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: needs_approval.proposal_ids[0].clone(),
            expected_version: 3,
            approver: None,
        })
        .await
        .expect("approval required");
    assert_eq!(denied.code, Some(CommitOutcomeCode::ApprovalRequired));

    let expired = store
        .propose_entity_update(&ProposeEntityUpdateRequest {
            entity_type: IdentityEntityType::RelationshipEntity,
            entity_id: "rel-1".to_string(),
            patch: json!({"recency_score": 0.2}),
            source: UpdateSource::ModelInferred,
            confidence: 0.4,
            reason: "old".to_string(),
            actor: "model".to_string(),
            receipt_id: None,
            expires_at: Some(Utc::now() - Duration::seconds(5)),
        })
        .await
        .expect("expired proposal");
    let expired_commit = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: expired.proposal_ids[0].clone(),
            expected_version: 3,
            approver: None,
        })
        .await
        .expect("expired commit");
    assert_eq!(
        expired_commit.code,
        Some(CommitOutcomeCode::ExpiredProposal)
    );

    store
        .seed_relationship(relationship("rel-revoked", RelationshipStatus::Revoked))
        .await
        .expect("revoked relationship");
    let reactivate = store
        .propose_entity_update(&ProposeEntityUpdateRequest {
            entity_type: IdentityEntityType::RelationshipEntity,
            entity_id: "rel-revoked".to_string(),
            patch: json!({"status": "active"}),
            source: UpdateSource::UserDirect,
            confidence: 1.0,
            reason: "reactivate".to_string(),
            actor: "owner".to_string(),
            receipt_id: None,
            expires_at: None,
        })
        .await
        .expect("reactivate proposal");
    let policy = store
        .commit_entity_update(&CommitEntityUpdateRequest {
            proposal_id: reactivate.proposal_ids[0].clone(),
            expected_version: 1,
            approver: Some("owner".to_string()),
        })
        .await
        .expect("policy decision");
    assert_eq!(policy.code, Some(CommitOutcomeCode::PolicyDenied));

    let err = store
        .seed_relationship(relationship("rel-next", RelationshipStatus::Active))
        .await
        .expect_err("replacement without predecessor");
    assert!(err.to_string().contains("derived_from_relationship_id"));

    let mut replacement = relationship("rel-next", RelationshipStatus::Active);
    replacement.derived_from_relationship_id = Some("rel-revoked".to_string());
    store
        .seed_relationship(replacement)
        .await
        .expect("replacement");
}

#[tokio::test]
async fn in_memory_identity_contract() {
    run_identity_contract(&InMemoryIdentityMemoryStore::default()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_identity_contract_and_reopen() {
    use std::sync::Arc;

    use crate::infrastructure::sqlite::identity_memory_store::SqliteIdentityMemoryStore;
    use crate::infrastructure::sqlite::{RusqliteExecutor, SqliteSession};

    let session = SqliteSession::open(":memory:").expect("sqlite");
    run_identity_contract(&SqliteIdentityMemoryStore::from_session(session)).await;

    let path = std::env::temp_dir().join(format!("stasis-identity-{}.sqlite", std::process::id()));
    let path = path.to_string_lossy().into_owned();
    {
        let store =
            SqliteIdentityMemoryStore::from_session(SqliteSession::open(&path).expect("create"));
        store.seed_user(user()).await.expect("write user");
        store.seed_persona(persona()).await.expect("write persona");
    }
    let reopened =
        SqliteIdentityMemoryStore::from_session(SqliteSession::open(&path).expect("reopen"));
    let context = reopened
        .get_identity_context(&GetIdentityContextRequest {
            user_id: "u1".to_string(),
            persona_id: "p1".to_string(),
            channel_id: "none".to_string(),
            relationship_limit: 4,
            ..Default::default()
        })
        .await
        .expect("recall");
    assert_eq!(
        context.user.as_ref().map(|row| row.user_id.as_str()),
        Some("u1")
    );
    assert_eq!(
        context
            .persona
            .as_ref()
            .map(|row| row.display_name.as_str()),
        Some("Medousa")
    );
    let _ = std::fs::remove_file(&path);

    let executor = Arc::new(RusqliteExecutor::open_in_memory().expect("executor"));
    let store = SqliteIdentityMemoryStore::new(executor.clone());
    store.ensure_schema().expect("schema");
    run_identity_contract(&store).await;
    drop(store);
    let reopened = SqliteIdentityMemoryStore::new(executor);
    let context = reopened
        .get_identity_context(&GetIdentityContextRequest {
            user_id: "u1".to_string(),
            persona_id: "p1".to_string(),
            channel_id: "none".to_string(),
            relationship_limit: 8,
            ..Default::default()
        })
        .await
        .expect("executor recall");
    assert_eq!(
        context.user.as_ref().map(|row| row.user_id.as_str()),
        Some("u1")
    );
    assert!(
        context.relationships.iter().any(
            |relationship| relationship.relationship_id == "rel-1" && relationship.version == 3
        )
    );
}

#[cfg(feature = "surreal")]
#[tokio::test]
async fn surreal_identity_contract() {
    use surrealdb::Surreal;
    use surrealdb::engine::any::Any;

    use crate::infrastructure::memory::surreal_identity_memory_store::SurrealIdentityMemoryStore;

    let db = Surreal::<Any>::init();
    db.connect("mem://").await.expect("connect");
    db.use_ns("test")
        .use_db("identity_contract")
        .await
        .expect("ns");
    SurrealIdentityMemoryStore::ensure_schema_for_db(&db)
        .await
        .expect("schema");
    run_identity_contract(&SurrealIdentityMemoryStore::new(db)).await;
}
