use std::sync::Arc;

use crate::domain::errors::Result;
use crate::ports::outbound::memory::identity_memory_models::{
    CommitEntityUpdateRequest, CommitEntityUpdateResponse, GetIdentityContextRequest,
    GetIdentityContextResponse, ListEntityHistoryRequest, ListEntityHistoryResponse,
    ProposeEntityUpdateRequest, ProposeEntityUpdateResponse, RollbackEntityVersionRequest,
    RollbackEntityVersionResponse, UpdateTier,
};
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

#[derive(Clone)]
pub struct IdentityMemoryService {
    store: Arc<dyn IdentityMemoryStore>,
}

#[derive(Clone, Debug, Default)]
pub struct ProposeAndCommitRequest {
    pub proposal_request: ProposeEntityUpdateRequest,
    pub expected_version: i32,
    pub approver: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ProposeAndCommitResponse {
    pub proposed: ProposeEntityUpdateResponse,
    pub commits: Vec<CommitEntityUpdateResponse>,
}

impl IdentityMemoryService {
    pub fn new(store: Arc<dyn IdentityMemoryStore>) -> Self {
        Self { store }
    }

    pub async fn get_identity_context(
        &self,
        request: &GetIdentityContextRequest,
    ) -> Result<GetIdentityContextResponse> {
        self.store.get_identity_context(request).await
    }

    pub async fn propose_entity_update(
        &self,
        request: &ProposeEntityUpdateRequest,
    ) -> Result<ProposeEntityUpdateResponse> {
        self.store.propose_entity_update(request).await
    }

    pub async fn commit_entity_update(
        &self,
        request: &CommitEntityUpdateRequest,
    ) -> Result<CommitEntityUpdateResponse> {
        self.store.commit_entity_update(request).await
    }

    pub async fn list_entity_history(
        &self,
        request: &ListEntityHistoryRequest,
    ) -> Result<ListEntityHistoryResponse> {
        self.store.list_entity_history(request).await
    }

    pub async fn rollback_entity_version(
        &self,
        request: &RollbackEntityVersionRequest,
    ) -> Result<RollbackEntityVersionResponse> {
        self.store.rollback_entity_version(request).await
    }

    pub async fn propose_and_commit_autocommit(
        &self,
        request: &ProposeAndCommitRequest,
    ) -> Result<ProposeAndCommitResponse> {
        let proposed = self
            .store
            .propose_entity_update(&request.proposal_request)
            .await?;

        let mut expected_version = request.expected_version;
        let mut commits = Vec::new();

        for (idx, proposal_id) in proposed.proposal_ids.iter().enumerate() {
            let tier = proposed
                .tiers
                .get(idx)
                .copied()
                .unwrap_or(UpdateTier::ApprovalRequired);
            if !matches!(tier, UpdateTier::AutoCommit) {
                continue;
            }

            let commit = self
                .store
                .commit_entity_update(&CommitEntityUpdateRequest {
                    proposal_id: proposal_id.clone(),
                    expected_version,
                    approver: request.approver.clone(),
                })
                .await?;

            if commit.committed {
                if let Some(new_version) = commit.new_version {
                    expected_version = new_version;
                }
            }

            commits.push(commit);
        }

        Ok(ProposeAndCommitResponse { proposed, commits })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use serde_json::json;

    use super::{IdentityMemoryService, ProposeAndCommitRequest};
    use crate::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;
    use crate::ports::outbound::memory::identity_memory_models::{
        EntityRef, IdentityEntityType, ProposeEntityUpdateRequest, RelationshipEntity,
        RelationshipStatus, UpdateSource,
    };

    #[tokio::test]
    async fn service_autocommits_only_auto_tier_proposals() {
        let store = Arc::new(InMemoryIdentityMemoryStore::default());
        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-1".to_string(),
                source_entity_ref: EntityRef {
                    entity_type: "PersonaEntity".to_string(),
                    entity_id: "p1".to_string(),
                },
                target_entity_ref: EntityRef {
                    entity_type: "UserEntity".to_string(),
                    entity_id: "u1".to_string(),
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
            .expect("seed relationship should succeed");

        let service = IdentityMemoryService::new(store);

        let result = service
            .propose_and_commit_autocommit(&ProposeAndCommitRequest {
                proposal_request: ProposeEntityUpdateRequest {
                    entity_type: IdentityEntityType::RelationshipEntity,
                    entity_id: "rel-1".to_string(),
                    patch: json!({
                        "recency_score": 0.9,
                        "autonomy_scope.allow": ["external_posting"]
                    }),
                    source: UpdateSource::ModelInferred,
                    confidence: 0.82,
                    reason: "mixed update".to_string(),
                    actor: "model".to_string(),
                    receipt_id: None,
                    expires_at: None,
                },
                expected_version: 1,
                approver: Some("owner".to_string()),
            })
            .await
            .expect("workflow should succeed");

        assert_eq!(result.proposed.proposal_ids.len(), 2);
        assert_eq!(result.commits.len(), 1);
        assert!(result.commits[0].committed);
    }
}
