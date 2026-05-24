use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Map, Value};

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::identity_memory_store_shared::{
    compute_graph_depth_with_cap, render_sttp_bridge_node,
};
use crate::ports::outbound::memory::identity_memory_models::{
    ChannelProfileEntity, CommitEntityUpdateRequest, CommitEntityUpdateResponse, CommitOutcomeCode,
    EntityUpdateProposalRecord, GetIdentityContextRequest, GetIdentityContextResponse,
    IdentityEntityType, ListEntityHistoryRequest, ListEntityHistoryResponse, PersonaEntity,
    PolicyProfileEntity, ProposalState, ProposeEntityUpdateRequest, ProposeEntityUpdateResponse,
    RelationshipEntity, RelationshipStatus, RelationshipTransitionEvent,
    RollbackEntityVersionRequest, RollbackEntityVersionResponse, UpdateTier,
};
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

const DEFAULT_GRAPH_MAX_DEPTH: usize = 2;
const DEFAULT_TRUST_DELTA_MAX_PER_WINDOW: f32 = 0.03;
static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_id(prefix: &str) -> String {
    let n = ID_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    format!("{prefix}_{}_{n}", Utc::now().timestamp_micros())
}

#[derive(Clone, Default)]
pub struct InMemoryIdentityMemoryStore {
    personas: Arc<RwLock<HashMap<String, PersonaEntity>>>,
    users: Arc<RwLock<HashMap<String, crate::ports::outbound::memory::identity_memory_models::UserEntity>>>,
    channels: Arc<RwLock<HashMap<String, ChannelProfileEntity>>>,
    policies: Arc<RwLock<HashMap<String, PolicyProfileEntity>>>,
    relationships: Arc<RwLock<HashMap<String, RelationshipEntity>>>,
    relationship_versions: Arc<RwLock<HashMap<String, BTreeMap<i32, RelationshipEntity>>>>,
    proposals: Arc<RwLock<HashMap<String, EntityUpdateProposalRecord>>>,
    transitions: Arc<RwLock<Vec<RelationshipTransitionEvent>>>,
}

impl InMemoryIdentityMemoryStore {
    pub fn upsert_persona(&self, persona: PersonaEntity) -> Result<()> {
        let mut state = self
            .personas
            .write()
            .map_err(|_| StasisError::PortFailure("persona store lock poisoned".to_string()))?;
        state.insert(persona.persona_id.clone(), persona);
        Ok(())
    }

    pub fn upsert_user(
        &self,
        user: crate::ports::outbound::memory::identity_memory_models::UserEntity,
    ) -> Result<()> {
        let mut state = self
            .users
            .write()
            .map_err(|_| StasisError::PortFailure("user store lock poisoned".to_string()))?;
        state.insert(user.user_id.clone(), user);
        Ok(())
    }

    pub fn upsert_channel(&self, channel: ChannelProfileEntity) -> Result<()> {
        let mut state = self
            .channels
            .write()
            .map_err(|_| StasisError::PortFailure("channel store lock poisoned".to_string()))?;
        state.insert(channel.channel_id.clone(), channel);
        Ok(())
    }

    pub fn upsert_policy(&self, policy: PolicyProfileEntity) -> Result<()> {
        let mut state = self
            .policies
            .write()
            .map_err(|_| StasisError::PortFailure("policy store lock poisoned".to_string()))?;
        state.insert(policy.policy_profile_id.clone(), policy);
        Ok(())
    }

    pub fn upsert_relationship(&self, relationship: RelationshipEntity) -> Result<()> {
        self.validate_replacement_continuity(&relationship)?;

        let mut rels = self.relationships.write().map_err(|_| {
            StasisError::PortFailure("relationship store lock poisoned".to_string())
        })?;
        let mut versions = self.relationship_versions.write().map_err(|_| {
            StasisError::PortFailure("relationship version store lock poisoned".to_string())
        })?;

        let id = relationship.relationship_id.clone();
        let version = relationship.version;
        rels.insert(id.clone(), relationship.clone());
        versions.entry(id).or_default().insert(version, relationship);
        Ok(())
    }

    fn validate_replacement_continuity(&self, relationship: &RelationshipEntity) -> Result<()> {
        let rels = self.relationships.read().map_err(|_| {
            StasisError::PortFailure("relationship store lock poisoned".to_string())
        })?;

        // Updates to an existing relationship id are validated in commit flow.
        if rels.contains_key(&relationship.relationship_id) {
            return Ok(());
        }

        let revoked_predecessors = rels
            .values()
            .filter(|candidate| {
                candidate.status == RelationshipStatus::Revoked
                    && candidate.source_entity_ref == relationship.source_entity_ref
                    && candidate.target_entity_ref == relationship.target_entity_ref
                    && candidate.relationship_kind == relationship.relationship_kind
            })
            .map(|candidate| candidate.relationship_id.clone())
            .collect::<Vec<_>>();

        if revoked_predecessors.is_empty() {
            return Ok(());
        }

        let Some(derived_from) = relationship.derived_from_relationship_id.as_ref() else {
            return Err(StasisError::PortFailure(
                "policy denied: replacement relationship requires derived_from_relationship_id"
                    .to_string(),
            ));
        };

        if !revoked_predecessors.contains(derived_from) {
            return Err(StasisError::PortFailure(
                "policy denied: derived_from_relationship_id must reference a revoked predecessor with matching endpoints and kind".to_string(),
            ));
        }

        Ok(())
    }

    fn classify_field_path(path: &str) -> UpdateTier {
        match path {
            // approval-required
            "status" | "trust_level" | "autonomy_scope.allow" | "autonomy_scope.deny"
            | "autonomy_scope.approval_required" | "approval_profile_id"
            | "escalation_policy.mode" | "parent_relationship_id"
            | "governing_relationship_ids" | "source_entity_ref.entity_type"
            | "source_entity_ref.entity_id" | "target_entity_ref.entity_type"
            | "target_entity_ref.entity_id" => UpdateTier::ApprovalRequired,

            // confirm-required
            "interruption_policy.quiet_hours" | "interruption_policy.allow_urgent_only"
            | "escalation_policy.fallback" | "policy_tags" | "relationship_kind" => {
                UpdateTier::ConfirmRequired
            }

            _ => UpdateTier::AutoCommit,
        }
    }

    fn flatten_patch(prefix: &str, value: &Value, out: &mut Vec<(String, Value)>) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    let next = if prefix.is_empty() {
                        key.to_string()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    Self::flatten_patch(&next, child, out);
                }
            }
            _ => out.push((prefix.to_string(), value.clone())),
        }
    }

    fn split_patch_by_tier(patch: &Value) -> Result<Vec<(UpdateTier, Value)>> {
        let mut flattened = Vec::new();
        Self::flatten_patch("", patch, &mut flattened);
        if flattened.is_empty() {
            return Err(StasisError::PortFailure(
                "identity patch must include at least one field".to_string(),
            ));
        }

        let mut grouped: HashMap<UpdateTier, Map<String, Value>> = HashMap::new();
        for (path, value) in flattened {
            let tier = Self::classify_field_path(&path);
            grouped.entry(tier).or_default().insert(path, value);
        }

        let mut split = grouped
            .into_iter()
            .map(|(tier, map)| (tier, Value::Object(map)))
            .collect::<Vec<_>>();

        split.sort_by(|a, b| {
            let rank = |tier: UpdateTier| match tier {
                UpdateTier::AutoCommit => 0,
                UpdateTier::ConfirmRequired => 1,
                UpdateTier::ApprovalRequired => 2,
            };
            rank(a.0).cmp(&rank(b.0))
        });

        Ok(split)
    }

    fn patch_requires_approval(tier: UpdateTier) -> bool {
        matches!(tier, UpdateTier::ApprovalRequired)
    }

    fn material_bridge_reason(patch: &Value, from: Option<RelationshipStatus>, to: Option<RelationshipStatus>) -> Option<String> {
        if from != to {
            return Some("relationship_status_transition".to_string());
        }

        let map = patch.as_object()?;

        let has_material_patch = map.keys().any(|key| {
            matches!(
                key.as_str(),
                "trust_level"
                    | "autonomy_scope.allow"
                    | "autonomy_scope.deny"
                    | "autonomy_scope.approval_required"
                    | "approval_profile_id"
                    | "escalation_policy.mode"
                    | "escalation_policy.fallback"
                    | "interruption_policy.quiet_hours"
                    | "interruption_policy.allow_urgent_only"
                    | "interruption_policy.urgent_threshold"
                    | "policy_tags"
            )
        });

        if has_material_patch {
            Some("relationship_policy_or_trust_update".to_string())
        } else {
            None
        }
    }

    fn trust_delta_max_for_relationship(&self, relationship: &RelationshipEntity) -> f32 {
        if let Some(profile_id) = relationship.approval_profile_id.as_ref()
            && let Ok(policies) = self.policies.read()
            && let Some(profile) = policies.get(profile_id)
            && profile.trust_delta_max_per_window.is_finite()
            && profile.trust_delta_max_per_window > 0.0
        {
            return profile.trust_delta_max_per_window;
        }
        DEFAULT_TRUST_DELTA_MAX_PER_WINDOW
    }

    fn apply_relationship_patch(
        &self,
        relationship: &mut RelationshipEntity,
        patch: &Value,
        actor: &str,
    ) -> Result<(Option<RelationshipStatus>, Option<RelationshipStatus>)> {
        let map = patch.as_object().ok_or_else(|| {
            StasisError::PortFailure("identity patch must be an object".to_string())
        })?;

        let previous_status = Some(relationship.status);
        let mut next_status = previous_status;

        for (path, value) in map {
            match path.as_str() {
                "status" => {
                    let status_raw = value.as_str().ok_or_else(|| {
                        StasisError::PortFailure("status must be a string".to_string())
                    })?;
                    let parsed = match status_raw {
                        "proposed" => RelationshipStatus::Proposed,
                        "active" => RelationshipStatus::Active,
                        "suspended" => RelationshipStatus::Suspended,
                        "deprecated" => RelationshipStatus::Deprecated,
                        "revoked" => RelationshipStatus::Revoked,
                        other => {
                            return Err(StasisError::PortFailure(format!(
                                "invalid relationship status: {other}"
                            )));
                        }
                    };

                    if relationship.status == RelationshipStatus::Revoked
                        && parsed == RelationshipStatus::Active
                    {
                        return Err(StasisError::PortFailure(
                            "policy denied: revoked relationship cannot be reactivated with same relationship_id"
                                .to_string(),
                        ));
                    }

                    relationship.status = parsed;
                    next_status = Some(parsed);
                }
                "trust_level" => {
                    let proposed = value.as_f64().ok_or_else(|| {
                        StasisError::PortFailure("trust_level must be numeric".to_string())
                    })? as f32;
                    let delta_max = self.trust_delta_max_for_relationship(relationship);
                    let clamped = proposed.min(relationship.trust_level + delta_max);
                    relationship.trust_level = clamped.max(0.0);
                }
                "confidence" => {
                    let proposed = value.as_f64().ok_or_else(|| {
                        StasisError::PortFailure("confidence must be numeric".to_string())
                    })? as f32;
                    relationship.confidence = proposed.clamp(0.0, 1.0);
                }
                "strength_score" => {
                    let proposed = value.as_f64().ok_or_else(|| {
                        StasisError::PortFailure("strength_score must be numeric".to_string())
                    })? as f32;
                    relationship.strength_score = proposed.clamp(0.0, 1.0);
                }
                "recency_score" => {
                    let proposed = value.as_f64().ok_or_else(|| {
                        StasisError::PortFailure("recency_score must be numeric".to_string())
                    })? as f32;
                    relationship.recency_score = proposed.clamp(0.0, 1.0);
                }
                "approval_profile_id" => {
                    relationship.approval_profile_id = value.as_str().map(|v| v.to_string());
                }
                "interruption_policy.quiet_hours" => {
                    relationship.interruption_policy.quiet_hours =
                        value.as_str().map(|v| v.to_string());
                }
                "interruption_policy.allow_urgent_only" => {
                    relationship.interruption_policy.allow_urgent_only = value.as_bool();
                }
                "interruption_policy.urgent_threshold" => {
                    relationship.interruption_policy.urgent_threshold =
                        value.as_f64().map(|v| v as f32);
                }
                "escalation_policy.mode" => {
                    relationship.escalation_policy.mode = value.as_str().map(|v| v.to_string());
                }
                "escalation_policy.fallback" => {
                    relationship.escalation_policy.fallback =
                        value.as_str().map(|v| v.to_string());
                }
                "autonomy_scope.allow" => {
                    relationship.autonomy_scope.allow = parse_string_array(value, "autonomy_scope.allow")?;
                }
                "autonomy_scope.deny" => {
                    relationship.autonomy_scope.deny = parse_string_array(value, "autonomy_scope.deny")?;
                }
                "autonomy_scope.approval_required" => {
                    relationship.autonomy_scope.approval_required =
                        parse_string_array(value, "autonomy_scope.approval_required")?;
                }
                "parent_relationship_id" => {
                    relationship.parent_relationship_id = value.as_str().map(|v| v.to_string());
                }
                "governing_relationship_ids" => {
                    relationship.governing_relationship_ids =
                        parse_string_array(value, "governing_relationship_ids")?;
                }
                "derived_from_relationship_id" => {
                    relationship.derived_from_relationship_id =
                        value.as_str().map(|v| v.to_string());
                }
                "relationship_kind" => {
                    relationship.relationship_kind = value.as_str().ok_or_else(|| {
                        StasisError::PortFailure("relationship_kind must be a string".to_string())
                    })?.to_string();
                }
                "policy_tags" => {
                    relationship.policy_tags = parse_string_array(value, "policy_tags")?;
                }
                "source_entity_ref.entity_type" => {
                    relationship.source_entity_ref.entity_type = value.as_str().ok_or_else(|| {
                        StasisError::PortFailure(
                            "source_entity_ref.entity_type must be a string".to_string(),
                        )
                    })?.to_string();
                }
                "source_entity_ref.entity_id" => {
                    relationship.source_entity_ref.entity_id = value.as_str().ok_or_else(|| {
                        StasisError::PortFailure(
                            "source_entity_ref.entity_id must be a string".to_string(),
                        )
                    })?.to_string();
                }
                "target_entity_ref.entity_type" => {
                    relationship.target_entity_ref.entity_type = value.as_str().ok_or_else(|| {
                        StasisError::PortFailure(
                            "target_entity_ref.entity_type must be a string".to_string(),
                        )
                    })?.to_string();
                }
                "target_entity_ref.entity_id" => {
                    relationship.target_entity_ref.entity_id = value.as_str().ok_or_else(|| {
                        StasisError::PortFailure(
                            "target_entity_ref.entity_id must be a string".to_string(),
                        )
                    })?.to_string();
                }
                other => {
                    return Err(StasisError::PortFailure(format!(
                        "unsupported relationship patch field: {other}"
                    )));
                }
            }
        }

        relationship.updated_at = Utc::now();
        relationship.version += 1;
        relationship.last_transition_reason = Some("patch_applied".to_string());
        relationship.transition_receipt_id = Some(next_id("rcpt"));

        if previous_status != next_status {
            relationship.last_transition_reason = Some("status_transition".to_string());
        }

        let _ = actor;
        Ok((previous_status, next_status))
    }
}

fn parse_string_array(value: &Value, field: &str) -> Result<Vec<String>> {
    let items = value.as_array().ok_or_else(|| {
        StasisError::PortFailure(format!("{field} must be an array of strings"))
    })?;

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = item.as_str() else {
            return Err(StasisError::PortFailure(format!(
                "{field} must contain only strings"
            )));
        };
        out.push(value.to_string());
    }
    Ok(out)
}

#[async_trait]
impl IdentityMemoryStore for InMemoryIdentityMemoryStore {
    async fn get_identity_context(
        &self,
        request: &GetIdentityContextRequest,
    ) -> Result<GetIdentityContextResponse> {
        let personas = self
            .personas
            .read()
            .map_err(|_| StasisError::PortFailure("persona store lock poisoned".to_string()))?;
        let users = self
            .users
            .read()
            .map_err(|_| StasisError::PortFailure("user store lock poisoned".to_string()))?;
        let channels = self
            .channels
            .read()
            .map_err(|_| StasisError::PortFailure("channel store lock poisoned".to_string()))?;
        let rels = self.relationships.read().map_err(|_| {
            StasisError::PortFailure("relationship store lock poisoned".to_string())
        })?;
        let policies = self
            .policies
            .read()
            .map_err(|_| StasisError::PortFailure("policy store lock poisoned".to_string()))?;

        let mut relationships = rels
            .values()
            .filter(|rel| rel.status == RelationshipStatus::Active)
            .filter(|rel| {
                (rel.source_entity_ref.entity_type == "PersonaEntity"
                    && rel.source_entity_ref.entity_id == request.persona_id
                    && rel.target_entity_ref.entity_type == "UserEntity"
                    && rel.target_entity_ref.entity_id == request.user_id)
                    || (rel.source_entity_ref.entity_type == "UserEntity"
                        && rel.source_entity_ref.entity_id == request.user_id
                        && rel.target_entity_ref.entity_type == "ChannelProfileEntity"
                        && rel.target_entity_ref.entity_id == request.channel_id)
                    || rel.source_entity_ref.entity_id == request.persona_id
            })
            .cloned()
            .collect::<Vec<_>>();

        relationships.sort_by(|a, b| {
            let a_score = a.confidence * a.strength_score;
            let b_score = b.confidence * b.strength_score;
            b_score
                .partial_cmp(&a_score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    b.recency_score
                        .partial_cmp(&a.recency_score)
                        .unwrap_or(Ordering::Equal)
                })
        });
        let limit = request.relationship_limit.max(1);
        relationships.truncate(limit);

        let mut policy_profiles = Vec::new();
        for rel in &relationships {
            if let Some(policy_id) = rel.approval_profile_id.as_ref()
                && let Some(profile) = policies.get(policy_id)
                && profile.status == "active"
            {
                policy_profiles.push(profile.clone());
            }
        }

        let (graph_depth_used, flattened_claims) = compute_graph_depth_with_cap(
            &relationships,
            DEFAULT_GRAPH_MAX_DEPTH,
            || next_id("claim"),
        );

        Ok(GetIdentityContextResponse {
            persona: personas.get(&request.persona_id).cloned(),
            user: users.get(&request.user_id).cloned(),
            channel: channels.get(&request.channel_id).cloned(),
            relationships,
            policy_profiles,
            graph_depth_used,
            flattened_claims,
        })
    }

    async fn propose_entity_update(
        &self,
        request: &ProposeEntityUpdateRequest,
    ) -> Result<ProposeEntityUpdateResponse> {
        let split = Self::split_patch_by_tier(&request.patch)?;
        let split_patch = split.len() > 1;

        let mut proposals = self
            .proposals
            .write()
            .map_err(|_| StasisError::PortFailure("proposal store lock poisoned".to_string()))?;

        let now = Utc::now();
        let mut proposal_ids = Vec::with_capacity(split.len());
        let mut tiers = Vec::with_capacity(split.len());
        let mut requires_approval = false;

        for (tier, patch) in split {
            let proposal_id = next_id("prop");
            let record = EntityUpdateProposalRecord {
                proposal_id: proposal_id.clone(),
                entity_type: request.entity_type.clone(),
                entity_id: request.entity_id.clone(),
                patch,
                tier,
                source: request.source,
                confidence: request.confidence,
                reason: request.reason.clone(),
                state: ProposalState::Proposed,
                approver: None,
                actor: request.actor.clone(),
                receipt_id: request.receipt_id.clone(),
                expires_at: request.expires_at,
                created_at: now,
                updated_at: now,
            };
            if Self::patch_requires_approval(tier) {
                requires_approval = true;
            }

            proposal_ids.push(proposal_id.clone());
            tiers.push(tier);
            proposals.insert(proposal_id, record);
        }

        Ok(ProposeEntityUpdateResponse {
            proposal_ids,
            tiers,
            requires_approval,
            split_patch,
            policy_notes: if split_patch {
                vec!["mixed-tier patch split into independent proposals".to_string()]
            } else {
                Vec::new()
            },
        })
    }

    async fn commit_entity_update(
        &self,
        request: &CommitEntityUpdateRequest,
    ) -> Result<CommitEntityUpdateResponse> {
        let mut proposals = self
            .proposals
            .write()
            .map_err(|_| StasisError::PortFailure("proposal store lock poisoned".to_string()))?;

        let Some(proposal) = proposals.get_mut(&request.proposal_id) else {
            return Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::NotFound),
                rationale: Some("proposal not found".to_string()),
                ..Default::default()
            });
        };

        if proposal.state != ProposalState::Proposed {
            return Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::InvalidPatch),
                rationale: Some("proposal is not in proposed state".to_string()),
                ..Default::default()
            });
        }

        if let Some(expires_at) = proposal.expires_at
            && Utc::now() > expires_at
        {
            proposal.state = ProposalState::Expired;
            proposal.updated_at = Utc::now();
            return Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::ExpiredProposal),
                rationale: Some("proposal expired".to_string()),
                ..Default::default()
            });
        }

        if Self::patch_requires_approval(proposal.tier) && request.approver.is_none() {
            return Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::ApprovalRequired),
                rationale: Some("approval required for this proposal tier".to_string()),
                ..Default::default()
            });
        }

        match proposal.entity_type {
            IdentityEntityType::RelationshipEntity => {
                let mut relationships = self.relationships.write().map_err(|_| {
                    StasisError::PortFailure("relationship store lock poisoned".to_string())
                })?;
                let mut versions = self.relationship_versions.write().map_err(|_| {
                    StasisError::PortFailure(
                        "relationship version store lock poisoned".to_string(),
                    )
                })?;

                let Some(relationship) = relationships.get_mut(&proposal.entity_id) else {
                    return Ok(CommitEntityUpdateResponse {
                        committed: false,
                        code: Some(CommitOutcomeCode::NotFound),
                        rationale: Some("target relationship not found".to_string()),
                        ..Default::default()
                    });
                };

                if relationship.version != request.expected_version {
                    proposal.state = ProposalState::Rejected;
                    proposal.updated_at = Utc::now();
                    return Ok(CommitEntityUpdateResponse {
                        committed: false,
                        code: Some(CommitOutcomeCode::StaleState),
                        entity_type: Some(IdentityEntityType::RelationshipEntity),
                        entity_id: Some(relationship.relationship_id.clone()),
                        rationale: Some(format!(
                            "stale_state expected_version={} current_version={}",
                            request.expected_version, relationship.version
                        )),
                        ..Default::default()
                    });
                }

                let (from_status, to_status) =
                    match self.apply_relationship_patch(relationship, &proposal.patch, &proposal.actor)
                    {
                        Ok(value) => value,
                        Err(err) => {
                            proposal.state = ProposalState::Rejected;
                            proposal.updated_at = Utc::now();
                            return Ok(CommitEntityUpdateResponse {
                                committed: false,
                                code: Some(CommitOutcomeCode::PolicyDenied),
                                entity_type: Some(IdentityEntityType::RelationshipEntity),
                                entity_id: Some(relationship.relationship_id.clone()),
                                rationale: Some(err.to_string()),
                                ..Default::default()
                            });
                        }
                    };

                let new_version = relationship.version;
                let relationship_id = relationship.relationship_id.clone();
                let transition_receipt_id = relationship.transition_receipt_id.clone();

                versions
                    .entry(relationship_id.clone())
                    .or_default()
                    .insert(new_version, relationship.clone());

                let mut transition_event_id = None;
                if from_status != to_status {
                    let event_id = next_id("rel_evt");
                    let event = RelationshipTransitionEvent {
                        event_id: event_id.clone(),
                        relationship_id: relationship_id.clone(),
                        from_status,
                        to_status: to_status.unwrap_or(RelationshipStatus::Active),
                        reason: proposal.reason.clone(),
                        actor: proposal.actor.clone(),
                        receipt_id: transition_receipt_id.clone(),
                        occurred_at: Utc::now(),
                        metadata: None,
                    };
                    let mut transitions = self.transitions.write().map_err(|_| {
                        StasisError::PortFailure("transition store lock poisoned".to_string())
                    })?;
                    transitions.push(event);
                    transition_event_id = Some(event_id);
                }

                proposal.state = ProposalState::Committed;
                proposal.approver = request.approver.clone();
                proposal.updated_at = Utc::now();

                let bridge_reason =
                    Self::material_bridge_reason(&proposal.patch, from_status, to_status);
                let sttp_bridge_node = bridge_reason.as_ref().map(|reason| {
                    render_sttp_bridge_node(
                        &relationship.relationship_id,
                        &proposal.actor,
                        &proposal.patch,
                        reason,
                        from_status,
                        to_status,
                    )
                });

                Ok(CommitEntityUpdateResponse {
                    committed: true,
                    code: Some(CommitOutcomeCode::Ok),
                    entity_type: Some(IdentityEntityType::RelationshipEntity),
                    entity_id: Some(relationship_id),
                    new_version: Some(new_version),
                    receipt_id: transition_receipt_id,
                    transition_event_id,
                    sttp_bridge_node,
                    sttp_bridge_reason: bridge_reason,
                    rationale: None,
                })
            }
            _ => Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::InvalidPatch),
                entity_type: Some(proposal.entity_type.clone()),
                entity_id: Some(proposal.entity_id.clone()),
                rationale: Some("commit path for this entity type is not implemented yet".to_string()),
                ..Default::default()
            }),
        }
    }

    async fn list_entity_history(
        &self,
        request: &ListEntityHistoryRequest,
    ) -> Result<ListEntityHistoryResponse> {
        let proposals = self
            .proposals
            .read()
            .map_err(|_| StasisError::PortFailure("proposal store lock poisoned".to_string()))?
            .values()
            .filter(|proposal| {
                proposal.entity_type == request.entity_type && proposal.entity_id == request.entity_id
            })
            .take(request.limit.max(1))
            .cloned()
            .collect::<Vec<_>>();

        let transitions = if request.entity_type == IdentityEntityType::RelationshipEntity {
            self.transitions
                .read()
                .map_err(|_| {
                    StasisError::PortFailure("transition store lock poisoned".to_string())
                })?
                .iter()
                .filter(|event| event.relationship_id == request.entity_id)
                .take(request.limit.max(1))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        Ok(ListEntityHistoryResponse {
            proposals,
            transitions,
        })
    }

    async fn rollback_entity_version(
        &self,
        request: &RollbackEntityVersionRequest,
    ) -> Result<RollbackEntityVersionResponse> {
        match request.entity_type {
            IdentityEntityType::RelationshipEntity => {
                let mut relationships = self.relationships.write().map_err(|_| {
                    StasisError::PortFailure("relationship store lock poisoned".to_string())
                })?;
                let versions = self.relationship_versions.read().map_err(|_| {
                    StasisError::PortFailure(
                        "relationship version store lock poisoned".to_string(),
                    )
                })?;

                let Some(version_map) = versions.get(&request.entity_id) else {
                    return Ok(RollbackEntityVersionResponse {
                        rolled_back: false,
                        rationale: Some("relationship history not found".to_string()),
                        ..Default::default()
                    });
                };

                let Some(target) = version_map.get(&request.target_version) else {
                    return Ok(RollbackEntityVersionResponse {
                        rolled_back: false,
                        rationale: Some("target version not found".to_string()),
                        ..Default::default()
                    });
                };

                let mut restored = target.clone();
                restored.version += 1;
                restored.updated_at = Utc::now();
                restored.last_transition_reason = Some(format!("rollback: {}", request.reason));
                restored.transition_receipt_id = Some(next_id("rcpt_rollback"));

                relationships.insert(request.entity_id.clone(), restored.clone());

                Ok(RollbackEntityVersionResponse {
                    rolled_back: true,
                    new_version: Some(restored.version),
                    rollback_receipt_id: restored.transition_receipt_id,
                    rationale: None,
                })
            }
            _ => Ok(RollbackEntityVersionResponse {
                rolled_back: false,
                rationale: Some("rollback for this entity type is not implemented yet".to_string()),
                ..Default::default()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::InMemoryIdentityMemoryStore;
    use crate::ports::outbound::memory::identity_memory_models::{
        CommitEntityUpdateRequest, EntityRef, GetIdentityContextRequest, IdentityEntityType,
        PersonaEntity, ProposeEntityUpdateRequest, RelationshipEntity, RelationshipStatus,
        UpdateSource, UserEntity,
    };
    use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

    #[tokio::test]
    async fn mixed_tier_patch_is_split_into_multiple_proposals() {
        let store = InMemoryIdentityMemoryStore::default();

        let response = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-1".to_string(),
                patch: json!({
                    "recency_score": 0.75,
                    "autonomy_scope.allow": ["external_posting"]
                }),
                source: UpdateSource::ModelInferred,
                confidence: 0.81,
                reason: "test split".to_string(),
                actor: "test".to_string(),
                receipt_id: None,
                expires_at: None,
            })
            .await
            .expect("proposal should succeed");

        assert!(response.split_patch);
        assert_eq!(response.proposal_ids.len(), 2);
    }

    #[tokio::test]
    async fn trust_level_commit_applies_delta_clamp() {
        let store = InMemoryIdentityMemoryStore::default();

        store
            .upsert_persona(PersonaEntity {
                persona_id: "p1".to_string(),
                display_name: "Medousa".to_string(),
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            })
            .expect("persona upsert should work");

        store
            .upsert_user(UserEntity {
                user_id: "u1".to_string(),
                timezone: "UTC".to_string(),
                language_variant: None,
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            })
            .expect("user upsert should work");

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-1".to_string(),
                source_entity_ref: crate::ports::outbound::memory::identity_memory_models::EntityRef {
                    entity_type: "PersonaEntity".to_string(),
                    entity_id: "p1".to_string(),
                },
                target_entity_ref: crate::ports::outbound::memory::identity_memory_models::EntityRef {
                    entity_type: "UserEntity".to_string(),
                    entity_id: "u1".to_string(),
                },
                relationship_kind: "assistant_user".to_string(),
                status: RelationshipStatus::Active,
                trust_level: 0.50,
                confidence: 0.80,
                strength_score: 0.80,
                recency_score: 0.80,
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
            .expect("relationship upsert should work");

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-1".to_string(),
                patch: json!({
                    "trust_level": 0.99
                }),
                source: UpdateSource::UserDirect,
                confidence: 1.0,
                reason: "test clamp".to_string(),
                actor: "user".to_string(),
                receipt_id: None,
                expires_at: None,
            })
            .await
            .expect("proposal should succeed");

        let commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposed.proposal_ids[0].clone(),
                expected_version: 1,
                approver: Some("owner".to_string()),
            })
            .await
            .expect("commit should succeed");

        assert!(commit.committed);

        let context = store
            .get_identity_context(&GetIdentityContextRequest {
                user_id: "u1".to_string(),
                persona_id: "p1".to_string(),
                channel_id: "none".to_string(),
                relationship_limit: 10,
            })
            .await
            .expect("context read should succeed");

        let rel = context
            .relationships
            .iter()
            .find(|value| value.relationship_id == "rel-1")
            .expect("relationship should exist");

        assert!(rel.trust_level <= 0.53);
    }

    #[tokio::test]
    async fn commit_returns_stale_state_on_version_conflict() {
        let store = InMemoryIdentityMemoryStore::default();

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-stale".to_string(),
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
                trust_level: 0.5,
                confidence: 0.8,
                strength_score: 0.8,
                recency_score: 0.8,
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
                version: 2,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .expect("seed relationship should work");

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-stale".to_string(),
                patch: json!({ "recency_score": 0.9 }),
                source: UpdateSource::ModelInferred,
                confidence: 0.8,
                reason: "stale test".to_string(),
                actor: "model".to_string(),
                receipt_id: None,
                expires_at: None,
            })
            .await
            .expect("proposal should work");

        let commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposed.proposal_ids[0].clone(),
                expected_version: 1,
                approver: None,
            })
            .await
            .expect("commit call should succeed");

        assert!(!commit.committed);
        assert_eq!(
            commit.code,
            Some(crate::ports::outbound::memory::identity_memory_models::CommitOutcomeCode::StaleState)
        );
    }

    #[tokio::test]
    async fn privileged_fields_require_approval() {
        let store = InMemoryIdentityMemoryStore::default();

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-approval".to_string(),
                patch: json!({ "autonomy_scope.allow": ["external_posting"] }),
                source: UpdateSource::ModelInferred,
                confidence: 0.7,
                reason: "privileged".to_string(),
                actor: "model".to_string(),
                receipt_id: None,
                expires_at: None,
            })
            .await
            .expect("proposal should work");

        assert!(proposed.requires_approval);
    }

    #[tokio::test]
    async fn revoked_relationship_cannot_be_reactivated_with_same_id() {
        let store = InMemoryIdentityMemoryStore::default();

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-revoked".to_string(),
                source_entity_ref: EntityRef {
                    entity_type: "PersonaEntity".to_string(),
                    entity_id: "p1".to_string(),
                },
                target_entity_ref: EntityRef {
                    entity_type: "UserEntity".to_string(),
                    entity_id: "u1".to_string(),
                },
                relationship_kind: "assistant_user".to_string(),
                status: RelationshipStatus::Revoked,
                trust_level: 0.1,
                confidence: 0.8,
                strength_score: 0.8,
                recency_score: 0.8,
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
            .expect("seed relationship should work");

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-revoked".to_string(),
                patch: json!({ "status": "active" }),
                source: UpdateSource::UserDirect,
                confidence: 1.0,
                reason: "attempt reactivate".to_string(),
                actor: "user".to_string(),
                receipt_id: None,
                expires_at: None,
            })
            .await
            .expect("proposal should work");

        let commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposed.proposal_ids[0].clone(),
                expected_version: 1,
                approver: Some("owner".to_string()),
            })
            .await
            .expect("commit call should succeed");

        assert!(!commit.committed);
        assert_eq!(
            commit.code,
            Some(crate::ports::outbound::memory::identity_memory_models::CommitOutcomeCode::PolicyDenied)
        );
    }

    #[tokio::test]
    async fn graph_depth_cap_produces_flattened_claims() {
        let store = InMemoryIdentityMemoryStore::default();

        for (id, governing) in [
            ("rel-a", vec!["rel-b"]),
            ("rel-b", vec!["rel-c"]),
            ("rel-c", vec!["rel-d"]),
            ("rel-d", vec![]),
        ] {
            store
                .upsert_relationship(RelationshipEntity {
                    relationship_id: id.to_string(),
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
                    trust_level: 0.5,
                    confidence: 0.8,
                    strength_score: 0.8,
                    recency_score: 0.8,
                    autonomy_scope: Default::default(),
                    approval_profile_id: None,
                    interruption_policy: Default::default(),
                    escalation_policy: Default::default(),
                    policy_tags: vec![],
                    provenance: UpdateSource::UserDirect,
                    parent_relationship_id: None,
                    governing_relationship_ids: governing.into_iter().map(|v| v.to_string()).collect(),
                    derived_from_relationship_id: None,
                    last_transition_reason: None,
                    transition_receipt_id: None,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                })
                .expect("relationship seed should work");
        }

        let context = store
            .get_identity_context(&GetIdentityContextRequest {
                user_id: "u1".to_string(),
                persona_id: "p1".to_string(),
                channel_id: "none".to_string(),
                relationship_limit: 10,
            })
            .await
            .expect("context should load");

        assert_eq!(context.graph_depth_used, 2);
        assert!(!context.flattened_claims.is_empty());
    }

    #[tokio::test]
    async fn material_transition_emits_sttp_bridge_node() {
        let store = InMemoryIdentityMemoryStore::default();

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-bridge".to_string(),
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
                trust_level: 0.5,
                confidence: 0.8,
                strength_score: 0.8,
                recency_score: 0.8,
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
            .expect("seed relationship should work");

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-bridge".to_string(),
                patch: json!({ "status": "suspended" }),
                source: UpdateSource::UserDirect,
                confidence: 1.0,
                reason: "material transition".to_string(),
                actor: "owner".to_string(),
                receipt_id: None,
                expires_at: None,
            })
            .await
            .expect("proposal should work");

        let commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposed.proposal_ids[0].clone(),
                expected_version: 1,
                approver: Some("owner".to_string()),
            })
            .await
            .expect("commit should work");

        assert!(commit.committed);
        assert!(commit.sttp_bridge_node.is_some());
        assert_eq!(
            commit.sttp_bridge_reason,
            Some("relationship_status_transition".to_string())
        );
    }

    #[tokio::test]
    async fn replacement_relationship_requires_derived_from_when_revoked_predecessor_exists() {
        let store = InMemoryIdentityMemoryStore::default();

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-old".to_string(),
                source_entity_ref: EntityRef {
                    entity_type: "PersonaEntity".to_string(),
                    entity_id: "p1".to_string(),
                },
                target_entity_ref: EntityRef {
                    entity_type: "UserEntity".to_string(),
                    entity_id: "u1".to_string(),
                },
                relationship_kind: "assistant_user".to_string(),
                status: RelationshipStatus::Revoked,
                trust_level: 0.1,
                confidence: 0.8,
                strength_score: 0.8,
                recency_score: 0.8,
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
            .expect("seed revoked relationship should work");

        let result = store.upsert_relationship(RelationshipEntity {
            relationship_id: "rel-new-missing-derived".to_string(),
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
            trust_level: 0.4,
            confidence: 0.8,
            strength_score: 0.8,
            recency_score: 0.8,
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
        });

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replacement_relationship_succeeds_with_valid_derived_from() {
        let store = InMemoryIdentityMemoryStore::default();

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-old-2".to_string(),
                source_entity_ref: EntityRef {
                    entity_type: "PersonaEntity".to_string(),
                    entity_id: "p1".to_string(),
                },
                target_entity_ref: EntityRef {
                    entity_type: "UserEntity".to_string(),
                    entity_id: "u1".to_string(),
                },
                relationship_kind: "assistant_user".to_string(),
                status: RelationshipStatus::Revoked,
                trust_level: 0.1,
                confidence: 0.8,
                strength_score: 0.8,
                recency_score: 0.8,
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
            .expect("seed revoked relationship should work");

        let result = store.upsert_relationship(RelationshipEntity {
            relationship_id: "rel-new-valid-derived".to_string(),
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
            trust_level: 0.4,
            confidence: 0.8,
            strength_score: 0.8,
            recency_score: 0.8,
            autonomy_scope: Default::default(),
            approval_profile_id: None,
            interruption_policy: Default::default(),
            escalation_policy: Default::default(),
            policy_tags: vec![],
            provenance: UpdateSource::UserDirect,
            parent_relationship_id: None,
            governing_relationship_ids: vec![],
            derived_from_relationship_id: Some("rel-old-2".to_string()),
            last_transition_reason: None,
            transition_receipt_id: None,
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });

        assert!(result.is_ok());
    }
}
