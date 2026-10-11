use std::cmp::Ordering;
use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::identity_context_filter::{
    apply_identity_context_mode, collect_contact_ids,
};
use crate::infrastructure::memory::identity_memory_store_shared::{
    compute_graph_depth_with_cap, render_sttp_bridge_node,
};
use crate::ports::outbound::memory::identity_memory_models::{
    ChannelProfileEntity, CommitEntityUpdateRequest, CommitEntityUpdateResponse, CommitOutcomeCode,
    ContactEntity, EntityUpdateProposalRecord, GetIdentityContextRequest, GetIdentityContextResponse,
    IdentityEntityType, ListEntityHistoryRequest, ListEntityHistoryResponse, PersonaEntity,
    PolicyProfileEntity, ProposalState, ProposeEntityUpdateRequest, ProposeEntityUpdateResponse,
    RelationshipEntity, RelationshipStatus, RelationshipTransitionEvent, RollbackEntityVersionRequest,
    RollbackEntityVersionResponse, UpdateTier, UserEntity,
};
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

use super::codec::{decode_json, encode_json, encode_time, sql_err};
use super::session::SqliteSession;

const DEFAULT_GRAPH_MAX_DEPTH: usize = 2;
const DEFAULT_TRUST_DELTA_MAX_PER_WINDOW: f32 = 0.03;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RelationshipVersionDoc {
    version: i32,
    snapshot: RelationshipEntity,
    created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct SqliteIdentityMemoryStore {
    session: SqliteSession,
}

impl SqliteIdentityMemoryStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    pub fn ensure_schema(&self) -> Result<()> {
        self.session.migrate()
    }

    fn now_id(prefix: &str) -> String {
        format!("{prefix}_{}", Utc::now().timestamp_micros())
    }

    fn classify_field_path(path: &str) -> UpdateTier {
        match path {
            "status" | "trust_level" | "autonomy_scope.allow" | "autonomy_scope.deny"
            | "autonomy_scope.approval_required" | "approval_profile_id"
            | "escalation_policy.mode" | "parent_relationship_id"
            | "governing_relationship_ids" | "source_entity_ref.entity_type"
            | "source_entity_ref.entity_id" | "target_entity_ref.entity_type"
            | "target_entity_ref.entity_id" => UpdateTier::ApprovalRequired,
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

    fn material_bridge_reason(
        patch: &Value,
        from: Option<RelationshipStatus>,
        to: Option<RelationshipStatus>,
    ) -> Option<String> {
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

    fn load_json<T: for<'de> Deserialize<'de>>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Option<T>> {
        let raw = self.session.query_opt(sql, params, |row| row.get::<_, String>(0))?;
        raw.map(|json| decode_json(&json)).transpose()
    }

    fn load_policy_profile(&self, policy_profile_id: &str) -> Result<Option<PolicyProfileEntity>> {
        self.load_json(
            "SELECT payload FROM identity_policy_profile WHERE policy_profile_id = ?1",
            rusqlite::params![policy_profile_id],
        )
    }

    fn load_contact(&self, contact_id: &str) -> Result<Option<ContactEntity>> {
        self.load_json(
            "SELECT payload FROM identity_contact WHERE contact_id = ?1",
            rusqlite::params![contact_id],
        )
    }

    fn load_relationship(&self, relationship_id: &str) -> Result<Option<RelationshipEntity>> {
        self.load_json(
            "SELECT payload FROM identity_relationship WHERE relationship_id = ?1",
            rusqlite::params![relationship_id],
        )
    }

    fn load_proposal(&self, proposal_id: &str) -> Result<Option<EntityUpdateProposalRecord>> {
        self.load_json(
            "SELECT payload FROM identity_entity_update_proposal WHERE proposal_id = ?1",
            rusqlite::params![proposal_id],
        )
    }

    fn save_proposal(&self, proposal: &EntityUpdateProposalRecord) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_entity_update_proposal (
                proposal_id, entity_type, entity_id, state, created_at, payload
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(proposal_id) DO UPDATE SET
                entity_type = excluded.entity_type,
                entity_id = excluded.entity_id,
                state = excluded.state,
                created_at = excluded.created_at,
                payload = excluded.payload",
            rusqlite::params![
                proposal.proposal_id,
                identity_entity_type_str(&proposal.entity_type),
                proposal.entity_id,
                proposal_state_str(proposal.state),
                encode_time(proposal.created_at),
                encode_json(proposal)?,
            ],
        )?;
        Ok(())
    }

    fn save_relationship_row(&self, relationship: &RelationshipEntity) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_relationship (
                relationship_id, status, source_entity_type, source_entity_id,
                target_entity_type, target_entity_id, relationship_kind, payload
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(relationship_id) DO UPDATE SET
                status = excluded.status,
                source_entity_type = excluded.source_entity_type,
                source_entity_id = excluded.source_entity_id,
                target_entity_type = excluded.target_entity_type,
                target_entity_id = excluded.target_entity_id,
                relationship_kind = excluded.relationship_kind,
                payload = excluded.payload",
            rusqlite::params![
                relationship.relationship_id,
                relationship_status_str(relationship.status),
                relationship.source_entity_ref.entity_type,
                relationship.source_entity_ref.entity_id,
                relationship.target_entity_ref.entity_type,
                relationship.target_entity_ref.entity_id,
                relationship.relationship_kind.as_str(),
                encode_json(relationship)?,
            ],
        )?;
        Ok(())
    }

    fn save_relationship_version(&self, relationship: &RelationshipEntity) -> Result<()> {
        let version_id = format!("{}:{}", relationship.relationship_id, relationship.version);
        let doc = RelationshipVersionDoc {
            version: relationship.version,
            snapshot: relationship.clone(),
            created_at: Utc::now(),
        };
        self.session.execute(
            "INSERT INTO identity_relationship_version (version_id, relationship_id, payload)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(version_id) DO UPDATE SET
                relationship_id = excluded.relationship_id,
                payload = excluded.payload",
            rusqlite::params![
                version_id,
                relationship.relationship_id,
                encode_json(&doc)?,
            ],
        )?;
        Ok(())
    }

    fn save_transition(&self, event: &RelationshipTransitionEvent) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_relationship_transition (
                event_id, relationship_id, occurred_at, payload
             ) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(event_id) DO UPDATE SET
                relationship_id = excluded.relationship_id,
                occurred_at = excluded.occurred_at,
                payload = excluded.payload",
            rusqlite::params![
                event.event_id,
                event.relationship_id,
                encode_time(event.occurred_at),
                encode_json(event)?,
            ],
        )?;
        Ok(())
    }

    fn trust_delta_max_for_relationship(&self, relationship: &RelationshipEntity) -> f32 {
        if let Some(profile_id) = relationship.approval_profile_id.as_ref()
            && let Ok(Some(profile)) = self.load_policy_profile(profile_id)
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
                    let parsed = parse_relationship_status(status_raw)?;

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
                    relationship.escalation_policy.fallback = value.as_str().map(|v| v.to_string());
                }
                "autonomy_scope.allow" => {
                    relationship.autonomy_scope.allow =
                        parse_string_array(value, "autonomy_scope.allow")?;
                }
                "autonomy_scope.deny" => {
                    relationship.autonomy_scope.deny =
                        parse_string_array(value, "autonomy_scope.deny")?;
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
                    relationship.relationship_kind =
                        crate::ports::outbound::memory::identity_memory_models::RelationshipKind::parse(
                            value.as_str().ok_or_else(|| {
                                StasisError::PortFailure(
                                    "relationship_kind must be a string".to_string(),
                                )
                            })?,
                        );
                }
                "policy_tags" => {
                    relationship.policy_tags = parse_string_array(value, "policy_tags")?;
                }
                "source_entity_ref.entity_type" => {
                    relationship.source_entity_ref.entity_type = value
                        .as_str()
                        .ok_or_else(|| {
                            StasisError::PortFailure(
                                "source_entity_ref.entity_type must be a string".to_string(),
                            )
                        })?
                        .to_string();
                }
                "source_entity_ref.entity_id" => {
                    relationship.source_entity_ref.entity_id = value
                        .as_str()
                        .ok_or_else(|| {
                            StasisError::PortFailure(
                                "source_entity_ref.entity_id must be a string".to_string(),
                            )
                        })?
                        .to_string();
                }
                "target_entity_ref.entity_type" => {
                    relationship.target_entity_ref.entity_type = value
                        .as_str()
                        .ok_or_else(|| {
                            StasisError::PortFailure(
                                "target_entity_ref.entity_type must be a string".to_string(),
                            )
                        })?
                        .to_string();
                }
                "target_entity_ref.entity_id" => {
                    relationship.target_entity_ref.entity_id = value
                        .as_str()
                        .ok_or_else(|| {
                            StasisError::PortFailure(
                                "target_entity_ref.entity_id must be a string".to_string(),
                            )
                        })?
                        .to_string();
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
        relationship.transition_receipt_id = Some(Self::now_id("rcpt"));

        if previous_status != next_status {
            relationship.last_transition_reason = Some("status_transition".to_string());
        }

        Ok((previous_status, next_status))
    }

    fn validate_replacement_continuity(&self, relationship: &RelationshipEntity) -> Result<()> {
        let existing: Option<String> = self.session.query_opt(
            "SELECT relationship_id FROM identity_relationship WHERE relationship_id = ?1",
            rusqlite::params![relationship.relationship_id],
            |row| row.get(0),
        )?;
        if existing.is_some() {
            return Ok(());
        }

        let predecessors: Vec<String> = self.session.query_map(
            "SELECT relationship_id FROM identity_relationship
             WHERE status = 'revoked'
               AND source_entity_type = ?1
               AND source_entity_id = ?2
               AND target_entity_type = ?3
               AND target_entity_id = ?4
               AND relationship_kind = ?5",
            rusqlite::params![
                relationship.source_entity_ref.entity_type,
                relationship.source_entity_ref.entity_id,
                relationship.target_entity_ref.entity_type,
                relationship.target_entity_ref.entity_id,
                relationship.relationship_kind.as_str(),
            ],
            |row| row.get(0),
        )?;

        if predecessors.is_empty() {
            return Ok(());
        }

        let Some(derived_from) = relationship.derived_from_relationship_id.as_ref() else {
            return Err(StasisError::PortFailure(
                "policy denied: replacement relationship requires derived_from_relationship_id"
                    .to_string(),
            ));
        };

        if !predecessors.iter().any(|id| id == derived_from) {
            return Err(StasisError::PortFailure(
                "policy denied: derived_from_relationship_id must reference a revoked predecessor with matching endpoints and kind".to_string(),
            ));
        }

        Ok(())
    }

    pub fn upsert_persona(&self, persona: PersonaEntity) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_persona (persona_id, payload) VALUES (?1, ?2)
             ON CONFLICT(persona_id) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![persona.persona_id, encode_json(&persona)?],
        )?;
        Ok(())
    }

    pub fn upsert_user(&self, user: UserEntity) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_user (user_id, payload) VALUES (?1, ?2)
             ON CONFLICT(user_id) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![user.user_id, encode_json(&user)?],
        )?;
        Ok(())
    }

    pub fn upsert_contact(&self, contact: ContactEntity) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_contact (contact_id, payload) VALUES (?1, ?2)
             ON CONFLICT(contact_id) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![contact.contact_id, encode_json(&contact)?],
        )?;
        Ok(())
    }

    pub fn upsert_channel(&self, channel: ChannelProfileEntity) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_channel_profile (channel_id, payload) VALUES (?1, ?2)
             ON CONFLICT(channel_id) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![channel.channel_id, encode_json(&channel)?],
        )?;
        Ok(())
    }

    pub fn upsert_policy(&self, policy: PolicyProfileEntity) -> Result<()> {
        self.session.execute(
            "INSERT INTO identity_policy_profile (policy_profile_id, payload) VALUES (?1, ?2)
             ON CONFLICT(policy_profile_id) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![policy.policy_profile_id, encode_json(&policy)?],
        )?;
        Ok(())
    }

    pub fn upsert_relationship(&self, relationship: RelationshipEntity) -> Result<()> {
        self.validate_replacement_continuity(&relationship)?;
        self.save_relationship_row(&relationship)?;
        self.save_relationship_version(&relationship)?;
        Ok(())
    }
}

fn parse_string_array(value: &Value, field: &str) -> Result<Vec<String>> {
    let items = value.as_array().ok_or_else(|| {
        StasisError::PortFailure(format!("{field} must be an array of strings"))
    })?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let text = item.as_str().ok_or_else(|| {
            StasisError::PortFailure(format!("{field} must be an array of strings"))
        })?;
        out.push(text.to_string());
    }
    Ok(out)
}

fn identity_entity_type_str(value: &IdentityEntityType) -> &'static str {
    match value {
        IdentityEntityType::PersonaEntity => "persona_entity",
        IdentityEntityType::UserEntity => "user_entity",
        IdentityEntityType::ContactEntity => "contact_entity",
        IdentityEntityType::ChannelProfileEntity => "channel_profile_entity",
        IdentityEntityType::PolicyProfileEntity => "policy_profile_entity",
        IdentityEntityType::RelationshipEntity => "relationship_entity",
    }
}

fn proposal_state_str(value: ProposalState) -> &'static str {
    match value {
        ProposalState::Proposed => "proposed",
        ProposalState::Committed => "committed",
        ProposalState::Rejected => "rejected",
        ProposalState::Expired => "expired",
    }
}

fn parse_relationship_status(value: &str) -> Result<RelationshipStatus> {
    match value {
        "proposed" => Ok(RelationshipStatus::Proposed),
        "active" => Ok(RelationshipStatus::Active),
        "suspended" => Ok(RelationshipStatus::Suspended),
        "deprecated" => Ok(RelationshipStatus::Deprecated),
        "revoked" => Ok(RelationshipStatus::Revoked),
        other => Err(sql_err("decode relationship status", other)),
    }
}

fn relationship_status_str(value: RelationshipStatus) -> &'static str {
    match value {
        RelationshipStatus::Proposed => "proposed",
        RelationshipStatus::Active => "active",
        RelationshipStatus::Suspended => "suspended",
        RelationshipStatus::Deprecated => "deprecated",
        RelationshipStatus::Revoked => "revoked",
    }
}

#[async_trait]
impl IdentityMemoryStore for SqliteIdentityMemoryStore {
    async fn get_identity_context(
        &self,
        request: &GetIdentityContextRequest,
    ) -> Result<GetIdentityContextResponse> {
        let persona: Option<PersonaEntity> = self.load_json(
            "SELECT payload FROM identity_persona WHERE persona_id = ?1",
            rusqlite::params![request.persona_id],
        )?;
        let user: Option<UserEntity> = self.load_json(
            "SELECT payload FROM identity_user WHERE user_id = ?1",
            rusqlite::params![request.user_id],
        )?;
        let channel: Option<ChannelProfileEntity> = self.load_json(
            "SELECT payload FROM identity_channel_profile WHERE channel_id = ?1",
            rusqlite::params![request.channel_id],
        )?;

        let limit = i64::try_from(request.relationship_limit.max(1)).unwrap_or(i64::MAX);
        let payloads: Vec<String> = self.session.query_map(
            "SELECT payload FROM identity_relationship
             WHERE status = 'active'
               AND ((source_entity_type = 'PersonaEntity' AND source_entity_id = ?1
                     AND target_entity_type = 'UserEntity' AND target_entity_id = ?2)
                 OR (source_entity_type = 'UserEntity' AND source_entity_id = ?2
                     AND target_entity_type = 'ChannelProfileEntity' AND target_entity_id = ?3)
                 OR (source_entity_type = 'UserEntity' AND source_entity_id = ?2
                     AND target_entity_type = 'ContactEntity')
                 OR (target_entity_type = 'UserEntity' AND target_entity_id = ?2
                     AND source_entity_type = 'ContactEntity')
                 OR source_entity_id = ?1)
             LIMIT ?4",
            rusqlite::params![
                request.persona_id,
                request.user_id,
                request.channel_id,
                limit
            ],
            |row| row.get(0),
        )?;
        let mut relationships = Vec::with_capacity(payloads.len());
        for payload in payloads {
            relationships.push(decode_json::<RelationshipEntity>(&payload)?);
        }

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
        relationships.truncate(request.relationship_limit.max(1));

        let mut policy_profiles = Vec::new();
        for rel in &relationships {
            if let Some(policy_id) = rel.approval_profile_id.as_ref()
                && let Some(profile) = self.load_policy_profile(policy_id)?
                && profile.status == "active"
            {
                policy_profiles.push(profile);
            }
        }

        let (graph_depth_used, flattened_claims) = compute_graph_depth_with_cap(
            &relationships,
            DEFAULT_GRAPH_MAX_DEPTH,
            || Self::now_id("claim"),
        );

        let contact_ids = collect_contact_ids(&relationships);
        let mut resolved_contacts = Vec::with_capacity(contact_ids.len());
        for contact_id in contact_ids {
            if let Some(contact) = self.load_contact(&contact_id)?
                && contact.status == "active"
            {
                resolved_contacts.push(contact);
            }
        }

        let response = GetIdentityContextResponse {
            persona,
            user,
            channel,
            contacts: resolved_contacts,
            relationships,
            policy_profiles,
            graph_depth_used,
            flattened_claims,
        };

        Ok(apply_identity_context_mode(request.mode, response))
    }

    async fn propose_entity_update(
        &self,
        request: &ProposeEntityUpdateRequest,
    ) -> Result<ProposeEntityUpdateResponse> {
        let split = Self::split_patch_by_tier(&request.patch)?;
        let split_patch = split.len() > 1;

        let now = Utc::now();
        let mut proposal_ids = Vec::with_capacity(split.len());
        let mut tiers = Vec::with_capacity(split.len());
        let mut requires_approval = false;

        for (tier, patch) in split {
            let proposal_id = Self::now_id("prop");
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
            self.save_proposal(&record)?;
            if Self::patch_requires_approval(tier) {
                requires_approval = true;
            }
            proposal_ids.push(proposal_id);
            tiers.push(tier);
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
        let Some(mut proposal) = self.load_proposal(&request.proposal_id)? else {
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
            self.save_proposal(&proposal)?;
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
                let Some(mut relationship) = self.load_relationship(&proposal.entity_id)? else {
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
                    self.save_proposal(&proposal)?;
                    return Ok(CommitEntityUpdateResponse {
                        committed: false,
                        code: Some(CommitOutcomeCode::StaleState),
                        entity_type: Some(IdentityEntityType::RelationshipEntity),
                        entity_id: Some(relationship.relationship_id),
                        rationale: Some(format!(
                            "stale_state expected_version={} current_version={}",
                            request.expected_version, relationship.version
                        )),
                        ..Default::default()
                    });
                }

                let (from_status, to_status) =
                    match self.apply_relationship_patch(&mut relationship, &proposal.patch) {
                        Ok(value) => value,
                        Err(err) => {
                            proposal.state = ProposalState::Rejected;
                            proposal.updated_at = Utc::now();
                            self.save_proposal(&proposal)?;
                            return Ok(CommitEntityUpdateResponse {
                                committed: false,
                                code: Some(CommitOutcomeCode::PolicyDenied),
                                entity_type: Some(IdentityEntityType::RelationshipEntity),
                                entity_id: Some(relationship.relationship_id),
                                rationale: Some(err.to_string()),
                                ..Default::default()
                            });
                        }
                    };

                let relationship_id = relationship.relationship_id.clone();
                let new_version = relationship.version;
                let receipt_id = relationship.transition_receipt_id.clone();
                self.save_relationship_row(&relationship)?;
                self.save_relationship_version(&relationship)?;

                let mut transition_event_id = None;
                if from_status != to_status {
                    let event_id = Self::now_id("rel_evt");
                    let event = RelationshipTransitionEvent {
                        event_id: event_id.clone(),
                        relationship_id: relationship_id.clone(),
                        from_status,
                        to_status: to_status.unwrap_or(RelationshipStatus::Active),
                        reason: proposal.reason.clone(),
                        actor: proposal.actor.clone(),
                        receipt_id: receipt_id.clone(),
                        occurred_at: Utc::now(),
                        metadata: None,
                    };
                    self.save_transition(&event)?;
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
                self.save_proposal(&proposal)?;

                Ok(CommitEntityUpdateResponse {
                    committed: true,
                    code: Some(CommitOutcomeCode::Ok),
                    entity_type: Some(IdentityEntityType::RelationshipEntity),
                    entity_id: Some(relationship_id),
                    new_version: Some(new_version),
                    receipt_id,
                    transition_event_id,
                    sttp_bridge_node,
                    sttp_bridge_reason: bridge_reason,
                    rationale: None,
                })
            }
            _ => Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::InvalidPatch),
                entity_type: Some(proposal.entity_type),
                entity_id: Some(proposal.entity_id),
                rationale: Some(
                    "commit path for this entity type is not implemented yet".to_string(),
                ),
                ..Default::default()
            }),
        }
    }

    async fn list_entity_history(
        &self,
        request: &ListEntityHistoryRequest,
    ) -> Result<ListEntityHistoryResponse> {
        let limit = i64::try_from(request.limit.max(1)).unwrap_or(i64::MAX);
        let proposal_payloads: Vec<String> = self.session.query_map(
            "SELECT payload FROM identity_entity_update_proposal
             WHERE entity_type = ?1 AND entity_id = ?2
             ORDER BY created_at DESC LIMIT ?3",
            rusqlite::params![
                identity_entity_type_str(&request.entity_type),
                request.entity_id,
                limit
            ],
            |row| row.get(0),
        )?;
        let mut proposals = Vec::with_capacity(proposal_payloads.len());
        for payload in proposal_payloads {
            proposals.push(decode_json(&payload)?);
        }

        let transitions = if request.entity_type == IdentityEntityType::RelationshipEntity {
            let payloads: Vec<String> = self.session.query_map(
                "SELECT payload FROM identity_relationship_transition
                 WHERE relationship_id = ?1
                 ORDER BY occurred_at DESC LIMIT ?2",
                rusqlite::params![request.entity_id, limit],
                |row| row.get(0),
            )?;
            let mut events = Vec::with_capacity(payloads.len());
            for payload in payloads {
                events.push(decode_json(&payload)?);
            }
            events
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
                let version_id = format!("{}:{}", request.entity_id, request.target_version);
                let Some(doc) = self.load_json::<RelationshipVersionDoc>(
                    "SELECT payload FROM identity_relationship_version WHERE version_id = ?1",
                    rusqlite::params![version_id],
                )?
                else {
                    return Ok(RollbackEntityVersionResponse {
                        rolled_back: false,
                        rationale: Some("target version not found".to_string()),
                        ..Default::default()
                    });
                };

                let mut restored = doc.snapshot;
                restored.version += 1;
                restored.updated_at = Utc::now();
                restored.last_transition_reason = Some(format!("rollback: {}", request.reason));
                restored.transition_receipt_id = Some(Self::now_id("rcpt_rollback"));
                self.save_relationship_row(&restored)?;
                self.save_relationship_version(&restored)?;

                Ok(RollbackEntityVersionResponse {
                    rolled_back: true,
                    new_version: Some(restored.version),
                    rollback_receipt_id: restored.transition_receipt_id,
                    rationale: None,
                })
            }
            _ => Ok(RollbackEntityVersionResponse {
                rolled_back: false,
                rationale: Some(
                    "rollback for this entity type is not implemented yet".to_string(),
                ),
                ..Default::default()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use serde_json::json;

    use super::SqliteIdentityMemoryStore;
    use crate::infrastructure::sqlite::session::SqliteSession;
    use crate::ports::outbound::memory::identity_memory_models::{
        CommitEntityUpdateRequest, CommitOutcomeCode, EntityRef, GetIdentityContextRequest,
        IdentityEntityType, PersonaEntity, ProposeEntityUpdateRequest, RelationshipEntity,
        RelationshipKind, RelationshipStatus, UpdateSource, UserEntity,
    };
    use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

    fn store() -> SqliteIdentityMemoryStore {
        SqliteIdentityMemoryStore::new(SqliteSession::open(":memory:").expect("sqlite"))
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

    #[tokio::test]
    async fn sqlite_identity_split_commit_and_context() {
        let store = store();
        store
            .upsert_persona(PersonaEntity {
                persona_id: "p1".to_string(),
                display_name: "Medousa".to_string(),
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            })
            .unwrap();
        store
            .upsert_user(UserEntity {
                user_id: "u1".to_string(),
                timezone: "UTC".to_string(),
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
                ..Default::default()
            })
            .unwrap();
        store
            .upsert_relationship(relationship("rel-1", RelationshipStatus::Active))
            .unwrap();

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
            .unwrap();
        assert!(proposal.split_patch);
        assert_eq!(proposal.proposal_ids.len(), 2);

        let first = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposal.proposal_ids[0].clone(),
                expected_version: 1,
                approver: None,
            })
            .await
            .unwrap();
        assert!(first.committed);

        let second = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposal.proposal_ids[1].clone(),
                expected_version: 2,
                approver: Some("owner".to_string()),
            })
            .await
            .unwrap();
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
            .unwrap();
        assert_eq!(context.relationships.len(), 1);
        assert_eq!(context.relationships[0].version, 3);
    }

    #[tokio::test]
    async fn sqlite_identity_commit_guards() {
        let store = store();
        store
            .upsert_relationship(relationship("rel-1", RelationshipStatus::Active))
            .unwrap();
        let proposal = store
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
            .unwrap();

        let stale = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposal.proposal_ids[0].clone(),
                expected_version: 99,
                approver: Some("owner".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(stale.code, Some(CommitOutcomeCode::StaleState));

        let missing = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: "missing".to_string(),
                expected_version: 1,
                approver: None,
            })
            .await
            .unwrap();
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
            .unwrap();
        let denied = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: needs_approval.proposal_ids[0].clone(),
                expected_version: 1,
                approver: None,
            })
            .await
            .unwrap();
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
            .unwrap();
        let expired_commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: expired.proposal_ids[0].clone(),
                expected_version: 1,
                approver: None,
            })
            .await
            .unwrap();
        assert_eq!(expired_commit.code, Some(CommitOutcomeCode::ExpiredProposal));
    }

    #[tokio::test]
    async fn sqlite_identity_policy_and_replacement_continuity() {
        let store = store();
        store
            .upsert_relationship(relationship("rel-revoked", RelationshipStatus::Revoked))
            .unwrap();
        let proposal = store
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
            .unwrap();
        let denied = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposal.proposal_ids[0].clone(),
                expected_version: 1,
                approver: Some("owner".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(denied.code, Some(CommitOutcomeCode::PolicyDenied));

        let err = store
            .upsert_relationship(relationship("rel-next", RelationshipStatus::Active))
            .unwrap_err();
        assert!(err.to_string().contains("derived_from_relationship_id"));

        let mut replacement = relationship("rel-next", RelationshipStatus::Active);
        replacement.derived_from_relationship_id = Some("rel-revoked".to_string());
        store.upsert_relationship(replacement).unwrap();
    }
}
