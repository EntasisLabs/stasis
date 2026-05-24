use std::cmp::Ordering;
use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use surrealdb::{engine::any::Any, Surreal};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::identity_memory_store_shared::{
    compute_graph_depth_with_cap, render_sttp_bridge_node,
};
use crate::ports::outbound::memory::identity_memory_models::{
    AutonomyScope, ChannelProfileEntity, CommitEntityUpdateRequest, CommitEntityUpdateResponse,
    CommitOutcomeCode, EntityRef, EntityUpdateProposalRecord, EscalationPolicy,
    GetIdentityContextRequest, GetIdentityContextResponse, IdentityEntityType,
    InterruptionPolicy, ListEntityHistoryRequest, ListEntityHistoryResponse, PersonaEntity,
    PolicyProfileEntity, ProposalState, ProposeEntityUpdateRequest, ProposeEntityUpdateResponse,
    RelationshipEntity, RelationshipStatus, RelationshipTransitionEvent,
    RollbackEntityVersionRequest, RollbackEntityVersionResponse, UpdateSource, UpdateTier,
    UserEntity,
};
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

const DEFAULT_GRAPH_MAX_DEPTH: usize = 2;
const DEFAULT_TRUST_DELTA_MAX_PER_WINDOW: f32 = 0.03;
const IDENTITY_SCHEMA_STATEMENTS: &[&str] = &[
    "DEFINE TABLE identity_persona SCHEMAFULL",
    "DEFINE FIELD persona_id ON TABLE identity_persona TYPE string",
    "DEFINE FIELD display_name ON TABLE identity_persona TYPE string",
    "DEFINE FIELD status ON TABLE identity_persona TYPE string",
    "DEFINE FIELD version ON TABLE identity_persona TYPE int",
    "DEFINE FIELD updated_at ON TABLE identity_persona TYPE datetime",
    "DEFINE TABLE identity_user SCHEMAFULL",
    "DEFINE FIELD user_id ON TABLE identity_user TYPE string",
    "DEFINE FIELD timezone ON TABLE identity_user TYPE string",
    "DEFINE FIELD language_variant ON TABLE identity_user TYPE option<string>",
    "DEFINE FIELD status ON TABLE identity_user TYPE string",
    "DEFINE FIELD version ON TABLE identity_user TYPE int",
    "DEFINE FIELD updated_at ON TABLE identity_user TYPE datetime",
    "DEFINE TABLE identity_channel_profile SCHEMAFULL",
    "DEFINE FIELD channel_id ON TABLE identity_channel_profile TYPE string",
    "DEFINE FIELD channel_type ON TABLE identity_channel_profile TYPE string",
    "DEFINE FIELD proactive_allowed ON TABLE identity_channel_profile TYPE bool",
    "DEFINE FIELD status ON TABLE identity_channel_profile TYPE string",
    "DEFINE FIELD version ON TABLE identity_channel_profile TYPE int",
    "DEFINE FIELD updated_at ON TABLE identity_channel_profile TYPE datetime",
    "DEFINE TABLE identity_policy_profile SCHEMAFULL",
    "DEFINE FIELD policy_profile_id ON TABLE identity_policy_profile TYPE string",
    "DEFINE FIELD graph_max_depth ON TABLE identity_policy_profile TYPE int",
    "DEFINE FIELD trust_delta_max_per_window ON TABLE identity_policy_profile TYPE float",
    "DEFINE FIELD status ON TABLE identity_policy_profile TYPE string",
    "DEFINE FIELD version ON TABLE identity_policy_profile TYPE int",
    "DEFINE FIELD updated_at ON TABLE identity_policy_profile TYPE datetime",
    "DEFINE TABLE identity_relationship SCHEMAFULL",
    "DEFINE FIELD relationship_id ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD source_entity_type ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD source_entity_id ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD target_entity_type ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD target_entity_id ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD relationship_kind ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD status ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD trust_level ON TABLE identity_relationship TYPE float",
    "DEFINE FIELD confidence ON TABLE identity_relationship TYPE float",
    "DEFINE FIELD strength_score ON TABLE identity_relationship TYPE float",
    "DEFINE FIELD recency_score ON TABLE identity_relationship TYPE float",
    "DEFINE FIELD autonomy_scope_allow ON TABLE identity_relationship TYPE array<string>",
    "DEFINE FIELD autonomy_scope_deny ON TABLE identity_relationship TYPE array<string>",
    "DEFINE FIELD autonomy_scope_approval_required ON TABLE identity_relationship TYPE array<string>",
    "DEFINE FIELD approval_profile_id ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD interruption_quiet_hours ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD interruption_allow_urgent_only ON TABLE identity_relationship TYPE option<bool>",
    "DEFINE FIELD interruption_urgent_threshold ON TABLE identity_relationship TYPE option<float>",
    "DEFINE FIELD escalation_mode ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD escalation_fallback ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD policy_tags ON TABLE identity_relationship TYPE array<string>",
    "DEFINE FIELD provenance ON TABLE identity_relationship TYPE string",
    "DEFINE FIELD parent_relationship_id ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD governing_relationship_ids ON TABLE identity_relationship TYPE array<string>",
    "DEFINE FIELD derived_from_relationship_id ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD last_transition_reason ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD transition_receipt_id ON TABLE identity_relationship TYPE option<string>",
    "DEFINE FIELD version ON TABLE identity_relationship TYPE int",
    "DEFINE FIELD created_at ON TABLE identity_relationship TYPE datetime",
    "DEFINE FIELD updated_at ON TABLE identity_relationship TYPE datetime",
    "DEFINE TABLE identity_relationship_version SCHEMAFULL",
    "DEFINE FIELD version_id ON TABLE identity_relationship_version TYPE string",
    "DEFINE FIELD relationship_id ON TABLE identity_relationship_version TYPE string",
    "DEFINE FIELD version ON TABLE identity_relationship_version TYPE int",
    "DEFINE FIELD snapshot ON TABLE identity_relationship_version TYPE object",
    "DEFINE FIELD created_at ON TABLE identity_relationship_version TYPE datetime",
    "DEFINE TABLE identity_entity_update_proposal SCHEMAFULL",
    "DEFINE FIELD proposal_id ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD entity_type ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD entity_id ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD patch_json ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD tier ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD source ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD confidence ON TABLE identity_entity_update_proposal TYPE float",
    "DEFINE FIELD reason ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD state ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD approver ON TABLE identity_entity_update_proposal TYPE option<string>",
    "DEFINE FIELD actor ON TABLE identity_entity_update_proposal TYPE string",
    "DEFINE FIELD receipt_id ON TABLE identity_entity_update_proposal TYPE option<string>",
    "DEFINE FIELD expires_at ON TABLE identity_entity_update_proposal TYPE option<datetime>",
    "DEFINE FIELD created_at ON TABLE identity_entity_update_proposal TYPE datetime",
    "DEFINE FIELD updated_at ON TABLE identity_entity_update_proposal TYPE datetime",
    "DEFINE TABLE identity_relationship_transition SCHEMAFULL",
    "DEFINE FIELD event_id ON TABLE identity_relationship_transition TYPE string",
    "DEFINE FIELD relationship_id ON TABLE identity_relationship_transition TYPE string",
    "DEFINE FIELD from_status ON TABLE identity_relationship_transition TYPE option<string>",
    "DEFINE FIELD to_status ON TABLE identity_relationship_transition TYPE string",
    "DEFINE FIELD reason ON TABLE identity_relationship_transition TYPE string",
    "DEFINE FIELD actor ON TABLE identity_relationship_transition TYPE string",
    "DEFINE FIELD receipt_id ON TABLE identity_relationship_transition TYPE option<string>",
    "DEFINE FIELD occurred_at ON TABLE identity_relationship_transition TYPE datetime",
    "DEFINE FIELD metadata_json ON TABLE identity_relationship_transition TYPE option<string>",
    "DEFINE INDEX idx_identity_relationship_status ON TABLE identity_relationship COLUMNS status",
    "DEFINE INDEX idx_identity_relationship_endpoints ON TABLE identity_relationship COLUMNS source_entity_type, source_entity_id, target_entity_type, target_entity_id",
    "DEFINE INDEX idx_identity_relationship_kind_status ON TABLE identity_relationship COLUMNS relationship_kind, status",
    "DEFINE INDEX idx_identity_proposal_lookup ON TABLE identity_entity_update_proposal COLUMNS entity_type, entity_id, state",
    "DEFINE INDEX idx_identity_transition_rel_time ON TABLE identity_relationship_transition COLUMNS relationship_id, occurred_at",
];

#[derive(Clone)]
pub struct SurrealIdentityMemoryStore {
    db: Surreal<Any>,
    persona_table: String,
    user_table: String,
    channel_table: String,
    policy_table: String,
    relationship_table: String,
    relationship_version_table: String,
    proposal_table: String,
    transition_table: String,
}

impl SurrealIdentityMemoryStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            persona_table: "identity_persona".to_string(),
            user_table: "identity_user".to_string(),
            channel_table: "identity_channel_profile".to_string(),
            policy_table: "identity_policy_profile".to_string(),
            relationship_table: "identity_relationship".to_string(),
            relationship_version_table: "identity_relationship_version".to_string(),
            proposal_table: "identity_entity_update_proposal".to_string(),
            transition_table: "identity_relationship_transition".to_string(),
        }
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        Self::ensure_schema_for_db(&self.db).await
    }

    pub async fn ensure_schema_for_db(db: &Surreal<Any>) -> Result<()> {
        for statement in IDENTITY_SCHEMA_STATEMENTS {
            if let Err(err) = db.query(*statement).await {
                let text = err.to_string();
                if !(text.contains("already exists")
                    || text.contains("already defined")
                    || text.contains("Overwrite index"))
                {
                    return Err(Self::port_err("identity schema bootstrap", text));
                }
            }
        }

        Ok(())
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    fn is_missing_table(err: &str, table: &str) -> bool {
        err.contains("does not exist") && err.contains(table)
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

    async fn load_policy_profile(&self, policy_profile_id: &str) -> Result<Option<PolicyProfileEntity>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.policy_table.clone()))
            .bind(("id", policy_profile_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.policy_table) {
                    return Ok(None);
                }
                return Err(Self::port_err("load policy profile", err));
            }
        };

        let row: Option<PolicyProfileRow> = match response.take(0) {
            Ok(row) => row,
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.policy_table) {
                    return Ok(None);
                }
                return Err(Self::port_err("decode policy profile", err));
            }
        };

        Ok(row.map(PolicyProfileEntity::from))
    }

    async fn trust_delta_max_for_relationship(&self, relationship: &RelationshipEntity) -> f32 {
        if let Some(profile_id) = relationship.approval_profile_id.as_ref()
            && let Ok(Some(profile)) = self.load_policy_profile(profile_id).await
            && profile.trust_delta_max_per_window.is_finite()
            && profile.trust_delta_max_per_window > 0.0
        {
            return profile.trust_delta_max_per_window;
        }
        DEFAULT_TRUST_DELTA_MAX_PER_WINDOW
    }

    async fn apply_relationship_patch(
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
                    let delta_max = self.trust_delta_max_for_relationship(relationship).await;
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
                    relationship.relationship_kind = value
                        .as_str()
                        .ok_or_else(|| {
                            StasisError::PortFailure(
                                "relationship_kind must be a string".to_string(),
                            )
                        })?
                        .to_string();
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

    async fn save_relationship_version(&self, relationship: &RelationshipEntity) -> Result<()> {
        let version_id = format!("{}:{}", relationship.relationship_id, relationship.version);
        let row = RelationshipVersionRow {
            version_id,
            relationship_id: relationship.relationship_id.clone(),
            version: relationship.version,
            snapshot: RelationshipRow::from(relationship.clone()),
            created_at: Utc::now(),
        };

        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.relationship_version_table.clone()))
            .bind(("id", row.version_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("save relationship version", e))?;

        Ok(())
    }

    async fn validate_replacement_continuity(&self, relationship: &RelationshipEntity) -> Result<()> {
        let mut existing_resp = self
            .db
            .query("SELECT relationship_id FROM type::record($table, $id)")
            .bind(("table", self.relationship_table.clone()))
            .bind(("id", relationship.relationship_id.clone()))
            .await
            .map_err(|e| Self::port_err("load existing relationship id", e))?;

        let existing: Vec<RelationshipIdRow> = match existing_resp.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.relationship_table) {
                    Vec::new()
                } else {
                    return Err(Self::port_err("decode existing relationship id", err));
                }
            }
        };

        if !existing.is_empty() {
            return Ok(());
        }

        let mut predecessor_resp = match self
            .db
            .query(
                "SELECT relationship_id FROM type::table($table) \
                 WHERE status = 'revoked' \
                   AND source_entity_type = $source_entity_type \
                   AND source_entity_id = $source_entity_id \
                   AND target_entity_type = $target_entity_type \
                   AND target_entity_id = $target_entity_id \
                   AND relationship_kind = $relationship_kind",
            )
            .bind(("table", self.relationship_table.clone()))
            .bind((
                "source_entity_type",
                relationship.source_entity_ref.entity_type.clone(),
            ))
            .bind((
                "source_entity_id",
                relationship.source_entity_ref.entity_id.clone(),
            ))
            .bind((
                "target_entity_type",
                relationship.target_entity_ref.entity_type.clone(),
            ))
            .bind((
                "target_entity_id",
                relationship.target_entity_ref.entity_id.clone(),
            ))
            .bind(("relationship_kind", relationship.relationship_kind.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.relationship_table) {
                    return Ok(());
                }
                return Err(Self::port_err("load revoked predecessors", err));
            }
        };

        let predecessor_rows: Vec<RelationshipIdRow> = match predecessor_resp.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.relationship_table) {
                    return Ok(());
                }
                return Err(Self::port_err("decode revoked predecessors", err));
            }
        };

        if predecessor_rows.is_empty() {
            return Ok(());
        }

        let Some(derived_from) = relationship.derived_from_relationship_id.as_ref() else {
            return Err(StasisError::PortFailure(
                "policy denied: replacement relationship requires derived_from_relationship_id"
                    .to_string(),
            ));
        };

        if !predecessor_rows.iter().any(|row| &row.relationship_id == derived_from) {
            return Err(StasisError::PortFailure(
                "policy denied: derived_from_relationship_id must reference a revoked predecessor with matching endpoints and kind".to_string(),
            ));
        }

        Ok(())
    }

    pub async fn upsert_persona(&self, persona: PersonaEntity) -> Result<()> {
        let row = PersonaRow::from(persona);
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.persona_table.clone()))
            .bind(("id", row.persona_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("upsert persona", e))?;
        Ok(())
    }

    pub async fn upsert_user(&self, user: UserEntity) -> Result<()> {
        let row = UserRow::from(user);
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.user_table.clone()))
            .bind(("id", row.user_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("upsert user", e))?;
        Ok(())
    }

    pub async fn upsert_channel(&self, channel: ChannelProfileEntity) -> Result<()> {
        let row = ChannelProfileRow::from(channel);
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.channel_table.clone()))
            .bind(("id", row.channel_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("upsert channel", e))?;
        Ok(())
    }

    pub async fn upsert_policy(&self, policy: PolicyProfileEntity) -> Result<()> {
        let row = PolicyProfileRow::from(policy);
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.policy_table.clone()))
            .bind(("id", row.policy_profile_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("upsert policy", e))?;
        Ok(())
    }

    pub async fn upsert_relationship(&self, relationship: RelationshipEntity) -> Result<()> {
        self.validate_replacement_continuity(&relationship).await?;

        let row = RelationshipRow::from(relationship.clone());
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.relationship_table.clone()))
            .bind(("id", row.relationship_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("upsert relationship", e))?;

        self.save_relationship_version(&relationship).await?;
        Ok(())
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

fn parse_identity_entity_type(value: &str) -> Result<IdentityEntityType> {
    match value {
        "persona_entity" => Ok(IdentityEntityType::PersonaEntity),
        "user_entity" => Ok(IdentityEntityType::UserEntity),
        "channel_profile_entity" => Ok(IdentityEntityType::ChannelProfileEntity),
        "policy_profile_entity" => Ok(IdentityEntityType::PolicyProfileEntity),
        "relationship_entity" => Ok(IdentityEntityType::RelationshipEntity),
        other => Err(StasisError::PortFailure(format!(
            "invalid identity entity type: {other}"
        ))),
    }
}

fn identity_entity_type_str(value: IdentityEntityType) -> &'static str {
    match value {
        IdentityEntityType::PersonaEntity => "persona_entity",
        IdentityEntityType::UserEntity => "user_entity",
        IdentityEntityType::ChannelProfileEntity => "channel_profile_entity",
        IdentityEntityType::PolicyProfileEntity => "policy_profile_entity",
        IdentityEntityType::RelationshipEntity => "relationship_entity",
    }
}

fn parse_update_tier(value: &str) -> Result<UpdateTier> {
    match value {
        "auto_commit" => Ok(UpdateTier::AutoCommit),
        "confirm_required" => Ok(UpdateTier::ConfirmRequired),
        "approval_required" => Ok(UpdateTier::ApprovalRequired),
        other => Err(StasisError::PortFailure(format!(
            "invalid update tier: {other}"
        ))),
    }
}

fn update_tier_str(value: UpdateTier) -> &'static str {
    match value {
        UpdateTier::AutoCommit => "auto_commit",
        UpdateTier::ConfirmRequired => "confirm_required",
        UpdateTier::ApprovalRequired => "approval_required",
    }
}

fn parse_proposal_state(value: &str) -> Result<ProposalState> {
    match value {
        "proposed" => Ok(ProposalState::Proposed),
        "committed" => Ok(ProposalState::Committed),
        "rejected" => Ok(ProposalState::Rejected),
        "expired" => Ok(ProposalState::Expired),
        other => Err(StasisError::PortFailure(format!(
            "invalid proposal state: {other}"
        ))),
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

fn parse_update_source(value: &str) -> Result<UpdateSource> {
    match value {
        "user_direct" => Ok(UpdateSource::UserDirect),
        "model_inferred" => Ok(UpdateSource::ModelInferred),
        "system_event" => Ok(UpdateSource::SystemEvent),
        other => Err(StasisError::PortFailure(format!(
            "invalid update source: {other}"
        ))),
    }
}

fn update_source_str(value: UpdateSource) -> &'static str {
    match value {
        UpdateSource::UserDirect => "user_direct",
        UpdateSource::ModelInferred => "model_inferred",
        UpdateSource::SystemEvent => "system_event",
    }
}

fn parse_relationship_status(value: &str) -> Result<RelationshipStatus> {
    match value {
        "proposed" => Ok(RelationshipStatus::Proposed),
        "active" => Ok(RelationshipStatus::Active),
        "suspended" => Ok(RelationshipStatus::Suspended),
        "deprecated" => Ok(RelationshipStatus::Deprecated),
        "revoked" => Ok(RelationshipStatus::Revoked),
        other => Err(StasisError::PortFailure(format!(
            "invalid relationship status: {other}"
        ))),
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

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct PersonaRow {
    persona_id: String,
    display_name: String,
    status: String,
    version: i32,
    updated_at: DateTime<Utc>,
}

impl From<PersonaEntity> for PersonaRow {
    fn from(value: PersonaEntity) -> Self {
        Self {
            persona_id: value.persona_id,
            display_name: value.display_name,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

impl From<PersonaRow> for PersonaEntity {
    fn from(value: PersonaRow) -> Self {
        Self {
            persona_id: value.persona_id,
            display_name: value.display_name,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct UserRow {
    user_id: String,
    timezone: String,
    language_variant: Option<String>,
    status: String,
    version: i32,
    updated_at: DateTime<Utc>,
}

impl From<UserEntity> for UserRow {
    fn from(value: UserEntity) -> Self {
        Self {
            user_id: value.user_id,
            timezone: value.timezone,
            language_variant: value.language_variant,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

impl From<UserRow> for UserEntity {
    fn from(value: UserRow) -> Self {
        Self {
            user_id: value.user_id,
            timezone: value.timezone,
            language_variant: value.language_variant,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ChannelProfileRow {
    channel_id: String,
    channel_type: String,
    proactive_allowed: bool,
    status: String,
    version: i32,
    updated_at: DateTime<Utc>,
}

impl From<ChannelProfileEntity> for ChannelProfileRow {
    fn from(value: ChannelProfileEntity) -> Self {
        Self {
            channel_id: value.channel_id,
            channel_type: value.channel_type,
            proactive_allowed: value.proactive_allowed,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

impl From<ChannelProfileRow> for ChannelProfileEntity {
    fn from(value: ChannelProfileRow) -> Self {
        Self {
            channel_id: value.channel_id,
            channel_type: value.channel_type,
            proactive_allowed: value.proactive_allowed,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct PolicyProfileRow {
    policy_profile_id: String,
    graph_max_depth: usize,
    trust_delta_max_per_window: f32,
    status: String,
    version: i32,
    updated_at: DateTime<Utc>,
}

impl From<PolicyProfileEntity> for PolicyProfileRow {
    fn from(value: PolicyProfileEntity) -> Self {
        Self {
            policy_profile_id: value.policy_profile_id,
            graph_max_depth: value.graph_max_depth,
            trust_delta_max_per_window: value.trust_delta_max_per_window,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

impl From<PolicyProfileRow> for PolicyProfileEntity {
    fn from(value: PolicyProfileRow) -> Self {
        Self {
            policy_profile_id: value.policy_profile_id,
            graph_max_depth: value.graph_max_depth,
            trust_delta_max_per_window: value.trust_delta_max_per_window,
            status: value.status,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct RelationshipRow {
    relationship_id: String,
    source_entity_type: String,
    source_entity_id: String,
    target_entity_type: String,
    target_entity_id: String,
    relationship_kind: String,
    status: String,
    trust_level: f32,
    confidence: f32,
    strength_score: f32,
    recency_score: f32,
    autonomy_scope_allow: Vec<String>,
    autonomy_scope_deny: Vec<String>,
    autonomy_scope_approval_required: Vec<String>,
    approval_profile_id: Option<String>,
    interruption_quiet_hours: Option<String>,
    interruption_allow_urgent_only: Option<bool>,
    interruption_urgent_threshold: Option<f32>,
    escalation_mode: Option<String>,
    escalation_fallback: Option<String>,
    policy_tags: Vec<String>,
    provenance: String,
    parent_relationship_id: Option<String>,
    governing_relationship_ids: Vec<String>,
    derived_from_relationship_id: Option<String>,
    last_transition_reason: Option<String>,
    transition_receipt_id: Option<String>,
    version: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<RelationshipRow> for RelationshipEntity {
    type Error = StasisError;

    fn try_from(value: RelationshipRow) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            relationship_id: value.relationship_id,
            source_entity_ref: EntityRef {
                entity_type: value.source_entity_type,
                entity_id: value.source_entity_id,
            },
            target_entity_ref: EntityRef {
                entity_type: value.target_entity_type,
                entity_id: value.target_entity_id,
            },
            relationship_kind: value.relationship_kind,
            status: parse_relationship_status(&value.status)?,
            trust_level: value.trust_level,
            confidence: value.confidence,
            strength_score: value.strength_score,
            recency_score: value.recency_score,
            autonomy_scope: AutonomyScope {
                allow: value.autonomy_scope_allow,
                deny: value.autonomy_scope_deny,
                approval_required: value.autonomy_scope_approval_required,
            },
            approval_profile_id: value.approval_profile_id,
            interruption_policy: InterruptionPolicy {
                quiet_hours: value.interruption_quiet_hours,
                allow_urgent_only: value.interruption_allow_urgent_only,
                urgent_threshold: value.interruption_urgent_threshold,
            },
            escalation_policy: EscalationPolicy {
                mode: value.escalation_mode,
                fallback: value.escalation_fallback,
            },
            policy_tags: value.policy_tags,
            provenance: parse_update_source(&value.provenance)?,
            parent_relationship_id: value.parent_relationship_id,
            governing_relationship_ids: value.governing_relationship_ids,
            derived_from_relationship_id: value.derived_from_relationship_id,
            last_transition_reason: value.last_transition_reason,
            transition_receipt_id: value.transition_receipt_id,
            version: value.version,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl From<RelationshipEntity> for RelationshipRow {
    fn from(value: RelationshipEntity) -> Self {
        Self {
            relationship_id: value.relationship_id,
            source_entity_type: value.source_entity_ref.entity_type,
            source_entity_id: value.source_entity_ref.entity_id,
            target_entity_type: value.target_entity_ref.entity_type,
            target_entity_id: value.target_entity_ref.entity_id,
            relationship_kind: value.relationship_kind,
            status: relationship_status_str(value.status).to_string(),
            trust_level: value.trust_level,
            confidence: value.confidence,
            strength_score: value.strength_score,
            recency_score: value.recency_score,
            autonomy_scope_allow: value.autonomy_scope.allow,
            autonomy_scope_deny: value.autonomy_scope.deny,
            autonomy_scope_approval_required: value.autonomy_scope.approval_required,
            approval_profile_id: value.approval_profile_id,
            interruption_quiet_hours: value.interruption_policy.quiet_hours,
            interruption_allow_urgent_only: value.interruption_policy.allow_urgent_only,
            interruption_urgent_threshold: value.interruption_policy.urgent_threshold,
            escalation_mode: value.escalation_policy.mode,
            escalation_fallback: value.escalation_policy.fallback,
            policy_tags: value.policy_tags,
            provenance: update_source_str(value.provenance).to_string(),
            parent_relationship_id: value.parent_relationship_id,
            governing_relationship_ids: value.governing_relationship_ids,
            derived_from_relationship_id: value.derived_from_relationship_id,
            last_transition_reason: value.last_transition_reason,
            transition_receipt_id: value.transition_receipt_id,
            version: value.version,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ProposalRow {
    proposal_id: String,
    entity_type: String,
    entity_id: String,
    patch_json: String,
    tier: String,
    source: String,
    confidence: f32,
    reason: String,
    state: String,
    approver: Option<String>,
    actor: String,
    receipt_id: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<ProposalRow> for EntityUpdateProposalRecord {
    type Error = StasisError;

    fn try_from(value: ProposalRow) -> std::result::Result<Self, Self::Error> {
        let patch: Value = serde_json::from_str(&value.patch_json).map_err(|e| {
            StasisError::PortFailure(format!("decode proposal patch json: {e}"))
        })?;

        Ok(Self {
            proposal_id: value.proposal_id,
            entity_type: parse_identity_entity_type(&value.entity_type)?,
            entity_id: value.entity_id,
            patch,
            tier: parse_update_tier(&value.tier)?,
            source: parse_update_source(&value.source)?,
            confidence: value.confidence,
            reason: value.reason,
            state: parse_proposal_state(&value.state)?,
            approver: value.approver,
            actor: value.actor,
            receipt_id: value.receipt_id,
            expires_at: value.expires_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl From<EntityUpdateProposalRecord> for ProposalRow {
    fn from(value: EntityUpdateProposalRecord) -> Self {
        Self {
            proposal_id: value.proposal_id,
            entity_type: identity_entity_type_str(value.entity_type).to_string(),
            entity_id: value.entity_id,
            patch_json: value.patch.to_string(),
            tier: update_tier_str(value.tier).to_string(),
            source: update_source_str(value.source).to_string(),
            confidence: value.confidence,
            reason: value.reason,
            state: proposal_state_str(value.state).to_string(),
            approver: value.approver,
            actor: value.actor,
            receipt_id: value.receipt_id,
            expires_at: value.expires_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct TransitionRow {
    event_id: String,
    relationship_id: String,
    from_status: Option<String>,
    to_status: String,
    reason: String,
    actor: String,
    receipt_id: Option<String>,
    occurred_at: DateTime<Utc>,
    metadata_json: Option<String>,
}

impl TryFrom<TransitionRow> for RelationshipTransitionEvent {
    type Error = StasisError;

    fn try_from(value: TransitionRow) -> std::result::Result<Self, Self::Error> {
        let metadata = match value.metadata_json {
            Some(raw) => Some(serde_json::from_str::<Value>(&raw).map_err(|e| {
                StasisError::PortFailure(format!("decode transition metadata json: {e}"))
            })?),
            None => None,
        };

        Ok(Self {
            event_id: value.event_id,
            relationship_id: value.relationship_id,
            from_status: value
                .from_status
                .as_deref()
                .map(parse_relationship_status)
                .transpose()?,
            to_status: parse_relationship_status(&value.to_status)?,
            reason: value.reason,
            actor: value.actor,
            receipt_id: value.receipt_id,
            occurred_at: value.occurred_at,
            metadata,
        })
    }
}

impl From<RelationshipTransitionEvent> for TransitionRow {
    fn from(value: RelationshipTransitionEvent) -> Self {
        Self {
            event_id: value.event_id,
            relationship_id: value.relationship_id,
            from_status: value.from_status.map(|v| relationship_status_str(v).to_string()),
            to_status: relationship_status_str(value.to_status).to_string(),
            reason: value.reason,
            actor: value.actor,
            receipt_id: value.receipt_id,
            occurred_at: value.occurred_at,
            metadata_json: value.metadata.map(|v| v.to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct RelationshipVersionRow {
    version_id: String,
    relationship_id: String,
    version: i32,
    snapshot: RelationshipRow,
    created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct RelationshipIdRow {
    relationship_id: String,
}

#[async_trait]
impl IdentityMemoryStore for SurrealIdentityMemoryStore {
    async fn get_identity_context(
        &self,
        request: &GetIdentityContextRequest,
    ) -> Result<GetIdentityContextResponse> {
        let persona: Option<PersonaRow> = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.persona_table.clone()))
            .bind(("id", request.persona_id.clone()))
            .await
        {
            Ok(mut response) => match response.take(0) {
                Ok(row) => row,
                Err(err) => {
                    let message = err.to_string();
                    if Self::is_missing_table(&message, &self.persona_table) {
                        None
                    } else {
                        return Err(Self::port_err("decode identity persona", err));
                    }
                }
            },
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.persona_table) {
                    None
                } else {
                    return Err(Self::port_err("get identity persona", err));
                }
            }
        };

        let user: Option<UserRow> = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.user_table.clone()))
            .bind(("id", request.user_id.clone()))
            .await
        {
            Ok(mut response) => match response.take(0) {
                Ok(row) => row,
                Err(err) => {
                    let message = err.to_string();
                    if Self::is_missing_table(&message, &self.user_table) {
                        None
                    } else {
                        return Err(Self::port_err("decode identity user", err));
                    }
                }
            },
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.user_table) {
                    None
                } else {
                    return Err(Self::port_err("get identity user", err));
                }
            }
        };

        let channel: Option<ChannelProfileRow> = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.channel_table.clone()))
            .bind(("id", request.channel_id.clone()))
            .await
        {
            Ok(mut response) => match response.take(0) {
                Ok(row) => row,
                Err(err) => {
                    let message = err.to_string();
                    if Self::is_missing_table(&message, &self.channel_table) {
                        None
                    } else {
                        return Err(Self::port_err("decode identity channel", err));
                    }
                }
            },
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.channel_table) {
                    None
                } else {
                    return Err(Self::port_err("get identity channel", err));
                }
            }
        };

        let rel_rows: Vec<RelationshipRow> = match self
            .db
            .query(
                "SELECT * FROM type::table($table) \
                 WHERE status = 'active' \
                   AND ((source_entity_type = 'PersonaEntity' AND source_entity_id = $persona_id AND target_entity_type = 'UserEntity' AND target_entity_id = $user_id) \
                     OR (source_entity_type = 'UserEntity' AND source_entity_id = $user_id AND target_entity_type = 'ChannelProfileEntity' AND target_entity_id = $channel_id) \
                     OR source_entity_id = $persona_id) \
                 LIMIT $limit",
            )
            .bind(("table", self.relationship_table.clone()))
            .bind(("persona_id", request.persona_id.clone()))
            .bind(("user_id", request.user_id.clone()))
            .bind(("channel_id", request.channel_id.clone()))
            .bind(("limit", request.relationship_limit.max(1)))
            .await
        {
            Ok(mut response) => match response.take(0) {
                Ok(rows) => rows,
                Err(err) => {
                    let message = err.to_string();
                    if Self::is_missing_table(&message, &self.relationship_table) {
                        Vec::new()
                    } else {
                        return Err(Self::port_err("decode identity relationships", err));
                    }
                }
            },
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_table(&message, &self.relationship_table) {
                    Vec::new()
                } else {
                    return Err(Self::port_err("list identity relationships", err));
                }
            }
        };
        let mut relationships = Vec::with_capacity(rel_rows.len());
        for row in rel_rows {
            relationships.push(RelationshipEntity::try_from(row)?);
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
                && let Some(profile) = self.load_policy_profile(policy_id).await?
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

        Ok(GetIdentityContextResponse {
            persona: persona.map(PersonaEntity::from),
            user: user.map(UserEntity::from),
            channel: channel.map(ChannelProfileEntity::from),
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

            let row = ProposalRow::from(record);
            self.db
                .query("UPSERT type::record($table, $id) CONTENT $data")
                .bind(("table", self.proposal_table.clone()))
                .bind(("id", row.proposal_id.clone()))
                .bind(("data", row))
                .await
                .map_err(|e| Self::port_err("create identity proposal", e))?;

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
        let mut proposal_resp = self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.proposal_table.clone()))
            .bind(("id", request.proposal_id.clone()))
            .await
            .map_err(|e| Self::port_err("load identity proposal", e))?;

        let proposal_row: Option<ProposalRow> = proposal_resp
            .take(0)
            .map_err(|e| Self::port_err("decode identity proposal", e))?;

        let Some(row) = proposal_row else {
            return Ok(CommitEntityUpdateResponse {
                committed: false,
                code: Some(CommitOutcomeCode::NotFound),
                rationale: Some("proposal not found".to_string()),
                ..Default::default()
            });
        };

        let mut proposal = EntityUpdateProposalRecord::try_from(row)?;
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
            self.db
                .query("UPSERT type::record($table, $id) CONTENT $data")
                .bind(("table", self.proposal_table.clone()))
                .bind(("id", proposal.proposal_id.clone()))
                .bind(("data", ProposalRow::from(proposal)))
                .await
                .map_err(|e| Self::port_err("expire identity proposal", e))?;

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
                let mut rel_resp = self
                    .db
                    .query("SELECT * FROM type::record($table, $id)")
                    .bind(("table", self.relationship_table.clone()))
                    .bind(("id", proposal.entity_id.clone()))
                    .await
                    .map_err(|e| Self::port_err("load relationship for proposal", e))?;
                let rel_row: Option<RelationshipRow> = rel_resp
                    .take(0)
                    .map_err(|e| Self::port_err("decode relationship for proposal", e))?;

                let Some(rel_row) = rel_row else {
                    return Ok(CommitEntityUpdateResponse {
                        committed: false,
                        code: Some(CommitOutcomeCode::NotFound),
                        rationale: Some("target relationship not found".to_string()),
                        ..Default::default()
                    });
                };

                let mut relationship = RelationshipEntity::try_from(rel_row)?;
                if relationship.version != request.expected_version {
                    proposal.state = ProposalState::Rejected;
                    proposal.updated_at = Utc::now();
                    self.db
                        .query("UPSERT type::record($table, $id) CONTENT $data")
                        .bind(("table", self.proposal_table.clone()))
                        .bind(("id", proposal.proposal_id.clone()))
                        .bind(("data", ProposalRow::from(proposal)))
                        .await
                        .map_err(|e| Self::port_err("mark stale proposal", e))?;

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
                    match self.apply_relationship_patch(&mut relationship, &proposal.patch).await {
                        Ok(value) => value,
                        Err(err) => {
                            proposal.state = ProposalState::Rejected;
                            proposal.updated_at = Utc::now();
                            self.db
                                .query("UPSERT type::record($table, $id) CONTENT $data")
                                .bind(("table", self.proposal_table.clone()))
                                .bind(("id", proposal.proposal_id.clone()))
                                .bind(("data", ProposalRow::from(proposal)))
                                .await
                                .map_err(|e| Self::port_err("reject invalid proposal", e))?;

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

                self.db
                    .query("UPSERT type::record($table, $id) CONTENT $data")
                    .bind(("table", self.relationship_table.clone()))
                    .bind(("id", relationship_id.clone()))
                    .bind(("data", RelationshipRow::from(relationship.clone())))
                    .await
                    .map_err(|e| Self::port_err("save committed relationship", e))?;

                self.save_relationship_version(&relationship).await?;

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
                    self.db
                        .query("UPSERT type::record($table, $id) CONTENT $data")
                        .bind(("table", self.transition_table.clone()))
                        .bind(("id", event.event_id.clone()))
                        .bind(("data", TransitionRow::from(event)))
                        .await
                        .map_err(|e| Self::port_err("save relationship transition", e))?;
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
                self.db
                    .query("UPSERT type::record($table, $id) CONTENT $data")
                    .bind(("table", self.proposal_table.clone()))
                    .bind(("id", proposal.proposal_id.clone()))
                    .bind(("data", ProposalRow::from(proposal)))
                    .await
                    .map_err(|e| Self::port_err("mark proposal committed", e))?;

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
                rationale: Some("commit path for this entity type is not implemented yet".to_string()),
                ..Default::default()
            }),
        }
    }

    async fn list_entity_history(
        &self,
        request: &ListEntityHistoryRequest,
    ) -> Result<ListEntityHistoryResponse> {
        let mut proposal_resp = self
            .db
            .query(
                "SELECT * FROM type::table($table) WHERE entity_type = $entity_type AND entity_id = $entity_id ORDER BY created_at DESC LIMIT $limit",
            )
            .bind(("table", self.proposal_table.clone()))
            .bind((
                "entity_type",
                identity_entity_type_str(request.entity_type.clone()).to_string(),
            ))
            .bind(("entity_id", request.entity_id.clone()))
            .bind(("limit", request.limit.max(1)))
            .await
            .map_err(|e| Self::port_err("list identity proposals", e))?;

        let proposal_rows: Vec<ProposalRow> = proposal_resp
            .take(0)
            .map_err(|e| Self::port_err("decode identity proposals", e))?;
        let mut proposals = Vec::with_capacity(proposal_rows.len());
        for row in proposal_rows {
            proposals.push(EntityUpdateProposalRecord::try_from(row)?);
        }

        let transitions = if request.entity_type == IdentityEntityType::RelationshipEntity {
            let mut transition_resp = self
                .db
                .query(
                    "SELECT * FROM type::table($table) WHERE relationship_id = $relationship_id ORDER BY occurred_at DESC LIMIT $limit",
                )
                .bind(("table", self.transition_table.clone()))
                .bind(("relationship_id", request.entity_id.clone()))
                .bind(("limit", request.limit.max(1)))
                .await
                .map_err(|e| Self::port_err("list relationship transitions", e))?;

            let transition_rows: Vec<TransitionRow> = transition_resp
                .take(0)
                .map_err(|e| Self::port_err("decode relationship transitions", e))?;
            let mut events = Vec::with_capacity(transition_rows.len());
            for row in transition_rows {
                events.push(RelationshipTransitionEvent::try_from(row)?);
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
                let mut version_resp = self
                    .db
                    .query("SELECT * FROM type::record($table, $id)")
                    .bind(("table", self.relationship_version_table.clone()))
                    .bind(("id", version_id))
                    .await
                    .map_err(|e| Self::port_err("load relationship version", e))?;

                let version_row: Option<RelationshipVersionRow> = version_resp
                    .take(0)
                    .map_err(|e| Self::port_err("decode relationship version", e))?;

                let Some(version_row) = version_row else {
                    return Ok(RollbackEntityVersionResponse {
                        rolled_back: false,
                        rationale: Some("target version not found".to_string()),
                        ..Default::default()
                    });
                };

                let mut restored = RelationshipEntity::try_from(version_row.snapshot)?;
                restored.version += 1;
                restored.updated_at = Utc::now();
                restored.last_transition_reason = Some(format!("rollback: {}", request.reason));
                restored.transition_receipt_id = Some(Self::now_id("rcpt_rollback"));

                self.db
                    .query("UPSERT type::record($table, $id) CONTENT $data")
                    .bind(("table", self.relationship_table.clone()))
                    .bind(("id", restored.relationship_id.clone()))
                    .bind(("data", RelationshipRow::from(restored.clone())))
                    .await
                    .map_err(|e| Self::port_err("save rollback relationship", e))?;

                self.save_relationship_version(&restored).await?;

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
    use chrono::{Duration, Utc};
    use serde_json::json;
    use surrealdb::engine::any::Any;
    use surrealdb::Surreal;

    use super::SurrealIdentityMemoryStore;
    use crate::ports::outbound::memory::identity_memory_models::{
        CommitEntityUpdateRequest, CommitOutcomeCode, EntityRef, GetIdentityContextRequest,
        IdentityEntityType, ListEntityHistoryRequest, PersonaEntity, ProposeEntityUpdateRequest,
        ProposalState, RelationshipEntity, RelationshipStatus, UpdateSource, UserEntity,
    };
    use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

    async fn new_store() -> SurrealIdentityMemoryStore {
        let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
        db.use_ns("test")
            .use_db("identity")
            .await
            .expect("ns/db select should succeed");
        SurrealIdentityMemoryStore::ensure_schema_for_db(&db)
            .await
            .expect("schema bootstrap should succeed");
        SurrealIdentityMemoryStore::new(db)
    }

    async fn relationship_version_count(
        store: &SurrealIdentityMemoryStore,
        relationship_id: &str,
    ) -> usize {
        let mut resp = store
            .db
            .query("SELECT * FROM type::table($table) WHERE relationship_id = $relationship_id")
            .bind(("table", store.relationship_version_table.clone()))
            .bind(("relationship_id", relationship_id.to_string()))
            .await
            .expect("relationship version query should succeed");

        let rows: Vec<serde_json::Value> = resp
            .take(0)
            .expect("relationship version decode should succeed");
        rows.len()
    }

    #[tokio::test]
    async fn surreal_identity_schema_bootstrap_is_idempotent() {
        let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
        db.use_ns("test")
            .use_db("identity_schema_bootstrap")
            .await
            .expect("ns/db select should succeed");

        SurrealIdentityMemoryStore::ensure_schema_for_db(&db)
            .await
            .expect("first schema bootstrap should succeed");
        SurrealIdentityMemoryStore::ensure_schema_for_db(&db)
            .await
            .expect("second schema bootstrap should be idempotent");

        for table in [
            "identity_persona",
            "identity_user",
            "identity_channel_profile",
            "identity_policy_profile",
            "identity_relationship",
            "identity_relationship_version",
            "identity_entity_update_proposal",
            "identity_relationship_transition",
        ] {
            db.query(format!("INFO FOR TABLE {table}"))
                .await
                .unwrap_or_else(|_| panic!("table should exist: {table}"));
        }

        for table_stmt in [
            "DEFINE TABLE identity_persona SCHEMAFULL",
            "DEFINE TABLE identity_user SCHEMAFULL",
            "DEFINE TABLE identity_channel_profile SCHEMAFULL",
            "DEFINE TABLE identity_policy_profile SCHEMAFULL",
            "DEFINE TABLE identity_relationship SCHEMAFULL",
            "DEFINE TABLE identity_relationship_version SCHEMAFULL",
            "DEFINE TABLE identity_entity_update_proposal SCHEMAFULL",
            "DEFINE TABLE identity_relationship_transition SCHEMAFULL",
        ] {
            assert!(
                super::IDENTITY_SCHEMA_STATEMENTS.contains(&table_stmt),
                "schema bootstrap should define schemafull table: {table_stmt}"
            );
        }
    }

    #[tokio::test]
    async fn surreal_identity_store_split_and_commit_flow_works() {
        let store = new_store().await;

        store
            .upsert_persona(PersonaEntity {
                persona_id: "p1".to_string(),
                display_name: "Medousa".to_string(),
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            })
            .await
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
            .await
            .expect("user upsert should work");

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
            .await
            .expect("relationship upsert should work");

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
            .expect("proposal should succeed");

        assert!(proposal.split_patch);
        assert_eq!(proposal.proposal_ids.len(), 2);

        let first_commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposal.proposal_ids[0].clone(),
                expected_version: 1,
                approver: None,
            })
            .await
            .expect("first commit should run");

        assert!(first_commit.committed);

        let second_commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposal.proposal_ids[1].clone(),
                expected_version: 2,
                approver: Some("owner".to_string()),
            })
            .await
            .expect("second commit should run");

        assert!(second_commit.committed);

        let context = store
            .get_identity_context(&GetIdentityContextRequest {
                user_id: "u1".to_string(),
                persona_id: "p1".to_string(),
                channel_id: "none".to_string(),
                relationship_limit: 10,
            })
            .await
            .expect("identity context should load");

        assert_eq!(context.relationships.len(), 1);
        assert_eq!(context.relationships[0].version, 3);
    }

    #[tokio::test]
    async fn surreal_commit_returns_stale_state_on_version_conflict() {
        let store = new_store().await;

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-stale-surreal".to_string(),
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
            .await
            .expect("seed relationship should work");

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-stale-surreal".to_string(),
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
        assert_eq!(commit.code, Some(CommitOutcomeCode::StaleState));

        let history = store
            .list_entity_history(&ListEntityHistoryRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-stale-surreal".to_string(),
                limit: 10,
            })
            .await
            .expect("entity history should load");
        assert_eq!(history.proposals.len(), 1);
        assert_eq!(history.proposals[0].state, ProposalState::Rejected);
    }

    #[tokio::test]
    async fn surreal_commit_requires_approval_for_privileged_patch_without_approver() {
        let store = new_store().await;

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-approval-surreal".to_string(),
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
            .await
            .expect("seed relationship should work");

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-approval-surreal".to_string(),
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

        let commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: proposed.proposal_ids[0].clone(),
                expected_version: 1,
                approver: None,
            })
            .await
            .expect("commit call should succeed");

        assert!(!commit.committed);
        assert_eq!(commit.code, Some(CommitOutcomeCode::ApprovalRequired));

        let context = store
            .get_identity_context(&GetIdentityContextRequest {
                user_id: "u1".to_string(),
                persona_id: "p1".to_string(),
                channel_id: "none".to_string(),
                relationship_limit: 10,
            })
            .await
            .expect("identity context should load");

        let rel = context
            .relationships
            .iter()
            .find(|value| value.relationship_id == "rel-approval-surreal")
            .expect("relationship should exist");
        assert_eq!(rel.version, 1);
    }

    #[tokio::test]
    async fn surreal_commit_returns_not_found_for_unknown_proposal_id() {
        let store = new_store().await;

        let commit = store
            .commit_entity_update(&CommitEntityUpdateRequest {
                proposal_id: "proposal-does-not-exist".to_string(),
                expected_version: 1,
                approver: None,
            })
            .await
            .expect("commit call should succeed");

        assert!(!commit.committed);
        assert_eq!(commit.code, Some(CommitOutcomeCode::NotFound));
    }

    #[tokio::test]
    async fn surreal_commit_returns_expired_proposal_when_commit_happens_after_expiry() {
        let store = new_store().await;

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-expired-surreal".to_string(),
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
            .await
            .expect("seed relationship should work");

        let versions_before = relationship_version_count(&store, "rel-expired-surreal").await;

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-expired-surreal".to_string(),
                patch: json!({ "recency_score": 0.9 }),
                source: UpdateSource::ModelInferred,
                confidence: 0.8,
                reason: "expired test".to_string(),
                actor: "model".to_string(),
                receipt_id: None,
                expires_at: Some(Utc::now() - Duration::seconds(1)),
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
        assert_eq!(commit.code, Some(CommitOutcomeCode::ExpiredProposal));

        let context = store
            .get_identity_context(&GetIdentityContextRequest {
                user_id: "u1".to_string(),
                persona_id: "p1".to_string(),
                channel_id: "none".to_string(),
                relationship_limit: 10,
            })
            .await
            .expect("identity context should load");

        let rel = context
            .relationships
            .iter()
            .find(|value| value.relationship_id == "rel-expired-surreal")
            .expect("relationship should exist");
        assert_eq!(rel.version, 1);
        assert!((rel.recency_score - 0.8).abs() < f32::EPSILON);

        let history = store
            .list_entity_history(&ListEntityHistoryRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-expired-surreal".to_string(),
                limit: 10,
            })
            .await
            .expect("entity history should load");
        assert_eq!(history.proposals.len(), 1);
        assert_eq!(history.proposals[0].state, ProposalState::Expired);

        let versions_after = relationship_version_count(&store, "rel-expired-surreal").await;
        assert_eq!(versions_before, versions_after);
    }

    #[tokio::test]
    async fn surreal_commit_returns_policy_denied_and_does_not_mutate_relationship() {
        let store = new_store().await;

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-policy-denied-surreal".to_string(),
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
            .await
            .expect("seed relationship should work");

        let versions_before = relationship_version_count(&store, "rel-policy-denied-surreal").await;

        let proposed = store
            .propose_entity_update(&ProposeEntityUpdateRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-policy-denied-surreal".to_string(),
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
        assert_eq!(commit.code, Some(CommitOutcomeCode::PolicyDenied));

        let history = store
            .list_entity_history(&ListEntityHistoryRequest {
                entity_type: IdentityEntityType::RelationshipEntity,
                entity_id: "rel-policy-denied-surreal".to_string(),
                limit: 20,
            })
            .await
            .expect("entity history should load");
        assert_eq!(history.proposals.len(), 1);
        assert_eq!(history.proposals[0].state, ProposalState::Rejected);
        assert_eq!(history.transitions.len(), 0);

        let versions_after = relationship_version_count(&store, "rel-policy-denied-surreal").await;
        assert_eq!(versions_before, versions_after);
    }

    #[tokio::test]
    async fn surreal_replacement_requires_derived_from_when_revoked_predecessor_exists() {
        let store = new_store().await;

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-sur-old".to_string(),
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
            .await
            .expect("seed revoked relationship should work");

        let result = store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-sur-new".to_string(),
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
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn surreal_replacement_succeeds_with_valid_derived_from() {
        let store = new_store().await;

        store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-sur-old-2".to_string(),
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
            .await
            .expect("seed revoked relationship should work");

        let result = store
            .upsert_relationship(RelationshipEntity {
                relationship_id: "rel-sur-new-2".to_string(),
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
                derived_from_relationship_id: Some("rel-sur-old-2".to_string()),
                last_transition_reason: None,
                transition_receipt_id: None,
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .await;

        assert!(result.is_ok());
    }
}
