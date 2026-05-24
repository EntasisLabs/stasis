use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum IdentityEntityType {
    PersonaEntity,
    UserEntity,
    ChannelProfileEntity,
    PolicyProfileEntity,
    RelationshipEntity,
}

impl Default for IdentityEntityType {
    fn default() -> Self {
        Self::RelationshipEntity
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum UpdateTier {
    AutoCommit,
    ConfirmRequired,
    ApprovalRequired,
}

impl Default for UpdateTier {
    fn default() -> Self {
        Self::AutoCommit
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ProposalState {
    Proposed,
    Committed,
    Rejected,
    Expired,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum UpdateSource {
    UserDirect,
    ModelInferred,
    SystemEvent,
}

impl Default for UpdateSource {
    fn default() -> Self {
        Self::ModelInferred
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RelationshipStatus {
    Proposed,
    Active,
    Suspended,
    Deprecated,
    Revoked,
}

impl Default for RelationshipStatus {
    fn default() -> Self {
        Self::Proposed
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EntityRef {
    pub entity_type: String,
    pub entity_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EscalationPolicy {
    pub mode: Option<String>,
    pub fallback: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct InterruptionPolicy {
    pub quiet_hours: Option<String>,
    pub allow_urgent_only: Option<bool>,
    pub urgent_threshold: Option<f32>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AutonomyScope {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
    pub approval_required: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PersonaEntity {
    pub persona_id: String,
    pub display_name: String,
    pub status: String,
    pub version: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UserEntity {
    pub user_id: String,
    pub timezone: String,
    pub language_variant: Option<String>,
    pub status: String,
    pub version: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChannelProfileEntity {
    pub channel_id: String,
    pub channel_type: String,
    pub proactive_allowed: bool,
    pub status: String,
    pub version: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PolicyProfileEntity {
    pub policy_profile_id: String,
    pub graph_max_depth: usize,
    pub trust_delta_max_per_window: f32,
    pub status: String,
    pub version: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RelationshipEntity {
    pub relationship_id: String,
    pub source_entity_ref: EntityRef,
    pub target_entity_ref: EntityRef,
    pub relationship_kind: String,
    pub status: RelationshipStatus,
    pub trust_level: f32,
    pub confidence: f32,
    pub strength_score: f32,
    pub recency_score: f32,
    pub autonomy_scope: AutonomyScope,
    pub approval_profile_id: Option<String>,
    pub interruption_policy: InterruptionPolicy,
    pub escalation_policy: EscalationPolicy,
    pub policy_tags: Vec<String>,
    pub provenance: UpdateSource,
    pub parent_relationship_id: Option<String>,
    pub governing_relationship_ids: Vec<String>,
    pub derived_from_relationship_id: Option<String>,
    pub last_transition_reason: Option<String>,
    pub transition_receipt_id: Option<String>,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FlattenedPolicyClaim {
    pub claim_id: String,
    pub source_relationship_ids: Vec<String>,
    pub summary: String,
    pub confidence: f32,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GetIdentityContextRequest {
    pub user_id: String,
    pub persona_id: String,
    pub channel_id: String,
    pub relationship_limit: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GetIdentityContextResponse {
    pub persona: Option<PersonaEntity>,
    pub user: Option<UserEntity>,
    pub channel: Option<ChannelProfileEntity>,
    pub relationships: Vec<RelationshipEntity>,
    pub policy_profiles: Vec<PolicyProfileEntity>,
    pub graph_depth_used: usize,
    pub flattened_claims: Vec<FlattenedPolicyClaim>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProposeEntityUpdateRequest {
    pub entity_type: IdentityEntityType,
    pub entity_id: String,
    pub patch: Value,
    pub source: UpdateSource,
    pub confidence: f32,
    pub reason: String,
    pub actor: String,
    pub receipt_id: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProposeEntityUpdateResponse {
    pub proposal_ids: Vec<String>,
    pub tiers: Vec<UpdateTier>,
    pub requires_approval: bool,
    pub split_patch: bool,
    pub policy_notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityUpdateProposalRecord {
    pub proposal_id: String,
    pub entity_type: IdentityEntityType,
    pub entity_id: String,
    pub patch: Value,
    pub tier: UpdateTier,
    pub source: UpdateSource,
    pub confidence: f32,
    pub reason: String,
    pub state: ProposalState,
    pub approver: Option<String>,
    pub actor: String,
    pub receipt_id: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommitEntityUpdateRequest {
    pub proposal_id: String,
    pub expected_version: i32,
    pub approver: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CommitOutcomeCode {
    Ok,
    StaleState,
    ApprovalRequired,
    PolicyDenied,
    InvalidPatch,
    ExpiredProposal,
    NotFound,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommitEntityUpdateResponse {
    pub committed: bool,
    pub code: Option<CommitOutcomeCode>,
    pub entity_type: Option<IdentityEntityType>,
    pub entity_id: Option<String>,
    pub new_version: Option<i32>,
    pub receipt_id: Option<String>,
    pub transition_event_id: Option<String>,
    pub sttp_bridge_node: Option<String>,
    pub sttp_bridge_reason: Option<String>,
    pub rationale: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationshipTransitionEvent {
    pub event_id: String,
    pub relationship_id: String,
    pub from_status: Option<RelationshipStatus>,
    pub to_status: RelationshipStatus,
    pub reason: String,
    pub actor: String,
    pub receipt_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListEntityHistoryRequest {
    pub entity_type: IdentityEntityType,
    pub entity_id: String,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListEntityHistoryResponse {
    pub proposals: Vec<EntityUpdateProposalRecord>,
    pub transitions: Vec<RelationshipTransitionEvent>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RollbackEntityVersionRequest {
    pub entity_type: IdentityEntityType,
    pub entity_id: String,
    pub target_version: i32,
    pub reason: String,
    pub approver: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RollbackEntityVersionResponse {
    pub rolled_back: bool,
    pub new_version: Option<i32>,
    pub rollback_receipt_id: Option<String>,
    pub rationale: Option<String>,
}
