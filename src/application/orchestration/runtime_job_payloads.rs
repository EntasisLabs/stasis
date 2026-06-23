use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::errors::{Result, StasisError};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolCallMode {
    Auto,
    Strict,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryFallbackPolicyPayload {
    Never,
    OnEmpty,
    Always,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStrictnessModePayload {
    Precision,
    Balanced,
    Recall,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStoreModePayload {
    Disabled,
    SummaryOnly,
    Full,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryMetricRangePayload {
    pub min: Option<f32>,
    pub max: Option<f32>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryFilterPayload {
    pub has_embedding: Option<bool>,
    pub embedding_model: Option<String>,
    pub psi: Option<MemoryMetricRangePayload>,
    pub rho: Option<MemoryMetricRangePayload>,
    pub kappa: Option<MemoryMetricRangePayload>,
    pub text_contains: Option<String>,
    pub tags_contains: Option<Vec<String>>,
    pub has_tag: Option<String>,
    pub indexed_tags: Option<Vec<String>>,
    pub tag_prefix: Option<String>,
    pub has_semantic_links: Option<bool>,
    pub link_rel: Option<String>,
    pub link_target: Option<String>,
    pub links_to_ref: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryPolicyPayload {
    pub tenant_id: Option<String>,
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub alpha: Option<f32>,
    pub beta: Option<f32>,
    pub gamma: Option<f32>,
    pub fallback_policy: Option<MemoryFallbackPolicyPayload>,
    pub strictness: Option<MemoryStrictnessModePayload>,
    pub query_text: Option<String>,
    pub include_explain: Option<bool>,
    pub store_mode: Option<MemoryStoreModePayload>,
    #[serde(default)]
    pub filter: MemoryFilterPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSessionParticipantPayload {
    pub agent_id: String,
    pub system_prompt: Option<String>,
    pub tool_name: String,
    pub tool_input: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSessionJobPayload {
    pub thread_id: Option<String>,
    pub initial_user_prompt: String,
    pub participants: Vec<AgentSessionParticipantPayload>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub max_turns: Option<usize>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl AgentSessionJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode agent-session payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentTurnJobPayload {
    pub agent_id: String,
    pub thread_id: Option<String>,
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub tool_name: String,
    pub tool_input: Option<Value>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl AgentTurnJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode agent-turn payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolLoopJobPayload {
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub tool_name: String,
    pub tool_input: Option<Value>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl ToolLoopJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode tool-loop payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptJobPayload {
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl PromptJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode prompt payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequentialStageJobPayload {
    pub stage_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequentialPatternJobPayload {
    pub thread_id: Option<String>,
    pub initial_user_prompt: String,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub stages: Vec<SequentialStageJobPayload>,
}

impl SequentialPatternJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!(
                "failed to encode sequential-pattern payload: {err}"
            ))
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrentBranchExecutionMode {
    #[default]
    Prompt,
    ToolLoop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConcurrentBranchJobPayload {
    pub branch_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub execution_mode: ConcurrentBranchExecutionMode,
    pub tool_name: Option<String>,
    pub tool_input: Option<Value>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConcurrentPatternJobPayload {
    pub thread_id: Option<String>,
    pub initial_user_prompt: String,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub merge_strategy: Option<String>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
    pub branches: Vec<ConcurrentBranchJobPayload>,
}

impl ConcurrentBranchJobPayload {
    pub fn prompt(
        branch_id: impl Into<String>,
        user_prompt_template: impl Into<String>,
    ) -> Self {
        Self {
            branch_id: branch_id.into(),
            user_prompt_template: user_prompt_template.into(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
            reasoning_effort: None,
            execution_mode: ConcurrentBranchExecutionMode::Prompt,
            tool_name: None,
            tool_input: None,
            tool_call_mode: None,
            memory_policy: None,
        }
    }

    pub fn tool_loop(
        branch_id: impl Into<String>,
        user_prompt_template: impl Into<String>,
        tool_name: impl Into<String>,
        tool_input: Option<Value>,
    ) -> Self {
        Self {
            branch_id: branch_id.into(),
            user_prompt_template: user_prompt_template.into(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
            reasoning_effort: None,
            execution_mode: ConcurrentBranchExecutionMode::ToolLoop,
            tool_name: Some(tool_name.into()),
            tool_input,
            tool_call_mode: None,
            memory_policy: None,
        }
    }
}

impl ConcurrentPatternJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!(
                "failed to encode concurrent-pattern payload: {err}"
            ))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandoffTurnJobPayload {
    pub actor_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandoffPatternJobPayload {
    pub thread_id: Option<String>,
    pub initial_user_prompt: String,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub turns: Vec<HandoffTurnJobPayload>,
}

impl HandoffPatternJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode handoff-pattern payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrchestratorRouteJobPayload {
    pub route_id: String,
    pub selector_keywords: Vec<String>,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrchestratorPatternJobPayload {
    pub thread_id: Option<String>,
    pub initial_user_prompt: String,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub routes: Vec<OrchestratorRouteJobPayload>,
}

impl OrchestratorPatternJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!(
                "failed to encode orchestrator-pattern payload: {err}"
            ))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryRecallJobPayload {
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl MemoryRecallJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-recall payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryFindJobPayload {
    pub tenant_id: Option<String>,
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub text_contains: Option<String>,
    pub tags_contains: Option<Vec<String>>,
    pub has_tag: Option<String>,
    pub indexed_tags: Option<Vec<String>>,
    pub tag_prefix: Option<String>,
    pub has_semantic_links: Option<bool>,
    pub link_rel: Option<String>,
    pub link_target: Option<String>,
    pub links_to_ref: Option<String>,
    pub sort_field: Option<String>,
    pub sort_direction: Option<String>,
}

impl MemoryFindJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-find payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryAggregateJobPayload {
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    pub max_groups: Option<usize>,
    pub max_nodes: Option<usize>,
}

impl MemoryAggregateJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-aggregate payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryTransformOperationPayload {
    EmbedBackfill,
    ReindexEmbeddings,
    EmbedTagBackfill,
    ReindexTagEmbeddings,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryTransformJobPayload {
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    pub operation: Option<MemoryTransformOperationPayload>,
    pub dry_run: Option<bool>,
    pub batch_size: Option<usize>,
    pub max_nodes: Option<usize>,
    pub provider_id: Option<String>,
    pub model: Option<String>,
}

impl MemoryTransformJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-transform payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRollupJobPayload {
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    pub max_days: Option<usize>,
    pub max_nodes: Option<usize>,
}

impl MemoryRollupJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-rollup payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemorySchemaJobPayload {}

impl MemorySchemaJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-schema payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEvictModePayload {
    BySyncKeys,
    ByNodeIds,
    ByFilter,
    PurgeSession,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEvictJobPayload {
    pub mode: Option<MemoryEvictModePayload>,
    pub tenant_id: Option<String>,
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    #[serde(default)]
    pub filter: MemoryFilterPayload,
    pub sync_keys: Option<Vec<String>>,
    pub node_ids: Option<Vec<String>>,
    pub dry_run: Option<bool>,
    pub force: Option<bool>,
    pub max_nodes: Option<usize>,
    pub include_calibration: Option<bool>,
    pub include_checkpoints: Option<bool>,
}

impl MemoryEvictJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-evict payload: {err}"))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryGraphJobPayload {
    pub tenant_id: Option<String>,
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    #[serde(default)]
    pub filter: MemoryFilterPayload,
    pub include_lineage: Option<bool>,
    pub include_semantic: Option<bool>,
    pub include_session_topology: Option<bool>,
    pub rel: Option<String>,
    pub target_prefix: Option<String>,
    pub limit: Option<usize>,
}

impl MemoryGraphJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode memory-graph payload: {err}"))
        })
    }
}
