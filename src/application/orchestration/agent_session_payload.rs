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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryPolicyPayload {
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub alpha: Option<f32>,
    pub beta: Option<f32>,
    pub fallback_policy: Option<MemoryFallbackPolicyPayload>,
    pub strictness: Option<MemoryStrictnessModePayload>,
    pub query_text: Option<String>,
    pub include_explain: Option<bool>,
    pub store_mode: Option<MemoryStoreModePayload>,
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
    pub max_turns: Option<usize>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl AgentSessionJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode agent-session payload: {err}")))
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
    pub tool_name: String,
    pub tool_input: Option<Value>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl AgentTurnJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode agent-turn payload: {err}")))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolLoopJobPayload {
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub tool_name: String,
    pub tool_input: Option<Value>,
    pub tool_call_mode: Option<AgentToolCallMode>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl ToolLoopJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode tool-loop payload: {err}")))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptJobPayload {
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl PromptJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode prompt payload: {err}")))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryRecallJobPayload {
    pub memory_policy: Option<MemoryPolicyPayload>,
}

impl MemoryRecallJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode memory-recall payload: {err}")))
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
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode memory-aggregate payload: {err}")))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryTransformOperationPayload {
    EmbedBackfill,
    ReindexEmbeddings,
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
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode memory-transform payload: {err}")))
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
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode memory-rollup payload: {err}")))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemorySchemaJobPayload {}

impl MemorySchemaJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode memory-schema payload: {err}")))
    }
}
