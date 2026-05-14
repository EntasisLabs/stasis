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
}

impl PromptJobPayload {
    pub fn to_payload_ref(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|err| StasisError::PortFailure(format!("failed to encode prompt payload: {err}")))
    }
}
