use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::application::orchestration::prompt_pipeline::PromptExecutionContext;
use crate::application::orchestration::tool_loop_pipeline::{
    ToolCallMode, ToolInvocation, ToolLoopExecutionRequest, ToolLoopPipeline,
};
use crate::domain::errors::{Result, StasisError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentIdentity {
    pub agent_id: String,
    pub thread_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AgentTurnExecutionPolicy {
    pub tool_call_mode: ToolCallMode,
}

impl Default for AgentTurnExecutionPolicy {
    fn default() -> Self {
        Self {
            tool_call_mode: ToolCallMode::Auto,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AgentTurnExecutionRequest {
    pub identity: AgentIdentity,
    pub user_prompt: String,
    pub system_prompt: Option<String>,
    pub context: PromptExecutionContext,
    pub tool_name: String,
    pub tool_input: Value,
    pub policy: AgentTurnExecutionPolicy,
}

#[derive(Clone, Debug)]
pub struct AgentTurnExecutionResponse {
    pub text: String,
    pub metadata: PromptExecutionContext,
    pub agent_id: String,
    pub thread_id: Option<String>,
    pub tool_name: String,
    pub tool_output: Value,
    pub tool_invocations: Vec<ToolInvocation>,
    pub rounds_executed: usize,
    pub termination_reason: String,
}

#[derive(Clone, Debug)]
pub struct AgentParticipant {
    pub agent_id: String,
    pub system_prompt: Option<String>,
    pub tool_name: String,
    pub tool_input: Value,
}

#[derive(Clone, Debug)]
pub struct AgentSessionRunRequest {
    pub thread_id: Option<String>,
    pub initial_user_prompt: String,
    pub participants: Vec<AgentParticipant>,
    pub context: PromptExecutionContext,
    pub max_turns_cap: usize,
    pub policy: AgentTurnExecutionPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentTurnRecord {
    pub turn_number: usize,
    pub agent_id: String,
    pub response_text: String,
    pub tool_name: String,
    pub rounds_executed: usize,
    pub termination_reason: String,
}

#[derive(Clone, Debug)]
pub struct AgentSessionRunResponse {
    pub thread_id: Option<String>,
    pub turns: Vec<AgentTurnRecord>,
    pub transcript: Vec<String>,
    pub terminated: bool,
}

#[derive(Clone)]
pub struct AgentSessionPipeline {
    tool_loop_pipeline: ToolLoopPipeline,
}

#[derive(Clone)]
struct SessionSharedInputs {
    thread_id: Arc<Option<String>>,
    context: Arc<PromptExecutionContext>,
    policy: Arc<AgentTurnExecutionPolicy>,
    participants_by_id: Arc<HashMap<String, Arc<AgentParticipant>>>,
}

impl SessionSharedInputs {
    fn thread_id_clone(&self) -> Option<String> {
        (*self.thread_id).clone()
    }

    fn context_clone(&self) -> PromptExecutionContext {
        (*self.context).clone()
    }

    fn policy_clone(&self) -> AgentTurnExecutionPolicy {
        (*self.policy).clone()
    }
}

impl AgentSessionPipeline {
    pub fn new(tool_loop_pipeline: ToolLoopPipeline) -> Self {
        Self { tool_loop_pipeline }
    }

    pub async fn execute_turn(
        &self,
        request: AgentTurnExecutionRequest,
    ) -> Result<AgentTurnExecutionResponse> {
        let AgentTurnExecutionRequest {
            identity,
            user_prompt,
            system_prompt,
            context,
            tool_name,
            tool_input,
            policy,
        } = request;
        let AgentIdentity {
            agent_id,
            thread_id,
        } = identity;

        let loop_request = ToolLoopExecutionRequest {
            user_prompt,
            system_prompt,
            context,
            tool_name,
            tool_input,
            tool_call_mode: policy.tool_call_mode,
        };

        let response = self.tool_loop_pipeline.execute(loop_request).await?;

        Ok(AgentTurnExecutionResponse {
            text: response.text,
            metadata: response.metadata,
            agent_id,
            thread_id,
            tool_name: response.tool_name,
            tool_output: response.tool_output,
            tool_invocations: response.tool_invocations,
            rounds_executed: response.rounds_executed,
            termination_reason: response.termination_reason,
        })
    }
}

// P-D hook: strategy contract for group-chat style next-agent routing.
pub trait AgentSelectionStrategy: Send + Sync {
    fn select_next_agent(
        &self,
        participants: &[String],
        thread_id: Option<&str>,
        transcript: &[String],
    ) -> Result<String>;
}

// P-D hook: strategy contract for deciding when a coordinated session ends.
pub trait AgentTerminationStrategy: Send + Sync {
    fn should_terminate(&self, turn_count: usize, last_response: &str) -> Result<bool>;
}

#[derive(Default)]
pub struct RoundRobinSelectionStrategy {
    cursor: AtomicUsize,
}

impl RoundRobinSelectionStrategy {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AgentSelectionStrategy for RoundRobinSelectionStrategy {
    fn select_next_agent(
        &self,
        participants: &[String],
        _thread_id: Option<&str>,
        _transcript: &[String],
    ) -> Result<String> {
        if participants.is_empty() {
            return Err(StasisError::PortFailure(
                "policy violation: agent session requires at least one participant".to_string(),
            ));
        }

        let index = self.cursor.fetch_add(1, Ordering::SeqCst);
        let selected = participants
            .get(index % participants.len())
            .ok_or_else(|| StasisError::PortFailure("failed to select participant".to_string()))?;

        Ok(selected.clone())
    }
}

pub struct MaxTurnsTerminationStrategy {
    max_turns: usize,
    done_token: Option<String>,
}

impl MaxTurnsTerminationStrategy {
    pub fn new(max_turns: usize) -> Self {
        Self {
            max_turns: max_turns.max(1),
            done_token: None,
        }
    }

    pub fn with_done_token(mut self, token: impl Into<String>) -> Self {
        self.done_token = Some(token.into());
        self
    }
}

impl AgentTerminationStrategy for MaxTurnsTerminationStrategy {
    fn should_terminate(&self, turn_count: usize, last_response: &str) -> Result<bool> {
        if turn_count >= self.max_turns {
            return Ok(true);
        }

        if let Some(token) = &self.done_token {
            return Ok(last_response.contains(token));
        }

        Ok(false)
    }
}

#[derive(Clone)]
pub struct AgentSessionCoordinator {
    pipeline: AgentSessionPipeline,
    selection_strategy: Arc<dyn AgentSelectionStrategy>,
    termination_strategy: Arc<dyn AgentTerminationStrategy>,
}

impl AgentSessionCoordinator {
    pub fn new(
        pipeline: AgentSessionPipeline,
        selection_strategy: Arc<dyn AgentSelectionStrategy>,
        termination_strategy: Arc<dyn AgentTerminationStrategy>,
    ) -> Self {
        Self {
            pipeline,
            selection_strategy,
            termination_strategy,
        }
    }

    pub async fn run_session(
        &self,
        request: AgentSessionRunRequest,
    ) -> Result<AgentSessionRunResponse> {
        let AgentSessionRunRequest {
            thread_id,
            initial_user_prompt,
            participants,
            context,
            max_turns_cap,
            policy,
        } = request;

        if participants.is_empty() {
            return Err(StasisError::PortFailure(
                "policy violation: agent session requires at least one participant".to_string(),
            ));
        }

        if initial_user_prompt.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "policy violation: agent session initial_user_prompt must be non-empty".to_string(),
            ));
        }

        let max_turns_cap = max_turns_cap.max(1);
        let participant_ids: Vec<String> = participants
            .iter()
            .map(|participant| participant.agent_id.clone())
            .collect();
        let participants_by_id = participants
            .iter()
            .map(|participant| (participant.agent_id.clone(), Arc::new(participant.clone())))
            .collect::<HashMap<_, _>>();
        let shared_inputs = SessionSharedInputs {
            thread_id: Arc::new(thread_id.clone()),
            context: Arc::new(context),
            policy: Arc::new(policy),
            participants_by_id: Arc::new(participants_by_id),
        };

        let mut transcript = Vec::with_capacity(max_turns_cap + 1);
        let mut transcript_text = String::with_capacity(initial_user_prompt.len() + 32);
        transcript_text.push_str("user: ");
        transcript_text.push_str(&initial_user_prompt);
        transcript.push(transcript_text.clone());
        let mut last_prompt = initial_user_prompt.clone();
        let mut turns = Vec::new();
        let mut terminated = false;

        for turn_index in 0..max_turns_cap {
            let selected_agent_id = self.selection_strategy.select_next_agent(
                &participant_ids,
                shared_inputs.thread_id.as_deref(),
                &transcript,
            )?;

            let participant = shared_inputs
                .participants_by_id
                .get(&selected_agent_id)
                .cloned()
                .ok_or_else(|| {
                    StasisError::PortFailure(format!(
                        "policy violation: selected participant '{}' not found in session",
                        selected_agent_id
                    ))
                })?;

            let turn_request = AgentTurnExecutionRequest {
                identity: AgentIdentity {
                    agent_id: selected_agent_id.clone(),
                    thread_id: shared_inputs.thread_id_clone(),
                },
                user_prompt: last_prompt.clone(),
                system_prompt: participant.system_prompt.clone(),
                context: shared_inputs.context_clone(),
                tool_name: participant.tool_name.clone(),
                tool_input: participant.tool_input.clone(),
                policy: shared_inputs.policy_clone(),
            };

            let turn_response = self.pipeline.execute_turn(turn_request).await?;
            let response_text = turn_response.text;
            let transcript_line = format!("{}: {}", selected_agent_id, response_text);
            transcript_text.push('\n');
            transcript_text.push_str(&transcript_line);
            transcript.push(transcript_line);
            last_prompt = build_session_continue_prompt(&transcript_text);

            let record = AgentTurnRecord {
                turn_number: turn_index + 1,
                agent_id: selected_agent_id,
                response_text,
                tool_name: turn_response.tool_name,
                rounds_executed: turn_response.rounds_executed,
                termination_reason: turn_response.termination_reason,
            };
            let should_terminate = self
                .termination_strategy
                .should_terminate(turn_index + 1, &record.response_text)?;
            turns.push(record);

            if should_terminate {
                terminated = true;
                break;
            }
        }

        Ok(AgentSessionRunResponse {
            thread_id,
            turns,
            transcript,
            terminated,
        })
    }
}

fn build_session_continue_prompt(transcript: &str) -> String {
    let mut prompt = String::with_capacity(transcript.len() + 96);
    prompt.push_str("Session transcript so far:\n");
    prompt.push_str(transcript);
    prompt.push_str("\n\nContinue the collaboration from your role.");
    prompt
}
