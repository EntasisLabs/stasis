use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::application::orchestration::agent_session_pipeline::{
    AgentParticipant, AgentSessionCoordinator, AgentSessionPipeline, AgentSessionRunRequest,
    AgentTurnExecutionPolicy, MaxTurnsTerminationStrategy, RoundRobinSelectionStrategy,
};
use crate::application::orchestration::agent_session_payload::{
    AgentSessionJobPayload, AgentToolCallMode,
};
use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline,
};
use crate::application::orchestration::tool_loop_pipeline::{ToolCallMode, ToolLoopPipeline};
use crate::application::orchestration::tool_registry::ToolRegistry;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::ai_chat_client::AiChatClient;

const DEFAULT_MAX_SESSION_TURNS: usize = 3;

pub struct AgentSessionJobHandler {
    pipeline: AgentSessionPipeline,
}

impl AgentSessionJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>, tool_registry: Arc<dyn ToolRegistry>) -> Self {
        let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
        let tool_loop_pipeline = ToolLoopPipeline::new(prompt_pipeline, tool_registry);
        Self {
            pipeline: AgentSessionPipeline::new(tool_loop_pipeline),
        }
    }

    fn parse_payload(raw: &str) -> std::result::Result<AgentSessionJobPayload, String> {
        let payload: AgentSessionJobPayload = serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid agent-session payload json: {err}"))?;

        if payload.initial_user_prompt.trim().is_empty() {
            return Err(
                "policy violation: agent-session payload.initial_user_prompt must be non-empty"
                    .to_string(),
            );
        }
        if payload.participants.is_empty() {
            return Err(
                "policy violation: agent-session payload.participants must be non-empty".to_string(),
            );
        }

        for participant in &payload.participants {
            if participant.agent_id.trim().is_empty() {
                return Err(
                    "policy violation: agent-session participant.agent_id must be non-empty"
                        .to_string(),
                );
            }
            if participant.tool_name.trim().is_empty() {
                return Err(
                    "policy violation: agent-session participant.tool_name must be non-empty"
                        .to_string(),
                );
            }
        }

        Ok(payload)
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "stasis-agent-session",
            "status": "failure",
            "guardrail_code": "POLICY_VIOLATION",
            "policy_reason": message.clone(),
        })
        .to_string();

        JobExecutionOutcome::FatalFailure {
            message,
            execution_id: None,
            diagnostics: Some(diagnostics),
        }
    }
}

#[async_trait]
impl JobHandler for AgentSessionJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.agent_session"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let context = PromptExecutionContext {
            trace_id: Some(job.trace_id.clone()),
            correlation_id: Some(job.correlation_id.clone()),
            policy_profile: payload.policy_profile,
            model_hint: payload.model_hint,
        };

        let participants: Vec<AgentParticipant> = payload
            .participants
            .into_iter()
            .map(|participant| AgentParticipant {
                agent_id: participant.agent_id,
                system_prompt: participant.system_prompt,
                tool_name: participant.tool_name,
                tool_input: participant.tool_input.unwrap_or(Value::Null),
            })
            .collect();

        let coordinator = AgentSessionCoordinator::new(
            self.pipeline.clone(),
            Arc::new(RoundRobinSelectionStrategy::new()),
            Arc::new(MaxTurnsTerminationStrategy::new(
                payload.max_turns.unwrap_or(DEFAULT_MAX_SESSION_TURNS),
            )),
        );

        let run_request = AgentSessionRunRequest {
            thread_id: payload.thread_id,
            initial_user_prompt: payload.initial_user_prompt,
            participants,
            context,
            max_turns_cap: payload.max_turns.unwrap_or(DEFAULT_MAX_SESSION_TURNS),
            policy: AgentTurnExecutionPolicy {
                tool_call_mode: match payload.tool_call_mode {
                    Some(AgentToolCallMode::Strict) => ToolCallMode::Strict,
                    _ => ToolCallMode::Auto,
                },
            },
        };

        let response = match coordinator.run_session(run_request).await {
            Ok(response) => response,
            Err(err) => {
                let error_text = err.to_string();
                let is_policy_violation = error_text.contains("policy violation");
                let diagnostics = if is_policy_violation {
                    json!({
                        "provider": "stasis-agent-session",
                        "status": "failure",
                        "guardrail_code": "POLICY_VIOLATION",
                        "policy_reason": error_text,
                    })
                    .to_string()
                } else {
                    json!({
                        "provider": "stasis-agent-session",
                        "status": "failure",
                        "error": error_text,
                    })
                    .to_string()
                };

                return Ok(JobExecutionOutcome::FatalFailure {
                    message: error_text,
                    execution_id: None,
                    diagnostics: Some(diagnostics),
                });
            }
        };

        let participant_ids: Vec<String> = response
            .turns
            .iter()
            .map(|turn| turn.agent_id.clone())
            .collect();

        let diagnostics = json!({
            "provider": "stasis-agent-session",
            "status": "success",
            "thread_id": response.thread_id,
            "turn_count": response.turns.len(),
            "terminated": response.terminated,
            "participant_ids": participant_ids,
            "turns": response.turns,
            "transcript_preview": response.transcript.last().cloned(),
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: format!("sttp:agent-session:{}", job.id),
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}
