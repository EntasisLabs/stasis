use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::application::orchestration::agent_session_payload::{
    AgentToolCallMode, AgentTurnJobPayload,
};
use crate::application::orchestration::agent_session_pipeline::{
    AgentIdentity, AgentSessionPipeline, AgentTurnExecutionPolicy, AgentTurnExecutionRequest,
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

pub struct AgentTurnJobHandler {
    pipeline: AgentSessionPipeline,
}

impl AgentTurnJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>, tool_registry: Arc<dyn ToolRegistry>) -> Self {
        let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
        let tool_loop_pipeline = ToolLoopPipeline::new(prompt_pipeline, tool_registry);
        Self {
            pipeline: AgentSessionPipeline::new(tool_loop_pipeline),
        }
    }

    fn parse_payload(raw: &str) -> std::result::Result<AgentTurnJobPayload, String> {
        let payload: AgentTurnJobPayload = serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid agent-turn payload json: {err}"))?;

        if payload.agent_id.trim().is_empty() {
            return Err("policy violation: agent-turn payload.agent_id must be non-empty".to_string());
        }
        if payload.user_prompt.trim().is_empty() {
            return Err("policy violation: agent-turn payload.user_prompt must be non-empty".to_string());
        }
        if payload.tool_name.trim().is_empty() {
            return Err("policy violation: agent-turn payload.tool_name must be non-empty".to_string());
        }

        Ok(payload)
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "stasis-agent-turn",
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
impl JobHandler for AgentTurnJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.agent_turn"
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

        let request = AgentTurnExecutionRequest {
            identity: AgentIdentity {
                agent_id: payload.agent_id,
                thread_id: payload.thread_id,
            },
            user_prompt: payload.user_prompt,
            system_prompt: payload.system_prompt,
            context,
            tool_name: payload.tool_name,
            tool_input: payload.tool_input.unwrap_or(Value::Null),
            policy: AgentTurnExecutionPolicy {
                tool_call_mode: match payload.tool_call_mode {
                    Some(AgentToolCallMode::Strict) => ToolCallMode::Strict,
                    _ => ToolCallMode::Auto,
                },
            },
        };

        let response = match self.pipeline.execute_turn(request).await {
            Ok(response) => response,
            Err(err) => {
                let error_text = err.to_string();
                let is_policy_violation = error_text.contains("policy violation");
                let diagnostics = if is_policy_violation {
                    json!({
                        "provider": "stasis-agent-turn",
                        "status": "failure",
                        "guardrail_code": "POLICY_VIOLATION",
                        "policy_reason": error_text,
                    })
                    .to_string()
                } else {
                    json!({
                        "provider": "stasis-agent-turn",
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

        let invoked_tools: Vec<String> = response
            .tool_invocations
            .iter()
            .map(|invocation| invocation.tool_name.clone())
            .collect();

        let diagnostics = json!({
            "provider": "stasis-agent-turn",
            "status": "success",
            "agent_id": response.agent_id,
            "thread_id": response.thread_id,
            "tool_name": response.tool_name,
            "tool_output": response.tool_output,
            "tool_invocations": response.tool_invocations,
            "invoked_tools": invoked_tools,
            "tool_rounds": response.rounds_executed,
            "termination_reason": response.termination_reason,
            "policy_profile": response.metadata.policy_profile,
            "model_hint": response.metadata.model_hint,
            "output_preview": response.text.chars().take(160).collect::<String>(),
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: format!("sttp:agent-turn:{}", job.id),
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}
