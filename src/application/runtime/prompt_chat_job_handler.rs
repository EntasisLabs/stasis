use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::agent_session_payload::PromptJobPayload;
use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::ai_chat_client::AiChatClient;

pub struct PromptChatJobHandler {
    pipeline: PromptExecutionPipeline,
}

impl PromptChatJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>) -> Self {
        Self {
            pipeline: PromptExecutionPipeline::new(chat_client),
        }
    }

    fn parse_payload(raw: &str) -> std::result::Result<PromptJobPayload, String> {
        let payload: PromptJobPayload = serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid prompt job payload json: {err}"))?;

        if payload.user_prompt.trim().is_empty() {
            return Err("policy violation: prompt payload.user_prompt must be non-empty".to_string());
        }

        Ok(payload)
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "stasis-pipeline",
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
impl JobHandler for PromptChatJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.prompt"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let context = PromptExecutionContext {
            trace_id: Some(job.trace_id.clone()),
            correlation_id: Some(job.correlation_id.clone()),
            policy_profile: payload.policy_profile.clone(),
            model_hint: payload.model_hint.clone(),
        };

        let mut request = PromptExecutionRequest::from_user_prompt(payload.user_prompt)
            .with_context(context.clone());
        if let Some(system_prompt) = payload.system_prompt {
            request = request.with_system_prompt(system_prompt);
        }

        let response = match self.pipeline.execute(request).await {
            Ok(response) => response,
            Err(err) => {
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: err.to_string(),
                    execution_id: None,
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-pipeline",
                            "status": "failure",
                            "error": err.to_string(),
                            "policy_profile": context.policy_profile,
                            "model_hint": context.model_hint,
                        })
                        .to_string(),
                    ),
                });
            }
        };

        let diagnostics = json!({
            "provider": "stasis-pipeline",
            "status": "success",
            "trace_id": context.trace_id,
            "correlation_id": context.correlation_id,
            "policy_profile": response.metadata.policy_profile,
            "model_hint": response.metadata.model_hint,
            "output_text": response.text,
            "output_preview": response.text.chars().take(160).collect::<String>(),
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: format!("sttp:prompt:{}", job.id),
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}
