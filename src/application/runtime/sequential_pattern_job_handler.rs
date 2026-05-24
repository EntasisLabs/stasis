use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    SequentialPatternJobPayload, SequentialStageJobPayload,
};
use crate::application::orchestration::prompt_pipeline::PromptExecutionPipeline;
use crate::application::orchestration::sequential_pattern_pipeline::{
    SequentialPatternExecutionRequest, SequentialPatternPipeline, SequentialPatternStage,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::domain::runtime::thread::{NewThread, NewThreadEvent};
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::runtime::thread_store::ThreadStore;

pub struct SequentialPatternJobHandler {
    pipeline: SequentialPatternPipeline,
    thread_store: Option<Arc<dyn ThreadStore>>,
}

impl SequentialPatternJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>) -> Self {
        Self::new_with_thread_store(chat_client, None)
    }

    pub fn new_with_thread_store(
        chat_client: Arc<dyn AiChatClient>,
        thread_store: Option<Arc<dyn ThreadStore>>,
    ) -> Self {
        let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
        Self {
            pipeline: SequentialPatternPipeline::new(prompt_pipeline),
            thread_store,
        }
    }

    async fn ensure_thread(&self, thread_id: &str, now: chrono::DateTime<Utc>) {
        let Some(store) = &self.thread_store else {
            return;
        };

        let exists = store.get_thread(thread_id).await.ok().flatten().is_some();
        if exists {
            return;
        }

        let _ = store
            .create_thread(NewThread {
                thread_id: thread_id.to_string(),
                parent_thread_id: None,
                branch_label: Some("sequential".to_string()),
                created_at: now,
            })
            .await;
    }

    async fn append_thread_event(
        &self,
        event_id: String,
        thread_id: &str,
        event_kind: &str,
        payload_ref: String,
        occurred_at: chrono::DateTime<Utc>,
    ) {
        let Some(store) = &self.thread_store else {
            return;
        };

        let _ = store
            .append_event(NewThreadEvent {
                event_id,
                thread_id: thread_id.to_string(),
                event_kind: event_kind.to_string(),
                payload_ref,
                occurred_at,
            })
            .await;
    }

    fn parse_payload(raw: &str) -> std::result::Result<SequentialPatternJobPayload, String> {
        let payload: SequentialPatternJobPayload = serde_json::from_str(raw).map_err(|err| {
            format!("policy violation: invalid sequential-pattern payload json: {err}")
        })?;

        if payload.initial_user_prompt.trim().is_empty() {
            return Err(
                "policy violation: sequential-pattern payload.initial_user_prompt must be non-empty"
                    .to_string(),
            );
        }
        if payload.stages.is_empty() {
            return Err(
                "policy violation: sequential-pattern payload.stages must include at least one stage"
                    .to_string(),
            );
        }

        for stage in &payload.stages {
            Self::validate_stage(stage)?;
        }

        Ok(payload)
    }

    fn validate_stage(stage: &SequentialStageJobPayload) -> std::result::Result<(), String> {
        if stage.stage_id.trim().is_empty() {
            return Err(
                "policy violation: sequential-pattern payload.stages[].stage_id must be non-empty"
                    .to_string(),
            );
        }
        if stage.user_prompt_template.trim().is_empty() {
            return Err(
                "policy violation: sequential-pattern payload.stages[].user_prompt_template must be non-empty"
                    .to_string(),
            );
        }

        Ok(())
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        JobExecutionOutcome::FatalFailure {
            message: message.clone(),
            execution_id: None,
            diagnostics: Some(
                json!({
                    "provider": "stasis-orchestration-sequential",
                    "status": "failure",
                    "pattern": "sequential",
                    "guardrail_code": "POLICY_VIOLATION",
                    "policy_reason": message,
                })
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl JobHandler for SequentialPatternJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.orchestration.sequential"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let now = Utc::now();
        let thread_id = payload
            .thread_id
            .clone()
            .unwrap_or_else(|| job.correlation_id.clone());
        self.ensure_thread(&thread_id, now).await;
        self.append_thread_event(
            format!("{}:sequential:start", job.id),
            &thread_id,
            "orchestration.sequential.started",
            payload.initial_user_prompt.clone(),
            now,
        )
        .await;

        let request = SequentialPatternExecutionRequest {
            initial_user_prompt: payload.initial_user_prompt,
            trace_id: Some(job.trace_id.clone()),
            correlation_id: Some(job.correlation_id.clone()),
            policy_profile: payload.policy_profile,
            model_hint: payload.model_hint,
            stages: payload
                .stages
                .into_iter()
                .map(|stage| SequentialPatternStage {
                    stage_id: stage.stage_id,
                    user_prompt_template: stage.user_prompt_template,
                    system_prompt: stage.system_prompt,
                    policy_profile: stage.policy_profile,
                    model_hint: stage.model_hint,
                })
                .collect(),
        };

        let response = match self.pipeline.execute(request).await {
            Ok(response) => response,
            Err(err) => {
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: err.to_string(),
                    execution_id: None,
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-orchestration-sequential",
                            "status": "failure",
                            "pattern": "sequential",
                            "error": err.to_string(),
                        })
                        .to_string(),
                    ),
                });
            }
        };

        let stage_ids: Vec<String> = response
            .stages
            .iter()
            .map(|stage| stage.stage_id.clone())
            .collect();

        self.append_thread_event(
            format!("{}:sequential:completed", job.id),
            &thread_id,
            "orchestration.sequential.completed",
            response.final_text.clone(),
            Utc::now(),
        )
        .await;

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: format!("sttp:orchestration:sequential:{}", job.id),
            execution_id: None,
            diagnostics: Some(
                json!({
                    "provider": "stasis-orchestration-sequential",
                    "status": "success",
                    "pattern": "sequential",
                    "stages_executed": response.stages.len(),
                    "stage_ids": stage_ids,
                    "thread_id": thread_id,
                    "final_text": response.final_text,
                    "termination_reason": response.termination_reason,
                })
                .to_string(),
            ),
        })
    }
}
