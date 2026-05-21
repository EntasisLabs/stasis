use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::agent_session_payload::{
    MemoryFallbackPolicyPayload, MemoryPolicyPayload, MemoryRecallJobPayload,
    MemoryStrictnessModePayload,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::{
    MemoryFallbackPolicy, MemoryRecallRequest, MemoryScope, MemoryStrictnessMode,
};

pub struct MemoryRecallJobHandler {
    reader: Arc<dyn MemoryContextReader>,
}

impl MemoryRecallJobHandler {
    pub fn new(reader: Arc<dyn MemoryContextReader>) -> Self {
        Self { reader }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryRecallJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-recall payload json: {err}"))
    }

    fn build_request(
        correlation_id: &str,
        policy: Option<&MemoryPolicyPayload>,
    ) -> MemoryRecallRequest {
        let mut request = MemoryRecallRequest::default();
        request.scope = MemoryScope {
            session_ids: policy
                .and_then(|value| value.session_ids.clone())
                .or_else(|| Some(vec![correlation_id.to_string()])),
            tiers: policy.and_then(|value| value.tiers.clone()),
            from_utc: policy.and_then(|value| value.from_utc),
            to_utc: policy.and_then(|value| value.to_utc),
        };
        request.query_text = policy.and_then(|value| value.query_text.clone());
        request.limit = policy
            .and_then(|value| value.limit)
            .unwrap_or(request.limit);
        request.alpha = policy
            .and_then(|value| value.alpha)
            .unwrap_or(request.alpha);
        request.beta = policy.and_then(|value| value.beta).unwrap_or(request.beta);
        request.include_explain = policy
            .and_then(|value| value.include_explain)
            .unwrap_or(true);
        request.fallback_policy = match policy.and_then(|value| value.fallback_policy.clone()) {
            Some(MemoryFallbackPolicyPayload::Never) => MemoryFallbackPolicy::Never,
            Some(MemoryFallbackPolicyPayload::Always) => MemoryFallbackPolicy::Always,
            _ => MemoryFallbackPolicy::OnEmpty,
        };
        request.strictness = match policy.and_then(|value| value.strictness.clone()) {
            Some(MemoryStrictnessModePayload::Precision) => MemoryStrictnessMode::Precision,
            Some(MemoryStrictnessModePayload::Recall) => MemoryStrictnessMode::Recall,
            _ => MemoryStrictnessMode::Balanced,
        };

        request
    }
}

#[async_trait]
impl JobHandler for MemoryRecallJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.recall"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => {
                let diagnostics = json!({
                    "provider": "stasis-memory-recall",
                    "status": "failure",
                    "guardrail_code": "POLICY_VIOLATION",
                    "policy_reason": message,
                })
                .to_string();
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: "invalid memory recall payload".to_string(),
                    execution_id: None,
                    diagnostics: Some(diagnostics),
                });
            }
        };

        let recall_request =
            Self::build_request(&job.correlation_id, payload.memory_policy.as_ref());
        match self.reader.recall(&recall_request).await {
            Ok(response) => Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:memory-recall:{}", job.id),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-recall",
                        "status": "success",
                        "retrieved": response.retrieved,
                        "retrieval_path": response.retrieval_path,
                        "fallback_triggered": response.fallback_triggered,
                        "fallback_reason": response.fallback_reason,
                        "has_more": response.has_more,
                    })
                    .to_string(),
                ),
            }),
            Err(err) => Ok(JobExecutionOutcome::FatalFailure {
                message: err.to_string(),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-recall",
                        "status": "failure",
                        "error": err.to_string(),
                    })
                    .to_string(),
                ),
            }),
        }
    }
}
