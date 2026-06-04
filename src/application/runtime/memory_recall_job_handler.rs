use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    MemoryPolicyPayload, MemoryRecallJobPayload,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_recall_request_builder::build_memory_recall_request;
use crate::application::runtime::runtime_diagnostics_helpers::memory_nodes_json;
use crate::application::telemetry::operation::OperationTelemetry;
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::MemoryRecallRequest;

pub struct MemoryRecallJobHandler {
    reader: Arc<dyn MemoryContextReader>,
    telemetry: Option<OperationTelemetry>,
}

impl MemoryRecallJobHandler {
    pub fn new(reader: Arc<dyn MemoryContextReader>) -> Self {
        Self {
            reader,
            telemetry: None,
        }
    }

    pub fn with_operation_telemetry(mut self, telemetry: Option<OperationTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryRecallJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-recall payload json: {err}"))
    }

    fn build_request(
        correlation_id: &str,
        policy: Option<&MemoryPolicyPayload>,
    ) -> MemoryRecallRequest {
        build_memory_recall_request(correlation_id, None, policy)
    }
}

#[async_trait]
impl JobHandler for MemoryRecallJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.recall"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let recall_started = Instant::now();
        let _recall_span = self.telemetry.as_ref().map(|telemetry| {
            telemetry.record_recall_started();
            telemetry.recall_span(&job.correlation_id)
        });

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
            Ok(response) => {
                if let Some(telemetry) = &self.telemetry {
                    telemetry.record_recall_success(recall_started);
                }
                Ok(JobExecutionOutcome::Success {
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
                            "node_sync_keys": response.node_sync_keys,
                            "nodes": memory_nodes_json(&response.nodes),
                        })
                        .to_string(),
                    ),
                })
            }
            Err(err) => {
                if let Some(telemetry) = &self.telemetry {
                    telemetry.record_recall_error(recall_started);
                }
                Ok(JobExecutionOutcome::FatalFailure {
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
                })
            }
        }
    }
}
