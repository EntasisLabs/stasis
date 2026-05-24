use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::MemorySchemaJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct MemorySchemaJobHandler {
    operations: Arc<dyn MemoryOperations>,
}

impl MemorySchemaJobHandler {
    pub fn new(operations: Arc<dyn MemoryOperations>) -> Self {
        Self { operations }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemorySchemaJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-schema payload json: {err}"))
    }
}

#[async_trait]
impl JobHandler for MemorySchemaJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.schema"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        if let Err(message) = Self::parse_payload(&job.payload_ref) {
            return Ok(JobExecutionOutcome::FatalFailure {
                message: message.clone(),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-schema",
                        "status": "failure",
                        "guardrail_code": "POLICY_VIOLATION",
                        "policy_reason": message,
                    })
                    .to_string(),
                ),
            });
        }

        match self.operations.schema().await {
            Ok(result) => Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:memory-schema:{}", job.id),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-schema",
                        "status": "success",
                        "schema_version": result.schema_version,
                        "transform_operations": result.transform_operations,
                    })
                    .to_string(),
                ),
            }),
            Err(err) => Ok(JobExecutionOutcome::FatalFailure {
                message: err.to_string(),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-schema",
                        "status": "failure",
                        "error": err.to_string(),
                    })
                    .to_string(),
                ),
            }),
        }
    }
}
