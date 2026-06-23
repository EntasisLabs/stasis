use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::MemorySchemaJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
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
            return Ok(policy_violation_failure("stasis-memory-schema", message));
        }

        match self.operations.schema().await {
            Ok(result) => Ok(operation_success(
                "stasis-memory-schema",
                "memory-schema",
                &job.id,
                json!({
                    "schema_version": result.schema_version,
                    "transform_operations": result.transform_operations,
                    "evict_operations": result.evict_operations,
                }),
            )),
            Err(err) => Ok(operation_failure("stasis-memory-schema", err.to_string())),
        }
    }
}
