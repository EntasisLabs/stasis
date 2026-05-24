use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::MemoryRollupJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_models::{MemoryRollupRequest, MemoryScope};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct MemoryRollupJobHandler {
    operations: Arc<dyn MemoryOperations>,
}

impl MemoryRollupJobHandler {
    pub fn new(operations: Arc<dyn MemoryOperations>) -> Self {
        Self { operations }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryRollupJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-rollup payload json: {err}"))
    }
}

#[async_trait]
impl JobHandler for MemoryRollupJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.rollup"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(policy_violation_failure("stasis-memory-rollup", message)),
        };

        let request = MemoryRollupRequest {
            scope: MemoryScope {
                session_ids: payload.session_ids,
                tiers: payload.tiers,
                from_utc: payload.from_utc,
                to_utc: payload.to_utc,
            },
            max_days: payload.max_days.unwrap_or(30),
            max_nodes: payload.max_nodes.unwrap_or(5000),
        };

        match self.operations.rollup(&request).await {
            Ok(result) => Ok(operation_success(
                "stasis-memory-rollup",
                "memory-rollup",
                &job.id,
                json!({
                    "total_groups": result.total_groups,
                    "scanned_nodes": result.scanned_nodes,
                }),
            )),
            Err(err) => Ok(operation_failure("stasis-memory-rollup", err.to_string())),
        }
    }
}
