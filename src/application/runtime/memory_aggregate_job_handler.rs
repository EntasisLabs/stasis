use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::agent_session_payload::MemoryAggregateJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_models::{MemoryAggregateRequest, MemoryScope};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct MemoryAggregateJobHandler {
    operations: Arc<dyn MemoryOperations>,
}

impl MemoryAggregateJobHandler {
    pub fn new(operations: Arc<dyn MemoryOperations>) -> Self {
        Self { operations }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryAggregateJobPayload, String> {
        serde_json::from_str(raw).map_err(|err| {
            format!("policy violation: invalid memory-aggregate payload json: {err}")
        })
    }
}

#[async_trait]
impl JobHandler for MemoryAggregateJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.aggregate"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => {
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: message.clone(),
                    execution_id: None,
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-memory-aggregate",
                            "status": "failure",
                            "guardrail_code": "POLICY_VIOLATION",
                            "policy_reason": message,
                        })
                        .to_string(),
                    ),
                });
            }
        };

        let request = MemoryAggregateRequest {
            scope: MemoryScope {
                session_ids: payload.session_ids,
                tiers: payload.tiers,
                from_utc: payload.from_utc,
                to_utc: payload.to_utc,
            },
            max_groups: payload.max_groups.unwrap_or(30),
            max_nodes: payload.max_nodes.unwrap_or(5000),
        };

        match self.operations.aggregate(&request).await {
            Ok(result) => Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:memory-aggregate:{}", job.id),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-aggregate",
                        "status": "success",
                        "total_groups": result.total_groups,
                        "scanned_nodes": result.scanned_nodes,
                    })
                    .to_string(),
                ),
            }),
            Err(err) => Ok(JobExecutionOutcome::FatalFailure {
                message: err.to_string(),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-aggregate",
                        "status": "failure",
                        "error": err.to_string(),
                    })
                    .to_string(),
                ),
            }),
        }
    }
}
