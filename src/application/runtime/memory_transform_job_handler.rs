use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::agent_session_payload::{
    MemoryTransformJobPayload, MemoryTransformOperationPayload,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_models::{
    MemoryScope, MemoryTransformOperation, MemoryTransformRequest,
};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct MemoryTransformJobHandler {
    operations: Arc<dyn MemoryOperations>,
}

impl MemoryTransformJobHandler {
    pub fn new(operations: Arc<dyn MemoryOperations>) -> Self {
        Self { operations }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryTransformJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-transform payload json: {err}"))
    }
}

#[async_trait]
impl JobHandler for MemoryTransformJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.transform"
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
                            "provider": "stasis-memory-transform",
                            "status": "failure",
                            "guardrail_code": "POLICY_VIOLATION",
                            "policy_reason": message,
                        })
                        .to_string(),
                    ),
                });
            }
        };

        let request = MemoryTransformRequest {
            scope: MemoryScope {
                session_ids: payload.session_ids,
                tiers: payload.tiers,
                from_utc: payload.from_utc,
                to_utc: payload.to_utc,
            },
            operation: match payload.operation {
                Some(MemoryTransformOperationPayload::ReindexEmbeddings) => {
                    MemoryTransformOperation::ReindexEmbeddings
                }
                _ => MemoryTransformOperation::EmbedBackfill,
            },
            dry_run: payload.dry_run.unwrap_or(true),
            batch_size: payload.batch_size.unwrap_or(100),
            max_nodes: payload.max_nodes.unwrap_or(5000),
            provider_id: payload.provider_id,
            model: payload.model,
        };

        match self.operations.transform(&request).await {
            Ok(result) => Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:memory-transform:{}", job.id),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-transform",
                        "status": "success",
                        "scanned": result.scanned,
                        "selected": result.selected,
                        "updated": result.updated,
                        "failed": result.failed,
                    })
                    .to_string(),
                ),
            }),
            Err(err) => Ok(JobExecutionOutcome::FatalFailure {
                message: err.to_string(),
                execution_id: None,
                diagnostics: Some(
                    json!({
                        "provider": "stasis-memory-transform",
                        "status": "failure",
                        "error": err.to_string(),
                    })
                    .to_string(),
                ),
            }),
        }
    }
}
