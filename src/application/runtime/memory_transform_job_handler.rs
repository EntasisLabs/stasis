use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    MemoryTransformJobPayload, MemoryTransformOperationPayload,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_job_request_helpers::memory_scope_from_fields;
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_models::{
    MemoryTransformOperation, MemoryTransformRequest,
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
        serde_json::from_str(raw).map_err(|err| {
            format!("policy violation: invalid memory-transform payload json: {err}")
        })
    }

    fn map_operation(value: Option<MemoryTransformOperationPayload>) -> MemoryTransformOperation {
        match value {
            Some(MemoryTransformOperationPayload::ReindexEmbeddings) => {
                MemoryTransformOperation::ReindexEmbeddings
            }
            Some(MemoryTransformOperationPayload::EmbedTagBackfill) => {
                MemoryTransformOperation::EmbedTagBackfill
            }
            Some(MemoryTransformOperationPayload::ReindexTagEmbeddings) => {
                MemoryTransformOperation::ReindexTagEmbeddings
            }
            _ => MemoryTransformOperation::EmbedBackfill,
        }
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
            Err(message) => return Ok(policy_violation_failure("stasis-memory-transform", message)),
        };

        let request = MemoryTransformRequest {
            scope: memory_scope_from_fields(
                None,
                payload.session_ids,
                payload.tiers,
                payload.from_utc,
                payload.to_utc,
            ),
            filter: Default::default(),
            operation: Self::map_operation(payload.operation),
            dry_run: payload.dry_run.unwrap_or(true),
            batch_size: payload.batch_size.unwrap_or(100),
            max_nodes: payload.max_nodes.unwrap_or(5000),
            provider_id: payload.provider_id,
            model: payload.model,
        };

        match self.operations.transform(&request).await {
            Ok(result) => Ok(operation_success(
                "stasis-memory-transform",
                "memory-transform",
                &job.id,
                json!({
                    "scanned": result.scanned,
                    "selected": result.selected,
                    "updated": result.updated,
                    "failed": result.failed,
                }),
            )),
            Err(err) => Ok(operation_failure("stasis-memory-transform", err.to_string())),
        }
    }
}
