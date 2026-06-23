use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    MemoryEvictJobPayload, MemoryEvictModePayload,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_job_request_helpers::memory_scope_from_fields;
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
use crate::application::runtime::memory_recall_request_builder::memory_filter_from_payload;
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_models::{MemoryEvictMode, MemoryEvictRequest};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct MemoryEvictJobHandler {
    operations: Arc<dyn MemoryOperations>,
}

impl MemoryEvictJobHandler {
    pub fn new(operations: Arc<dyn MemoryOperations>) -> Self {
        Self { operations }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryEvictJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-evict payload json: {err}"))
    }

    fn map_mode(value: Option<MemoryEvictModePayload>) -> MemoryEvictMode {
        match value {
            Some(MemoryEvictModePayload::ByNodeIds) => MemoryEvictMode::ByNodeIds,
            Some(MemoryEvictModePayload::ByFilter) => MemoryEvictMode::ByFilter,
            Some(MemoryEvictModePayload::PurgeSession) => MemoryEvictMode::PurgeSession,
            _ => MemoryEvictMode::BySyncKeys,
        }
    }
}

#[async_trait]
impl JobHandler for MemoryEvictJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.evict"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(policy_violation_failure("stasis-memory-evict", message)),
        };

        let request = MemoryEvictRequest {
            mode: Self::map_mode(payload.mode),
            scope: memory_scope_from_fields(
                payload.tenant_id,
                payload.session_ids,
                payload.tiers,
                payload.from_utc,
                payload.to_utc,
            ),
            filter: memory_filter_from_payload(&payload.filter),
            sync_keys: payload.sync_keys,
            node_ids: payload.node_ids,
            dry_run: payload.dry_run.unwrap_or(true),
            force: payload.force.unwrap_or(false),
            max_nodes: payload.max_nodes.unwrap_or(5000),
            include_calibration: payload.include_calibration.unwrap_or(false),
            include_checkpoints: payload.include_checkpoints.unwrap_or(false),
        };

        match self.operations.evict(&request).await {
            Ok(result) => Ok(operation_success(
                "stasis-memory-evict",
                "memory-evict",
                &job.id,
                json!({
                    "dry_run": result.dry_run,
                    "deleted": result.deleted,
                    "blocked": result.blocked,
                    "not_found": result.not_found,
                    "skipped": result.skipped,
                    "would_delete": result.would_delete,
                    "calibrations_deleted": result.calibrations_deleted,
                    "checkpoints_deleted": result.checkpoints_deleted,
                    "records": result.records,
                }),
            )),
            Err(err) => Ok(operation_failure("stasis-memory-evict", err.to_string())),
        }
    }
}
