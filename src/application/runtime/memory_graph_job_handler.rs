use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::MemoryGraphJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_job_request_helpers::memory_scope_from_fields;
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
use crate::application::runtime::memory_recall_request_builder::memory_filter_from_payload;
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::MemoryGraphRequest;

pub struct MemoryGraphJobHandler {
    reader: Arc<dyn MemoryContextReader>,
}

impl MemoryGraphJobHandler {
    pub fn new(reader: Arc<dyn MemoryContextReader>) -> Self {
        Self { reader }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryGraphJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-graph payload json: {err}"))
    }
}

#[async_trait]
impl JobHandler for MemoryGraphJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.graph"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(policy_violation_failure("stasis-memory-graph", message)),
        };

        let request = MemoryGraphRequest {
            scope: memory_scope_from_fields(
                payload.tenant_id,
                payload.session_ids,
                payload.tiers,
                payload.from_utc,
                payload.to_utc,
            ),
            filter: memory_filter_from_payload(&payload.filter),
            include_lineage: payload.include_lineage.unwrap_or(true),
            include_semantic: payload.include_semantic.unwrap_or(true),
            include_session_topology: payload.include_session_topology.unwrap_or(true),
            rel: payload.rel,
            target_prefix: payload.target_prefix,
            limit: payload.limit.unwrap_or(200),
        };

        match self.reader.graph(&request).await {
            Ok(result) => Ok(operation_success(
                "stasis-memory-graph",
                "memory-graph",
                &job.id,
                json!({
                    "retrieved": result.retrieved,
                    "sessions": result.sessions,
                    "nodes": result.nodes,
                    "edges": result.edges,
                }),
            )),
            Err(err) => Ok(operation_failure("stasis-memory-graph", err.to_string())),
        }
    }
}
