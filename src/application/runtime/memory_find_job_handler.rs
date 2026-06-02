use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::MemoryFindJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::{
    MemoryFilter, MemoryFindRequest, MemoryScope, MemorySortDirection, MemorySortField,
};

pub struct MemoryFindJobHandler {
    reader: Arc<dyn MemoryContextReader>,
}

impl MemoryFindJobHandler {
    pub fn new(reader: Arc<dyn MemoryContextReader>) -> Self {
        Self { reader }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryFindJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-find payload json: {err}"))
    }

    fn map_sort_field(value: Option<&str>) -> MemorySortField {
        match value {
            Some("updated_at") => MemorySortField::UpdatedAt,
            Some("psi") => MemorySortField::Psi,
            Some("rho") => MemorySortField::Rho,
            Some("kappa") => MemorySortField::Kappa,
            _ => MemorySortField::Timestamp,
        }
    }

    fn map_sort_direction(value: Option<&str>) -> MemorySortDirection {
        match value {
            Some("asc") => MemorySortDirection::Asc,
            _ => MemorySortDirection::Desc,
        }
    }
}

#[async_trait]
impl JobHandler for MemoryFindJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.find"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(policy_violation_failure("stasis-memory-find", message)),
        };

        let request = MemoryFindRequest {
            scope: MemoryScope {
                session_ids: payload.session_ids,
                tiers: payload.tiers,
                from_utc: payload.from_utc,
                to_utc: payload.to_utc,
            },
            filter: MemoryFilter {
                text_contains: payload.text_contains,
                ..Default::default()
            },
            limit: payload.limit.unwrap_or(50),
            cursor: payload.cursor,
            sort_field: Self::map_sort_field(payload.sort_field.as_deref()),
            sort_direction: Self::map_sort_direction(payload.sort_direction.as_deref()),
        };

        match self.reader.find(&request).await {
            Ok(result) => Ok(operation_success(
                "stasis-memory-find",
                "memory-find",
                &job.id,
                json!({
                    "retrieved": result.retrieved,
                    "has_more": result.has_more,
                    "next_cursor": result.next_cursor,
                    "node_sync_keys": result.node_sync_keys,
                }),
            )),
            Err(err) => Ok(operation_failure("stasis-memory-find", err.to_string())),
        }
    }
}
