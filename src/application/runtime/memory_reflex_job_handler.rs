use std::sync::Arc;

use crate::application::orchestration::runtime_job_payloads::{
    MemoryReflexJobPayload, MemoryReflexPolicyPayload,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_job_request_helpers::memory_scope_from_fields;
use crate::application::runtime::memory_operation_job_outcome_helpers::{
    operation_failure, operation_success, policy_violation_failure,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::memory::memory_models::{MemoryReflexPolicy, MemoryReflexRequest};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct MemoryReflexJobHandler {
    operations: Arc<dyn MemoryOperations>,
}

impl MemoryReflexJobHandler {
    pub fn new(operations: Arc<dyn MemoryOperations>) -> Self {
        Self { operations }
    }

    fn parse_payload(raw: &str) -> std::result::Result<MemoryReflexJobPayload, String> {
        serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid memory-reflex payload json: {err}"))
    }

    fn map_policy(payload: Option<MemoryReflexPolicyPayload>) -> Option<MemoryReflexPolicy> {
        let payload = payload?;
        let base = MemoryReflexPolicy::default();
        Some(MemoryReflexPolicy {
            min_choice_confidence: payload
                .min_choice_confidence
                .unwrap_or(base.min_choice_confidence),
            min_salience: payload.min_salience.unwrap_or(base.min_salience),
            read_floor: payload.read_floor.unwrap_or(base.read_floor),
            write_floor: payload.write_floor.unwrap_or(base.write_floor),
            escalate_at: payload.escalate_at.unwrap_or(base.escalate_at),
            page_limit: payload.page_limit.unwrap_or(base.page_limit),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl JobHandler for MemoryReflexJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.memory.reflex"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(policy_violation_failure("stasis-memory-reflex", message)),
        };

        let request = MemoryReflexRequest {
            text: payload.text,
            role: payload.role,
            scope: memory_scope_from_fields(
                payload.tenant_id,
                payload.session_ids,
                payload.tiers,
                payload.from_utc,
                payload.to_utc,
            ),
            metadata: payload.metadata.unwrap_or_default(),
            policy: Self::map_policy(payload.policy),
            system1_endpoint: payload.system1_endpoint,
            system1_model: payload.system1_model,
            system1_api_key: payload.system1_api_key,
            system1_response: payload.system1_response,
        };

        match self.operations.reflex(&request).await {
            Ok(result) => {
                let details = serde_json::to_value(&result).unwrap_or_else(|_| {
                    serde_json::json!({
                        "error": "failed to encode memory reflex envelope"
                    })
                });
                Ok(operation_success(
                    "stasis-memory-reflex",
                    "memory-reflex",
                    &job.id,
                    details,
                ))
            }
            Err(err) => Ok(operation_failure("stasis-memory-reflex", err.to_string())),
        }
    }
}
