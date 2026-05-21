use std::fs;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde_json::json;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::errors::StasisError;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

const INLINE_PREFIX: &str = "grapheme:inline:";
const FILE_PREFIX: &str = "grapheme:file:";

pub struct GraphemeJobHandler {
    engine: Arc<dyn WorkflowEngine>,
}

impl GraphemeJobHandler {
    pub fn new(engine: Arc<dyn WorkflowEngine>) -> Self {
        Self { engine }
    }

    fn resolve_source(payload_ref: &str) -> Result<String> {
        if let Some(path) = payload_ref.strip_prefix(FILE_PREFIX) {
            return fs::read_to_string(path).map_err(|e| {
                crate::domain::errors::StasisError::PortFailure(format!(
                    "read grapheme source file '{}': {}",
                    path, e
                ))
            });
        }

        if let Some(inline) = payload_ref.strip_prefix(INLINE_PREFIX) {
            return Ok(inline.to_string());
        }

        Ok(payload_ref.to_string())
    }

    fn classify_guardrail_code(message: &str) -> &'static str {
        if message.contains("not allowlisted") {
            return "IMPORT_NOT_ALLOWLISTED";
        }

        if message.contains("source size") {
            return "SOURCE_TOO_LARGE";
        }

        if message.contains("timed out") {
            return "EXECUTION_TIMEOUT";
        }

        if message.contains("timeout must be greater than 0ms") {
            return "INVALID_TIMEOUT_CONFIG";
        }

        if message.contains("policy violation") {
            return "POLICY_VIOLATION";
        }

        "EXECUTION_ERROR"
    }

    fn build_success_diagnostics(
        duration_ms: u128,
        execution_id: &str,
        execution: &serde_json::Value,
        final_state: &serde_json::Value,
    ) -> String {
        json!({
            "provider": "grapheme-sdk",
            "status": "success",
            "duration_ms": duration_ms,
            "execution_id": execution_id,
            "execution": execution,
            "final_state": final_state
        })
        .to_string()
    }

    fn build_failure_diagnostics(duration_ms: u128, err: &StasisError) -> String {
        let message = err.to_string();
        let guardrail_code = Self::classify_guardrail_code(&message);
        let policy_reason = if message.contains("policy violation") {
            Some(message.clone())
        } else {
            None
        };

        json!({
            "provider": "grapheme-sdk",
            "status": "failure",
            "duration_ms": duration_ms,
            "guardrail_code": guardrail_code,
            "policy_reason": policy_reason,
            "error": message,
        })
        .to_string()
    }
}

#[async_trait]
impl JobHandler for GraphemeJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.grapheme.run"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let started = Instant::now();
        let source = match Self::resolve_source(&job.payload_ref) {
            Ok(source) => source,
            Err(err) => {
                let duration_ms = started.elapsed().as_millis();
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: err.to_string(),
                    execution_id: None,
                    diagnostics: Some(Self::build_failure_diagnostics(duration_ms, &err)),
                });
            }
        };

        match self.engine.execute_grapheme_source(&source).await {
            Ok(output) => {
                let duration_ms = started.elapsed().as_millis();
                Ok(JobExecutionOutcome::Success {
                    sttp_output_node_id: format!("sttp:{}:{}", output.run_id, job.id),
                    execution_id: Some(output.run_id.clone()),
                    diagnostics: Some(Self::build_success_diagnostics(
                        duration_ms,
                        &output.run_id,
                        &output.execution,
                        &output.final_state,
                    )),
                })
            }
            Err(err) => {
                let duration_ms = started.elapsed().as_millis();
                Ok(JobExecutionOutcome::FatalFailure {
                    message: err.to_string(),
                    execution_id: None,
                    diagnostics: Some(Self::build_failure_diagnostics(duration_ms, &err)),
                })
            }
        }
    }
}
