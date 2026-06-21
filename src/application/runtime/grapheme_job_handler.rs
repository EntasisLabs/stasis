use std::fs;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::telemetry::operation::OperationTelemetry;
use crate::domain::errors::Result;
use crate::domain::errors::StasisError;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

const INLINE_PREFIX: &str = "grapheme:inline:";
const FILE_PREFIX: &str = "grapheme:file:";
const JSON_PREFIX: &str = "grapheme:json:";

#[derive(Debug, Deserialize)]
struct GraphemeExecutionPayload {
    source: String,
    #[serde(default)]
    state_current: Option<Value>,
}

pub struct GraphemeJobHandler {
    engine: Arc<dyn WorkflowEngine>,
    telemetry: Option<OperationTelemetry>,
}

impl GraphemeJobHandler {
    pub fn new(engine: Arc<dyn WorkflowEngine>) -> Self {
        Self {
            engine,
            telemetry: None,
        }
    }

    pub fn with_operation_telemetry(mut self, telemetry: Option<OperationTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    fn resolve_payload(payload_ref: &str) -> Result<(String, Option<Value>)> {
        if let Some(path) = payload_ref.strip_prefix(FILE_PREFIX) {
            return fs::read_to_string(path)
                .map(|source| (source, None))
                .map_err(|e| {
                crate::domain::errors::StasisError::PortFailure(format!(
                    "read grapheme source file '{}': {}",
                    path, e
                ))
            });
        }

        if let Some(inline) = payload_ref.strip_prefix(INLINE_PREFIX) {
            return Ok((inline.to_string(), None));
        }

        if let Some(payload_json) = payload_ref.strip_prefix(JSON_PREFIX) {
            let payload: GraphemeExecutionPayload = serde_json::from_str(payload_json).map_err(
                |e| {
                    StasisError::PortFailure(format!(
                        "invalid grapheme execution payload json: {}",
                        e
                    ))
                },
            )?;
            return Ok((payload.source, payload.state_current));
        }

        if payload_ref.trim_start().starts_with('{')
            && payload_ref.contains("\"source\"")
            && let Ok(payload) = serde_json::from_str::<GraphemeExecutionPayload>(payload_ref)
        {
            return Ok((payload.source, payload.state_current));
        }

        Ok((payload_ref.to_string(), None))
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
        lint_warnings: &serde_json::Value,
    ) -> String {
        json!({
            "provider": "grapheme-sdk",
            "status": "success",
            "duration_ms": duration_ms,
            "execution_id": execution_id,
            "execution": execution,
            "final_state": final_state,
            "lint_warnings": lint_warnings,
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
        let _grapheme_span = self
            .telemetry
            .as_ref()
            .map(|telemetry| telemetry.grapheme_span(&job.id));

        let (source, state_current) = match Self::resolve_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(err) => {
                let duration_ms = started.elapsed().as_millis();
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: err.to_string(),
                    execution_id: None,
                    diagnostics: Some(Self::build_failure_diagnostics(duration_ms, &err)),
                });
            }
        };

        match self
            .engine
            .execute_grapheme_source(&source, state_current.as_ref())
            .await
        {
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
                        &output.lint_warnings,
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::Utc;

    use super::*;
    use crate::domain::runtime::job::{BackoffPolicy, NewJob};
    use crate::ports::outbound::runtime::workflow_engine::WorkflowExecutionOutput;

    struct RecordingWorkflowEngine {
        seen_source: Mutex<Option<String>>,
        seen_state_current: Mutex<Option<Value>>,
    }

    impl RecordingWorkflowEngine {
        fn new() -> Self {
            Self {
                seen_source: Mutex::new(None),
                seen_state_current: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl WorkflowEngine for RecordingWorkflowEngine {
        async fn execute_grapheme_source(
            &self,
            source: &str,
            state_current: Option<&Value>,
        ) -> Result<WorkflowExecutionOutput> {
            *self.seen_source.lock().expect("source mutex poisoned") = Some(source.to_string());
            *self
                .seen_state_current
                .lock()
                .expect("state mutex poisoned") = state_current.cloned();
            Ok(WorkflowExecutionOutput {
                run_id: "run-1".to_string(),
                execution: json!({"ok": true}),
                final_state: json!({"done": true}),
                lint_warnings: json!([]),
            })
        }
    }

    fn sample_job(payload_ref: &str) -> Job {
        NewJob {
            id: "job-1".to_string(),
            queue: "workflow".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: payload_ref.to_string(),
            priority: 0,
            max_attempts: 1,
            idempotency_key: "idem-1".to_string(),
            correlation_id: "corr-1".to_string(),
            causation_id: "cause-1".to_string(),
            trace_id: "trace-1".to_string(),
            sttp_input_node_id: "sttp:input:1".to_string(),
            scheduled_at: Utc::now(),
            backoff_policy: BackoffPolicy::default(),
        }
        .into_job()
    }

    #[test]
    fn resolve_payload_supports_json_prefix_with_state_current() {
        let payload = r#"grapheme:json:{"source":"op echo()","state_current":{"count":3}}"#;
        let (source, state_current) =
            GraphemeJobHandler::resolve_payload(payload).expect("payload should parse");
        assert_eq!(source, "op echo()");
        assert_eq!(state_current, Some(json!({"count": 3})));
    }

    #[test]
    fn resolve_payload_supports_legacy_inline_source() {
        let payload = "grapheme:inline:op echo()";
        let (source, state_current) =
            GraphemeJobHandler::resolve_payload(payload).expect("payload should parse");
        assert_eq!(source, "op echo()");
        assert_eq!(state_current, None);
    }

    #[tokio::test]
    async fn execute_passes_state_current_to_engine_when_present() {
        let engine = Arc::new(RecordingWorkflowEngine::new());
        let handler = GraphemeJobHandler::new(engine.clone());
        let job = sample_job(
            r#"grapheme:json:{"source":"op echo()","state_current":{"cursor":"abc"}}"#,
        );

        let outcome = handler
            .execute(&job)
            .await
            .expect("handler execution should succeed");

        match outcome {
            JobExecutionOutcome::Success { execution_id, .. } => {
                assert_eq!(execution_id, Some("run-1".to_string()));
            }
            _ => panic!("expected success outcome"),
        }

        assert_eq!(
            *engine
                .seen_source
                .lock()
                .expect("source mutex poisoned"),
            Some("op echo()".to_string())
        );
        assert_eq!(
            *engine
                .seen_state_current
                .lock()
                .expect("state mutex poisoned"),
            Some(json!({"cursor": "abc"}))
        );
    }
}
