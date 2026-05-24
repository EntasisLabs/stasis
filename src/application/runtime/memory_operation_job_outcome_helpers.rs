use serde_json::{Value, json};

use crate::application::runtime::in_memory_runtime::JobExecutionOutcome;

pub fn policy_violation_failure(provider: &str, message: String) -> JobExecutionOutcome {
    let diagnostics = json!({
        "provider": provider,
        "status": "failure",
        "guardrail_code": "POLICY_VIOLATION",
        "policy_reason": &message,
    })
    .to_string();

    JobExecutionOutcome::FatalFailure {
        message,
        execution_id: None,
        diagnostics: Some(diagnostics),
    }
}

pub fn operation_failure(provider: &str, error: String) -> JobExecutionOutcome {
    let diagnostics = json!({
        "provider": provider,
        "status": "failure",
        "error": &error,
    })
    .to_string();

    JobExecutionOutcome::FatalFailure {
        message: error,
        execution_id: None,
        diagnostics: Some(diagnostics),
    }
}

pub fn operation_success(
    provider: &str,
    sttp_kind: &str,
    job_id: &str,
    details: Value,
) -> JobExecutionOutcome {
    let mut diagnostics = json!({
        "provider": provider,
        "status": "success",
    });

    if let (Value::Object(base), Value::Object(extra)) = (&mut diagnostics, details) {
        base.extend(extra);
    }

    JobExecutionOutcome::Success {
        sttp_output_node_id: format!("sttp:{sttp_kind}:{job_id}"),
        execution_id: None,
        diagnostics: Some(diagnostics.to_string()),
    }
}