use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

const MAX_MESSAGE_LEN: usize = 512;

#[derive(Deserialize)]
struct EchoPayload {
    message: String,
}

pub struct GraphemeEchoJobHandler {
    delegate: GraphemeJobHandler,
}

impl GraphemeEchoJobHandler {
    pub fn new(engine: Arc<dyn WorkflowEngine>) -> Self {
        Self {
            delegate: GraphemeJobHandler::new(engine),
        }
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "grapheme-sdk",
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

    fn parse_payload(raw: &str) -> std::result::Result<EchoPayload, String> {
        let payload: EchoPayload = serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid echo payload json: {err}"))?;

        if payload.message.trim().is_empty() {
            return Err("policy violation: echo payload.message must be non-empty".to_string());
        }

        if payload.message.len() > MAX_MESSAGE_LEN {
            return Err(format!(
                "policy violation: echo payload.message exceeds max length {}",
                MAX_MESSAGE_LEN
            ));
        }

        Ok(payload)
    }

    fn build_inline_source(message: &str) -> String {
        let cleaned = message
            .replace('"', "'")
            .replace(['\n', '\r'], " ");

        format!(
            "import core from \"grapheme/core\"\n\nquery Echo {{\n  core.echo(message: \"{}\") {{\n    state {{ current }}\n  }}\n}}\n",
            cleaned
        )
    }
}

#[async_trait]
impl JobHandler for GraphemeEchoJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.grapheme.echo"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let source = Self::build_inline_source(&payload.message);
        let synthetic_job = Job {
            payload_ref: format!("grapheme:inline:{}", source),
            ..job.clone()
        };

        self.delegate.execute(&synthetic_job).await
    }
}
