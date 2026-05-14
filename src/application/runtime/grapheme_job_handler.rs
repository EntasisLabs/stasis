use std::fs;
use std::sync::Arc;

use async_trait::async_trait;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
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
}

#[async_trait]
impl JobHandler for GraphemeJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.grapheme.run"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let source = Self::resolve_source(&job.payload_ref)?;
        let output = self.engine.execute_grapheme_source(&source).await?;

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: format!("sttp:{}:{}", output.run_id, job.id),
            execution_id: Some(output.run_id),
            diagnostics: None,
        })
    }
}
