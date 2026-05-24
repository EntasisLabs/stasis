use std::sync::Arc;

use async_trait::async_trait;

use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

pub struct GraphemeHealthcheckJobHandler {
    delegate: GraphemeJobHandler,
}

impl GraphemeHealthcheckJobHandler {
    pub fn new(engine: Arc<dyn WorkflowEngine>) -> Self {
        Self {
            delegate: GraphemeJobHandler::new(engine),
        }
    }

    fn build_inline_source(message: &str) -> String {
        let cleaned = message
            .replace('"', "'")
            .replace(['\n', '\r'], " ");

        format!(
            "import core from \"grapheme/core\"\n\nquery Healthcheck {{\n  core.echo(message: \"{}\") {{\n    state {{ current }}\n  }}\n}}\n",
            cleaned
        )
    }
}

#[async_trait]
impl JobHandler for GraphemeHealthcheckJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.grapheme.healthcheck"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let message = if job.payload_ref.trim().is_empty() {
            "stasis grapheme healthcheck"
        } else {
            job.payload_ref.as_str()
        };

        let source = Self::build_inline_source(message);
        let synthetic_job = Job {
            payload_ref: format!("grapheme:inline:{}", source),
            ..job.clone()
        };

        self.delegate.execute(&synthetic_job).await
    }
}
