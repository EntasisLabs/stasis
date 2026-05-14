use async_trait::async_trait;

use crate::domain::errors::Result;

#[derive(Clone, Debug)]
pub struct WorkflowExecutionOutput {
    pub run_id: String,
}

#[async_trait]
pub trait WorkflowEngine: Send + Sync {
    async fn execute_grapheme_source(&self, source: &str) -> Result<WorkflowExecutionOutput>;
}
