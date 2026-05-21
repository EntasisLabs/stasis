use async_trait::async_trait;
use serde_json::Value;

use crate::domain::errors::Result;

#[derive(Clone, Debug)]
pub struct WorkflowExecutionOutput {
    pub run_id: String,
    pub execution: Value,
    pub final_state: Value,
}

#[async_trait]
pub trait WorkflowEngine: Send + Sync {
    async fn execute_grapheme_source(&self, source: &str) -> Result<WorkflowExecutionOutput>;
}
