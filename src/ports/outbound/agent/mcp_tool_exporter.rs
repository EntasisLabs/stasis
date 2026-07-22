use async_trait::async_trait;
use serde_json::Value;

use crate::domain::agent::mcp::{McpInvocationContext, McpToolDescriptor};
use crate::domain::errors::Result;

/// Projects selected Stasis tools outward as MCP tools (ADR-0007).
#[async_trait]
pub trait McpToolExporter: Send + Sync {
    async fn exported_tools(&self) -> Result<Vec<McpToolDescriptor>>;
    async fn invoke_exported(
        &self,
        tool_name: &str,
        input: Value,
        context: McpInvocationContext,
    ) -> Result<Value>;
}
