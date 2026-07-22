use async_trait::async_trait;
use serde_json::Value;

use crate::domain::agent::mcp::{McpInvocationContext, McpToolDescriptor};
use crate::domain::errors::Result;

/// Injects remote MCP tools into Stasis as a tool source (ADR-0007).
#[async_trait]
pub trait McpToolProvider: Send + Sync {
    async fn list_tools(&self) -> Result<Vec<McpToolDescriptor>>;
    async fn invoke(
        &self,
        tool_name: &str,
        input: Value,
        context: McpInvocationContext,
    ) -> Result<Value>;
}
