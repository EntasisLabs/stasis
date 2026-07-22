use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Descriptor for a tool projected through the MCP bridge contract (ADR-0007).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpToolDescriptor {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Option<Value>,
}
