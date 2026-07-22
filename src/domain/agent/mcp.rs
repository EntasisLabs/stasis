use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::errors::{Result, StasisError};

/// Descriptor for a tool projected through the MCP bridge contract (ADR-0007).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpToolDescriptor {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Option<Value>,
}

/// Recursion budget carried across MCP ↔ Stasis boundary crossings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct McpInvocationContext {
    pub remaining_depth: u8,
}

impl McpInvocationContext {
    pub const DEFAULT_MAX_DEPTH: u8 = 4;

    pub fn new(remaining_depth: u8) -> Self {
        Self { remaining_depth }
    }

    pub fn default_budget() -> Self {
        Self::new(Self::DEFAULT_MAX_DEPTH)
    }

    /// Consume one hop of budget before re-entering the opposite plane.
    pub fn descend(self) -> Result<Self> {
        if self.remaining_depth == 0 {
            return Err(StasisError::PortFailure(
                "policy violation: mcp recursion budget exhausted".into(),
            ));
        }
        Ok(Self {
            remaining_depth: self.remaining_depth - 1,
        })
    }
}
