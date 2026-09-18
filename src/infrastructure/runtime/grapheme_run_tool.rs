use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::application::orchestration::tool_registry::StasisTool;
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

/// Kernel tool that compiles and executes Grapheme source via [`WorkflowEngine`].
pub struct GraphemeRunTool {
    engine: Arc<dyn WorkflowEngine>,
}

impl GraphemeRunTool {
    pub fn new(engine: Arc<dyn WorkflowEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl StasisTool for GraphemeRunTool {
    fn name(&self) -> &'static str {
        "grapheme_run"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Compile and execute a Grapheme workflow (wasm-safe stdlib: core/json/csv/yaml/html)")
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "required": ["source"],
            "additionalProperties": true,
            "properties": {
                "source": { "type": "string" },
                "state": { "type": "object" }
            }
        }))
    }

    async fn invoke(&self, input: Value) -> Result<Value> {
        let source = input
            .get("source")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                StasisError::PortFailure(
                    "policy violation: grapheme_run requires a non-empty source string".to_string(),
                )
            })?;
        let state = input.get("state").or_else(|| input.get("state_current"));
        let output = self.engine.execute_grapheme_source(source, state).await?;
        Ok(json!({
            "run_id": output.run_id,
            "execution": output.execution,
            "final_state": output.final_state,
            "lint_warnings": output.lint_warnings,
        }))
    }
}
