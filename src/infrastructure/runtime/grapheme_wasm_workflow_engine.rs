//! In-guest Grapheme engine using published `grapheme-wasm` 0.7.1.
//!
//! Compiles source and executes it in-process with the wasm-safe stdlib
//! (`core` / `json` / `csv` / `yaml` / `html`). Host-only ops (`http`, `sql`, …)
//! fail honestly. No `spawn_blocking` (unavailable on `wasm32-unknown-unknown`).

use async_trait::async_trait;
use grapheme_wasm::{ExecuteRequest, execute};
use serde_json::{Value, json};
use std::time::Duration;

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::runtime::workflow_engine::{WorkflowEngine, WorkflowExecutionOutput};

#[derive(Clone, Debug)]
pub struct GraphemeWorkflowGuardrails {
    pub allowed_imports: Vec<String>,
    pub max_source_bytes: usize,
    pub execution_timeout: Duration,
    pub max_steps: Option<usize>,
    pub max_call_depth: Option<usize>,
}

impl Default for GraphemeWorkflowGuardrails {
    fn default() -> Self {
        Self {
            allowed_imports: vec![
                "grapheme/*".to_string(),
                "grapheme/core".to_string(),
                "grapheme/json".to_string(),
                "grapheme/csv".to_string(),
                "grapheme/yaml".to_string(),
                "grapheme/html".to_string(),
            ],
            max_source_bytes: 128 * 1024,
            execution_timeout: Duration::from_millis(2_000),
            max_steps: Some(10_000),
            max_call_depth: Some(16),
        }
    }
}

pub struct GraphemeWasmWorkflowEngine {
    guardrails: GraphemeWorkflowGuardrails,
}

impl GraphemeWasmWorkflowEngine {
    pub fn new() -> Self {
        Self::with_guardrails(GraphemeWorkflowGuardrails::default())
    }

    pub fn with_guardrails(guardrails: GraphemeWorkflowGuardrails) -> Self {
        Self { guardrails }
    }

    fn validate_source(&self, source: &str) -> Result<()> {
        if source.len() > self.guardrails.max_source_bytes {
            return Err(StasisError::PortFailure(format!(
                "grapheme policy violation: source size {} exceeds max {} bytes",
                source.len(),
                self.guardrails.max_source_bytes
            )));
        }

        for import in extract_imports(source) {
            if !self
                .guardrails
                .allowed_imports
                .iter()
                .any(|pattern| import_is_allowed(pattern, &import))
            {
                return Err(StasisError::PortFailure(format!(
                    "grapheme policy violation: import '{import}' is not allowlisted"
                )));
            }
        }

        Ok(())
    }
}

impl Default for GraphemeWasmWorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn import_is_allowed(pattern: &str, import: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        return import.starts_with(prefix);
    }
    pattern == import
}

fn extract_imports(source: &str) -> Vec<String> {
    source
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !trimmed.starts_with("import ") {
                return None;
            }
            let quote = if trimmed.contains('"') { '"' } else { '\'' };
            let start = trimmed.find(quote)?;
            let tail = &trimmed[(start + 1)..];
            let end = tail.find(quote)?;
            Some(tail[..end].to_string())
        })
        .collect()
}

#[async_trait]
impl WorkflowEngine for GraphemeWasmWorkflowEngine {
    async fn execute_grapheme_source(
        &self,
        source: &str,
        state_current: Option<&Value>,
    ) -> Result<WorkflowExecutionOutput> {
        self.validate_source(source)?;

        let request = ExecuteRequest {
            source: Some(source.to_string()),
            artifact: None,
            initial_current: state_current.cloned(),
            args: None,
            entrypoint: None,
        };
        let response = execute(&request);

        if !response.ok {
            let message = response
                .error
                .as_ref()
                .map(|err| format!("grapheme-wasm {}: {}", err.code, err.message))
                .unwrap_or_else(|| "grapheme-wasm execution failed".to_string());
            if message.contains("policy")
                || message.contains("allowlisted")
                || message.contains("COMPILE_ERROR")
            {
                return Err(StasisError::PortFailure(format!(
                    "grapheme policy violation: {message}"
                )));
            }
            return Err(StasisError::PortFailure(message));
        }

        let artifact_id = response
            .artifact_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        Ok(WorkflowExecutionOutput {
            run_id: format!("grapheme:{artifact_id}"),
            execution: serde_json::to_value(&response.execution).unwrap_or(Value::Null),
            final_state: response.final_state.unwrap_or(Value::Null),
            lint_warnings: serde_json::to_value(&response.lint_warnings).unwrap_or(json!([])),
        })
    }
}
