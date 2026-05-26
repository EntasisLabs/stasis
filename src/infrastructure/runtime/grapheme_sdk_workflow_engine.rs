use async_trait::async_trait;
use grapheme_sdk::{GraphemeEngine, GraphemeSdkError};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time;

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::runtime::workflow_engine::{WorkflowEngine, WorkflowExecutionOutput};

pub struct GraphemeSdkWorkflowEngine {
    engine: Arc<GraphemeEngine>,
    guardrails: GraphemeWorkflowGuardrails,
}

const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 2_000;

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
            // Allow all built-in Grapheme namespace modules by default.
            allowed_imports: vec!["grapheme/*".to_string()],
            max_source_bytes: 128 * 1024,
            execution_timeout: resolve_execution_timeout_from_env(),
            max_steps: Some(10_000),
            max_call_depth: Some(16),
        }
    }
}

fn resolve_execution_timeout_from_env() -> Duration {
    let raw_value = std::env::var("MEDOUSA_GRAPHEME_EXECUTION_TIMEOUT_MS")
        .ok()
        .or_else(|| std::env::var("STASIS_GRAPHEME_EXECUTION_TIMEOUT_MS").ok())
        .or_else(|| std::env::var("GRAPHEME_EXECUTION_TIMEOUT_MS").ok());

    let timeout_ms = raw_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_EXECUTION_TIMEOUT_MS);

    Duration::from_millis(timeout_ms)
}

impl GraphemeSdkWorkflowEngine {
    pub fn new() -> Self {
        Self::with_guardrails(GraphemeWorkflowGuardrails::default())
    }

    pub fn with_guardrails(guardrails: GraphemeWorkflowGuardrails) -> Self {
        Self {
            engine: Arc::new(
                GraphemeEngine::builder()
                    .with_max_steps(guardrails.max_steps)
                    .with_max_call_depth(guardrails.max_call_depth)
                    .build(),
            ),
            guardrails,
        }
    }

    fn validate_source(&self, source: &str) -> Result<()> {
        if source.len() > self.guardrails.max_source_bytes {
            return Err(StasisError::PortFailure(format!(
                "grapheme policy violation: source size {} exceeds max {} bytes",
                source.len(),
                self.guardrails.max_source_bytes
            )));
        }

        let imports = Self::extract_imports(source);
        for import in imports {
            if !self
                .guardrails
                .allowed_imports
                .iter()
                .any(|pattern| Self::import_is_allowed(pattern, &import))
            {
                return Err(StasisError::PortFailure(format!(
                    "grapheme policy violation: import '{}' is not allowlisted",
                    import
                )));
            }
        }

        Ok(())
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

    fn map_error(err: GraphemeSdkError) -> StasisError {
        let msg = err.to_string();
        if msg.contains("policy:") {
            return StasisError::PortFailure(format!("grapheme policy violation: {msg}"));
        }

        StasisError::PortFailure(format!("grapheme sdk execution error: {err}"))
    }
}

impl Default for GraphemeSdkWorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WorkflowEngine for GraphemeSdkWorkflowEngine {
    async fn execute_grapheme_source(
        &self,
        source: &str,
        state_current: Option<&Value>,
    ) -> Result<WorkflowExecutionOutput> {
        self.validate_source(source)?;

        if self.guardrails.execution_timeout.is_zero() {
            return Err(StasisError::PortFailure(
                "grapheme policy violation: execution timeout must be greater than 0ms".to_string(),
            ));
        }

        let source_owned = source.to_string();
        let state_current_owned = state_current.cloned();
        let guardrails = self.guardrails.clone();
        let engine = Arc::clone(&self.engine);
        let handle = task::spawn_blocking(move || {
            if let Some(initial_state_current) = state_current_owned {
                let state_engine = GraphemeEngine::builder()
                    .with_max_steps(guardrails.max_steps)
                    .with_max_call_depth(guardrails.max_call_depth)
                    .with_initial_state_current(initial_state_current)
                    .build();
                state_engine.execute_source(&source_owned)
            } else {
                engine.execute_source(&source_owned)
            }
        });
        let result = time::timeout(self.guardrails.execution_timeout, handle)
            .await
            .map_err(|_| {
                StasisError::PortFailure(format!(
                    "grapheme policy violation: execution timed out after {} ms",
                    self.guardrails.execution_timeout.as_millis()
                ))
            })?
            .map_err(|e| StasisError::PortFailure(format!("grapheme sdk worker join error: {e}")))?
            .map_err(Self::map_error)?;

        Ok(WorkflowExecutionOutput {
            run_id: format!("grapheme:{}", result.artifact_id),
            execution: serde_json::to_value(&result.execution).unwrap_or(Value::Null),
            final_state: result.final_state,
        })
    }
}
