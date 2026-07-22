use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::application::orchestration::mcp_recursion::scope_remaining_depth;
use crate::application::orchestration::tool_registry::ToolRegistry;
use crate::domain::agent::mcp::{McpInvocationContext, McpToolDescriptor};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent::mcp_tool_exporter::McpToolExporter;

/// Exports an explicit allowlist of local registry tools as MCP tools.
///
/// Default allowlist is empty (export nothing). Recursion budget is enforced on
/// each `invoke_exported` re-entry into Stasis.
pub struct AllowlistedLocalMcpExporter {
    registry: Arc<dyn ToolRegistry>,
    allowlist: HashSet<String>,
}

impl AllowlistedLocalMcpExporter {
    pub fn new(registry: Arc<dyn ToolRegistry>, allowlist: impl IntoIterator<Item = String>) -> Self {
        Self {
            registry,
            allowlist: allowlist.into_iter().collect(),
        }
    }

    /// Convenience: export nothing.
    pub fn empty(registry: Arc<dyn ToolRegistry>) -> Self {
        Self::new(registry, Vec::new())
    }
}

#[async_trait]
impl McpToolExporter for AllowlistedLocalMcpExporter {
    async fn exported_tools(&self) -> Result<Vec<McpToolDescriptor>> {
        if self.allowlist.is_empty() {
            return Ok(Vec::new());
        }
        let tools = self.registry.list_tools().await?;
        Ok(tools
            .into_iter()
            .filter(|tool| self.allowlist.contains(tool.name.as_ref()))
            .map(|tool| McpToolDescriptor {
                name: tool.name.as_ref().to_string(),
                description: tool.description.as_ref().map(|d| d.to_string()),
                input_schema: tool.schema.clone(),
            })
            .collect())
    }

    async fn invoke_exported(
        &self,
        tool_name: &str,
        input: Value,
        context: McpInvocationContext,
    ) -> Result<Value> {
        if !self.allowlist.contains(tool_name) {
            return Err(StasisError::PortFailure(format!(
                "policy violation: tool '{tool_name}' is not in the mcp export allowlist"
            )));
        }
        let next = context.descend()?;
        scope_remaining_depth(next.remaining_depth, async {
            self.registry.invoke_tool(tool_name, input).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::orchestration::mcp_bridged_tool_registry::McpBridgedToolRegistry;
    use crate::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
    use crate::ports::outbound::agent::mcp_tool_provider::McpToolProvider;
    use serde_json::json;
    use std::sync::Mutex;

    struct EchoTool;

    #[async_trait]
    impl StasisTool for EchoTool {
        fn name(&self) -> &'static str {
            "echo_upper"
        }

        fn description(&self) -> Option<&'static str> {
            Some("uppercases")
        }

        async fn invoke(&self, input: Value) -> Result<Value> {
            let text = input
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_uppercase();
            Ok(json!({"upper": text}))
        }
    }

    struct BounceTool {
        provider: Arc<dyn McpToolProvider>,
    }

    #[async_trait]
    impl StasisTool for BounceTool {
        fn name(&self) -> &'static str {
            "bounce"
        }

        async fn invoke(&self, input: Value) -> Result<Value> {
            let depth = crate::application::orchestration::mcp_recursion::current_remaining_depth()
                .unwrap_or(0);
            self.provider
                .invoke(
                    "remote_bounce",
                    input,
                    McpInvocationContext::new(depth),
                )
                .await
        }
    }

    struct BounceProvider {
        exporter: Mutex<Option<Arc<dyn McpToolExporter>>>,
    }

    #[async_trait]
    impl McpToolProvider for BounceProvider {
        async fn list_tools(&self) -> Result<Vec<McpToolDescriptor>> {
            Ok(vec![McpToolDescriptor {
                name: "remote_bounce".into(),
                description: Some("bounces back into exporter".into()),
                input_schema: None,
            }])
        }

        async fn invoke(
            &self,
            _tool_name: &str,
            input: Value,
            context: McpInvocationContext,
        ) -> Result<Value> {
            let exporter = self
                .exporter
                .lock()
                .map_err(|_| StasisError::PortFailure("exporter lock poisoned".into()))?
                .clone()
                .ok_or_else(|| StasisError::PortFailure("exporter not wired".into()))?;
            exporter
                .invoke_exported("bounce", input, context)
                .await
        }
    }

    #[tokio::test]
    async fn default_allowlist_exports_nothing() {
        let local = InMemoryToolRegistry::default();
        local.register_tool(EchoTool).unwrap();
        let exporter = AllowlistedLocalMcpExporter::empty(Arc::new(local));
        assert!(exporter.exported_tools().await.unwrap().is_empty());
        let err = exporter
            .invoke_exported(
                "echo_upper",
                json!({"text": "hi"}),
                McpInvocationContext::default_budget(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("allowlist"));
    }

    #[tokio::test]
    async fn exports_and_invokes_local_tool() {
        let local = InMemoryToolRegistry::default();
        local.register_tool(EchoTool).unwrap();
        let exporter = AllowlistedLocalMcpExporter::new(
            Arc::new(local),
            vec!["echo_upper".into()],
        );
        let exported = exporter.exported_tools().await.unwrap();
        assert_eq!(exported.len(), 1);
        assert_eq!(exported[0].name, "echo_upper");

        let out = exporter
            .invoke_exported(
                "echo_upper",
                json!({"text": "hi"}),
                McpInvocationContext::default_budget(),
            )
            .await
            .unwrap();
        assert_eq!(out["upper"], "HI");
    }

    #[tokio::test]
    async fn recursive_export_provider_bounce_exhausts_budget() {
        let provider = Arc::new(BounceProvider {
            exporter: Mutex::new(None),
        });
        let local = InMemoryToolRegistry::default();
        local
            .register_tool(BounceTool {
                provider: provider.clone(),
            })
            .unwrap();

        let bridged = McpBridgedToolRegistry::connect(local.clone(), vec![provider.clone()])
            .await
            .unwrap();
        let exporter: Arc<dyn McpToolExporter> = Arc::new(AllowlistedLocalMcpExporter::new(
            Arc::new(bridged),
            vec!["bounce".into()],
        ));
        *provider.exporter.lock().unwrap() = Some(exporter.clone());

        let err = exporter
            .invoke_exported(
                "bounce",
                json!({}),
                McpInvocationContext::new(2),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("recursion budget exhausted"),
            "unexpected error: {err}"
        );
    }
}
