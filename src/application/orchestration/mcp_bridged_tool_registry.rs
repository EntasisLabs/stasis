use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use genai::chat::Tool;
use serde_json::Value;

use crate::application::orchestration::mcp_recursion::{
    current_remaining_depth, scope_remaining_depth,
};
use crate::application::orchestration::tool_registry::{InMemoryToolRegistry, ToolRegistry};
use crate::domain::agent::mcp::{McpInvocationContext, McpToolDescriptor};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent::mcp_tool_provider::McpToolProvider;

/// Local `StasisTool` registry merged with injectable [`McpToolProvider`] sources.
#[derive(Clone)]
pub struct McpBridgedToolRegistry {
    local: InMemoryToolRegistry,
    providers: Vec<Arc<dyn McpToolProvider>>,
    /// Snapshot of provider tool name → provider index (built at connect time).
    provider_index: Arc<HashMap<String, usize>>,
    provider_descriptors: Arc<Vec<McpToolDescriptor>>,
}

impl McpBridgedToolRegistry {
    /// Merge local tools with provider descriptors. Rejects name collisions.
    pub async fn connect(
        local: InMemoryToolRegistry,
        providers: Vec<Arc<dyn McpToolProvider>>,
    ) -> Result<Self> {
        let local_tools = local.list_tools().await?;
        let mut local_names = local_tools
            .iter()
            .map(|tool| tool.name.as_ref().to_string())
            .collect::<std::collections::HashSet<_>>();

        let mut provider_index = HashMap::new();
        let mut provider_descriptors = Vec::new();

        for (provider_idx, provider) in providers.iter().enumerate() {
            let tools = provider.list_tools().await?;
            for descriptor in tools {
                let name = descriptor.name.trim().to_string();
                if name.is_empty() {
                    return Err(StasisError::PortFailure(
                        "policy violation: mcp provider tool name must be non-empty".into(),
                    ));
                }
                if local_names.contains(&name) {
                    return Err(StasisError::PortFailure(format!(
                        "policy violation: mcp provider tool '{name}' collides with local tool"
                    )));
                }
                if provider_index.contains_key(&name) {
                    return Err(StasisError::PortFailure(format!(
                        "policy violation: duplicate mcp provider tool name '{name}'"
                    )));
                }
                provider_index.insert(name.clone(), provider_idx);
                local_names.insert(name);
                provider_descriptors.push(descriptor);
            }
        }

        Ok(Self {
            local,
            providers,
            provider_index: Arc::new(provider_index),
            provider_descriptors: Arc::new(provider_descriptors),
        })
    }

    pub fn local_registry(&self) -> &InMemoryToolRegistry {
        &self.local
    }

    async fn invoke_provider(&self, tool_name: &str, input: Value) -> Result<Value> {
        let provider_idx = self.provider_index.get(tool_name).copied().ok_or_else(|| {
            StasisError::PortFailure(format!("tool not registered: {tool_name}"))
        })?;
        let provider = self.providers.get(provider_idx).ok_or_else(|| {
            StasisError::PortFailure(format!("mcp provider missing for tool '{tool_name}'"))
        })?;

        let remaining = current_remaining_depth()
            .unwrap_or(McpInvocationContext::DEFAULT_MAX_DEPTH);
        if remaining == 0 {
            return Err(StasisError::PortFailure(
                "policy violation: mcp recursion budget exhausted".into(),
            ));
        }

        let context = McpInvocationContext::new(remaining);
        let next_depth = remaining.saturating_sub(1);
        scope_remaining_depth(next_depth, provider.invoke(tool_name, input, context)).await
    }
}

#[async_trait]
impl ToolRegistry for McpBridgedToolRegistry {
    async fn list_tools(&self) -> Result<Vec<Tool>> {
        let mut tools = self.local.list_tools().await?;
        for descriptor in self.provider_descriptors.iter() {
            let mut definition = Tool::new(descriptor.name.clone());
            if let Some(description) = &descriptor.description {
                definition = definition.with_description(description.clone());
            }
            if let Some(schema) = &descriptor.input_schema {
                definition = definition.with_schema(schema.clone());
            }
            tools.push(definition);
        }
        Ok(tools)
    }

    async fn invoke_tool(&self, tool_name: &str, input: Value) -> Result<Value> {
        if self.provider_index.contains_key(tool_name) {
            return self.invoke_provider(tool_name, input).await;
        }
        self.local.invoke_tool(tool_name, input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::orchestration::tool_registry::StasisTool;
    use serde_json::json;

    struct LocalEcho;

    #[async_trait]
    impl StasisTool for LocalEcho {
        fn name(&self) -> &'static str {
            "local_echo"
        }

        async fn invoke(&self, input: Value) -> Result<Value> {
            Ok(json!({"echo": input}))
        }
    }

    struct FakeProvider {
        tools: Vec<McpToolDescriptor>,
    }

    #[async_trait]
    impl McpToolProvider for FakeProvider {
        async fn list_tools(&self) -> Result<Vec<McpToolDescriptor>> {
            Ok(self.tools.clone())
        }

        async fn invoke(
            &self,
            tool_name: &str,
            input: Value,
            _context: McpInvocationContext,
        ) -> Result<Value> {
            Ok(json!({
                "provider": "fake",
                "tool": tool_name,
                "input": input,
            }))
        }
    }

    #[tokio::test]
    async fn merges_provider_tools_and_invokes() {
        let local = InMemoryToolRegistry::default();
        local.register_tool(LocalEcho).unwrap();
        let provider = Arc::new(FakeProvider {
            tools: vec![McpToolDescriptor {
                name: "remote_search".into(),
                description: Some("fake remote".into()),
                input_schema: Some(json!({"type": "object"})),
            }],
        });

        let registry = McpBridgedToolRegistry::connect(local, vec![provider])
            .await
            .unwrap();
        let tools = registry.list_tools().await.unwrap();
        let names: Vec<_> = tools.iter().map(|t| t.name.as_ref().to_string()).collect();
        assert!(names.contains(&"local_echo".to_string()));
        assert!(names.contains(&"remote_search".to_string()));

        let out = registry
            .invoke_tool("remote_search", json!({"q": "hi"}))
            .await
            .unwrap();
        assert_eq!(out["provider"], "fake");
        assert_eq!(out["tool"], "remote_search");
    }

    #[tokio::test]
    async fn rejects_local_provider_name_collision() {
        let local = InMemoryToolRegistry::default();
        local.register_tool(LocalEcho).unwrap();
        let provider = Arc::new(FakeProvider {
            tools: vec![McpToolDescriptor {
                name: "local_echo".into(),
                description: None,
                input_schema: None,
            }],
        });
        let err = match McpBridgedToolRegistry::connect(local, vec![provider]).await {
            Ok(_) => panic!("expected collision error"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("collides"));
    }
}
