use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
#[cfg(feature = "llm-genai")]
pub use genai::chat::Tool;
use serde_json::Value;

use crate::domain::errors::{Result, StasisError};

#[cfg(not(feature = "llm-genai"))]
#[derive(Clone, Debug)]
pub struct Tool {
    pub name: String,
    pub description: Option<String>,
    pub schema: Option<Value>,
}

#[cfg(not(feature = "llm-genai"))]
impl Tool {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            schema: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_schema(mut self, schema: Value) -> Self {
        self.schema = Some(schema);
        self
    }
}

pub(crate) fn tool_advertised_name(tool: &Tool) -> &str {
    #[cfg(feature = "llm-genai")]
    {
        tool.name.as_ref()
    }
    #[cfg(not(feature = "llm-genai"))]
    {
        tool.name.as_str()
    }
}

pub(crate) fn tool_description(tool: &Tool) -> Option<&str> {
    #[cfg(feature = "llm-genai")]
    {
        tool.description.as_deref()
    }
    #[cfg(not(feature = "llm-genai"))]
    {
        tool.description.as_deref()
    }
}

#[async_trait]
pub trait StasisTool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&'static str> {
        None
    }
    fn input_schema(&self) -> Option<Value> {
        None
    }
    fn output_schema(&self) -> Option<Value> {
        None
    }
    async fn invoke(&self, input: Value) -> Result<Value>;
}

#[async_trait]
pub trait ToolRegistry: Send + Sync {
    async fn list_tools(&self) -> Result<Vec<Tool>>;
    async fn invoke_tool(&self, tool_name: &str, input: Value) -> Result<Value>;
}

#[derive(Clone)]
struct RegisteredTool {
    implementation: Arc<dyn StasisTool>,
    description: Option<String>,
    input_schema: Option<Value>,
    errors_as_results: bool,
}

#[derive(Clone, Default)]
pub struct InMemoryToolRegistry {
    tools: Arc<RwLock<HashMap<String, RegisteredTool>>>,
    alias_by_original: Arc<RwLock<HashMap<String, String>>>,
    original_by_alias: Arc<RwLock<HashMap<String, String>>>,
}

impl InMemoryToolRegistry {
    pub fn register_tool<T: StasisTool + 'static>(&self, tool: T) -> Result<()> {
        let tool_name = tool.name().to_string();
        let description = tool.description().map(str::to_string);
        let input_schema = tool.input_schema();
        self.register_tool_entry(tool_name, description, input_schema, false, Arc::new(tool))
    }

    /// Registers a tool with host-owned metadata instead of requiring static Rust strings.
    ///
    /// This is primarily useful to language bindings. Existing [`StasisTool`] registration
    /// remains unchanged. When `errors_as_results` is true, input-schema failures are returned
    /// as structured tool output so the model can observe and correct malformed arguments.
    pub fn register_dynamic_tool<T: StasisTool + 'static>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        errors_as_results: bool,
        tool: T,
    ) -> Result<()> {
        self.register_tool_entry(
            name.into(),
            Some(description.into()),
            Some(input_schema),
            errors_as_results,
            Arc::new(tool),
        )
    }

    pub fn contains(&self, name: &str) -> Result<bool> {
        self.tools
            .read()
            .map(|tools| tools.contains_key(name))
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))
    }

    fn register_tool_entry(
        &self,
        tool_name: String,
        description: Option<String>,
        input_schema: Option<Value>,
        errors_as_results: bool,
        implementation: Arc<dyn StasisTool>,
    ) -> Result<()> {
        let mut tools = self
            .tools
            .write()
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;
        let mut alias_by_original = self
            .alias_by_original
            .write()
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;
        let mut original_by_alias = self
            .original_by_alias
            .write()
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

        let alias = Self::allocate_alias(&tool_name, &original_by_alias);
        alias_by_original.insert(tool_name.clone(), alias.clone());
        original_by_alias.insert(alias, tool_name.clone());
        tools.insert(
            tool_name,
            RegisteredTool {
                implementation,
                description,
                input_schema,
                errors_as_results,
            },
        );
        Ok(())
    }

    fn allocate_alias(tool_name: &str, original_by_alias: &HashMap<String, String>) -> String {
        let base = Self::sanitize_tool_name(tool_name);
        if !original_by_alias.contains_key(&base) {
            return base;
        }

        let mut suffix = 2usize;
        loop {
            let candidate = format!("{base}_{suffix}");
            if !original_by_alias.contains_key(&candidate) {
                return candidate;
            }
            suffix += 1;
        }
    }

    fn sanitize_tool_name(name: &str) -> String {
        let mut out = String::with_capacity(name.len());
        for ch in name.chars() {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                out.push(ch);
            } else {
                out.push('_');
            }
        }

        let trimmed = out.trim_matches('_');
        if trimmed.is_empty() {
            "tool".to_string()
        } else {
            trimmed.to_string()
        }
    }

    fn validate_input_against_schema(schema: &Value, input: &Value) -> Result<()> {
        let schema_obj = schema.as_object().ok_or_else(|| {
            StasisError::PortFailure(
                "policy violation: tool schema must be a JSON object".to_string(),
            )
        })?;

        let expected_type = schema_obj.get("type").and_then(|value| value.as_str());
        if let Some(expected) = expected_type {
            Self::assert_json_type("$", input, expected)?;
        }

        if let Some(required) = schema_obj
            .get("required")
            .and_then(|value| value.as_array())
        {
            let input_obj = input.as_object().ok_or_else(|| {
                StasisError::PortFailure(
                    "policy violation: tool input must be an object for required fields"
                        .to_string(),
                )
            })?;

            for key in required.iter().filter_map(|value| value.as_str()) {
                if !input_obj.contains_key(key) {
                    return Err(StasisError::PortFailure(format!(
                        "policy violation: tool input is missing required field '{}'",
                        key
                    )));
                }
            }
        }

        if let Some(properties) = schema_obj
            .get("properties")
            .and_then(|value| value.as_object())
        {
            let input_obj = input.as_object().ok_or_else(|| {
                StasisError::PortFailure(
                    "policy violation: tool input must be an object for property validation"
                        .to_string(),
                )
            })?;

            for (key, schema_entry) in properties {
                let Some(value) = input_obj.get(key) else {
                    continue;
                };

                if let Some(expected) = schema_entry.get("type").and_then(|v| v.as_str()) {
                    Self::assert_json_type(key, value, expected)?;
                }

                if let Some(choices) = schema_entry.get("enum").and_then(|v| v.as_array())
                    && !choices.iter().any(|choice| choice == value)
                {
                    return Err(StasisError::PortFailure(format!(
                        "policy violation: tool input field '{}' must match one of enum values",
                        key
                    )));
                }
            }

            let additional_allowed = schema_obj
                .get("additionalProperties")
                .and_then(|value| value.as_bool())
                .unwrap_or(true);

            if !additional_allowed {
                for key in input_obj.keys() {
                    if !properties.contains_key(key) {
                        return Err(StasisError::PortFailure(format!(
                            "policy violation: tool input field '{}' is not allowed by schema",
                            key
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn assert_json_type(path: &str, value: &Value, expected: &str) -> Result<()> {
        let matches = match expected {
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "boolean" => value.is_boolean(),
            "object" => value.is_object(),
            "array" => value.is_array(),
            "null" => value.is_null(),
            _ => true,
        };

        if matches {
            return Ok(());
        }

        Err(StasisError::PortFailure(format!(
            "policy violation: tool input field '{}' expected type '{}', got {}",
            path,
            expected,
            Self::json_type_name(value)
        )))
    }

    fn json_type_name(value: &Value) -> &'static str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(number) if number.as_i64().is_some() || number.as_u64().is_some() => {
                "integer"
            }
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }
}

#[async_trait]
impl ToolRegistry for InMemoryToolRegistry {
    async fn list_tools(&self) -> Result<Vec<Tool>> {
        let tools = self
            .tools
            .read()
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

        let alias_by_original = self
            .alias_by_original
            .read()
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

        let mut definitions = Vec::with_capacity(tools.len());
        for (original_name, tool) in tools.iter() {
            let advertised_name = alias_by_original
                .get(original_name)
                .cloned()
                .unwrap_or_else(|| original_name.clone());

            let mut definition = Tool::new(advertised_name);
            if let Some(description) = tool.description.as_deref() {
                definition = definition.with_description(description);
            }
            if let Some(schema) = tool.input_schema.clone() {
                definition = definition.with_schema(schema);
            }
            definitions.push(definition);
        }

        Ok(definitions)
    }

    async fn invoke_tool(&self, tool_name: &str, input: Value) -> Result<Value> {
        let resolved_name = {
            let original_by_alias = self
                .original_by_alias
                .read()
                .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

            original_by_alias
                .get(tool_name)
                .cloned()
                .unwrap_or_else(|| tool_name.to_string())
        };

        let tool = {
            let tools = self
                .tools
                .read()
                .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

            tools.get(&resolved_name).cloned().ok_or_else(|| {
                StasisError::PortFailure(format!("tool not registered: {}", tool_name))
            })?
        };

        if let Some(schema) = tool.input_schema.as_ref()
            && let Err(error) = Self::validate_input_against_schema(schema, &input)
        {
            if tool.errors_as_results {
                return Ok(structured_tool_error(
                    tool_name,
                    "invalid_arguments",
                    error.to_string(),
                ));
            }
            return Err(error);
        }

        tool.implementation.invoke(input).await
    }
}

fn structured_tool_error(tool_name: &str, code: &str, message: String) -> Value {
    serde_json::json!({
        "ok": false,
        "error": {
            "tool": tool_name,
            "code": code,
            "message": message,
        }
    })
}

#[cfg(test)]
mod dynamic_tool_tests {
    use super::*;
    use serde_json::json;

    struct TestDynamicTool;

    #[async_trait]
    impl StasisTool for TestDynamicTool {
        fn name(&self) -> &'static str {
            "internal-placeholder"
        }

        async fn invoke(&self, input: Value) -> Result<Value> {
            Ok(json!({ "received": input }))
        }
    }

    #[tokio::test]
    async fn dynamic_metadata_is_advertised_and_shared_across_clones() {
        let registry = InMemoryToolRegistry::default();
        let runtime_handle = registry.clone();
        registry
            .register_dynamic_tool(
                "host.lookup",
                "Look up a value",
                json!({
                    "type": "object",
                    "properties": { "query": { "type": "string" } },
                    "required": ["query"]
                }),
                true,
                TestDynamicTool,
            )
            .unwrap();

        let tools = runtime_handle.list_tools().await.unwrap();
        let definition = tools
            .iter()
            .find(|tool| tool_advertised_name(tool) == "host_lookup")
            .unwrap();
        assert_eq!(tool_description(definition), Some("Look up a value"));

        let output = runtime_handle
            .invoke_tool("host.lookup", json!({ "query": "status" }))
            .await
            .unwrap();
        assert_eq!(output["received"]["query"], "status");
    }

    #[tokio::test]
    async fn dynamic_validation_errors_can_be_returned_to_the_tool_loop() {
        let registry = InMemoryToolRegistry::default();
        registry
            .register_dynamic_tool(
                "host.lookup",
                "Look up a value",
                json!({
                    "type": "object",
                    "properties": { "query": { "type": "string" } },
                    "required": ["query"],
                    "additionalProperties": false
                }),
                true,
                TestDynamicTool,
            )
            .unwrap();

        let output = registry
            .invoke_tool("host.lookup", json!({ "unexpected": true }))
            .await
            .unwrap();
        assert_eq!(output["ok"], false);
        assert_eq!(output["error"]["code"], "invalid_arguments");
        assert_eq!(output["error"]["tool"], "host.lookup");
    }
}
