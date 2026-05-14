use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use genai::chat::Tool;
use serde_json::Value;

use crate::domain::errors::{Result, StasisError};

#[async_trait]
pub trait StasisTool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&'static str> {
        None
    }
    fn input_schema(&self) -> Option<Value> {
        None
    }
    async fn invoke(&self, input: Value) -> Result<Value>;
}

#[async_trait]
pub trait ToolRegistry: Send + Sync {
    async fn list_tools(&self) -> Result<Vec<Tool>>;
    async fn invoke_tool(&self, tool_name: &str, input: Value) -> Result<Value>;
}

#[derive(Clone, Default)]
pub struct InMemoryToolRegistry {
    tools: Arc<RwLock<HashMap<String, Arc<dyn StasisTool>>>>,
}

impl InMemoryToolRegistry {
    pub fn register_tool<T: StasisTool + 'static>(&self, tool: T) -> Result<()> {
        let mut tools = self
            .tools
            .write()
            .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

        tools.insert(tool.name().to_string(), Arc::new(tool));
        Ok(())
    }

    fn validate_input_against_schema(schema: &Value, input: &Value) -> Result<()> {
        let schema_obj = schema.as_object().ok_or_else(|| {
            StasisError::PortFailure("policy violation: tool schema must be a JSON object".to_string())
        })?;

        let expected_type = schema_obj.get("type").and_then(|value| value.as_str());
        if let Some(expected) = expected_type {
            Self::assert_json_type("$", input, expected)?;
        }

        if let Some(required) = schema_obj.get("required").and_then(|value| value.as_array()) {
            let input_obj = input.as_object().ok_or_else(|| {
                StasisError::PortFailure(
                    "policy violation: tool input must be an object for required fields".to_string(),
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

        if let Some(properties) = schema_obj.get("properties").and_then(|value| value.as_object()) {
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

                if let Some(choices) = schema_entry.get("enum").and_then(|v| v.as_array()) {
                    if !choices.iter().any(|choice| choice == value) {
                        return Err(StasisError::PortFailure(format!(
                            "policy violation: tool input field '{}' must match one of enum values",
                            key
                        )));
                    }
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

        let mut definitions = Vec::with_capacity(tools.len());
        for tool in tools.values() {
            let mut definition = Tool::new(tool.name());
            if let Some(description) = tool.description() {
                definition = definition.with_description(description);
            }
            if let Some(schema) = tool.input_schema() {
                definition = definition.with_schema(schema);
            }
            definitions.push(definition);
        }

        Ok(definitions)
    }

    async fn invoke_tool(&self, tool_name: &str, input: Value) -> Result<Value> {
        let tool = {
            let tools = self
                .tools
                .read()
                .map_err(|_| StasisError::PortFailure("tool registry lock poisoned".to_string()))?;

            tools.get(tool_name).cloned().ok_or_else(|| {
                StasisError::PortFailure(format!("tool not registered: {}", tool_name))
            })?
        };

        if let Some(schema) = tool.input_schema() {
            Self::validate_input_against_schema(&schema, &input)?;
        }

        tool.invoke(input).await
    }
}
