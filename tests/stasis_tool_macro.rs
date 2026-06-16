use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use stasis::application::orchestration::tool_registry::{
    InMemoryToolRegistry, StasisTool, ToolRegistry,
};
use stasis::domain::errors::Result;
use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct EchoInput {
    text: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct EchoOutput {
    upper: String,
}

#[stasis_tool(
    name = "echo_upper",
    description = "Uppercases text",
    output_schema = true
)]
async fn echo_upper(input: EchoInput) -> Result<EchoOutput> {
    Ok(EchoOutput {
        upper: input.text.to_uppercase(),
    })
}

#[tokio::test]
async fn stasis_tool_macro_generates_registry_compatible_tool() {
    let generated_tool = echo_upper_tool();
    let output_schema = generated_tool
        .output_schema()
        .expect("output schema should be generated when output_schema=true");
    assert_eq!(output_schema["type"], "object");
    assert_eq!(output_schema["properties"]["upper"]["type"], "string");

    let registry = InMemoryToolRegistry::default();
    registry
        .register_tool(generated_tool)
        .expect("macro-generated tool should register");

    let tools = registry
        .list_tools()
        .await
        .expect("tool listing should succeed");

    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name.as_ref(), "echo_upper");

    let schema = tools[0]
        .schema
        .as_ref()
        .expect("macro-generated tool should include schema");
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["text"]["type"], "string");

    let output = registry
        .invoke_tool("echo_upper", json!({ "text": "hello" }))
        .await
        .expect("tool invocation should succeed");

    assert_eq!(output["upper"], "HELLO");
}
