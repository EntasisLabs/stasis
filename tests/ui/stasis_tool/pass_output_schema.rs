use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use stasis::application::orchestration::tool_registry::StasisTool;
use stasis::domain::errors::Result;
use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct Input {
    value: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct Output {
    value: String,
}

#[stasis_tool(
    name = "pass_output_schema",
    description = "passes output schema",
    output_schema = true
)]
async fn pass_output_schema(input: Input) -> Result<Output> {
    Ok(Output { value: input.value })
}

fn main() {
    let tool = pass_output_schema_tool();
    let schema = tool.output_schema();
    assert!(schema.is_some());
}
