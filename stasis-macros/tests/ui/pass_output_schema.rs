mod support;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stasis_macros::stasis_tool;

use support::fake_stasis;
use support::fake_stasis::application::orchestration::tool_registry::StasisTool;

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
    output_schema = true,
    crate_path = "crate::support::fake_stasis"
)]
async fn pass_output_schema(input: Input) -> fake_stasis::domain::errors::Result<Output> {
    Ok(Output { value: input.value })
}

fn main() {
    let tool = pass_output_schema_tool();
    let schema = tool.output_schema();
    assert!(schema.is_some());
}
