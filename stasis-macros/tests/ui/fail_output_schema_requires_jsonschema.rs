mod support;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stasis_macros::stasis_tool;

use support::fake_stasis;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct Input {
    value: String,
}

#[derive(Debug, Clone, Serialize)]
struct Output {
    value: String,
}

#[stasis_tool(
    name = "fail_output_schema",
    output_schema = true,
    crate_path = "crate::support::fake_stasis"
)]
async fn fail_output_schema(input: Input) -> fake_stasis::domain::errors::Result<Output> {
    Ok(Output { value: input.value })
}

fn main() {}
