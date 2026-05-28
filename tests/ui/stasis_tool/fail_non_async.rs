use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use stasis::domain::errors::Result;
use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct Input {
    value: String,
}

#[derive(Debug, Clone, Serialize)]
struct Output {
    value: String,
}

#[stasis_tool(name = "fail_non_async")]
fn fail_non_async(input: Input) -> Result<Output> {
    Ok(Output { value: input.value })
}

fn main() {}
