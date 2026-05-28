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

#[stasis_tool(name = "pass_basic", description = "passes basic signature")]
async fn pass_basic(input: Input) -> Result<Output> {
    Ok(Output { value: input.value })
}

fn main() {
    let _tool = pass_basic_tool();
}
