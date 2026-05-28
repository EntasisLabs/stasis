use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct Input {
    value: String,
}

#[derive(Debug, Clone, Serialize)]
struct Output {
    value: String,
}

#[stasis_tool(name = "fail_return_type")]
async fn fail_return_type(input: Input) -> Output {
    Output { value: input.value }
}

fn main() {}
