mod support;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stasis_macros::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct Input {
    value: String,
}

#[derive(Debug, Clone, Serialize)]
struct Output {
    value: String,
}

#[stasis_tool(name = "fail_return_type", crate_path = "crate::support::fake_stasis")]
async fn fail_return_type(input: Input) -> Output {
    Output { value: input.value }
}

fn main() {}
