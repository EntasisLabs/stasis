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

#[stasis_tool(name = "fail_multiple_args")]
async fn fail_multiple_args(a: Input, b: Input) -> Result<Output> {
    Ok(Output {
        value: format!("{}{}", a.value, b.value),
    })
}

fn main() {}
