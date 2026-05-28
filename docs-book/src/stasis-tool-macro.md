# Stasis Tool Macro

## Outcome

Use `#[stasis_tool(...)]` to generate `StasisTool` implementations from typed async functions with compile-time contract checks.

## Why This Exists

`genai` exposes runtime tool metadata and schema wiring (`Tool::new(...).with_schema(...)`), but does not currently provide a function-signature-driven proc macro for Stasis runtime registration.

The Stasis macro adds that ergonomics layer while preserving explicit JSON contracts.

## Basic Usage

```rust
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stasis::domain::errors::Result;
use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct SearchInput {
    query: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct SearchOutput {
    summary: String,
}

#[stasis_tool(
    name = "search_docs",
    description = "Search internal knowledge base",
    output_schema = true
)]
async fn search_docs(input: SearchInput) -> Result<SearchOutput> {
    Ok(SearchOutput {
        summary: format!("query={}", input.query),
    })
}

// Generated symbols:
// - struct SearchDocsTool;
// - fn search_docs_tool() -> SearchDocsTool;
```

Register generated tool with runtime builder:

```rust
let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    .with_locus_memory()
    .with_tool(search_docs_tool())?;
```

## Attribute Arguments

- `name = "..."` (required): advertised tool name.
- `description = "..."` (optional): LLM-visible tool description.
- `output_schema = true|false` (optional, default `false`): generate `output_schema()` from output type.
- `crate_path = "..."` (optional): override crate path for macro expansion in advanced/internal scenarios.

## Compile-Time Contract

The macro enforces:

1. Function is `async`.
2. Function has exactly one typed input argument.
3. Function is non-generic.
4. Return type is `Result<OutputType>`.
5. Input type implements `Deserialize + JsonSchema`.
6. Output type implements `Serialize`.
7. When `output_schema = true`, output type also implements `JsonSchema`.

## Generated Behavior

The generated `StasisTool` implementation:

- Exposes `name`, `description`, and input schema.
- Optionally exposes output schema.
- Deserializes runtime JSON input into your typed input struct.
- Calls your function and maps output back to JSON.
- Maps serde conversion failures to `StasisError::PortFailure` with tool-scoped messages.

## UI Contract Tests

Compile-fail and compile-pass cases are covered by trybuild tests under `tests/ui/stasis_tool`.
