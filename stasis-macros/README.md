# stasis-rs-macros

Procedural macros for the Stasis framework ([`stasis-rs`](https://crates.io/crates/stasis-rs)).

Licensed under MIT OR Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`).

## Provided Macro

- `#[stasis_tool(...)]`: generates a `StasisTool` implementation from a typed async function.

This crate is used by `stasis` and is typically consumed through `stasis::stasis_tool`.
