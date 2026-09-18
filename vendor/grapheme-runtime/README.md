# Vendored `grapheme-runtime` 0.7.1 (wasm Instant shim)

Published `grapheme-runtime` 0.7.1 calls `std::time::Instant::now()` on every
function execution. That panics on `wasm32-unknown-unknown` (`time not
implemented on this platform`).

This tree is crates.io 0.7.1 plus `web-time::Instant` on wasm32 so
`grapheme-wasm` 0.7.1 can actually execute in the Stasis guest. Remove the
`[patch.crates-io]` entry in the workspace `Cargo.toml` once upstream uses a
wasm-safe clock.

Do not treat this as a Grapheme fork for new features.
