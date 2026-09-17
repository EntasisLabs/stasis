//! Monotonic and wall clocks that work on `wasm32-unknown-unknown`.
//!
//! `std::time::{Instant, SystemTime}` panic on this target without WASI.
//! `web-time` uses the JS `Date` / `performance` clocks that `wasm-bindgen-test`
//! (and browser hosts) provide.

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_arch = "wasm32")]
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};
