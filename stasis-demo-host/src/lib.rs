//! Thin OpenAI loopback proxy + public mint endpoint for the runstasis.io demo.
//!
//! The binary (`stasis-demo-host`) holds `OPENAI_API_KEY`, shares the proxy
//! through `urspace::Site`, and mints subject-bound `usi1.` tokens. This library
//! is the proxy, mint, and quota logic so tests can run without starting Iroh.

#![forbid(unsafe_code)]

pub mod config;
pub mod error;
pub mod mint;
pub mod proxy;
pub mod quota;
pub mod rate_limit;
pub mod subject;

pub use config::Config;
pub use error::ApiError;
pub use quota::{QuotaLimits, QuotaStore};
pub use rate_limit::SlidingWindowLimiter;
pub use subject::{parse_public_key_bytes, parse_subject_hex, subject_hex};
