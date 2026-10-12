#![cfg(feature = "sqlite")]
//! The retained safety regression suite on native SQLite, not a new behavioral oracle.
include!("support/native_review_fixtures.rs");
review_fixtures!(
    stasis::application::runtime::sqlite_runtime::SqliteRuntime,
    stasis::application::runtime::sqlite_runtime::SqliteRuntime::open_memory().unwrap()
);
