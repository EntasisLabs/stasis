#![cfg(feature = "sqlite")]
//! The retained safety regression suite through the DO transaction executor.
include!("support/native_review_fixtures.rs");
review_fixtures!(
    stasis::infrastructure::sqlite::durable::DurableObjectRuntime,
    stasis::infrastructure::sqlite::DurableSqliteKernel::open(std::sync::Arc::new(
        stasis::infrastructure::sqlite::RusqliteExecutor::open_in_memory().unwrap()
    ))
    .unwrap()
    .into_runtime()
);
