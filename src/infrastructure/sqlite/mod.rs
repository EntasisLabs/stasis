//! SQLite storage for the Stasis runtime.
//!
//! [`schema`] holds the driver-free DDL. Native stores (`feature = "sqlite"`)
//! execute it through rusqlite. The Durable Object kernel
//! ([`DurableSqliteKernel`]) executes the same DDL through [`SqlExecutor`],
//! which the wasm guest binds to `ctx.storage.sql`.

pub(crate) mod atomic_state;
pub mod bridge_codec;
pub mod codec;
pub mod durable;
pub mod durable_ids;
pub mod executor;
pub mod executor_attempt_store;
pub mod executor_inbound_store;
pub mod executor_job_store;
pub mod executor_lease_store;
pub mod executor_workflow_store;
pub mod migrate;
pub mod safety_stores;
pub mod schema;
pub mod script;

#[cfg(feature = "sqlite")]
pub mod cluster_forward_outcome_store;
#[cfg(feature = "sqlite")]
pub mod cluster_node_store;
#[cfg(feature = "sqlite")]
pub mod delivery_endpoint_store;
#[cfg(feature = "sqlite")]
pub mod durable_wait_store;
#[cfg(feature = "sqlite")]
pub mod endpoint_delivery_status_store;
#[cfg(feature = "sqlite")]
pub mod identity_memory_store;
#[cfg(feature = "sqlite")]
pub mod inbound_trigger_store;
#[cfg(feature = "sqlite")]
pub mod job_attempt_store;
#[cfg(feature = "sqlite")]
pub mod job_continuation_store;
#[cfg(feature = "sqlite")]
pub mod job_store;
#[cfg(feature = "sqlite")]
pub mod outbox_store;
#[cfg(feature = "sqlite")]
pub mod recurring_store;
#[cfg(feature = "sqlite")]
pub mod resource_lease_store;
#[cfg(feature = "sqlite")]
pub mod rusqlite_executor;
#[cfg(feature = "sqlite")]
pub mod session;
#[cfg(feature = "sqlite")]
pub mod thread_store;
#[cfg(feature = "sqlite")]
pub mod workflow_definition_store;

pub use durable::{DurableObjectRuntime, DurableSqliteKernel, ProcessedJob};
pub use executor::{SqlExecutor, SqlRow, SqlValue};
pub use executor_attempt_store::ExecutorJobAttemptStore;
pub use executor_inbound_store::ExecutorInboundTriggerStore;
pub use executor_job_store::ExecutorJobStore;
pub use executor_lease_store::ExecutorResourceLeaseStore;
pub use executor_workflow_store::ExecutorWorkflowDefinitionStore;
#[cfg(feature = "sqlite")]
pub use rusqlite_executor::RusqliteExecutor;
#[cfg(feature = "sqlite")]
pub use session::SqliteSession;
