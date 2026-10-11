//! Native SQLite storage for the Stasis runtime.
//!
//! [`schema`] holds the driver-free DDL. [`session`] is the rusqlite connection
//! used by the native stores. A Durable Object backend can reuse `schema` and
//! reimplement the stores against the Workers SQLite API.

pub mod codec;
pub mod schema;
pub mod session;

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
pub mod thread_store;
#[cfg(feature = "sqlite")]
pub mod workflow_definition_store;

#[cfg(feature = "sqlite")]
pub use session::SqliteSession;
