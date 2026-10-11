//! Native SQLite backend. Orchestration lives in [`StoreRuntime`](crate::application::runtime::store_runtime::StoreRuntime).

use std::sync::Arc;

use crate::application::runtime::store_runtime::StoreRuntime;
use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::infrastructure::sqlite::durable_wait_store::SqliteDurableWaitStore;
use crate::infrastructure::sqlite::inbound_trigger_store::SqliteInboundTriggerStore;
use crate::infrastructure::sqlite::job_attempt_store::SqliteJobAttemptStore;
use crate::infrastructure::sqlite::job_continuation_store::SqliteJobContinuationStore;
use crate::infrastructure::sqlite::job_store::SqliteJobStore;
use crate::infrastructure::sqlite::outbox_store::SqliteOutboxStore;
use crate::infrastructure::sqlite::recurring_store::SqliteRecurringStore;
use crate::infrastructure::sqlite::resource_lease_store::SqliteResourceLeaseStore;
use crate::infrastructure::sqlite::session::SqliteSession;
use crate::infrastructure::telemetry::NoopRuntimeTracing;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;

/// SQLite backend: [`StoreRuntime`] over the SQLite store adapters.
///
/// `runtime.job_store` stays a [`SqliteJobStore`], so `job_store.session()` still
/// returns the shared [`SqliteSession`].
pub type SqliteRuntime = StoreRuntime<
    SqliteJobStore,
    SqliteRecurringStore,
    SqliteOutboxStore,
    SqliteJobAttemptStore,
    SqliteDurableWaitStore,
    SqliteJobContinuationStore,
    SqliteInboundTriggerStore,
    SqliteResourceLeaseStore,
>;

impl
    StoreRuntime<
        SqliteJobStore,
        SqliteRecurringStore,
        SqliteOutboxStore,
        SqliteJobAttemptStore,
        SqliteDurableWaitStore,
        SqliteJobContinuationStore,
        SqliteInboundTriggerStore,
        SqliteResourceLeaseStore,
    >
{
    pub fn open_memory() -> crate::domain::errors::Result<Self> {
        Self::open_path(":memory:")
    }

    pub fn open_path(path: impl AsRef<str>) -> crate::domain::errors::Result<Self> {
        Ok(Self::new(SqliteSession::open(path.as_ref())?))
    }

    pub fn new(session: SqliteSession) -> Self {
        Self::with_dependencies_and_telemetry(
            session,
            Arc::new(SystemClock),
            Arc::new(AtomicIdGenerator::new(1)),
            Arc::new(NoopRuntimeMetrics),
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies(
        session: SqliteSession,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
    ) -> Self {
        Self::with_dependencies_and_telemetry(
            session,
            clock,
            id_generator,
            Arc::new(NoopRuntimeMetrics),
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies_and_metrics(
        session: SqliteSession,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
    ) -> Self {
        Self::with_dependencies_and_telemetry(
            session,
            clock,
            id_generator,
            metrics,
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies_and_telemetry(
        session: SqliteSession,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
        tracing: Arc<dyn RuntimeTracing>,
    ) -> Self {
        Self::from_stores(
            SqliteJobStore::new(session.clone()),
            SqliteRecurringStore::new(session.clone()),
            SqliteOutboxStore::new(session.clone()),
            SqliteJobAttemptStore::new(session.clone()),
            SqliteDurableWaitStore::new(session.clone()),
            SqliteJobContinuationStore::new(session.clone()),
            SqliteInboundTriggerStore::new(session.clone()),
            SqliteResourceLeaseStore::new(session),
            clock,
            id_generator,
            metrics,
            tracing,
        )
    }
}
