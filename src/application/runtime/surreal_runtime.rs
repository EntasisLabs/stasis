//! SurrealDB backend. Orchestration lives in [`StoreRuntime`](crate::application::runtime::store_runtime::StoreRuntime).

use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::application::runtime::store_runtime::StoreRuntime;
use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::infrastructure::runtime::surreal_durable_wait_store::SurrealDurableWaitStore;
use crate::infrastructure::runtime::surreal_inbound_trigger_store::SurrealInboundTriggerStore;
use crate::infrastructure::runtime::surreal_job_attempt_store::SurrealJobAttemptStore;
use crate::infrastructure::runtime::surreal_job_continuation_store::SurrealJobContinuationStore;
use crate::infrastructure::runtime::surreal_job_store::SurrealJobStore;
use crate::infrastructure::runtime::surreal_outbox_store::SurrealOutboxStore;
use crate::infrastructure::runtime::surreal_recurring_store::SurrealRecurringStore;
use crate::infrastructure::runtime::surreal_resource_lease_store::SurrealResourceLeaseStore;
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::infrastructure::telemetry::NoopRuntimeTracing;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;

/// Surreal backend: [`StoreRuntime`] over the Surreal store adapters.
///
/// `runtime.job_store` stays a [`SurrealJobStore`], so `job_store.db()` still
/// returns the shared `Surreal<Any>` handle.
pub type SurrealRuntime = StoreRuntime<
    SurrealJobStore,
    SurrealRecurringStore,
    SurrealOutboxStore,
    SurrealJobAttemptStore,
    SurrealDurableWaitStore,
    SurrealJobContinuationStore,
    SurrealInboundTriggerStore,
    SurrealResourceLeaseStore,
>;

impl
    StoreRuntime<
        SurrealJobStore,
        SurrealRecurringStore,
        SurrealOutboxStore,
        SurrealJobAttemptStore,
        SurrealDurableWaitStore,
        SurrealJobContinuationStore,
        SurrealInboundTriggerStore,
        SurrealResourceLeaseStore,
    >
{
    pub fn new(db: Surreal<Any>) -> Self {
        Self::with_dependencies_and_telemetry(
            db,
            Arc::new(SystemClock),
            Arc::new(AtomicIdGenerator::new(1)),
            Arc::new(NoopRuntimeMetrics),
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies(
        db: Surreal<Any>,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
    ) -> Self {
        Self::with_dependencies_and_telemetry(
            db,
            clock,
            id_generator,
            Arc::new(NoopRuntimeMetrics),
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies_and_metrics(
        db: Surreal<Any>,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
    ) -> Self {
        Self::with_dependencies_and_telemetry(
            db,
            clock,
            id_generator,
            metrics,
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies_and_telemetry(
        db: Surreal<Any>,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
        tracing: Arc<dyn RuntimeTracing>,
    ) -> Self {
        Self::from_stores(
            SurrealJobStore::new(db.clone()),
            SurrealRecurringStore::new(db.clone()),
            SurrealOutboxStore::new(db.clone()),
            SurrealJobAttemptStore::new(db.clone()),
            SurrealDurableWaitStore::new(db.clone()),
            SurrealJobContinuationStore::new(db.clone()),
            SurrealInboundTriggerStore::new(db.clone()),
            SurrealResourceLeaseStore::new(db),
            clock,
            id_generator,
            metrics,
            tracing,
        )
    }
}
