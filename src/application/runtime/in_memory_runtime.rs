use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::infrastructure::runtime::portable_time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::application::runtime::execution_commit::{
    CleanupRecord, CommitResult, ExecutionGrant, ExecutionReceipt, cancellation, outcome_digest,
    publication,
};
use crate::application::runtime::inbound_trigger::{
    accept_inbound_trigger, decode_inbound_json, trigger_from_job,
};
use crate::application::runtime::job_context::{JobContext, JobContextServices};
use crate::application::runtime::job_continuation::{ContinuationBuilder, settle_continuations};
use crate::application::runtime::job_lifecycle::{
    DEFAULT_JOB_LEASE_SECONDS, JobLifecycleEvent, STALE_LEASE_MESSAGE, StaleRecoverReport,
};
use crate::application::runtime::replay_report::ReplayReport;
use crate::application::runtime::retention::{RetentionPolicy, RetentionPruneReport};
use crate::application::runtime::runtime_job_dx::{self, InFlightMap};
use crate::application::runtime::runtime_job_identity_context::RuntimeJobIdentityContext;
use crate::application::runtime::typed_job::{JobConsumer, TypedEnqueueBuilder, TypedJobHandler};
use crate::application::telemetry::keys as metric_keys;
use crate::application::telemetry::operation::{OperationTelemetry, runtime_event_type_name};
use crate::application::telemetry::propagation::{
    job_execute_span_attributes, parent_trace_context,
};
use crate::application::telemetry::request_context::{
    inbound_trace_context_for_propagation, trace_id_for_enqueue,
};
use crate::application::telemetry::spans as span_names;
use crate::application::use_cases::investigate_runtime_lineage::{
    InvestigateRuntimeLineage, RuntimeLineageQuery, RuntimeLineageReport,
};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::inbound_trigger::{InboundAccept, InboundProtocol};
use crate::domain::runtime::job::{Job, JobState, NewJob};
use crate::domain::runtime::job_attempt::JobAttempt;
use crate::domain::runtime::outbox::{OutboxEvent, OutboxPublishPolicy, OutboxStatus};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::domain::runtime::typed_contract::{StasisEvent, StasisJob};
use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
use crate::infrastructure::runtime::in_memory_durable_wait_store::InMemoryDurableWaitStore;
use crate::infrastructure::runtime::in_memory_inbound_trigger_store::InMemoryInboundTriggerStore;
use crate::infrastructure::runtime::in_memory_job_continuation_store::InMemoryJobContinuationStore;
use crate::infrastructure::runtime::in_memory_resource_lease_store::InMemoryResourceLeaseStore;
use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::infrastructure::telemetry::NoopRuntimeTracing;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::event_publisher::EventPublisher;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::{OtelAttribute, RuntimeTracing};

pub use crate::domain::runtime::execution::JobExecutionOutcome;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait JobHandler: Send + Sync {
    fn job_type(&self) -> &'static str;
    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome>;
    async fn execute_with_context(
        &self,
        job: &Job,
        _ctx: JobContext,
    ) -> Result<JobExecutionOutcome> {
        self.execute(job).await
    }

    async fn on_lifecycle_committed(
        &self,
        job: &Job,
        event: &JobLifecycleEvent,
        _idempotency_key: &str,
    ) -> Result<()> {
        self.on_lifecycle(job, event).await
    }

    async fn on_lifecycle(&self, job: &Job, event: &JobLifecycleEvent) -> Result<()> {
        let _ = (job, event);
        Ok(())
    }
}

#[derive(Clone)]
pub struct InMemoryRuntime {
    pub job_store: InMemoryJobStore,
    pub recurring_store: InMemoryRecurringStore,
    pub outbox_store: InMemoryOutboxStore,
    pub job_attempt_store: InMemoryJobAttemptStore,
    pub wait_store: InMemoryDurableWaitStore,
    pub continuation_store: InMemoryJobContinuationStore,
    pub inbound_trigger_store: InMemoryInboundTriggerStore,
    pub lease_store: InMemoryResourceLeaseStore,
    handlers: Arc<RwLock<HashMap<String, Arc<dyn JobHandler>>>>,
    publisher: Arc<RwLock<Option<Arc<dyn EventPublisher>>>>,
    publish_policy: Arc<RwLock<OutboxPublishPolicy>>,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
    metrics: Arc<dyn RuntimeMetrics>,
    tracing: Arc<dyn RuntimeTracing>,
    retention_policy: Arc<RwLock<RetentionPolicy>>,
    in_flight: InFlightMap,
    lifecycle_delivery: Arc<tokio::sync::Mutex<()>>,
}

impl Default for InMemoryRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRuntime {
    pub fn new() -> Self {
        Self::with_dependencies_and_telemetry(
            Arc::new(SystemClock),
            Arc::new(AtomicIdGenerator::new(1)),
            Arc::new(NoopRuntimeMetrics),
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies(clock: Arc<dyn Clock>, id_generator: Arc<dyn IdGenerator>) -> Self {
        Self::with_dependencies_and_telemetry(
            clock,
            id_generator,
            Arc::new(NoopRuntimeMetrics),
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies_and_metrics(
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
    ) -> Self {
        Self::with_dependencies_and_telemetry(
            clock,
            id_generator,
            metrics,
            Arc::new(NoopRuntimeTracing),
        )
    }

    pub fn with_dependencies_and_telemetry(
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
        tracing: Arc<dyn RuntimeTracing>,
    ) -> Self {
        let state = Arc::new(RwLock::new(NativeState::default()));
        Self {
            job_store: InMemoryJobStore {
                state: state.clone(),
                clock: clock.clone(),
            },
            recurring_store: InMemoryRecurringStore::default(),
            outbox_store: InMemoryOutboxStore {
                state: state.clone(),
            },
            job_attempt_store: InMemoryJobAttemptStore {
                state: state.clone(),
            },
            wait_store: InMemoryDurableWaitStore::from_native(state.clone()),
            continuation_store: InMemoryJobContinuationStore::from_native(state.clone()),
            inbound_trigger_store: InMemoryInboundTriggerStore::default(),
            lease_store: InMemoryResourceLeaseStore::default(),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            publisher: Arc::new(RwLock::new(None)),
            publish_policy: Arc::new(RwLock::new(OutboxPublishPolicy::default())),
            clock,
            id_generator,
            metrics,
            tracing,
            retention_policy: Arc::new(RwLock::new(RetentionPolicy::default())),
            in_flight: runtime_job_dx::new_in_flight_map(),
            lifecycle_delivery: Default::default(),
        }
    }

    pub fn replace_telemetry(
        &mut self,
        metrics: Arc<dyn RuntimeMetrics>,
        tracing: Arc<dyn RuntimeTracing>,
    ) {
        self.metrics = metrics;
        self.tracing = tracing;
    }

    pub fn tracing(&self) -> Arc<dyn RuntimeTracing> {
        self.tracing.clone()
    }

    pub fn configure_retention_policy(&self, policy: RetentionPolicy) -> Result<()> {
        let mut state = self
            .retention_policy
            .write()
            .map_err(|_| StasisError::PortFailure("retention policy lock poisoned".to_string()))?;
        *state = policy;
        Ok(())
    }

    pub fn register_handler<H: JobHandler + 'static>(&self, handler: H) -> Result<()> {
        let mut handlers = self
            .handlers
            .write()
            .map_err(|_| StasisError::PortFailure("handlers lock poisoned".to_string()))?;

        handlers.insert(handler.job_type().to_string(), Arc::new(handler));
        Ok(())
    }

    pub fn register_consumer<T, H>(&self, handler: H) -> Result<()>
    where
        T: StasisJob,
        H: JobConsumer<T> + 'static,
    {
        self.register_handler(TypedJobHandler::<T, H>::new(handler))
    }

    pub async fn accept_inbound_json(
        &self,
        protocol: InboundProtocol,
        body: &[u8],
    ) -> Result<InboundAccept> {
        let trigger = decode_inbound_json(protocol, body)?;
        self.accept_inbound_trigger(trigger).await
    }

    pub async fn accept_inbound_job<T: StasisJob>(
        &self,
        protocol: InboundProtocol,
        idempotency_key: impl Into<String>,
        payload: T,
    ) -> Result<InboundAccept> {
        let trigger = trigger_from_job(protocol, idempotency_key, &payload)?;
        self.accept_inbound_trigger(trigger).await
    }

    async fn accept_inbound_trigger(
        &self,
        trigger: crate::domain::runtime::inbound_trigger::InboundJobTrigger,
    ) -> Result<InboundAccept> {
        accept_inbound_trigger(
            &self.job_store,
            &self.inbound_trigger_store,
            self.clock.as_ref(),
            self.id_generator.as_ref(),
            trigger,
        )
        .await
    }

    pub fn enqueue_job<T: StasisJob>(&self, payload: T) -> TypedEnqueueBuilder<T> {
        TypedEnqueueBuilder::new(
            payload,
            self.clock.clone(),
            self.id_generator.clone(),
            Arc::new(self.job_store.clone()),
        )
    }

    pub fn continue_with<C: StasisJob>(
        &self,
        parent_job_id: impl Into<String>,
        child: C,
    ) -> ContinuationBuilder<C> {
        ContinuationBuilder::new(
            parent_job_id.into(),
            child,
            self.clock.clone(),
            self.id_generator.clone(),
            Arc::new(self.job_store.clone()),
            Arc::new(self.continuation_store.clone()),
        )
    }

    pub fn job_context_services(&self) -> JobContextServices {
        JobContextServices {
            job_store: Arc::new(self.job_store.clone()),
            outbox_store: Arc::new(self.outbox_store.clone()),
            wait_store: Arc::new(self.wait_store.clone()),
            continuation_store: Arc::new(self.continuation_store.clone()),
            clock: self.clock.clone(),
            id_generator: self.id_generator.clone(),
        }
    }

    fn handler_for(&self, job_type: &str) -> Result<Option<Arc<dyn JobHandler>>> {
        let handlers = self
            .handlers
            .read()
            .map_err(|_| StasisError::PortFailure("handlers lock poisoned".to_string()))?;
        Ok(handlers.get(job_type).cloned())
    }

    async fn emit_lifecycle_with_output(
        &self,
        job: &Job,
        event: JobLifecycleEvent,
        parent_output_json: Option<String>,
    ) -> Result<()> {
        if let Some(handler) = self.handler_for(&job.job_type)? {
            if let Err(err) = handler
                .on_lifecycle_committed(
                    job,
                    &event,
                    &format!(
                        "{}:{}:{}:{}",
                        job.id, job.incarnation, job.execution_epoch, job.version
                    ),
                )
                .await
            {
                self.metrics
                    .incr_counter(metric_keys::JOB_LIFECYCLE_HOOK_FAILURE_TOTAL, 1);
                return Err(err);
            }
        }
        settle_continuations(
            &self.continuation_store,
            &self.job_store,
            self.clock.as_ref(),
            self.id_generator.as_ref(),
            job,
            &event,
            parent_output_json,
        )
        .await
    }

    pub async fn deliver_execution_lifecycle(&self, job_id: &str, epoch: u64) -> Result<()> {
        let _delivery = self.lifecycle_delivery.lock().await;
        let key = (job_id.to_string(), epoch);
        let (receipt, job) = {
            let state = self
                .job_store
                .state
                .read()
                .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
            let Some(receipt) = state.receipts.get(&key).filter(|r| !r.delivered) else {
                return Ok(());
            };
            let job: Job = serde_json::from_str(&receipt.job_json)
                .map_err(|e| StasisError::PortFailure(e.to_string()))?;
            (receipt.clone(), job)
        };
        self.emit_lifecycle_with_output(&job, receipt.lifecycle, receipt.output_json)
            .await?;
        let mut state = self
            .job_store
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if let Some(receipt) = state.receipts.get_mut(&key) {
            receipt.delivered = true;
        }
        Ok(())
    }

    pub async fn recover_stale_now(&self) -> Result<StaleRecoverReport> {
        self.recover_stale(self.clock.now()).await
    }

    pub async fn recover_stale(&self, now: DateTime<Utc>) -> Result<StaleRecoverReport> {
        let expired = self.job_store.list_expired_leases(now).await?;
        let mut report = StaleRecoverReport::default();
        for job in expired {
            let result = self.job_store.recover_execution(&job).await?;
            if result == CommitResult::Committed {
                let current = self.job_store.get(&job.id).await?.unwrap();
                if current.state == JobState::DeadLetter {
                    report.dead_lettered += 1;
                } else {
                    report.recovered += 1;
                }
                self.deliver_execution_lifecycle(&job.id, job.execution_epoch)
                    .await?;
            }
        }
        self.replay_lifecycle().await?;
        Ok(report)
    }

    pub async fn cancel(&self, job_id: &str) -> Result<bool> {
        let Some(job) = self.job_store.cancel_execution(job_id).await? else {
            return Ok(false);
        };
        runtime_job_dx::request_cancel(&self.in_flight, job_id, job.execution_epoch);
        self.deliver_cancellation_lifecycle(&job.id).await?;
        Ok(true)
    }

    pub async fn deliver_cancellation_lifecycle(&self, job_id: &str) -> Result<()> {
        let keys = {
            let state = self
                .job_store
                .state
                .read()
                .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
            state
                .cancellations
                .iter()
                .filter(|(key, r)| {
                    !r.delivered && (r.grant.job_id == job_id || key.as_str() == job_id)
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>()
        };
        for key in keys {
            self.deliver_control_receipt(&key).await?;
        }
        Ok(())
    }

    async fn deliver_control_receipt(&self, key: &str) -> Result<()> {
        let _delivery = self.lifecycle_delivery.lock().await;
        let receipt = {
            let state = self
                .job_store
                .state
                .read()
                .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
            let Some(receipt) = state.cancellations.get(key).filter(|r| !r.delivered) else {
                return Ok(());
            };
            receipt.clone()
        };
        let job: Job = serde_json::from_str(&receipt.job_json)
            .map_err(|e| StasisError::PortFailure(e.to_string()))?;
        self.emit_lifecycle_with_output(&job, receipt.lifecycle, None)
            .await?;
        // Native cancellation already settled A's waits atomically. Replaying a
        // retained receipt must never cancel a recreated successor's waits.
        let mut state = self
            .job_store
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if let Some(receipt) = state.cancellations.get_mut(key) {
            receipt.delivered = true;
        }
        Ok(())
    }

    pub async fn replay_lifecycle(&self) -> Result<usize> {
        let (executions, cancellations) = {
            let state = self
                .job_store
                .state
                .read()
                .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
            (
                state
                    .receipts
                    .iter()
                    .filter(|(_, r)| !r.delivered)
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<_>>(),
                state
                    .cancellations
                    .iter()
                    .filter(|(_, r)| !r.delivered)
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<_>>(),
            )
        };
        let count = executions.len() + cancellations.len();
        for (id, epoch) in executions {
            self.deliver_execution_lifecycle(&id, epoch).await?;
        }
        for id in cancellations {
            self.deliver_cancellation_lifecycle(&id).await?;
        }
        Ok(count)
    }

    pub async fn fail(&self, job_id: &str) -> Result<bool> {
        let Some(job) = self.job_store.fail_execution(job_id).await? else {
            return Ok(false);
        };
        runtime_job_dx::request_cancel(&self.in_flight, job_id, job.execution_epoch);
        self.deliver_cancellation_lifecycle(job_id).await?;
        self.metrics
            .incr_counter(metric_keys::JOB_DEAD_LETTER_TOTAL, 1);
        Ok(true)
    }

    pub async fn delete(&self, job_id: &str) -> Result<bool> {
        let Some(job) = self.job_store.get(job_id).await? else {
            return Ok(false);
        };
        if !runtime_job_dx::is_terminal(&job.state) {
            return Err(StasisError::PortFailure(format!(
                "refusing to delete non-terminal job {} ({:?}); cancel or fail first",
                job.id, job.state
            )));
        }
        self.job_store.delete(job_id).await
    }

    pub async fn signal<E: StasisEvent>(
        &self,
        correlation_key: impl Into<String>,
        event: E,
    ) -> Result<bool> {
        runtime_job_dx::signal_event(
            &self.job_store,
            &self.wait_store,
            self.clock.as_ref(),
            correlation_key.into(),
            event,
        )
        .await
    }

    pub async fn acquire_lease(
        &self,
        resource: impl Into<String>,
        owner: impl Into<String>,
        ttl: std::time::Duration,
    ) -> Result<ResourceLease> {
        self.lease_store
            .acquire(
                ResourceKey(resource.into()),
                OwnerId(owner.into()),
                runtime_job_dx::chrono_ttl(ttl),
                self.clock.now(),
                false,
            )
            .await
    }

    pub async fn force_acquire_lease(
        &self,
        resource: impl Into<String>,
        owner: impl Into<String>,
        ttl: std::time::Duration,
    ) -> Result<ResourceLease> {
        self.lease_store
            .acquire(
                ResourceKey(resource.into()),
                OwnerId(owner.into()),
                runtime_job_dx::chrono_ttl(ttl),
                self.clock.now(),
                true,
            )
            .await
    }

    pub async fn renew_lease(
        &self,
        resource: impl Into<String>,
        owner: impl Into<String>,
        fencing_token: FencingToken,
        ttl: std::time::Duration,
    ) -> Result<ResourceLease> {
        self.lease_store
            .renew(
                &ResourceKey(resource.into()),
                &OwnerId(owner.into()),
                fencing_token,
                runtime_job_dx::chrono_ttl(ttl),
                self.clock.now(),
            )
            .await
    }

    pub async fn release_lease(
        &self,
        resource: impl Into<String>,
        owner: impl Into<String>,
        fencing_token: FencingToken,
    ) -> Result<bool> {
        self.lease_store
            .release(
                &ResourceKey(resource.into()),
                &OwnerId(owner.into()),
                fencing_token,
                self.clock.now(),
            )
            .await
    }

    pub async fn transfer_lease(
        &self,
        resource: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        fencing_token: FencingToken,
        ttl: std::time::Duration,
    ) -> Result<ResourceLease> {
        self.lease_store
            .transfer(
                &ResourceKey(resource.into()),
                &OwnerId(from.into()),
                OwnerId(to.into()),
                fencing_token,
                runtime_job_dx::chrono_ttl(ttl),
                self.clock.now(),
            )
            .await
    }

    pub async fn validate_fence(
        &self,
        resource: impl Into<String>,
        fencing_token: FencingToken,
    ) -> Result<bool> {
        self.lease_store
            .validate_fence(
                &ResourceKey(resource.into()),
                fencing_token,
                self.clock.now(),
            )
            .await
    }

    pub async fn watch_lease(&self, resource: impl Into<String>) -> Result<Option<ResourceLease>> {
        self.lease_store.get(&ResourceKey(resource.into())).await
    }

    pub fn register_event_publisher<P: EventPublisher + 'static>(
        &self,
        publisher: P,
    ) -> Result<()> {
        let mut state = self
            .publisher
            .write()
            .map_err(|_| StasisError::PortFailure("publisher lock poisoned".to_string()))?;

        *state = Some(Arc::new(publisher));
        Ok(())
    }

    pub fn configure_outbox_publish_policy(&self, policy: OutboxPublishPolicy) -> Result<()> {
        let mut state = self
            .publish_policy
            .write()
            .map_err(|_| StasisError::PortFailure("publish policy lock poisoned".to_string()))?;

        *state = policy;
        Ok(())
    }

    pub async fn enqueue(&self, job: NewJob) -> Result<()> {
        self.job_store.insert(job.into_job()).await
    }

    pub async fn register_recurring(&self, definition: RecurringDefinition) -> Result<()> {
        self.recurring_store.insert(definition).await
    }

    pub async fn list_job_attempts(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        self.job_attempt_store.list_by_job_id(job_id).await
    }

    pub async fn list_attempts_by_guardrail_code(
        &self,
        guardrail_code: &str,
    ) -> Result<Vec<JobAttempt>> {
        self.job_attempt_store
            .list_by_guardrail_code(guardrail_code)
            .await
    }

    pub async fn list_attempts_by_execution_id(
        &self,
        execution_id: &str,
    ) -> Result<Vec<JobAttempt>> {
        self.job_attempt_store
            .list_by_execution_id(execution_id)
            .await
    }

    pub async fn list_lineage_events(&self, job_id: &str) -> Result<Vec<OutboxEvent>> {
        self.outbox_store.list_by_job_id(job_id).await
    }

    pub async fn list_lineage_events_by_execution_id(
        &self,
        execution_id: &str,
    ) -> Result<Vec<OutboxEvent>> {
        self.outbox_store.list_by_execution_id(execution_id).await
    }

    pub async fn list_lineage_events_by_thread_id(
        &self,
        thread_id: &str,
    ) -> Result<Vec<OutboxEvent>> {
        self.outbox_store.list_by_thread_id(thread_id).await
    }

    pub async fn investigate_lineage(
        &self,
        query: RuntimeLineageQuery,
    ) -> Result<RuntimeLineageReport> {
        InvestigateRuntimeLineage::new(self.job_attempt_store.clone(), self.outbox_store.clone())
            .execute(query)
            .await
    }

    pub async fn get_replay_report(&self, job_id: &str) -> Result<ReplayReport> {
        Ok(ReplayReport {
            job_id: job_id.to_string(),
            attempts: self.list_job_attempts(job_id).await?,
            lineage_events: self.list_lineage_events(job_id).await?,
        })
    }

    pub async fn process_once_now(&self, queue: &str, worker_id: &str) -> Result<Option<String>> {
        self.process_once(queue, worker_id, self.clock.now()).await
    }

    pub async fn process_once_now_with_capabilities(
        &self,
        queue: &str,
        worker_id: &str,
        worker: &crate::domain::runtime::placement::WorkerCapabilities,
    ) -> Result<Option<String>> {
        self.process_once_with_capabilities(queue, worker_id, self.clock.now(), worker)
            .await
    }

    pub async fn replay_dead_letter_now(&self, job_id: &str) -> Result<bool> {
        self.replay_dead_letter(job_id, self.clock.now()).await
    }

    pub async fn publish_pending_events_now(&self, limit: usize) -> Result<usize> {
        self.publish_pending_events(limit, self.clock.now()).await
    }

    pub async fn materialize_recurring_now(&self, scheduler_id: &str) -> Result<usize> {
        self.materialize_recurring(self.clock.now(), scheduler_id)
            .await
    }

    pub async fn prune_terminal_records(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<RetentionPruneReport> {
        Ok(RetentionPruneReport {
            jobs_pruned: self.job_store.prune_terminal_before(cutoff).await?,
            attempts_pruned: self.job_attempt_store.prune_finished_before(cutoff).await?,
            outbox_events_pruned: self.outbox_store.prune_non_pending_before(cutoff).await?,
        })
    }

    pub async fn enforce_retention(&self, now: DateTime<Utc>) -> Result<RetentionPruneReport> {
        let policy = self
            .retention_policy
            .read()
            .map_err(|_| StasisError::PortFailure("retention policy lock poisoned".to_string()))?
            .clone();
        let cutoff = now - Duration::days(policy.terminal_ttl_days.max(0));
        self.prune_terminal_records(cutoff).await
    }

    pub async fn enforce_retention_now(&self) -> Result<RetentionPruneReport> {
        self.enforce_retention(self.clock.now()).await
    }

    pub async fn materialize_recurring(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
    ) -> Result<usize> {
        let due = self
            .recurring_store
            .lease_due(now, scheduler_id, 30)
            .await?;

        let mut produced = 0usize;

        for mut definition in due {
            if !definition.enabled {
                continue;
            }

            let id = self.id_generator.next_id(&definition.id).to_string();

            let scheduled_at = now + Duration::seconds(definition.jitter_seconds.max(0));

            let job = NewJob {
                id,
                queue: definition.queue.clone(),
                job_type: definition.job_type.clone(),
                payload_ref: definition.payload_template_ref.clone(),
                priority: 100,
                max_attempts: definition.max_attempts,
                idempotency_key: format!("recurring:{}:{}", definition.id, now.timestamp()),
                correlation_id: definition.id.clone(),
                causation_id: definition.id.clone(),
                trace_id: trace_id_for_enqueue(|| definition.id.clone()),
                input_provenance: Some(crate::domain::runtime::provenance::ProvenanceRef::sttp(
                    definition.payload_template_ref.clone(),
                )),
                placement: crate::domain::runtime::placement::PlacementConstraints::default(),
                scheduled_at,
                backoff_policy: Default::default(),
            };

            self.enqueue(job).await?;

            definition.last_run_at = Some(now);
            definition.next_run_at = definition.compute_next_run_at(now)?;
            definition.lease_owner = None;
            definition.lease_expires_at = None;
            self.recurring_store.save(definition).await?;
            produced += 1;
        }

        Ok(produced)
    }

    pub async fn process_once(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<String>> {
        self.process_once_with_capabilities(
            queue,
            worker_id,
            now,
            &crate::domain::runtime::placement::WorkerCapabilities::any(),
        )
        .await
    }

    pub async fn process_once_with_capabilities(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        worker: &crate::domain::runtime::placement::WorkerCapabilities,
    ) -> Result<Option<String>> {
        let worker_started = Instant::now();
        let worker_parent = inbound_trace_context_for_propagation();
        let _worker_span = self.tracing.start_span_with_trace_context(
            span_names::WORKER_PROCESS_ONCE,
            &[
                OtelAttribute::string("stasis.queue", queue.to_string()),
                OtelAttribute::string("stasis.worker_id", worker_id.to_string()),
            ],
            worker_parent.as_ref(),
        );
        self.metrics
            .incr_counter(metric_keys::WORKER_PROCESS_ONCE_TOTAL, 1);

        self.recover_stale(now).await?;
        // Native recovery may schedule an immediate retry after the caller sampled `now`.
        let now = now.max(self.clock.now());

        let Some(mut job) = self
            .job_store
            .lease_due(queue, worker_id, now, DEFAULT_JOB_LEASE_SECONDS, worker)
            .await?
        else {
            self.metrics.observe_duration_ms(
                metric_keys::WORKER_PROCESS_ONCE_DURATION_MS,
                worker_started.elapsed().as_millis() as u64,
            );
            return Ok(None);
        };

        job.state = JobState::Running;
        job.started_at = job.started_at.or(Some(now));
        let job_identity = RuntimeJobIdentityContext::from(&job);
        let Some(running) = runtime_job_dx::running_snapshot(&self.job_store, job).await? else {
            return Ok(None);
        };
        job = running;
        let processing_started = Instant::now();

        let handler = {
            let handlers = self
                .handlers
                .read()
                .map_err(|_| StasisError::PortFailure("handlers lock poisoned".to_string()))?;
            handlers.get(&job.job_type).cloned()
        };

        let job_parent =
            parent_trace_context(&job.trace_id).or_else(inbound_trace_context_for_propagation);
        let _job_span = self.tracing.start_span_with_trace_context(
            span_names::JOB_EXECUTE,
            &job_execute_span_attributes(&job),
            job_parent.as_ref(),
        );

        let outcome = {
            let (outcome, _guard) = runtime_job_dx::execute_handler(
                handler,
                &job,
                worker_id,
                self.job_context_services(),
                &self.in_flight,
            )
            .await?;
            outcome
        };

        let counter = match &outcome {
            JobExecutionOutcome::Success { .. } => metric_keys::JOB_SUCCEEDED_TOTAL,
            JobExecutionOutcome::RetryableFailure { .. } => {
                metric_keys::JOB_RETRYABLE_FAILURE_TOTAL
            }
            JobExecutionOutcome::FatalFailure { .. } => metric_keys::JOB_FATAL_FAILURE_TOTAL,
            JobExecutionOutcome::Deferred { .. } => metric_keys::JOB_DEFERRED_TOTAL,
        };
        self.metrics.observe_duration_ms(
            metric_keys::JOB_PROCESS_DURATION_MS,
            processing_started.elapsed().as_millis() as u64,
        );
        let observation =
            serde_json::to_string(&outcome).map_err(|e| StasisError::PortFailure(e.to_string()))?;
        let committed = self.job_store.commit_execution(&job, outcome).await;
        if !matches!(
            &committed,
            Ok(CommitResult::Committed | CommitResult::AlreadyCommittedSameDigest)
        ) {
            if let Some(grant) = ExecutionGrant::from_job(&job) {
                self.job_store.observe_cleanup(&grant, observation).await?;
            }
        }
        let committed = committed?;
        if committed == CommitResult::Committed {
            self.metrics.incr_counter(counter, 1);
            self.deliver_execution_lifecycle(&job.id, job.execution_epoch)
                .await?;
        }

        self.metrics.observe_duration_ms(
            metric_keys::WORKER_PROCESS_ONCE_DURATION_MS,
            worker_started.elapsed().as_millis() as u64,
        );
        Ok(Some(job_identity.job_id))
    }

    pub async fn replay_dead_letter(&self, job_id: &str, now: DateTime<Utc>) -> Result<bool> {
        let Some(mut job) = self.job_store.get(job_id).await? else {
            return Ok(false);
        };

        if job.state != JobState::DeadLetter {
            return Ok(false);
        }

        job.state = JobState::Enqueued;
        job.attempts = 0;
        job.last_error = None;
        job.scheduled_at = now;
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.heartbeat_at = None;
        job.finished_at = None;

        self.job_store.save(job).await?;
        Ok(true)
    }

    pub async fn publish_pending_events(&self, limit: usize, now: DateTime<Utc>) -> Result<usize> {
        let publisher = {
            let state = self
                .publisher
                .read()
                .map_err(|_| StasisError::PortFailure("publisher lock poisoned".to_string()))?;
            state.clone()
        };

        let Some(publisher) = publisher else {
            return Ok(0);
        };

        let policy = self
            .publish_policy
            .read()
            .map_err(|_| StasisError::PortFailure("publish policy lock poisoned".to_string()))?
            .clone();

        let pending = self.outbox_store.list_pending(limit).await?;
        let mut published = 0usize;
        let operation_telemetry =
            OperationTelemetry::new(self.metrics.clone(), self.tracing.clone());

        for mut event in pending {
            if event
                .next_attempt_at
                .map(|next| next > now)
                .unwrap_or(false)
            {
                continue;
            }

            let event_type = runtime_event_type_name(&event.event.event_type);
            let _publish_span =
                operation_telemetry.outbox_publish_span(event_type, &event.event.job_id);

            match publisher.publish(&event).await {
                Ok(()) => {
                    event.status = OutboxStatus::Published;
                    event.publish_attempts = event.publish_attempts.saturating_add(1);
                    event.published_at = Some(now);
                    event.next_attempt_at = None;
                    event.last_publish_error = None;
                    self.outbox_store.save(event).await?;
                    published += 1;
                    self.metrics
                        .incr_counter(metric_keys::OUTBOX_PUBLISH_SUCCESS_TOTAL, 1);
                }
                Err(err) => {
                    event.publish_attempts = event.publish_attempts.saturating_add(1);
                    event.published_at = None;
                    event.last_publish_error = Some(err.to_string());

                    if event.publish_attempts >= policy.max_attempts {
                        event.status = OutboxStatus::Failed;
                        event.next_attempt_at = None;
                    } else {
                        let exponent = event.publish_attempts - 1;
                        let mut delay = policy
                            .base_delay_seconds
                            .saturating_mul(2_i64.saturating_pow(exponent));
                        delay = delay.min(policy.max_delay_seconds);
                        event.status = OutboxStatus::Pending;
                        event.next_attempt_at = Some(now + Duration::seconds(delay.max(0)));
                    }

                    self.outbox_store.save(event).await?;
                    self.metrics
                        .incr_counter(metric_keys::OUTBOX_PUBLISH_FAILURE_TOTAL, 1);
                }
            }
        }

        Ok(published)
    }
}

#[derive(Default)]
struct StagedEffects {
    children: Vec<Job>,
    continuations: Vec<crate::domain::runtime::job_continuation::JobContinuation>,
    events: Vec<OutboxEvent>,
}
impl StagedEffects {
    fn count(&self) -> usize {
        self.children.len() + self.continuations.len() + self.events.len()
    }
}
#[derive(Default)]
pub(crate) struct NativeState {
    jobs: HashMap<String, Job>,
    pub(crate) continuations:
        HashMap<String, crate::domain::runtime::job_continuation::JobContinuation>,
    staged: HashMap<(String, u64, u64), StagedEffects>,
    pub(crate) waits: HashMap<String, crate::domain::runtime::durable_wait::DurableWaitRecord>,
    pub(crate) signals: HashMap<String, crate::domain::runtime::durable_wait::DurableSignalRecord>,
    epochs: HashMap<String, u64>,
    attempts: HashMap<String, Vec<JobAttempt>>,
    events: HashMap<String, OutboxEvent>,
    receipts: HashMap<(String, u64), ExecutionReceipt>,
    cleanup: HashMap<(String, u64), CleanupRecord>,
    cancellations: HashMap<String, ExecutionReceipt>,
}
#[derive(Clone)]
pub struct InMemoryJobStore {
    state: Arc<RwLock<NativeState>>,
    clock: Arc<dyn Clock>,
}
impl Default for InMemoryJobStore {
    fn default() -> Self {
        Self {
            state: Default::default(),
            clock: Arc::new(SystemClock),
        }
    }
}

impl InMemoryJobStore {
    /// Native views share this store's single transaction boundary. Do not mix unrelated defaults.
    pub fn native_outbox_store(&self) -> InMemoryOutboxStore {
        InMemoryOutboxStore {
            state: self.state.clone(),
        }
    }
    pub fn native_attempt_store(&self) -> InMemoryJobAttemptStore {
        InMemoryJobAttemptStore {
            state: self.state.clone(),
        }
    }
    pub fn native_continuation_store(&self) -> InMemoryJobContinuationStore {
        InMemoryJobContinuationStore::from_native(self.state.clone())
    }
    pub fn native_wait_store(&self) -> InMemoryDurableWaitStore {
        InMemoryDurableWaitStore::from_native(self.state.clone())
    }

    async fn commit_native(
        &self,
        snapshot: &Job,
        outcome: JobExecutionOutcome,
        recovery: bool,
    ) -> Result<CommitResult> {
        let Some(grant) = ExecutionGrant::from_job(snapshot) else {
            return Ok(CommitResult::Stale);
        };
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let key = (snapshot.id.clone(), grant.epoch);
        let staged_key = (snapshot.id.clone(), grant.epoch, grant.version);
        let digest = outcome_digest(&outcome);
        if let Some(receipt) = state.receipts.get(&key) {
            return Ok(if receipt.grant == grant && receipt.digest == digest {
                CommitResult::AlreadyCommittedSameDigest
            } else {
                CommitResult::Conflict
            });
        }
        let now = self.clock.now(); // Read INSIDE native lock, never worker's process timestamp.
        let Some(current) = state.jobs.get(&snapshot.id) else {
            return Ok(CommitResult::Stale);
        };
        let valid = if recovery {
            grant.matches(current)
                && matches!(current.state, JobState::Running | JobState::Leased)
                && current.lease_expires_at.is_some_and(|expiry| expiry <= now)
        } else {
            grant.permits(current, now)
        };
        if !valid {
            return Ok(CommitResult::Stale);
        }
        // The worker snapshot supplies only a grant; all job metadata and counters
        // come from the canonical row held under this same native commit lock.
        let mut snapshot = current.clone();
        let outcome = match outcome {
            JobExecutionOutcome::Deferred {
                message,
                execution_id,
                diagnostics,
                ..
            } if current.wakeup_pending => JobExecutionOutcome::Deferred {
                scheduled_at: now,
                message,
                execution_id,
                diagnostics,
            },
            other => other,
        };
        snapshot.wakeup_pending = false;
        let mut publication = publication(&snapshot, outcome, now);
        // Receipt digest identifies the submitted outcome, not the native wakeup adjustment.
        publication.receipt.digest = digest;
        if !recovery {
            if let Some(staged) = state.staged.get(&staged_key) {
                let mut child_ids = std::collections::HashSet::new();
                if staged.children.iter().any(|j| {
                    state.jobs.contains_key(&j.id)
                        || !child_ids.insert(j.id.clone())
                        || state.epochs.get(&j.id).copied() == Some(u64::MAX)
                }) || staged
                    .continuations
                    .iter()
                    .any(|c| state.continuations.contains_key(&c.id))
                    || staged
                        .events
                        .iter()
                        .any(|e| state.events.contains_key(&e.event_id))
                {
                    return Err(StasisError::PortFailure(
                        "staged canonical identity conflict".into(),
                    ));
                }
            }
        }
        let staged = state.staged.remove(&staged_key).unwrap_or_default();
        state
            .staged
            .retain(|(id, epoch, _), _| id != &snapshot.id || *epoch != grant.epoch);
        if !recovery {
            for mut child in staged.children {
                issue_incarnation(&mut state, &mut child)?;
                state.jobs.insert(child.id.clone(), child);
            }
            for continuation in staged.continuations {
                state
                    .continuations
                    .insert(continuation.id.clone(), continuation);
            }
            for event in staged.events {
                state.events.insert(event.event_id.clone(), event);
            }
        }
        state.jobs.insert(snapshot.id.clone(), publication.job);
        state
            .attempts
            .entry(snapshot.id.clone())
            .or_default()
            .push(publication.attempt);
        state
            .events
            .insert(publication.outbox.event_id.clone(), publication.outbox);
        state.receipts.insert(key, publication.receipt);
        Ok(CommitResult::Committed)
    }

    pub fn cleanup_record(&self, job_id: &str, epoch: u64) -> Result<Option<CleanupRecord>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        Ok(state.cleanup.get(&(job_id.into(), epoch)).cloned())
    }
}

fn issue_incarnation(state: &mut NativeState, job: &mut Job) -> Result<()> {
    job.normalize_new()?;
    let generation = state.epochs.entry(job.id.clone()).or_default();
    *generation = generation
        .checked_add(1)
        .ok_or_else(|| StasisError::PortFailure("native incarnation exhausted".into()))?;
    job.incarnation = *generation;
    job.terminal_cycle = 1;
    job.wakeup_pending = false;
    Ok(())
}

#[async_trait]
impl JobStore for InMemoryJobStore {
    async fn insert(&self, mut job: Job) -> Result<()> {
        if job.state != JobState::Enqueued || job.output_provenance.is_some() {
            return Err(StasisError::PortFailure(
                "insert requires a new enqueued job without terminal output".into(),
            ));
        }
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.heartbeat_at = None;
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if state.jobs.contains_key(&job.id) {
            return Err(StasisError::PortFailure("job already exists".into()));
        }
        issue_incarnation(&mut state, &mut job)?;
        job.wakeup_pending = false;
        job.execution_epoch = 0;
        job.version = 0;
        state.jobs.insert(job.id.clone(), job);
        Ok(())
    }
    async fn save(&self, mut job: Job) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let current = state
            .jobs
            .get(&job.id)
            .ok_or_else(|| StasisError::PortFailure("job missing".into()))?;
        if runtime_job_dx::is_terminal(&job.state)
            || (runtime_job_dx::is_terminal(&current.state)
                && !(current.state == JobState::DeadLetter && job.state == JobState::Enqueued))
            || job.incarnation != current.incarnation
            || job.terminal_cycle != current.terminal_cycle
            || job.lease_owner != current.lease_owner
            || job.lease_expires_at != current.lease_expires_at
            || job.heartbeat_at != current.heartbeat_at
            || (matches!(current.state, JobState::Leased | JobState::Running)
                && !ExecutionGrant::from_job(&job)
                    .is_some_and(|g| g.permits(current, self.clock.now())))
        {
            return Err(StasisError::PortFailure(
                "save is control-plane CAS only; terminal publication requires native commit"
                    .into(),
            ));
        }
        if job.version != current.version
            || job.execution_epoch != current.execution_epoch
            || (job.state == JobState::Running
                && !ExecutionGrant::from_job(&job)
                    .is_some_and(|g| g.permits(current, self.clock.now())))
        {
            return Err(StasisError::PortFailure("stale job snapshot".into()));
        }
        if current.state == JobState::DeadLetter && job.state == JobState::Enqueued {
            if current.terminal_cycle == 0 {
                return Err(StasisError::PortFailure(
                    "legacy terminal cycle requires reconciliation".into(),
                ));
            }
            job.terminal_cycle = current
                .terminal_cycle
                .checked_add(1)
                .ok_or_else(|| StasisError::PortFailure("terminal cycle exhausted".into()))?;
        }
        job.wakeup_pending = current.wakeup_pending;
        if current.wakeup_pending {
            job.scheduled_at = current.scheduled_at;
        }
        job.version = job
            .version
            .checked_add(1)
            .ok_or_else(|| StasisError::PortFailure("version exhausted".into()))?;
        state.jobs.insert(job.id.clone(), job);
        Ok(())
    }
    async fn wake_incarnation(&self, job_id: &str, incarnation: u64) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if let Some(job) = state.jobs.get_mut(job_id).filter(|j| {
            j.incarnation == incarnation
                && incarnation != 0
                && !runtime_job_dx::is_terminal(&j.state)
        }) {
            job.scheduled_at = self.clock.now();
            job.wakeup_pending = matches!(job.state, JobState::Leased | JobState::Running);
        }
        Ok(())
    }
    async fn wake_execution(&self, job_id: &str) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if let Some(job) = state
            .jobs
            .get_mut(job_id)
            .filter(|j| !runtime_job_dx::is_terminal(&j.state))
        {
            job.scheduled_at = self.clock.now();
            job.wakeup_pending = matches!(job.state, JobState::Leased | JobState::Running);
        }
        Ok(())
    }
    async fn commit_execution(
        &self,
        snapshot: &Job,
        outcome: JobExecutionOutcome,
    ) -> Result<CommitResult> {
        self.commit_native(snapshot, outcome, false).await
    }
    async fn recover_execution(&self, snapshot: &Job) -> Result<CommitResult> {
        self.commit_native(
            snapshot,
            JobExecutionOutcome::RetryableFailure {
                message: STALE_LEASE_MESSAGE.into(),
                execution_id: None,
                diagnostics: None,
            },
            true,
        )
        .await
    }
    async fn heartbeat_granted(
        &self,
        grant: &ExecutionGrant,
        _now: DateTime<Utc>,
        lease_seconds: i64,
        progress: Option<String>,
    ) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let now = self.clock.now();
        let Some(job) = state.jobs.get_mut(&grant.job_id) else {
            return Err(StasisError::PortFailure("stale execution grant".into()));
        };
        if !grant.permits(job, now) {
            return Err(StasisError::PortFailure("stale execution grant".into()));
        }
        if let Some(progress) = progress {
            job.progress_json = Some(progress);
        } else {
            job.heartbeat_at = Some(now);
            job.lease_expires_at = Some(now + Duration::seconds(lease_seconds.max(0)));
        }
        Ok(())
    }
    async fn cancel_execution(&self, job_id: &str) -> Result<Option<Job>> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let Some(current) = state
            .jobs
            .get(job_id)
            .cloned()
            .filter(|j| !runtime_job_dx::is_terminal(&j.state))
        else {
            return Ok(None);
        };
        let now = self.clock.now();
        if let Some(grant) = ExecutionGrant::from_job(&current) {
            state.cleanup.insert(
                (job_id.into(), grant.epoch),
                CleanupRecord {
                    grant,
                    evidence_json: None,
                    return_observations: Vec::new(),
                    acknowledged_at: None,
                },
            );
        }
        for wait in state.waits.values_mut().filter(|w| {
            w.job_id == job_id
                && w.incarnation == current.incarnation
                && w.status == crate::domain::runtime::durable_wait::DurableWaitStatus::Pending
        }) {
            wait.status = crate::domain::runtime::durable_wait::DurableWaitStatus::Cancelled;
            wait.updated_at = now;
        }
        state
            .staged
            .retain(|(id, epoch, _), _| id != job_id || *epoch != current.execution_epoch);
        let (job, event, receipt) = cancellation(&current, now);
        state.jobs.insert(job_id.into(), job.clone());
        state.events.insert(event.event_id.clone(), event);
        state.cancellations.insert(
            format!(
                "{}:incarnation:{}:cancel:{}",
                current.id, current.incarnation, current.version
            ),
            receipt,
        );
        Ok(Some(job))
    }
    async fn acknowledge_cleanup(
        &self,
        grant: &ExecutionGrant,
        evidence_json: String,
    ) -> Result<CommitResult> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let Some(record) = state
            .cleanup
            .get_mut(&(grant.job_id.clone(), grant.epoch))
            .filter(|r| r.grant == *grant)
        else {
            return Ok(CommitResult::Stale);
        };
        if let Some(existing) = &record.evidence_json {
            return Ok(if existing == &evidence_json {
                CommitResult::AlreadyCommittedSameDigest
            } else {
                CommitResult::Conflict
            });
        }
        record.evidence_json = Some(evidence_json);
        record.acknowledged_at = Some(self.clock.now());
        Ok(CommitResult::Committed)
    }
    async fn publish_granted(&self, grant: &ExecutionGrant, event: OutboxEvent) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if !state
            .jobs
            .get(&grant.job_id)
            .is_some_and(|j| grant.permits(j, self.clock.now()))
            || event.event.job_id != grant.job_id
        {
            return Err(StasisError::PortFailure(
                "stale execution publication".into(),
            ));
        }
        let count = state
            .staged
            .iter()
            .filter(|((id, epoch, _), _)| id == &grant.job_id && *epoch == grant.epoch)
            .map(|(_, effects)| effects.count())
            .sum::<usize>();
        if count >= 128 {
            return Err(StasisError::PortFailure(
                "execution staging limit exceeded".into(),
            ));
        }
        let staged = state
            .staged
            .entry((grant.job_id.clone(), grant.epoch, grant.version))
            .or_default();
        if staged.count() >= 128 {
            return Err(StasisError::PortFailure(
                "execution staging limit exceeded".into(),
            ));
        }
        staged.events.push(event);
        Ok(())
    }
    async fn stage_child(&self, grant: &ExecutionGrant, mut child: Job) -> Result<()> {
        child.normalize_new()?;
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if !state
            .jobs
            .get(&grant.job_id)
            .is_some_and(|j| grant.permits(j, self.clock.now()))
        {
            return Err(StasisError::PortFailure("stale execution child".into()));
        }
        let count = state
            .staged
            .iter()
            .filter(|((id, epoch, _), _)| id == &grant.job_id && *epoch == grant.epoch)
            .map(|(_, effects)| effects.count())
            .sum::<usize>();
        if count >= 128 {
            return Err(StasisError::PortFailure(
                "execution staging limit exceeded".into(),
            ));
        }
        let staged = state
            .staged
            .entry((grant.job_id.clone(), grant.epoch, grant.version))
            .or_default();
        if staged.count() >= 128 {
            return Err(StasisError::PortFailure(
                "execution staging limit exceeded".into(),
            ));
        }
        staged.children.push(child);
        Ok(())
    }
    async fn stage_continuation(
        &self,
        grant: &ExecutionGrant,
        continuation: crate::domain::runtime::job_continuation::JobContinuation,
    ) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        if continuation.parent_job_id != grant.job_id
            || !state.jobs.get(&grant.job_id).is_some_and(|j| {
                j.incarnation == continuation.parent_incarnation
                    && continuation.parent_version_floor == Some(j.version)
                    && continuation.parent_terminal_cycle == j.terminal_cycle
                    && j.terminal_cycle != 0
            })
            || !state
                .jobs
                .get(&grant.job_id)
                .is_some_and(|j| grant.permits(j, self.clock.now()))
        {
            return Err(StasisError::PortFailure(
                "stale execution continuation".into(),
            ));
        }
        let count = state
            .staged
            .iter()
            .filter(|((id, epoch, _), _)| id == &grant.job_id && *epoch == grant.epoch)
            .map(|(_, effects)| effects.count())
            .sum::<usize>();
        if count >= 128 {
            return Err(StasisError::PortFailure(
                "execution staging limit exceeded".into(),
            ));
        }
        let staged = state
            .staged
            .entry((grant.job_id.clone(), grant.epoch, grant.version))
            .or_default();
        if staged.count() >= 128 {
            return Err(StasisError::PortFailure(
                "execution staging limit exceeded".into(),
            ));
        }
        staged.continuations.push(continuation);
        Ok(())
    }
    async fn settle_continuation(
        &self,
        parent: &Job,
        mut record: crate::domain::runtime::job_continuation::JobContinuation,
        child: Option<Job>,
        detail: Option<String>,
    ) -> Result<bool> {
        use crate::domain::runtime::job_continuation::ContinuationStatus;
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let matches_parent = |j: &Job| {
            j.id == parent.id
                && j.version == parent.version
                && j.execution_epoch == parent.execution_epoch
                && j.incarnation == parent.incarnation
                && j.terminal_cycle == parent.terminal_cycle
                && j.state == parent.state
                && runtime_job_dx::is_terminal(&j.state)
        };
        let retained = state
            .receipts
            .values()
            .chain(state.cancellations.values())
            .any(|r| serde_json::from_str::<Job>(&r.job_json).is_ok_and(|j| matches_parent(&j)));
        if !state.jobs.get(&parent.id).is_some_and(matches_parent) && !retained {
            return Ok(false);
        }
        let Some(current) = state.continuations.get(&record.id) else {
            return Ok(false);
        };
        if current.parent_job_id != parent.id
            || current.parent_incarnation != parent.incarnation
            || record.parent_incarnation != current.parent_incarnation
            || current.parent_terminal_cycle == 0
            || current.parent_terminal_cycle != parent.terminal_cycle
            || record.parent_terminal_cycle != current.parent_terminal_cycle
            || current.parent_version_floor.is_none()
            || current.parent_version_floor != record.parent_version_floor
            || current
                .parent_version_floor
                .is_some_and(|floor| parent.version < floor)
        {
            return Ok(false);
        }
        let actual = match parent.state {
            JobState::Succeeded => {
                crate::domain::runtime::job_continuation::ContinuationTrigger::Succeeded
            }
            JobState::Canceled => {
                crate::domain::runtime::job_continuation::ContinuationTrigger::Canceled
            }
            _ => crate::domain::runtime::job_continuation::ContinuationTrigger::Failed,
        };
        if child.is_some() != current.trigger.matches_actual(actual) {
            return Ok(false);
        }
        record = current.clone();
        if current.status == ContinuationStatus::Materialized
            || current.status == ContinuationStatus::Discarded
        {
            return Ok(false);
        }
        if let Some(mut child) = child {
            child.normalize_new()?;
            if state.jobs.contains_key(&child.id) {
                return Err(StasisError::PortFailure(
                    "continuation child identity conflict".into(),
                ));
            }
            issue_incarnation(&mut state, &mut child)?;
            record.child_job_id = Some(child.id.clone());
            state.jobs.insert(child.id.clone(), child);
            record.status = ContinuationStatus::Materialized;
        } else {
            record.status = ContinuationStatus::Discarded;
        }
        record.parent_output_json = detail;
        record.claim_token = None;
        state.continuations.insert(record.id.clone(), record);
        Ok(true)
    }
    async fn poll_wait(
        &self,
        grant: &ExecutionGrant,
        template: crate::domain::runtime::durable_wait::DurableWaitRecord,
    ) -> Result<crate::domain::runtime::durable_wait::DurableWaitRecord> {
        use crate::domain::runtime::durable_wait::DurableWaitStatus;
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let now = self.clock.now();
        if state
            .waits
            .values()
            .any(|w| w.job_id == grant.job_id && w.incarnation == 0)
        {
            return Err(StasisError::PortFailure(
                "legacy wait identity requires reconciliation".into(),
            ));
        }
        if template.job_id != grant.job_id
            || !state
                .jobs
                .get(&grant.job_id)
                .is_some_and(|j| j.incarnation == template.incarnation && template.incarnation != 0)
            || !state
                .jobs
                .get(&grant.job_id)
                .is_some_and(|j| grant.permits(j, now))
        {
            return Err(StasisError::PortFailure("stale execution wait".into()));
        }
        let is_new = !state.waits.contains_key(&template.wait_id);
        let mut wait = state
            .waits
            .get(&template.wait_id)
            .cloned()
            .unwrap_or(template);
        if wait.job_id != grant.job_id || wait.incarnation != state.jobs[&grant.job_id].incarnation
        {
            return Err(StasisError::PortFailure("wait identity conflict".into()));
        }
        if wait.status == DurableWaitStatus::Pending {
            if !is_new && wait.deadline_at.is_some_and(|d| d <= now) {
                wait.status = DurableWaitStatus::TimedOut;
            } else if let Some(signal) = state
                .signals
                .values()
                .filter(|signal| {
                    signal.signal_type == wait.signal_type
                        && signal.correlation_key == wait.correlation_key
                        && signal
                            .target_incarnations
                            .as_ref()
                            .is_none_or(|targets| targets.contains(&wait.target_identity()))
                        && !wait.consumed_signal_ids.contains(&signal.signal_id)
                        && !state.waits.values().any(|old| {
                            old.job_id == wait.job_id
                                && old.incarnation != wait.incarnation
                                && old.consumed_signal_ids.contains(&signal.signal_id)
                        })
                })
                .min_by_key(|s| s.created_at)
            {
                wait.status = DurableWaitStatus::Signaled;
                wait.signal_payload = Some(signal.payload_json.clone());
                wait.consumed_signal_ids.push(signal.signal_id.clone());
            }
            wait.updated_at = now;
            state.waits.insert(wait.wait_id.clone(), wait.clone());
        }
        Ok(wait)
    }
    async fn fail_execution(&self, job_id: &str) -> Result<Option<Job>> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let Some(current) = state
            .jobs
            .get(job_id)
            .cloned()
            .filter(|j| !runtime_job_dx::is_terminal(&j.state))
        else {
            return Ok(None);
        };
        let now = self.clock.now();
        if let Some(grant) = ExecutionGrant::from_job(&current) {
            state.cleanup.insert(
                (job_id.into(), grant.epoch),
                CleanupRecord {
                    grant,
                    evidence_json: None,
                    return_observations: Vec::new(),
                    acknowledged_at: None,
                },
            );
        }
        let p = crate::application::runtime::execution_commit::operator_failure(&current, now);
        state
            .staged
            .retain(|(id, epoch, _), _| id != job_id || *epoch != current.execution_epoch);
        for wait in state.waits.values_mut().filter(|w| {
            w.job_id == job_id
                && w.incarnation == current.incarnation
                && w.status == crate::domain::runtime::durable_wait::DurableWaitStatus::Pending
        }) {
            wait.status = crate::domain::runtime::durable_wait::DurableWaitStatus::Cancelled;
            wait.updated_at = now;
        }
        let job = p.job.clone();
        state.jobs.insert(job_id.into(), p.job);
        state
            .attempts
            .entry(job_id.into())
            .or_default()
            .push(p.attempt);
        state.events.insert(p.outbox.event_id.clone(), p.outbox);
        state.cancellations.insert(
            format!(
                "{}:incarnation:{}:operator-fail:{}",
                current.id, current.incarnation, current.version
            ),
            p.receipt,
        );
        Ok(Some(job))
    }
    async fn execution_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<ExecutionReceipt>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        Ok(state.receipts.get(&(job_id.into(), epoch)).cloned())
    }
    async fn cleanup_receipt(&self, job_id: &str, epoch: u64) -> Result<Option<CleanupRecord>> {
        self.cleanup_record(job_id, epoch)
    }

    async fn observe_cleanup(
        &self,
        grant: &ExecutionGrant,
        observation: String,
    ) -> Result<CommitResult> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("native state poisoned".into()))?;
        let Some(record) = state
            .cleanup
            .get_mut(&(grant.job_id.clone(), grant.epoch))
            .filter(|r| r.grant == *grant)
        else {
            return Ok(CommitResult::Stale);
        };
        if record.return_observations.contains(&observation) {
            return Ok(CommitResult::AlreadyCommittedSameDigest);
        }
        if observation.len() > 65536 || record.return_observations.len() >= 8 {
            return Ok(CommitResult::Conflict);
        }
        record.return_observations.push(observation);
        Ok(CommitResult::Committed)
    }
    async fn get(&self, id: &str) -> Result<Option<Job>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        let state = &state.jobs;

        Ok(state.get(id).cloned())
    }

    async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
        worker: &crate::domain::runtime::placement::WorkerCapabilities,
    ) -> Result<Option<Job>> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        let native = &mut *state;
        let state = &mut native.jobs;

        let selected_id = state
            .iter()
            .filter(|(_, job)| {
                let lease_expired = job
                    .lease_expires_at
                    .map(|expiry| expiry <= now)
                    .unwrap_or(true);

                job.queue == queue
                    && job.state == JobState::Enqueued
                    && job.scheduled_at <= now
                    && lease_expired
                    && job.placement.matches(worker)
            })
            .min_by_key(|(_, job)| (job.scheduled_at, job.priority))
            .map(|(id, _)| id.clone());

        let Some(job_id) = selected_id else {
            return Ok(None);
        };

        let Some(job) = state.get_mut(&job_id) else {
            return Ok(None);
        };

        let epoch = native.epochs.entry(job_id.clone()).or_default();
        *epoch = epoch
            .checked_add(1)
            .ok_or_else(|| StasisError::PortFailure("execution epoch exhausted".into()))?;
        job.execution_epoch = *epoch;
        job.version = job
            .version
            .checked_add(1)
            .ok_or_else(|| StasisError::PortFailure("version exhausted".into()))?;
        job.state = JobState::Leased;
        job.lease_owner = Some(worker_id.to_string());
        job.lease_expires_at = Some(self.clock.now() + Duration::seconds(lease_seconds));
        job.heartbeat_at = Some(self.clock.now());

        Ok(Some(job.clone()))
    }

    async fn heartbeat(
        &self,
        _job_id: &str,
        _worker_id: &str,
        _now: DateTime<Utc>,
        _lease_seconds: i64,
    ) -> Result<()> {
        Err(StasisError::PortFailure(
            "owner-only heartbeat retired; use ExecutionGrant".into(),
        ))
    }

    async fn list_by_state(&self, state_filter: JobState) -> Result<Vec<Job>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        let state = &state.jobs;

        Ok(state
            .values()
            .filter(|job| job.state == state_filter)
            .cloned()
            .collect())
    }

    async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        let state = &state.jobs;
        Ok(state
            .values()
            .filter(|job| {
                matches!(job.state, JobState::Leased | JobState::Running)
                    && job
                        .lease_expires_at
                        .map(|expiry| expiry <= now)
                        .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        let removed = state
            .jobs
            .get(id)
            .is_some_and(|j| runtime_job_dx::is_terminal(&j.state));
        if removed {
            state.jobs.remove(id);
            state.staged.retain(|(parent, _, _), _| parent != id);
        }
        Ok(removed)
    }

    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        let mut native = state;
        let state = &mut native.jobs;

        let before = state.len();
        state.retain(|_, job| {
            let terminal = matches!(
                job.state,
                JobState::Succeeded | JobState::Failed | JobState::DeadLetter | JobState::Canceled
            );
            let old_enough = job.finished_at.map(|t| t <= cutoff).unwrap_or(false);
            !(terminal && old_enough)
        });

        let removed = before.saturating_sub(state.len());
        let live = native
            .jobs
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        native.staged.retain(|(id, _, _), _| live.contains(id));
        Ok(removed)
    }
}

#[derive(Clone, Default)]
pub struct InMemoryRecurringStore {
    defs: Arc<RwLock<HashMap<String, RecurringDefinition>>>,
}

#[derive(Clone, Default)]
pub struct InMemoryOutboxStore {
    state: Arc<RwLock<NativeState>>,
}

#[derive(Clone, Default)]
pub struct InMemoryJobAttemptStore {
    state: Arc<RwLock<NativeState>>,
}

impl InMemoryJobAttemptStore {
    fn list_filtered_attempts<F>(&self, predicate: F) -> Result<Vec<JobAttempt>>
    where
        F: Fn(&JobAttempt) -> bool,
    {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;
        let state = &state.attempts;

        let mut attempts: Vec<JobAttempt> = state
            .values()
            .flat_map(|attempts| attempts.iter())
            .filter(|attempt| predicate(attempt))
            .cloned()
            .collect();
        attempts.sort_by_key(|attempt| attempt.attempt_number);
        Ok(attempts)
    }
}

#[async_trait]
impl JobAttemptStore for InMemoryJobAttemptStore {
    async fn insert(&self, attempt: JobAttempt) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;
        let state = &mut state.attempts;

        state
            .entry(attempt.job_id.clone())
            .or_insert_with(Vec::new)
            .push(attempt);
        Ok(())
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;
        let state = &state.attempts;

        let mut attempts = state.get(job_id).cloned().unwrap_or_default();
        attempts.sort_by_key(|attempt| attempt.attempt_number);
        Ok(attempts)
    }

    async fn list_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>> {
        self.list_filtered_attempts(|attempt| {
            attempt.guardrail_code.as_deref() == Some(guardrail_code)
        })
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> {
        self.list_filtered_attempts(|attempt| attempt.execution_id.as_deref() == Some(execution_id))
    }

    async fn prune_finished_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;
        let state = &mut state.attempts;

        let mut removed = 0usize;
        for attempts in state.values_mut() {
            let before = attempts.len();
            attempts.retain(|attempt| attempt.finished_at > cutoff);
            removed += before.saturating_sub(attempts.len());
        }
        state.retain(|_, attempts| !attempts.is_empty());

        Ok(removed)
    }
}

#[async_trait]
impl OutboxStore for InMemoryOutboxStore {
    async fn insert(&self, event: OutboxEvent) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &mut state.events;

        state.insert(event.event_id.clone(), event);
        Ok(())
    }

    async fn save(&self, event: OutboxEvent) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &mut state.events;

        state.insert(event.event_id.clone(), event);
        Ok(())
    }

    async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &state.events;

        Ok(state.get(event_id).cloned())
    }

    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &state.events;

        let mut pending: Vec<OutboxEvent> = state
            .values()
            .filter(|evt| evt.status == OutboxStatus::Pending)
            .cloned()
            .collect();

        pending.sort_by_key(|evt| evt.next_attempt_at.unwrap_or(evt.event.occurred_at));
        pending.truncate(limit);
        Ok(pending)
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<OutboxEvent>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &state.events;

        let mut events: Vec<OutboxEvent> = state
            .values()
            .filter(|evt| evt.event.job_id == job_id)
            .cloned()
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        Ok(events)
    }

    async fn list_by_thread_id(&self, thread_id: &str) -> Result<Vec<OutboxEvent>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &state.events;

        let mut events: Vec<OutboxEvent> = state
            .values()
            .filter(|evt| evt.event.thread_id.as_deref() == Some(thread_id))
            .cloned()
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        Ok(events)
    }

    async fn list_by_thread_prefix(&self, thread_prefix: &str) -> Result<Vec<OutboxEvent>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &state.events;

        let mut events: Vec<OutboxEvent> = state
            .values()
            .filter(|evt| {
                evt.event
                    .thread_id
                    .as_deref()
                    .map(|thread_id| thread_id.starts_with(thread_prefix))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        Ok(events)
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<OutboxEvent>> {
        let state = self
            .state
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &state.events;

        let mut events: Vec<OutboxEvent> = state
            .values()
            .filter(|evt| evt.event.execution_id.as_deref() == Some(execution_id))
            .cloned()
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        Ok(events)
    }

    async fn prune_non_pending_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let mut state = self
            .state
            .write()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;
        let state = &mut state.events;

        let before = state.len();
        state.retain(|_, evt| {
            let terminal = evt.status != OutboxStatus::Pending;
            let old_enough = evt.event.occurred_at <= cutoff;
            !(terminal && old_enough)
        });

        Ok(before.saturating_sub(state.len()))
    }
}

#[async_trait]
impl RecurringStore for InMemoryRecurringStore {
    async fn insert(&self, definition: RecurringDefinition) -> Result<()> {
        let mut state = self
            .defs
            .write()
            .map_err(|_| StasisError::PortFailure("recurring store lock poisoned".to_string()))?;

        state.insert(definition.id.clone(), definition);
        Ok(())
    }

    async fn save(&self, definition: RecurringDefinition) -> Result<()> {
        let mut state = self
            .defs
            .write()
            .map_err(|_| StasisError::PortFailure("recurring store lock poisoned".to_string()))?;

        state.insert(definition.id.clone(), definition);
        Ok(())
    }

    async fn lease_due(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
        lease_seconds: i64,
    ) -> Result<Vec<RecurringDefinition>> {
        let mut state = self
            .defs
            .write()
            .map_err(|_| StasisError::PortFailure("recurring store lock poisoned".to_string()))?;

        let mut leased = Vec::new();

        for definition in state.values_mut() {
            let lease_expired = definition
                .lease_expires_at
                .map(|expiry| expiry <= now)
                .unwrap_or(true);

            if definition.enabled && definition.next_run_at <= now && lease_expired {
                definition.lease_owner = Some(scheduler_id.to_string());
                definition.lease_expires_at = Some(now + Duration::seconds(lease_seconds));
                leased.push(definition.clone());
            }
        }

        Ok(leased)
    }

    async fn list(&self) -> Result<Vec<RecurringDefinition>> {
        let state = self
            .defs
            .read()
            .map_err(|_| StasisError::PortFailure("recurring store lock poisoned".to_string()))?;

        Ok(state.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::{Duration, Utc};

    use super::*;

    struct AlwaysSuccessHandler;

    #[async_trait]
    impl JobHandler for AlwaysSuccessHandler {
        fn job_type(&self) -> &'static str {
            "test.success"
        }

        async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
            Ok(JobExecutionOutcome::Success {
                output_provenance: Some(crate::domain::runtime::provenance::ProvenanceRef::sttp(
                    "sttp:out:1",
                )),
                execution_id: None,
                diagnostics: None,
            })
        }
    }

    struct FlakyHandler {
        failures_before_success: usize,
        calls: AtomicUsize,
    }

    impl FlakyHandler {
        fn new(failures_before_success: usize) -> Self {
            Self {
                failures_before_success,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl JobHandler for FlakyHandler {
        fn job_type(&self) -> &'static str {
            "test.flaky"
        }

        async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
            let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;

            if calls <= self.failures_before_success {
                Ok(JobExecutionOutcome::RetryableFailure {
                    message: "transient failure".to_string(),
                    execution_id: None,
                    diagnostics: None,
                })
            } else {
                Ok(JobExecutionOutcome::Success {
                    output_provenance: Some(
                        crate::domain::runtime::provenance::ProvenanceRef::sttp("sttp:out:flaky"),
                    ),
                    execution_id: None,
                    diagnostics: None,
                })
            }
        }
    }

    struct AlwaysFatalHandler;

    #[async_trait]
    impl JobHandler for AlwaysFatalHandler {
        fn job_type(&self) -> &'static str {
            "test.fatal"
        }

        async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
            Ok(JobExecutionOutcome::FatalFailure {
                message: "non retryable".to_string(),
                execution_id: None,
                diagnostics: None,
            })
        }
    }

    #[derive(Clone)]
    struct FlakyPublisher {
        failures_before_success: usize,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventPublisher for FlakyPublisher {
        async fn publish(&self, _event: &OutboxEvent) -> Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call <= self.failures_before_success {
                return Err(StasisError::PortFailure(
                    "synthetic publish failure".to_string(),
                ));
            }

            Ok(())
        }
    }

    fn build_new_job(job_type: &str, now: chrono::DateTime<Utc>) -> NewJob {
        NewJob {
            id: format!("job-{job_type}"),
            queue: "default".to_string(),
            job_type: job_type.to_string(),
            payload_ref: "payload:ref".to_string(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: format!("idem-{job_type}"),
            correlation_id: "corr-1".to_string(),
            causation_id: "cause-1".to_string(),
            trace_id: "trace-1".to_string(),
            input_provenance: Some(crate::domain::runtime::provenance::ProvenanceRef::sttp(
                "sttp:in:1",
            )),
            placement: crate::domain::runtime::placement::PlacementConstraints::default(),
            scheduled_at: now,
            backoff_policy: crate::domain::runtime::job::BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        }
    }

    #[tokio::test]
    async fn lease_and_successful_processing_works() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(AlwaysSuccessHandler)
            .expect("handler should register");

        let now = Utc::now();
        runtime
            .enqueue(build_new_job("test.success", now))
            .await
            .expect("job should enqueue");

        let processed = runtime
            .process_once("default", "worker-1", now)
            .await
            .expect("processing should succeed");

        assert_eq!(processed, Some("job-test.success".to_string()));

        let job = runtime
            .job_store
            .get("job-test.success")
            .await
            .expect("job get should succeed")
            .expect("job should exist");

        assert_eq!(job.state, JobState::Succeeded);
        assert_eq!(job.sttp_output_node_id(), Some("sttp:out:1".to_string()));
    }

    #[tokio::test]
    async fn retry_path_reenqueues_then_succeeds() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(FlakyHandler::new(1))
            .expect("handler should register");

        let now = Utc::now();
        runtime
            .enqueue(build_new_job("test.flaky", now))
            .await
            .expect("job should enqueue");

        runtime
            .process_once("default", "worker-1", now)
            .await
            .expect("first run should complete");

        let retry_job = runtime
            .job_store
            .get("job-test.flaky")
            .await
            .expect("job get should succeed")
            .expect("job should exist");

        assert_eq!(retry_job.state, JobState::Enqueued);
        assert_eq!(retry_job.attempts, 1);
        assert!(retry_job.scheduled_at > now);

        runtime
            .process_once("default", "worker-2", now + Duration::seconds(2))
            .await
            .expect("second run should complete");

        let final_job = runtime
            .job_store
            .get("job-test.flaky")
            .await
            .expect("job get should succeed")
            .expect("job should exist");

        assert_eq!(final_job.state, JobState::Succeeded);
        assert_eq!(final_job.attempts, 1);
    }

    #[tokio::test]
    async fn dead_letter_path_works_for_fatal_error() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(AlwaysFatalHandler)
            .expect("handler should register");

        let now = Utc::now();
        runtime
            .enqueue(build_new_job("test.fatal", now))
            .await
            .expect("job should enqueue");

        runtime
            .process_once("default", "worker-1", now)
            .await
            .expect("processing should complete");

        let job = runtime
            .job_store
            .get("job-test.fatal")
            .await
            .expect("job get should succeed")
            .expect("job should exist");

        assert_eq!(job.state, JobState::DeadLetter);
        assert_eq!(job.attempts, 1);
        assert_eq!(job.last_error, Some("non retryable".to_string()));
    }

    #[tokio::test]
    async fn dead_letter_jobs_can_be_replayed() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(AlwaysFatalHandler)
            .expect("handler should register");

        let now = Utc::now();
        runtime
            .enqueue(build_new_job("test.fatal", now))
            .await
            .expect("job should enqueue");

        runtime
            .process_once("default", "worker-1", now)
            .await
            .expect("processing should complete");

        let replayed = runtime
            .replay_dead_letter("job-test.fatal", now + Duration::seconds(5))
            .await
            .expect("replay should succeed");
        assert!(replayed);

        let replayed_job = runtime
            .job_store
            .get("job-test.fatal")
            .await
            .expect("job get should succeed")
            .expect("job should exist");

        assert_eq!(replayed_job.state, JobState::Enqueued);
        assert_eq!(replayed_job.attempts, 0);
        assert_eq!(replayed_job.last_error, None);
    }

    #[tokio::test]
    async fn outbox_publish_failures_are_retried_with_backoff() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(AlwaysSuccessHandler)
            .expect("handler should register");
        runtime
            .configure_outbox_publish_policy(OutboxPublishPolicy {
                max_attempts: 3,
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            })
            .expect("policy should configure");

        let calls = Arc::new(AtomicUsize::new(0));
        runtime
            .register_event_publisher(FlakyPublisher {
                failures_before_success: 1,
                calls: calls.clone(),
            })
            .expect("publisher should register");

        let now = Utc::now();
        runtime
            .enqueue(build_new_job("test.success", now))
            .await
            .expect("job should enqueue");

        runtime
            .process_once("default", "worker-1", now)
            .await
            .expect("processing should succeed");

        let first_publish = runtime
            .publish_pending_events(10, now)
            .await
            .expect("first publish attempt should complete");
        assert_eq!(first_publish, 0);

        let pending = runtime
            .outbox_store
            .list_pending(10)
            .await
            .expect("pending list should succeed");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].publish_attempts, 1);
        assert_eq!(pending[0].status, OutboxStatus::Pending);
        assert_eq!(
            pending[0].last_publish_error,
            Some("port failure: synthetic publish failure".to_string())
        );
        assert_eq!(pending[0].next_attempt_at, Some(now + Duration::seconds(1)));

        let premature = runtime
            .publish_pending_events(10, now)
            .await
            .expect("premature publish attempt should complete");
        assert_eq!(premature, 0);

        let second_publish = runtime
            .publish_pending_events(10, now + Duration::seconds(1))
            .await
            .expect("second publish attempt should complete");
        assert_eq!(second_publish, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let pending_after = runtime
            .outbox_store
            .list_pending(10)
            .await
            .expect("pending list should succeed");
        assert!(pending_after.is_empty());
    }

    #[tokio::test]
    async fn due_outbox_event_is_not_starved_by_future_retry_when_limit_is_low() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(AlwaysSuccessHandler)
            .expect("handler should register");
        runtime
            .configure_outbox_publish_policy(OutboxPublishPolicy {
                max_attempts: 3,
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            })
            .expect("policy should configure");

        let calls = Arc::new(AtomicUsize::new(0));
        runtime
            .register_event_publisher(FlakyPublisher {
                failures_before_success: 1,
                calls: Arc::clone(&calls),
            })
            .expect("publisher should register");

        let now = Utc::now();
        runtime
            .enqueue(NewJob {
                id: "job-fairness-1".to_string(),
                queue: "default".to_string(),
                job_type: "test.success".to_string(),
                payload_ref: "payload:fairness-1".to_string(),
                priority: 100,
                max_attempts: 1,
                idempotency_key: "idem-fairness-1".to_string(),
                correlation_id: "corr-fairness-1".to_string(),
                causation_id: "cause-fairness-1".to_string(),
                trace_id: "trace-fairness-1".to_string(),
                input_provenance: Some(crate::domain::runtime::provenance::ProvenanceRef::sttp(
                    "sttp:in:fairness-1",
                )),
                placement: crate::domain::runtime::placement::PlacementConstraints::default(),
                scheduled_at: now,
                backoff_policy: crate::domain::runtime::job::BackoffPolicy::default(),
            })
            .await
            .expect("first job should enqueue");
        runtime
            .enqueue(NewJob {
                id: "job-fairness-2".to_string(),
                queue: "default".to_string(),
                job_type: "test.success".to_string(),
                payload_ref: "payload:fairness-2".to_string(),
                priority: 100,
                max_attempts: 1,
                idempotency_key: "idem-fairness-2".to_string(),
                correlation_id: "corr-fairness-2".to_string(),
                causation_id: "cause-fairness-2".to_string(),
                trace_id: "trace-fairness-2".to_string(),
                input_provenance: Some(crate::domain::runtime::provenance::ProvenanceRef::sttp(
                    "sttp:in:fairness-2",
                )),
                placement: crate::domain::runtime::placement::PlacementConstraints::default(),
                scheduled_at: now,
                backoff_policy: crate::domain::runtime::job::BackoffPolicy::default(),
            })
            .await
            .expect("second job should enqueue");

        runtime
            .process_once("default", "worker-1", now)
            .await
            .expect("first processing should succeed");
        runtime
            .process_once("default", "worker-1", now + Duration::milliseconds(1))
            .await
            .expect("second processing should succeed");

        let first_attempt = runtime
            .publish_pending_events(1, now + Duration::milliseconds(2))
            .await
            .expect("first publish attempt should complete");
        assert_eq!(first_attempt, 0);

        let second_attempt = runtime
            .publish_pending_events(1, now + Duration::milliseconds(2))
            .await
            .expect("second publish attempt should complete");
        assert_eq!(second_attempt, 1);

        let pending_after_second = runtime
            .outbox_store
            .list_pending(10)
            .await
            .expect("pending list should succeed");
        assert_eq!(pending_after_second.len(), 1);
        assert_eq!(pending_after_second[0].publish_attempts, 1);
        assert_eq!(
            pending_after_second[0].next_attempt_at,
            Some(now + Duration::milliseconds(2) + Duration::seconds(1))
        );

        let third_attempt = runtime
            .publish_pending_events(1, now + Duration::seconds(1) + Duration::milliseconds(2))
            .await
            .expect("third publish attempt should complete");
        assert_eq!(third_attempt, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let pending_final = runtime
            .outbox_store
            .list_pending(10)
            .await
            .expect("pending list should succeed");
        assert!(pending_final.is_empty());
    }

    #[tokio::test]
    async fn outbox_backlog_completes_within_bounded_ticks_under_mixed_failures() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(AlwaysSuccessHandler)
            .expect("handler should register");
        runtime
            .configure_outbox_publish_policy(OutboxPublishPolicy {
                max_attempts: 5,
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            })
            .expect("policy should configure");

        let calls = Arc::new(AtomicUsize::new(0));
        runtime
            .register_event_publisher(FlakyPublisher {
                failures_before_success: 3,
                calls: Arc::clone(&calls),
            })
            .expect("publisher should register");

        let now = Utc::now();
        let mut job_ids = Vec::new();
        for idx in 0..12 {
            let job_id = format!("job-backlog-{idx}");
            runtime
                .enqueue(NewJob {
                    id: job_id.clone(),
                    queue: "default".to_string(),
                    job_type: "test.success".to_string(),
                    payload_ref: format!("payload:backlog-{idx}"),
                    priority: 100,
                    max_attempts: 1,
                    idempotency_key: format!("idem-backlog-{idx}"),
                    correlation_id: format!("corr-backlog-{idx}"),
                    causation_id: format!("cause-backlog-{idx}"),
                    trace_id: format!("trace-backlog-{idx}"),
                    input_provenance: Some(
                        crate::domain::runtime::provenance::ProvenanceRef::sttp(format!(
                            "sttp:in:backlog-{idx}"
                        )),
                    ),
                    placement: crate::domain::runtime::placement::PlacementConstraints::default(),
                    scheduled_at: now,
                    backoff_policy: crate::domain::runtime::job::BackoffPolicy::default(),
                })
                .await
                .expect("job should enqueue");
            job_ids.push(job_id);
        }

        for idx in 0..job_ids.len() {
            runtime
                .process_once(
                    "default",
                    "worker-1",
                    now + Duration::milliseconds(idx as i64),
                )
                .await
                .expect("processing should succeed");
        }

        let mut total_published = 0usize;
        for tick in 0..20 {
            total_published += runtime
                .publish_pending_events(3, now + Duration::seconds(tick))
                .await
                .expect("publish sweep should succeed");

            let pending = runtime
                .outbox_store
                .list_pending(50)
                .await
                .expect("pending list should succeed");
            if pending.is_empty() {
                break;
            }
        }

        let pending_final = runtime
            .outbox_store
            .list_pending(50)
            .await
            .expect("pending list should succeed");
        assert!(
            pending_final.is_empty(),
            "expected backlog to drain within bounded ticks"
        );
        assert_eq!(total_published, job_ids.len());

        for job_id in job_ids {
            let events = runtime
                .outbox_store
                .list_by_job_id(&job_id)
                .await
                .expect("outbox list by job should succeed");
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].status, OutboxStatus::Published);
            assert!(events[0].publish_attempts >= 1);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 15);
    }

    #[tokio::test]
    async fn recurring_materialization_creates_due_jobs() {
        let runtime = InMemoryRuntime::new();

        let now = Utc::now();
        runtime
            .register_recurring(RecurringDefinition {
                id: "recur.scrape".to_string(),
                queue: "default".to_string(),
                job_type: "test.success".to_string(),
                payload_template_ref: "sttp:in:recurring".to_string(),
                cron_expr: "0/1 * * * * * *".to_string(),
                timezone: "UTC".to_string(),
                jitter_seconds: 0,
                enabled: true,
                max_attempts: 4,
                next_run_at: now,
                last_run_at: None,
                lease_owner: None,
                lease_expires_at: None,
            })
            .await
            .expect("recurring should register");

        let created = runtime
            .materialize_recurring(now, "scheduler-1")
            .await
            .expect("materialization should succeed");

        assert_eq!(created, 1);

        let enqueued = runtime
            .job_store
            .list_by_state(JobState::Enqueued)
            .await
            .expect("list should succeed");

        assert_eq!(enqueued.len(), 1);
        assert!(enqueued[0].id.starts_with("recur.scrape-"));

        let defs = runtime
            .recurring_store
            .list()
            .await
            .expect("list recurring should succeed");

        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].last_run_at, Some(now));
        assert!(defs[0].next_run_at > now);
    }
}
