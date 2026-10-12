//! One orchestration engine for every durable backend.
//!
//! `InMemoryRuntime`, `SurrealRuntime`, and `SqliteRuntime` are type aliases of
//! [`StoreRuntime`] over their concrete stores. A new backend implements the
//! store ports and constructs `StoreRuntime` — it does not copy enqueue,
//! `process_once`, leases, retries, waits, continuations, recurring schedules,
//! or inbound triggers.
//!
//! Public store fields keep their concrete types (`rt.job_store` is still a
//! `SurrealJobStore` on the Surreal alias), so adapter helpers such as
//! `job_store.db()` and `job_store.session()` stay available.

use crate::application::runtime::execution_commit::{CommitResult, ExecutionGrant};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};

use crate::application::runtime::inbound_trigger::{
    accept_inbound_trigger, decode_inbound_json, trigger_from_job,
};
use crate::application::runtime::job_context::JobContextServices;
use crate::application::runtime::job_continuation::{ContinuationBuilder, settle_continuations};
use crate::application::runtime::job_lifecycle::{
    DEFAULT_JOB_LEASE_SECONDS, JobLifecycleEvent, StaleRecoverReport,
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
use crate::infrastructure::runtime::portable_time::Instant;
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

use crate::application::runtime::job_handler::{JobExecutionOutcome, JobHandler};
use crate::ports::outbound::runtime::{
    durable_wait_store::DurableWaitStore, inbound_trigger_store::InboundTriggerStore,
    job_continuation_store::JobContinuationStore,
};
/// Store parameters: job, recurring, outbox, attempt, wait, continuation, inbound, lease.
#[derive(Clone)]
pub struct StoreRuntime<J, R, O, A, W, Cn, I, L> {
    pub job_store: J,
    pub recurring_store: R,
    pub outbox_store: O,
    pub job_attempt_store: A,
    pub wait_store: W,
    pub continuation_store: Cn,
    pub inbound_trigger_store: I,
    pub lease_store: L,
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

impl<J, R, O, A, W, Cn, I, L> StoreRuntime<J, R, O, A, W, Cn, I, L>
where
    J: JobStore + Clone + 'static,
    R: RecurringStore + Clone + 'static,
    O: OutboxStore + Clone + 'static,
    A: JobAttemptStore + Clone + 'static,
    W: DurableWaitStore + Clone + 'static,
    Cn: JobContinuationStore + Clone + 'static,
    I: InboundTriggerStore + Clone + 'static,
    L: ResourceLeaseStore + Clone + 'static,
{
    /// Assemble an engine from already-built stores.
    ///
    /// Backends call this from their constructors. Stores must be cheap to clone
    /// (shared handles); the engine clones them into `Arc<dyn _>` for job context,
    /// typed enqueue, and continuations.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stores(
        job_store: J,
        recurring_store: R,
        outbox_store: O,
        job_attempt_store: A,
        wait_store: W,
        continuation_store: Cn,
        inbound_trigger_store: I,
        lease_store: L,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
        tracing: Arc<dyn RuntimeTracing>,
    ) -> Self {
        Self {
            job_store,
            recurring_store,
            outbox_store,
            job_attempt_store,
            wait_store,
            continuation_store,
            inbound_trigger_store,
            lease_store,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            publisher: Arc::new(RwLock::new(None)),
            publish_policy: Arc::new(RwLock::new(OutboxPublishPolicy::default())),
            clock,
            id_generator,
            metrics,
            tracing,
            retention_policy: Arc::new(RwLock::new(RetentionPolicy::default())),
            in_flight: runtime_job_dx::new_in_flight_map(),
            lifecycle_delivery: Arc::new(tokio::sync::Mutex::new(())),
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
        self.replay_lifecycle().await?;
        Ok(true)
    }

    pub async fn replay_lifecycle(&self) -> Result<usize> {
        let _delivery = self.lifecycle_delivery.lock().await;
        let pending = self.job_store.pending_lifecycle().await?;
        let count = pending.len();
        for (key, receipt) in pending {
            let job: Job = serde_json::from_str(&receipt.job_json)
                .map_err(|e| StasisError::PortFailure(e.to_string()))?;
            self.emit_lifecycle_with_output(&job, receipt.lifecycle, receipt.output_json)
                .await?;
            self.job_store
                .mark_lifecycle_delivered(&key, &receipt.digest)
                .await?;
        }
        Ok(count)
    }

    pub async fn fail(&self, job_id: &str) -> Result<bool> {
        let Some(job) = self.job_store.fail_execution(job_id).await? else {
            return Ok(false);
        };
        runtime_job_dx::request_cancel(&self.in_flight, job_id, job.execution_epoch);
        self.replay_lifecycle().await?;
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

            let id = self.id_generator.try_next_id(&definition.id)?.to_string();

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
            self.replay_lifecycle().await?;
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
