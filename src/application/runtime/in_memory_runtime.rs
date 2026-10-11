use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};

use crate::application::runtime::store_runtime::StoreRuntime;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{Job, JobState};
use crate::domain::runtime::job_attempt::JobAttempt;
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
use crate::infrastructure::runtime::in_memory_durable_wait_store::InMemoryDurableWaitStore;
use crate::infrastructure::runtime::in_memory_inbound_trigger_store::InMemoryInboundTriggerStore;
use crate::infrastructure::runtime::in_memory_job_continuation_store::InMemoryJobContinuationStore;
use crate::infrastructure::runtime::in_memory_resource_lease_store::InMemoryResourceLeaseStore;
use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::infrastructure::telemetry::NoopRuntimeTracing;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;

pub use crate::application::runtime::job_handler::{JobExecutionOutcome, JobHandler};

/// In-memory backend: [`StoreRuntime`] over the process-local stores.
pub type InMemoryRuntime = StoreRuntime<
    InMemoryJobStore,
    InMemoryRecurringStore,
    InMemoryOutboxStore,
    InMemoryJobAttemptStore,
    InMemoryDurableWaitStore,
    InMemoryJobContinuationStore,
    InMemoryInboundTriggerStore,
    InMemoryResourceLeaseStore,
>;

impl
    StoreRuntime<
        InMemoryJobStore,
        InMemoryRecurringStore,
        InMemoryOutboxStore,
        InMemoryJobAttemptStore,
        InMemoryDurableWaitStore,
        InMemoryJobContinuationStore,
        InMemoryInboundTriggerStore,
        InMemoryResourceLeaseStore,
    >
{
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
        Self::from_stores(
            InMemoryJobStore::default(),
            InMemoryRecurringStore::default(),
            InMemoryOutboxStore::default(),
            InMemoryJobAttemptStore::default(),
            InMemoryDurableWaitStore::default(),
            InMemoryJobContinuationStore::default(),
            InMemoryInboundTriggerStore::default(),
            InMemoryResourceLeaseStore::default(),
            clock,
            id_generator,
            metrics,
            tracing,
        )
    }
}

impl Default for InMemoryRuntime {
    fn default() -> Self {
        Self::new()
    }
}

/// Concrete in-memory stores are `Send` on native and on wasm32.
#[allow(dead_code)]
fn in_memory_runtime_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<InMemoryRuntime>();
}

#[derive(Clone, Default)]
pub struct InMemoryJobStore {
    jobs: Arc<RwLock<HashMap<String, Job>>>,
}

crate::async_store! {
impl JobStore for InMemoryJobStore {
    async fn insert(&self, job: Job) -> Result<()> {
        let mut state = self
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

        state.insert(job.id.clone(), job);
        Ok(())
    }

    async fn save(&self, job: Job) -> Result<()> {
        let mut state = self
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

        state.insert(job.id.clone(), job);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Job>> {
        let state = self
            .jobs
            .read()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

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
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

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

        job.state = JobState::Leased;
        job.lease_owner = Some(worker_id.to_string());
        job.lease_expires_at = Some(now + Duration::seconds(lease_seconds));
        job.heartbeat_at = Some(now);

        Ok(Some(job.clone()))
    }

    async fn heartbeat(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<()> {
        let mut state = self
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

        let Some(job) = state.get_mut(job_id) else {
            return Ok(());
        };

        if job.lease_owner.as_deref() == Some(worker_id) {
            job.heartbeat_at = Some(now);
            job.lease_expires_at = Some(now + Duration::seconds(lease_seconds.max(0)));
        }

        Ok(())
    }

    async fn list_by_state(&self, state_filter: JobState) -> Result<Vec<Job>> {
        let state = self
            .jobs
            .read()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

        Ok(state
            .values()
            .filter(|job| job.state == state_filter)
            .cloned()
            .collect())
    }

    async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> {
        let state = self
            .jobs
            .read()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
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
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;
        Ok(state.remove(id).is_some())
    }

    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let mut state = self
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

        let before = state.len();
        state.retain(|_, job| {
            let terminal = matches!(
                job.state,
                JobState::Succeeded | JobState::Failed | JobState::DeadLetter | JobState::Canceled
            );
            let old_enough = job.finished_at.map(|t| t <= cutoff).unwrap_or(false);
            !(terminal && old_enough)
        });

        Ok(before.saturating_sub(state.len()))
    }
}
}

#[derive(Clone, Default)]
pub struct InMemoryRecurringStore {
    defs: Arc<RwLock<HashMap<String, RecurringDefinition>>>,
}

#[derive(Clone, Default)]
pub struct InMemoryOutboxStore {
    events: Arc<RwLock<HashMap<String, OutboxEvent>>>,
}

#[derive(Clone, Default)]
pub struct InMemoryJobAttemptStore {
    attempts: Arc<RwLock<HashMap<String, Vec<JobAttempt>>>>,
}

impl InMemoryJobAttemptStore {
    fn list_filtered_attempts<F>(&self, predicate: F) -> Result<Vec<JobAttempt>>
    where
        F: Fn(&JobAttempt) -> bool,
    {
        let state = self
            .attempts
            .read()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;

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

crate::async_store! {
impl JobAttemptStore for InMemoryJobAttemptStore {
    async fn insert(&self, attempt: JobAttempt) -> Result<()> {
        let mut state = self
            .attempts
            .write()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;

        state
            .entry(attempt.job_id.clone())
            .or_insert_with(Vec::new)
            .push(attempt);
        Ok(())
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        let state = self
            .attempts
            .read()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;

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
            .attempts
            .write()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;

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
}

crate::async_store! {
impl OutboxStore for InMemoryOutboxStore {
    async fn insert(&self, event: OutboxEvent) -> Result<()> {
        let mut state = self
            .events
            .write()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

        state.insert(event.event_id.clone(), event);
        Ok(())
    }

    async fn save(&self, event: OutboxEvent) -> Result<()> {
        let mut state = self
            .events
            .write()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

        state.insert(event.event_id.clone(), event);
        Ok(())
    }

    async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>> {
        let state = self
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

        Ok(state.get(event_id).cloned())
    }

    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        let state = self
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

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
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

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
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

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
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

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
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

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
            .events
            .write()
            .map_err(|_| StasisError::PortFailure("outbox store lock poisoned".to_string()))?;

        let before = state.len();
        state.retain(|_, evt| {
            let terminal = evt.status != OutboxStatus::Pending;
            let old_enough = evt.event.occurred_at <= cutoff;
            !(terminal && old_enough)
        });

        Ok(before.saturating_sub(state.len()))
    }
}
}

crate::async_store! {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::{Duration, Utc};

    use crate::domain::runtime::job::NewJob;
    use crate::domain::runtime::outbox::OutboxPublishPolicy;
    use crate::ports::outbound::runtime::event_publisher::EventPublisher;

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
