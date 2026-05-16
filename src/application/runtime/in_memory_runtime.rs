use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value as JsonValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{Job, JobState, NewJob};
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::outbox::{
    OutboxEvent, OutboxPublishPolicy, OutboxStatus, RuntimeEvent, RuntimeEventType,
};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::application::runtime::replay_report::ReplayReport;
use crate::application::runtime::retention::{RetentionPolicy, RetentionPruneReport};
use crate::application::use_cases::investigate_runtime_lineage::{
    InvestigateRuntimeLineage, RuntimeLineageQuery, RuntimeLineageReport,
};
use crate::ports::outbound::runtime::event_publisher::EventPublisher;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::infrastructure::runtime::system_clock::SystemClock;

const METRIC_JOB_SUCCEEDED_TOTAL: &str = "runtime.job.succeeded.total";
const METRIC_JOB_RETRYABLE_FAILURE_TOTAL: &str = "runtime.job.retryable_failure.total";
const METRIC_JOB_FATAL_FAILURE_TOTAL: &str = "runtime.job.fatal_failure.total";
const METRIC_JOB_DEAD_LETTER_TOTAL: &str = "runtime.job.dead_letter.total";
const METRIC_JOB_RETRY_SCHEDULED_TOTAL: &str = "runtime.job.retry_scheduled.total";
const METRIC_JOB_PROCESS_DURATION_MS: &str = "runtime.job.process.duration_ms";
const METRIC_OUTBOX_PUBLISH_SUCCESS_TOTAL: &str = "runtime.outbox.publish.success.total";
const METRIC_OUTBOX_PUBLISH_FAILURE_TOTAL: &str = "runtime.outbox.publish.failure.total";
const METRIC_GRAPHEME_GUARDRAIL_FAILURE_TOTAL: &str = "runtime.grapheme.guardrail_failure.total";

#[derive(Clone, Debug)]
pub enum JobExecutionOutcome {
    Success {
        sttp_output_node_id: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    RetryableFailure {
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    FatalFailure {
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
}

#[async_trait]
pub trait JobHandler: Send + Sync {
    fn job_type(&self) -> &'static str;
    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome>;
}

#[derive(Clone)]
pub struct InMemoryRuntime {
    pub job_store: InMemoryJobStore,
    pub recurring_store: InMemoryRecurringStore,
    pub outbox_store: InMemoryOutboxStore,
    pub job_attempt_store: InMemoryJobAttemptStore,
    handlers: Arc<RwLock<HashMap<String, Arc<dyn JobHandler>>>>,
    publisher: Arc<RwLock<Option<Arc<dyn EventPublisher>>>>,
    publish_policy: Arc<RwLock<OutboxPublishPolicy>>,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
    metrics: Arc<dyn RuntimeMetrics>,
    retention_policy: Arc<RwLock<RetentionPolicy>>,
}

impl Default for InMemoryRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRuntime {
    pub fn new() -> Self {
        Self::with_dependencies_and_metrics(
            Arc::new(SystemClock),
            Arc::new(AtomicIdGenerator::new(1)),
            Arc::new(NoopRuntimeMetrics),
        )
    }

    pub fn with_dependencies(clock: Arc<dyn Clock>, id_generator: Arc<dyn IdGenerator>) -> Self {
        Self::with_dependencies_and_metrics(clock, id_generator, Arc::new(NoopRuntimeMetrics))
    }

    pub fn with_dependencies_and_metrics(
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        metrics: Arc<dyn RuntimeMetrics>,
    ) -> Self {
        Self {
            job_store: InMemoryJobStore::default(),
            recurring_store: InMemoryRecurringStore::default(),
            outbox_store: InMemoryOutboxStore::default(),
            job_attempt_store: InMemoryJobAttemptStore::default(),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            publisher: Arc::new(RwLock::new(None)),
            publish_policy: Arc::new(RwLock::new(OutboxPublishPolicy::default())),
            clock,
            id_generator,
            metrics,
            retention_policy: Arc::new(RwLock::new(RetentionPolicy::default())),
        }
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

    pub fn register_event_publisher<P: EventPublisher + 'static>(&self, publisher: P) -> Result<()> {
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

    pub async fn list_attempts_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>> {
        self.job_attempt_store.list_by_guardrail_code(guardrail_code).await
    }

    pub async fn list_attempts_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> {
        self.job_attempt_store.list_by_execution_id(execution_id).await
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

    pub async fn list_lineage_events_by_thread_id(&self, thread_id: &str) -> Result<Vec<OutboxEvent>> {
        self.outbox_store.list_by_thread_id(thread_id).await
    }

    pub async fn investigate_lineage(&self, query: RuntimeLineageQuery) -> Result<RuntimeLineageReport> {
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

    pub async fn replay_dead_letter_now(&self, job_id: &str) -> Result<bool> {
        self.replay_dead_letter(job_id, self.clock.now()).await
    }

    pub async fn publish_pending_events_now(&self, limit: usize) -> Result<usize> {
        self.publish_pending_events(limit, self.clock.now()).await
    }

    pub async fn materialize_recurring_now(&self, scheduler_id: &str) -> Result<usize> {
        self.materialize_recurring(self.clock.now(), scheduler_id).await
    }

    pub async fn prune_terminal_records(&self, cutoff: DateTime<Utc>) -> Result<RetentionPruneReport> {
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

            let id = format!(
                "{}",
                self.id_generator.next_id(&definition.id)
            );

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
                trace_id: definition.id.clone(),
                sttp_input_node_id: definition.payload_template_ref.clone(),
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
        let Some(mut job) = self.job_store.lease_due(queue, worker_id, now, 30).await? else {
            return Ok(None);
        };

        job.state = JobState::Running;
        job.started_at = job.started_at.or(Some(now));
        job.heartbeat_at = Some(now);
        self.job_store.save(job.clone()).await?;
        let processing_started = Instant::now();

        let handler = {
            let handlers = self
                .handlers
                .read()
                .map_err(|_| StasisError::PortFailure("handlers lock poisoned".to_string()))?;
            handlers.get(&job.job_type).cloned()
        };

        let outcome = if let Some(handler) = handler {
            handler.execute(&job).await?
        } else {
            JobExecutionOutcome::FatalFailure {
                message: format!("no handler registered for job_type={}", job.job_type),
                execution_id: None,
                diagnostics: None,
            }
        };

        let attempt_number = job.attempts + 1;
        let attempt_started_at = now;

        match outcome {
            JobExecutionOutcome::Success {
                sttp_output_node_id,
                execution_id,
                diagnostics,
            } => {
                job.state = JobState::Succeeded;
                job.sttp_output_node_id = Some(sttp_output_node_id.clone());
                job.finished_at = Some(now);
                job.lease_owner = None;
                job.lease_expires_at = None;
                job.heartbeat_at = None;
                self.job_store.save(job.clone()).await?;

                self.append_outbox(
                    RuntimeEventType::JobSucceeded,
                    &job,
                    Some(sttp_output_node_id.clone()),
                    None,
                    now,
                    execution_id.clone(),
                    diagnostics.as_deref(),
                )
                    .await?;

                self.append_job_attempt(
                    &job,
                    worker_id,
                    attempt_number,
                    attempt_started_at,
                    now,
                    JobAttemptOutcome::Succeeded,
                    None,
                    Some(sttp_output_node_id),
                    execution_id,
                    diagnostics,
                )
                .await?;

                self.metrics.incr_counter(METRIC_JOB_SUCCEEDED_TOTAL, 1);
                self.metrics.observe_duration_ms(
                    METRIC_JOB_PROCESS_DURATION_MS,
                    processing_started.elapsed().as_millis() as u64,
                );
            }
            JobExecutionOutcome::RetryableFailure {
                message,
                execution_id,
                diagnostics,
            } => {
                let guardrail_failure = diagnostics
                    .as_deref()
                    .map(|v| v.contains("\"guardrail_code\""))
                    .unwrap_or(false);
                job.attempts += 1;
                job.last_error = Some(message.clone());
                job.lease_owner = None;
                job.lease_expires_at = None;
                job.heartbeat_at = None;

                if job.attempts >= job.max_attempts {
                    job.state = JobState::DeadLetter;
                    job.finished_at = Some(now);
                    self.append_outbox(
                        RuntimeEventType::JobDeadLettered,
                        &job,
                        None,
                        Some(message.clone()),
                        now,
                        execution_id.clone(),
                        diagnostics.as_deref(),
                    )
                        .await?;

                    self.metrics.incr_counter(METRIC_JOB_DEAD_LETTER_TOTAL, 1);
                } else {
                    job.state = JobState::Enqueued;
                    let exponent = (job.attempts - 1) as u32;
                    let mut delay = job
                        .backoff_policy
                        .base_delay_seconds
                        .saturating_mul(2_i64.saturating_pow(exponent));
                    delay = delay.min(job.backoff_policy.max_delay_seconds);
                    job.scheduled_at = now + Duration::seconds(delay.max(0));

                    self.append_outbox(
                        RuntimeEventType::JobRetryScheduled,
                        &job,
                        None,
                        Some(message.clone()),
                        now,
                        execution_id.clone(),
                        diagnostics.as_deref(),
                    )
                        .await?;

                    self.metrics.incr_counter(METRIC_JOB_RETRY_SCHEDULED_TOTAL, 1);
                }

                self.job_store.save(job.clone()).await?;

                self.append_job_attempt(
                    &job,
                    worker_id,
                    attempt_number,
                    attempt_started_at,
                    now,
                    JobAttemptOutcome::RetryableFailure,
                    Some(message),
                    None,
                    execution_id,
                    diagnostics,
                )
                .await?;

                self.metrics
                    .incr_counter(METRIC_JOB_RETRYABLE_FAILURE_TOTAL, 1);
                self.metrics.observe_duration_ms(
                    METRIC_JOB_PROCESS_DURATION_MS,
                    processing_started.elapsed().as_millis() as u64,
                );
                if guardrail_failure {
                    self.metrics
                        .incr_counter(METRIC_GRAPHEME_GUARDRAIL_FAILURE_TOTAL, 1);
                }
            }
            JobExecutionOutcome::FatalFailure {
                message,
                execution_id,
                diagnostics,
            } => {
                let guardrail_failure = diagnostics
                    .as_deref()
                    .map(|v| v.contains("\"guardrail_code\""))
                    .unwrap_or(false);
                job.attempts += 1;
                job.state = JobState::DeadLetter;
                job.last_error = Some(message.clone());
                job.finished_at = Some(now);
                job.lease_owner = None;
                job.lease_expires_at = None;
                job.heartbeat_at = None;
                self.job_store.save(job.clone()).await?;

                self.append_outbox(
                    RuntimeEventType::JobDeadLettered,
                    &job,
                    None,
                    Some(message.clone()),
                    now,
                    execution_id.clone(),
                    diagnostics.as_deref(),
                )
                    .await?;

                self.append_job_attempt(
                    &job,
                    worker_id,
                    attempt_number,
                    attempt_started_at,
                    now,
                    JobAttemptOutcome::FatalFailure,
                    Some(message),
                    None,
                    execution_id,
                    diagnostics,
                )
                .await?;

                self.metrics.incr_counter(METRIC_JOB_FATAL_FAILURE_TOTAL, 1);
                self.metrics.incr_counter(METRIC_JOB_DEAD_LETTER_TOTAL, 1);
                self.metrics.observe_duration_ms(
                    METRIC_JOB_PROCESS_DURATION_MS,
                    processing_started.elapsed().as_millis() as u64,
                );
                if guardrail_failure {
                    self.metrics
                        .incr_counter(METRIC_GRAPHEME_GUARDRAIL_FAILURE_TOTAL, 1);
                }
            }
        }

        Ok(Some(job.id))
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

        for mut event in pending {
            if event.next_attempt_at.map(|next| next > now).unwrap_or(false) {
                continue;
            }

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
                        .incr_counter(METRIC_OUTBOX_PUBLISH_SUCCESS_TOTAL, 1);
                }
                Err(err) => {
                    event.publish_attempts = event.publish_attempts.saturating_add(1);
                    event.published_at = None;
                    event.last_publish_error = Some(err.to_string());

                    if event.publish_attempts >= policy.max_attempts {
                        event.status = OutboxStatus::Failed;
                        event.next_attempt_at = None;
                    } else {
                        let exponent = (event.publish_attempts - 1) as u32;
                        let mut delay = policy
                            .base_delay_seconds
                            .saturating_mul(2_i64.saturating_pow(exponent));
                        delay = delay.min(policy.max_delay_seconds);
                        event.status = OutboxStatus::Pending;
                        event.next_attempt_at = Some(now + Duration::seconds(delay.max(0)));
                    }

                    self.outbox_store.save(event).await?;
                    self.metrics
                        .incr_counter(METRIC_OUTBOX_PUBLISH_FAILURE_TOTAL, 1);
                }
            }
        }

        Ok(published)
    }

    async fn append_outbox(
        &self,
        event_type: RuntimeEventType,
        job: &Job,
        sttp_output_node_id: Option<String>,
        message: Option<String>,
        now: DateTime<Utc>,
        execution_id: Option<String>,
        diagnostics: Option<&str>,
    ) -> Result<()> {
        let (
            input_memory_query_id,
            input_memory_query_fingerprint,
            output_memory_node_id,
            retrieval_path,
        ) =
            Self::extract_memory_lineage_fields(diagnostics);
        let thread_id = Self::extract_thread_id(diagnostics);
        let event = OutboxEvent {
            event_id: self.id_generator.next_id(&format!("evt-{}", job.id)),
            status: OutboxStatus::Pending,
            publish_attempts: 0,
            published_at: None,
            next_attempt_at: None,
            last_publish_error: None,
            event: RuntimeEvent {
                event_type,
                job_id: job.id.clone(),
                thread_id,
                correlation_id: job.correlation_id.clone(),
                causation_id: job.causation_id.clone(),
                trace_id: job.trace_id.clone(),
                sttp_input_node_id: job.sttp_input_node_id.clone(),
                sttp_output_node_id,
                execution_id,
                input_memory_query_id,
                input_memory_query_fingerprint,
                output_memory_node_id,
                retrieval_path,
                occurred_at: now,
                message,
            },
        };

        self.outbox_store.insert(event).await
    }

    async fn append_job_attempt(
        &self,
        job: &Job,
        worker_id: &str,
        attempt_number: u32,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        outcome: JobAttemptOutcome,
        error_message: Option<String>,
        sttp_output_node_id: Option<String>,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    ) -> Result<()> {
        let (guardrail_code, policy_reason, duration_ms) =
            Self::extract_diagnostics_fields(diagnostics.as_deref());

        let attempt = JobAttempt {
            attempt_id: self.id_generator.next_id(&format!("attempt-{}", job.id)),
            job_id: job.id.clone(),
            attempt_number,
            worker_id: worker_id.to_string(),
            started_at,
            finished_at,
            outcome,
            error_message,
            sttp_output_node_id,
            execution_id,
            guardrail_code,
            policy_reason,
            duration_ms,
            diagnostics,
        };

        self.job_attempt_store.insert(attempt).await
    }

    fn extract_diagnostics_fields(
        diagnostics: Option<&str>,
    ) -> (Option<String>, Option<String>, Option<u64>) {
        let Some(raw) = diagnostics else {
            return (None, None, None);
        };

        let Ok(json) = serde_json::from_str::<JsonValue>(raw) else {
            return (None, None, None);
        };

        let guardrail_code = json
            .get("guardrail_code")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());
        let policy_reason = json
            .get("policy_reason")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());
        let duration_ms = json.get("duration_ms").and_then(|v| v.as_u64());

        (guardrail_code, policy_reason, duration_ms)
    }

    fn extract_memory_lineage_fields(
        diagnostics: Option<&str>,
    ) -> (Option<String>, Option<String>, Option<String>, Option<String>) {
        let Some(raw) = diagnostics else {
            return (None, None, None, None);
        };

        let Ok(json) = serde_json::from_str::<JsonValue>(raw) else {
            return (None, None, None, None);
        };

        let input_memory_query_id = json
            .get("input_memory_query_id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());

        let input_memory_query_fingerprint = json
            .get("input_memory_query_fingerprint")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
            .or_else(|| {
                json.get("memory_recall")
                    .and_then(|v| v.get("query_fingerprint"))
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string())
            });

        let output_memory_node_id = json
            .get("output_memory_node_id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
            .or_else(|| {
                json.get("memory_store_node_id")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string())
            });

        let retrieval_path = json
            .get("memory_retrieval_path")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());

        (
            input_memory_query_id,
            input_memory_query_fingerprint,
            output_memory_node_id,
            retrieval_path,
        )
    }

    fn extract_thread_id(diagnostics: Option<&str>) -> Option<String> {
        let raw = diagnostics?;
        let json = serde_json::from_str::<JsonValue>(raw).ok()?;
        json.get("thread_id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }
}

#[derive(Clone, Default)]
pub struct InMemoryJobStore {
    jobs: Arc<RwLock<HashMap<String, Job>>>,
}

#[async_trait]
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

    async fn heartbeat(&self, job_id: &str, worker_id: &str, now: DateTime<Utc>) -> Result<()> {
        let mut state = self
            .jobs
            .write()
            .map_err(|_| StasisError::PortFailure("job store lock poisoned".to_string()))?;

        let Some(job) = state.get_mut(job_id) else {
            return Ok(());
        };

        if job.lease_owner.as_deref() == Some(worker_id) {
            job.heartbeat_at = Some(now);
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

#[async_trait]
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
        let state = self
            .attempts
            .read()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;

        let mut attempts: Vec<JobAttempt> = state
            .values()
            .flat_map(|attempts| attempts.iter())
            .filter(|attempt| attempt.guardrail_code.as_deref() == Some(guardrail_code))
            .cloned()
            .collect();

        attempts.sort_by_key(|attempt| attempt.attempt_number);
        Ok(attempts)
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> {
        let state = self
            .attempts
            .read()
            .map_err(|_| StasisError::PortFailure("job attempt store lock poisoned".to_string()))?;

        let mut attempts: Vec<JobAttempt> = state
            .values()
            .flat_map(|attempts| attempts.iter())
            .filter(|attempt| attempt.execution_id.as_deref() == Some(execution_id))
            .cloned()
            .collect();

        attempts.sort_by_key(|attempt| attempt.attempt_number);
        Ok(attempts)
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

#[async_trait]
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

        pending.sort_by_key(|evt| evt.event.occurred_at);
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
                sttp_output_node_id: "sttp:out:1".to_string(),
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
                    sttp_output_node_id: "sttp:out:flaky".to_string(),
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
                return Err(StasisError::PortFailure("synthetic publish failure".to_string()));
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
            sttp_input_node_id: "sttp:in:1".to_string(),
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
        assert_eq!(job.sttp_output_node_id, Some("sttp:out:1".to_string()));
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
