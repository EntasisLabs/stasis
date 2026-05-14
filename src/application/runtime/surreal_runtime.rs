use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};
use surrealdb::Surreal;
use surrealdb::engine::local::Db;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{Job, JobState, NewJob};
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::outbox::{
    OutboxEvent, OutboxPublishPolicy, OutboxStatus, RuntimeEvent, RuntimeEventType,
};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::infrastructure::runtime::surreal_job_attempt_store::SurrealJobAttemptStore;
use crate::infrastructure::runtime::surreal_job_store::SurrealJobStore;
use crate::infrastructure::runtime::surreal_outbox_store::SurrealOutboxStore;
use crate::infrastructure::runtime::surreal_recurring_store::SurrealRecurringStore;
use crate::ports::outbound::runtime::event_publisher::EventPublisher;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;

#[derive(Clone)]
pub struct SurrealRuntime {
    pub job_store: SurrealJobStore,
    pub recurring_store: SurrealRecurringStore,
    pub outbox_store: SurrealOutboxStore,
    pub job_attempt_store: SurrealJobAttemptStore,
    handlers: Arc<RwLock<HashMap<String, Arc<dyn JobHandler>>>>,
    publisher: Arc<RwLock<Option<Arc<dyn EventPublisher>>>>,
    publish_policy: Arc<RwLock<OutboxPublishPolicy>>,
    id_counter: Arc<AtomicU64>,
}

impl SurrealRuntime {
    pub fn new(db: Surreal<Db>) -> Self {
        Self {
            job_store: SurrealJobStore::new(db.clone()),
            recurring_store: SurrealRecurringStore::new(db.clone()),
            outbox_store: SurrealOutboxStore::new(db.clone()),
            job_attempt_store: SurrealJobAttemptStore::new(db),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            publisher: Arc::new(RwLock::new(None)),
            publish_policy: Arc::new(RwLock::new(OutboxPublishPolicy::default())),
            id_counter: Arc::new(AtomicU64::new(1)),
        }
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
                "{}-{}",
                definition.id,
                self.id_counter.fetch_add(1, Ordering::SeqCst)
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
            }
            JobExecutionOutcome::RetryableFailure {
                message,
                execution_id,
                diagnostics,
            } => {
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
                    )
                        .await?;
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
                    )
                        .await?;
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
            }
            JobExecutionOutcome::FatalFailure {
                message,
                execution_id,
                diagnostics,
            } => {
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
    ) -> Result<()> {
        let event = OutboxEvent {
            event_id: format!("evt-{}-{}", job.id, self.id_counter.fetch_add(1, Ordering::SeqCst)),
            status: OutboxStatus::Pending,
            publish_attempts: 0,
            published_at: None,
            next_attempt_at: None,
            last_publish_error: None,
            event: RuntimeEvent {
                event_type,
                job_id: job.id.clone(),
                correlation_id: job.correlation_id.clone(),
                causation_id: job.causation_id.clone(),
                trace_id: job.trace_id.clone(),
                sttp_input_node_id: job.sttp_input_node_id.clone(),
                sttp_output_node_id,
                execution_id,
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
        let attempt = JobAttempt {
            attempt_id: format!(
                "attempt-{}-{}",
                job.id,
                self.id_counter.fetch_add(1, Ordering::SeqCst)
            ),
            job_id: job.id.clone(),
            attempt_number,
            worker_id: worker_id.to_string(),
            started_at,
            finished_at,
            outcome,
            error_message,
            sttp_output_node_id,
            execution_id,
            diagnostics,
        };

        self.job_attempt_store.insert(attempt).await
    }
}
