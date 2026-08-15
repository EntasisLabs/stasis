use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::watch;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{DurableWaitRecord, DurableWaitStatus};
use crate::domain::runtime::job::{Job, NewJob};
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType};
use crate::domain::runtime::resource_lease::FencingToken;
use crate::domain::runtime::typed_contract::{StasisEvent, StasisJob, TypedJobEnvelope};
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;

#[derive(Debug)]
pub enum JobConsumeError {
    Deferred {
        scheduled_at: DateTime<Utc>,
        message: String,
    },
    Fatal(String),
    Timeout(String),
    Cancelled,
    Port(StasisError),
}

impl From<StasisError> for JobConsumeError {
    fn from(value: StasisError) -> Self {
        Self::Port(value)
    }
}

impl std::fmt::Display for JobConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deferred { message, .. } => write!(f, "deferred: {message}"),
            Self::Fatal(message) => write!(f, "{message}"),
            Self::Timeout(message) => write!(f, "{message}"),
            Self::Cancelled => write!(f, "job cancelled"),
            Self::Port(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for JobConsumeError {}

pub type JobResult<T> = std::result::Result<T, JobConsumeError>;

#[derive(Clone)]
pub struct JobContextServices {
    pub job_store: Arc<dyn JobStore>,
    pub outbox_store: Arc<dyn OutboxStore>,
    pub wait_store: Arc<dyn DurableWaitStore>,
    pub clock: Arc<dyn Clock>,
    pub id_generator: Arc<dyn IdGenerator>,
}

#[derive(Clone)]
pub struct JobContext {
    pub job_id: String,
    pub attempt: u32,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub cancellation: watch::Receiver<bool>,
    pub deadline: Option<DateTime<Utc>>,
    pub fencing_token: Option<FencingToken>,
    worker_id: String,
    lease_seconds: i64,
    job: Job,
    services: JobContextServices,
}

impl JobContext {
    pub fn new(
        job: &Job,
        worker_id: impl Into<String>,
        cancellation: watch::Receiver<bool>,
        services: JobContextServices,
        lease_seconds: i64,
    ) -> Self {
        Self {
            job_id: job.id.clone(),
            attempt: job.attempts + 1,
            correlation_id: job.correlation_id.clone(),
            causation_id: Some(job.causation_id.clone()),
            cancellation,
            deadline: None,
            fencing_token: None,
            worker_id: worker_id.into(),
            lease_seconds,
            job: job.clone(),
            services,
        }
    }

    pub async fn heartbeat(&self) -> Result<()> {
        self.services
            .job_store
            .heartbeat(
                &self.job_id,
                &self.worker_id,
                self.services.clock.now(),
                self.lease_seconds,
            )
            .await
    }

    pub async fn progress<T: Serialize>(&self, value: T) -> Result<()> {
        let progress_json = serde_json::to_string(&value)
            .map_err(|err| StasisError::PortFailure(format!("serialize job progress: {err}")))?;
        let Some(mut job) = self.services.job_store.get(&self.job_id).await? else {
            return Err(StasisError::PortFailure(format!(
                "job not found for progress: {}",
                self.job_id
            )));
        };
        job.progress_json = Some(progress_json);
        self.services.job_store.save(job).await
    }

    pub async fn publish<E: StasisEvent>(&self, event: E) -> Result<()> {
        let payload = serde_json::to_string(&event)
            .map_err(|err| StasisError::PortFailure(format!("serialize published event: {err}")))?;
        let now = self.services.clock.now();
        let event_id = self
            .services
            .id_generator
            .next_id(&format!("evt-{}", self.job_id));
        self.services
            .outbox_store
            .insert(OutboxEvent {
                event_id,
                status: OutboxStatus::Pending,
                publish_attempts: 0,
                published_at: None,
                next_attempt_at: None,
                last_publish_error: None,
                event: RuntimeEvent {
                    event_type: RuntimeEventType::JobPublished,
                    job_id: self.job_id.clone(),
                    thread_id: None,
                    correlation_id: self.correlation_id.clone(),
                    causation_id: self.job_id.clone(),
                    trace_id: self.job.trace_id.clone(),
                    sttp_input_node_id: self.job.sttp_input_node_id.clone(),
                    sttp_output_node_id: None,
                    execution_id: None,
                    input_memory_query_id: None,
                    input_memory_query_fingerprint: None,
                    output_memory_node_id: None,
                    retrieval_path: None,
                    occurred_at: now,
                    message: Some(format!("{}:{payload}", E::NAME)),
                },
            })
            .await
    }

    pub async fn enqueue<T: StasisJob>(&self, payload: T) -> Result<String> {
        let envelope = TypedJobEnvelope {
            version: T::VERSION,
            payload,
        };
        let payload_ref = serde_json::to_string(&envelope)
            .map_err(|err| StasisError::PortFailure(format!("serialize child job: {err}")))?;
        let now = self.services.clock.now();
        let id = self.services.id_generator.next_id("job");
        self.services
            .job_store
            .insert(
                NewJob {
                    id: id.clone(),
                    queue: self.job.queue.clone(),
                    job_type: T::NAME.to_string(),
                    payload_ref,
                    priority: self.job.priority,
                    max_attempts: self.job.max_attempts,
                    idempotency_key: format!("idem-{id}"),
                    correlation_id: self.correlation_id.clone(),
                    causation_id: self.job_id.clone(),
                    trace_id: self.job.trace_id.clone(),
                    sttp_input_node_id: self.job.sttp_input_node_id.clone(),
                    scheduled_at: now,
                    backoff_policy: self.job.backoff_policy.clone(),
                }
                .into_job(),
            )
            .await?;
        Ok(id)
    }

    pub fn wait_for<E: StasisEvent>(&self) -> WaitRequest<'_, E> {
        WaitRequest {
            ctx: self,
            correlation_key: None,
            timeout: None,
            _ty: PhantomData,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancellation.borrow()
    }
}

pub struct WaitRequest<'a, E> {
    ctx: &'a JobContext,
    correlation_key: Option<String>,
    timeout: Option<Duration>,
    _ty: PhantomData<E>,
}

impl<'a, E: StasisEvent> WaitRequest<'a, E> {
    pub fn correlated_by(mut self, key: impl Into<String>) -> Self {
        self.correlation_key = Some(key.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    async fn poll(self) -> JobResult<E> {
        if self.ctx.is_cancelled() {
            return Err(JobConsumeError::Cancelled);
        }
        let correlation_key = self
            .correlation_key
            .unwrap_or_else(|| self.ctx.job_id.clone());
        let now = self.ctx.services.clock.now();
        let deadline = self.timeout.map(|timeout| {
            now + chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::seconds(0))
        });

        if let Some(existing) = self
            .ctx
            .services
            .wait_store
            .get_pending_wait(&self.ctx.job_id, E::NAME, &correlation_key)
            .await?
        {
            return settle_wait(&self.ctx.services, existing, now).await;
        }

        if let Some(signaled) = self
            .ctx
            .services
            .wait_store
            .get_wait(&wait_id(&self.ctx.job_id, E::NAME, &correlation_key))
            .await?
            && signaled.status == DurableWaitStatus::Signaled
        {
            return decode_signal(signaled.signal_payload.as_deref());
        }

        if let Some(signal) = self
            .ctx
            .services
            .wait_store
            .take_unconsumed_signal(E::NAME, &correlation_key, &[])
            .await?
        {
            return decode_signal(Some(&signal.payload_json));
        }

        let wait = DurableWaitRecord {
            wait_id: wait_id(&self.ctx.job_id, E::NAME, &correlation_key),
            job_id: self.ctx.job_id.clone(),
            signal_type: E::NAME.to_string(),
            correlation_key: correlation_key.clone(),
            status: DurableWaitStatus::Pending,
            deadline_at: deadline,
            created_at: now,
            updated_at: now,
            signal_payload: None,
            consumed_signal_ids: Vec::new(),
        };
        self.ctx
            .services
            .wait_store
            .insert_wait(wait.clone())
            .await?;

        if let Some(signal) = self
            .ctx
            .services
            .wait_store
            .take_unconsumed_signal(E::NAME, &correlation_key, &[])
            .await?
        {
            let _ = self
                .ctx
                .services
                .wait_store
                .complete_wait(
                    &wait.wait_id,
                    DurableWaitStatus::Signaled,
                    Some(signal.payload_json.clone()),
                    Some(signal.signal_id),
                    now,
                )
                .await?;
            return decode_signal(Some(&signal.payload_json));
        }

        Err(JobConsumeError::Deferred {
            scheduled_at: next_poll_at(now, deadline),
            message: format!("waiting for signal {}", E::NAME),
        })
    }
}

impl<'a, E: StasisEvent> IntoFuture for WaitRequest<'a, E> {
    type Output = JobResult<E>;
    type IntoFuture = Pin<Box<dyn Future<Output = JobResult<E>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.poll())
    }
}

async fn settle_wait<E: StasisEvent>(
    services: &JobContextServices,
    wait: DurableWaitRecord,
    now: DateTime<Utc>,
) -> JobResult<E> {
    if let Some(deadline) = wait.deadline_at
        && now >= deadline
    {
        let _ = services
            .wait_store
            .complete_wait(&wait.wait_id, DurableWaitStatus::TimedOut, None, None, now)
            .await?;
        return Err(JobConsumeError::Timeout(format!(
            "wait for {} timed out",
            wait.signal_type
        )));
    }

    if let Some(signal) = services
        .wait_store
        .take_unconsumed_signal(
            &wait.signal_type,
            &wait.correlation_key,
            &wait.consumed_signal_ids,
        )
        .await?
    {
        let _ = services
            .wait_store
            .complete_wait(
                &wait.wait_id,
                DurableWaitStatus::Signaled,
                Some(signal.payload_json.clone()),
                Some(signal.signal_id),
                now,
            )
            .await?;
        return decode_signal(Some(&signal.payload_json));
    }

    Err(JobConsumeError::Deferred {
        scheduled_at: next_poll_at(now, wait.deadline_at),
        message: format!("waiting for signal {}", wait.signal_type),
    })
}

fn decode_signal<E: StasisEvent>(payload: Option<&str>) -> JobResult<E> {
    let Some(payload) = payload else {
        return Err(JobConsumeError::Fatal(
            "signaled wait is missing payload".into(),
        ));
    };
    serde_json::from_str(payload)
        .map_err(|err| JobConsumeError::Fatal(format!("decode signal payload: {err}")))
}

fn wait_id(job_id: &str, signal_type: &str, correlation_key: &str) -> String {
    format!("wait:{job_id}:{signal_type}:{correlation_key}")
}

fn next_poll_at(now: DateTime<Utc>, deadline: Option<DateTime<Utc>>) -> DateTime<Utc> {
    let poll = now + chrono::Duration::seconds(5);
    match deadline {
        Some(deadline) if deadline < poll => deadline,
        _ => poll,
    }
}
