use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::watch;

use crate::application::runtime::job_continuation::ContinuationBuilder;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{DurableWaitRecord, DurableWaitStatus};
use crate::domain::runtime::job::{Job, NewJob};
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType};
use crate::domain::runtime::resource_lease::FencingToken;
use crate::domain::runtime::typed_contract::{StasisEvent, StasisJob, TypedJobEnvelope};
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore;
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
    pub continuation_store: Arc<dyn JobContinuationStore>,
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
    pub execution_grant: Option<crate::application::runtime::execution_commit::ExecutionGrant>,
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
        let worker_id = worker_id.into();
        let execution_grant = crate::domain::runtime::execution::ExecutionGrant::from_job(job)
            .filter(|grant| {
                grant.owner == worker_id
                    && grant.epoch != 0
                    && matches!(
                        job.state,
                        crate::domain::runtime::job::JobState::Leased
                            | crate::domain::runtime::job::JobState::Running
                    )
            });
        Self {
            job_id: job.id.clone(),
            attempt: job.attempts.saturating_add(1),
            correlation_id: job.correlation_id.clone(),
            causation_id: Some(job.causation_id.clone()),
            cancellation,
            deadline: None,
            fencing_token: execution_grant
                .as_ref()
                .map(|grant| FencingToken(grant.epoch)),
            execution_grant,
            lease_seconds,
            job: job.clone(),
            services,
        }
    }

    pub async fn heartbeat(&self) -> Result<()> {
        let grant = self
            .execution_grant
            .as_ref()
            .ok_or_else(|| StasisError::PortFailure("native execution grant missing".into()))?;
        self.services
            .job_store
            .heartbeat_granted(grant, self.services.clock.now(), self.lease_seconds, None)
            .await
    }
    pub async fn progress<T: Serialize>(&self, value: T) -> Result<()> {
        let grant = self
            .execution_grant
            .as_ref()
            .ok_or_else(|| StasisError::PortFailure("native execution grant missing".into()))?;
        let progress =
            serde_json::to_string(&value).map_err(|e| StasisError::PortFailure(e.to_string()))?;
        self.services
            .job_store
            .heartbeat_granted(
                grant,
                self.services.clock.now(),
                self.lease_seconds,
                Some(progress),
            )
            .await
    }

    /// Cleanup-only authority exists only after native cancellation. Evidence is retained for
    /// this retired execution; it cannot change the job state, output or newer attempt.
    pub async fn acknowledge_cleanup<T: Serialize>(
        &self,
        evidence: T,
    ) -> Result<crate::application::runtime::execution_commit::CommitResult> {
        let grant = self
            .execution_grant
            .as_ref()
            .ok_or_else(|| StasisError::PortFailure("native execution grant missing".into()))?;
        let json = serde_json::to_string(&evidence)
            .map_err(|e| StasisError::PortFailure(e.to_string()))?;
        self.services
            .job_store
            .acknowledge_cleanup(grant, json)
            .await
    }

    pub async fn publish<E: StasisEvent>(&self, event: E) -> Result<()> {
        let payload = serde_json::to_string(&event)
            .map_err(|err| StasisError::PortFailure(format!("serialize published event: {err}")))?;
        let now = self.services.clock.now();
        let event_id = self
            .services
            .id_generator
            .next_id(&format!("evt-{}", self.job_id));
        let grant = self
            .execution_grant
            .as_ref()
            .ok_or_else(|| StasisError::PortFailure("native execution grant missing".into()))?;
        self.services
            .job_store
            .publish_granted(
                grant,
                OutboxEvent {
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
                        input_provenance: self.job.input_provenance.clone(),
                        output_provenance: None,
                        execution_id: None,
                        input_memory_query_id: None,
                        input_memory_query_fingerprint: None,
                        output_memory_node_id: None,
                        retrieval_path: None,
                        occurred_at: now,
                        message: Some(format!("{}:{payload}", E::NAME)),
                    },
                },
            )
            .await
    }

    pub async fn enqueue<T: StasisJob>(&self, payload: T) -> Result<String> {
        let declared = T::declaration();
        let envelope = TypedJobEnvelope {
            version: T::VERSION,
            payload,
        };
        let payload_ref = serde_json::to_string(&envelope)
            .map_err(|err| StasisError::PortFailure(format!("serialize child job: {err}")))?;
        let now = self.services.clock.now();
        let id = self.services.id_generator.next_id("job");
        let grant = self
            .execution_grant
            .as_ref()
            .ok_or_else(|| StasisError::PortFailure("native execution grant missing".into()))?;
        self.services
            .job_store
            .stage_child(
                grant,
                NewJob {
                    id: id.clone(),
                    queue: declared.queue,
                    job_type: T::NAME.to_string(),
                    payload_ref,
                    priority: declared.priority,
                    max_attempts: declared.retry.max_attempts,
                    idempotency_key: format!("idem-{id}"),
                    correlation_id: self.correlation_id.clone(),
                    causation_id: self.job_id.clone(),
                    trace_id: self.job.trace_id.clone(),
                    input_provenance: self.job.input_provenance.clone(),
                    placement: declared.placement,
                    scheduled_at: now,
                    backoff_policy: declared.retry.backoff,
                }
                .into_job(),
            )
            .await?;
        Ok(id)
    }

    /// Register a child that runs after this job reaches `trigger` (default: succeeded).
    pub fn continue_with<C: StasisJob>(&self, child: C) -> ContinuationBuilder<C> {
        ContinuationBuilder::new(
            self.job_id.clone(),
            child,
            self.services.clock.clone(),
            self.services.id_generator.clone(),
            self.services.job_store.clone(),
            self.services.continuation_store.clone(),
        )
        .execution_grant(self.execution_grant.clone())
    }

    pub async fn parent_job_id(&self) -> Result<Option<String>> {
        Ok(self
            .services
            .continuation_store
            .get_by_child(&self.job_id)
            .await?
            .map(|record| record.parent_job_id))
    }

    pub async fn parent_output_json(&self) -> Result<Option<String>> {
        Ok(self
            .services
            .continuation_store
            .get_by_child(&self.job_id)
            .await?
            .and_then(|record| record.parent_output_json))
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

        let wait = DurableWaitRecord {
            wait_id: wait_id(
                &self.ctx.job_id,
                self.ctx.job.incarnation,
                E::NAME,
                &correlation_key,
            ),
            job_id: self.ctx.job_id.clone(),
            incarnation: self.ctx.job.incarnation,
            signal_type: E::NAME.to_string(),
            correlation_key: correlation_key.clone(),
            status: DurableWaitStatus::Pending,
            deadline_at: deadline,
            created_at: now,
            updated_at: now,
            signal_payload: None,
            consumed_signal_ids: Vec::new(),
        };
        let grant = self
            .ctx
            .execution_grant
            .as_ref()
            .ok_or_else(|| StasisError::PortFailure("native execution grant missing".into()))?;
        let wait = self.ctx.services.job_store.poll_wait(grant, wait).await?;
        match wait.status {
            DurableWaitStatus::Signaled => decode_signal(wait.signal_payload.as_deref()),
            DurableWaitStatus::TimedOut => Err(JobConsumeError::Timeout(format!(
                "wait for {} timed out",
                E::NAME
            ))),
            DurableWaitStatus::Cancelled => Err(JobConsumeError::Cancelled),
            DurableWaitStatus::Pending => Err(JobConsumeError::Deferred {
                scheduled_at: next_poll_at(now, wait.deadline_at),
                message: format!("waiting for signal {}", E::NAME),
            }),
        }
    }
}

impl<'a, E: StasisEvent> IntoFuture for WaitRequest<'a, E> {
    type Output = JobResult<E>;
    type IntoFuture = Pin<Box<dyn Future<Output = JobResult<E>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.poll())
    }
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

fn wait_id(job_id: &str, incarnation: u64, signal_type: &str, correlation_key: &str) -> String {
    format!("wait:{job_id}:incarnation:{incarnation}:{signal_type}:{correlation_key}")
}

fn next_poll_at(now: DateTime<Utc>, deadline: Option<DateTime<Utc>>) -> DateTime<Utc> {
    let poll = now + chrono::Duration::seconds(5);
    match deadline {
        Some(deadline) if deadline < poll => deadline,
        _ => poll,
    }
}
