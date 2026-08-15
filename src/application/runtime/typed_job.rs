use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::job_context::{JobConsumeError, JobContext, JobResult};
use crate::application::runtime::job_lifecycle::JobLifecycleEvent;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{Job, NewJob};
use crate::domain::runtime::typed_contract::{RetryPolicy, StasisJob, TypedJobEnvelope};
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_store::JobStore;

pub struct TypedJobHandler<T, H> {
    handler: H,
    _ty: std::marker::PhantomData<T>,
}

impl<T, H> TypedJobHandler<T, H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            _ty: std::marker::PhantomData,
        }
    }
}

#[async_trait]
pub trait JobConsumer<T: StasisJob>: Send + Sync {
    async fn consume(&self, job: T, ctx: JobContext) -> JobResult<T::Output>;
    async fn on_lifecycle(&self, job: &Job, event: &JobLifecycleEvent) -> Result<()> {
        let _ = (job, event);
        Ok(())
    }
}

#[async_trait]
impl<T, H> JobHandler for TypedJobHandler<T, H>
where
    T: StasisJob,
    H: JobConsumer<T> + 'static,
{
    fn job_type(&self) -> &'static str {
        T::NAME
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let _ = job;
        Ok(JobExecutionOutcome::FatalFailure {
            message: format!(
                "typed consumer {} requires JobContext; execute_with_context was not called",
                T::NAME
            ),
            execution_id: None,
            diagnostics: None,
        })
    }

    async fn execute_with_context(
        &self,
        job: &Job,
        ctx: JobContext,
    ) -> Result<JobExecutionOutcome> {
        let envelope: TypedJobEnvelope<T> = match serde_json::from_str(&job.payload_ref) {
            Ok(envelope) => envelope,
            Err(err) => {
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: format!("malformed typed payload for {}: {err}", T::NAME),
                    execution_id: None,
                    diagnostics: Some(
                        serde_json::json!({
                            "status": "failure",
                            "guardrail_code": "POLICY_VIOLATION",
                            "policy_reason": format!("invalid payload json: {err}"),
                        })
                        .to_string(),
                    ),
                });
            }
        };

        match self.handler.consume(envelope.payload, ctx).await {
            Ok(output) => {
                let diagnostics = serialize_output(&output);
                Ok(JobExecutionOutcome::Success {
                    sttp_output_node_id: format!("sttp:typed:{}:{}", T::NAME, job.id),
                    execution_id: Some(job.id.clone()),
                    diagnostics: Some(diagnostics),
                })
            }
            Err(JobConsumeError::Deferred {
                scheduled_at,
                message,
            }) => Ok(JobExecutionOutcome::Deferred {
                scheduled_at,
                message,
                execution_id: Some(job.id.clone()),
                diagnostics: None,
            }),
            Err(JobConsumeError::Timeout(message) | JobConsumeError::Fatal(message)) => {
                Ok(JobExecutionOutcome::FatalFailure {
                    message,
                    execution_id: Some(job.id.clone()),
                    diagnostics: None,
                })
            }
            Err(JobConsumeError::Cancelled) => Ok(JobExecutionOutcome::FatalFailure {
                message: "job cancelled".into(),
                execution_id: Some(job.id.clone()),
                diagnostics: None,
            }),
            Err(JobConsumeError::Port(err)) => Err(err),
        }
    }

    async fn on_lifecycle(&self, job: &Job, event: &JobLifecycleEvent) -> Result<()> {
        self.handler.on_lifecycle(job, event).await
    }
}

fn serialize_output<T: Serialize>(output: &T) -> String {
    serde_json::to_string(&serde_json::json!({
        "status": "success",
        "output": output,
    }))
    .unwrap_or_else(|_| "{\"status\":\"success\"}".to_string())
}

pub fn encode_typed_payload<T: StasisJob>(payload: &T) -> Result<String> {
    serde_json::to_string(&TypedJobEnvelope {
        version: T::VERSION,
        payload,
    })
    .map_err(|err| StasisError::PortFailure(format!("serialize typed job: {err}")))
}

pub struct TypedEnqueueBuilder<T> {
    payload: T,
    queue: String,
    priority: i32,
    retry: RetryPolicy,
    idempotency_key: Option<String>,
    correlation_id: Option<String>,
    scheduled_at: Option<DateTime<Utc>>,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
    job_store: Arc<dyn JobStore>,
}

impl<T: StasisJob> TypedEnqueueBuilder<T> {
    pub fn new(
        payload: T,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        job_store: Arc<dyn JobStore>,
    ) -> Self {
        Self {
            payload,
            queue: "default".into(),
            priority: 100,
            retry: RetryPolicy::default(),
            idempotency_key: None,
            correlation_id: None,
            scheduled_at: None,
            clock,
            id_generator,
            job_store,
        }
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    pub fn scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(scheduled_at);
        self
    }

    pub async fn send(self) -> Result<String> {
        let payload_ref = encode_typed_payload(&self.payload)?;
        let id = self.id_generator.next_id("job");
        let scheduled_at = self.scheduled_at.unwrap_or_else(|| self.clock.now());
        let idempotency_key = self.idempotency_key.unwrap_or_else(|| format!("idem-{id}"));
        let correlation_id = self.correlation_id.unwrap_or_else(|| id.clone());
        self.job_store
            .insert(
                NewJob {
                    id: id.clone(),
                    queue: self.queue,
                    job_type: T::NAME.to_string(),
                    payload_ref,
                    priority: self.priority,
                    max_attempts: self.retry.max_attempts,
                    idempotency_key,
                    correlation_id,
                    causation_id: "stasis-client".into(),
                    trace_id: crate::application::telemetry::propagation::generate_w3c_trace_id(),
                    sttp_input_node_id: format!("sttp:in:typed:{}", T::NAME),
                    scheduled_at,
                    backoff_policy: self.retry.backoff,
                }
                .into_job(),
            )
            .await?;
        Ok(id)
    }
}
