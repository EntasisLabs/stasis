//! Durable Object session over the shared SQLite schema.
//!
//! Jobs, attempts, inbound keys, and resource leases are SQLite stores passed
//! to [`StoreRuntime::from_stores`](crate::application::runtime::store_runtime::StoreRuntime::from_stores).
//! Recurring jobs, the outbox, durable waits, and continuations are in-memory
//! fillers. Grapheme workflow revisions stay beside the engine.
//!
//! The four SQLite impls use [`crate::async_store!`]. `JsSqlExecutor` is still
//! `Send`: the JavaScript handle lives in a thread-local.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::application::runtime::in_memory_runtime::{InMemoryOutboxStore, InMemoryRecurringStore};
use crate::application::runtime::inbound_trigger::accept_inbound_trigger;
use crate::application::runtime::job_handler::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::store_runtime::StoreRuntime;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::inbound_trigger::{
    InboundAccept, InboundDisposition, InboundJobTrigger, InboundProtocol,
};
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::domain::runtime::job_attempt::JobAttempt;
use crate::domain::runtime::placement::WorkerCapabilities;
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::infrastructure::runtime::in_memory_durable_wait_store::InMemoryDurableWaitStore;
use crate::infrastructure::runtime::in_memory_job_continuation_store::InMemoryJobContinuationStore;
use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::infrastructure::telemetry::NoopRuntimeTracing;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;
use crate::ports::outbound::runtime::workflow_definition_store::WorkflowDefinitionStore;

use super::codec::sql_err;
use super::executor::SqlExecutor;
use super::executor_attempt_store::ExecutorJobAttemptStore;
use super::executor_inbound_store::ExecutorInboundTriggerStore;
use super::executor_job_store::ExecutorJobStore;
use super::executor_lease_store::ExecutorResourceLeaseStore;
use super::executor_workflow_store::ExecutorWorkflowDefinitionStore;
use super::migrate::{apply_schema, next_sequence};

const DEFAULT_QUEUE: &str = "default";
const DEFAULT_JOB_TYPE: &str = "durable.job";
const GRAPHEME_JOB_TYPE: &str = "grapheme.workflow";
const GRAPHEME_JSON_PREFIX: &str = "grapheme:json:";
const LEASE_SECONDS: i64 = 30;

/// `StoreRuntime` over the Durable Object SQLite stores.
///
/// Recurring, outbox, wait, and continuation ports are in memory.
pub type DurableObjectRuntime = StoreRuntime<
    ExecutorJobStore,
    InMemoryRecurringStore,
    InMemoryOutboxStore,
    ExecutorJobAttemptStore,
    InMemoryDurableWaitStore,
    InMemoryJobContinuationStore,
    ExecutorInboundTriggerStore,
    ExecutorResourceLeaseStore,
>;

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => unreachable!("durable sqlite stores do not yield"),
    }
}

struct SequenceIds {
    executor: Arc<dyn SqlExecutor>,
}

impl IdGenerator for SequenceIds {
    fn next_id(&self, prefix: &str) -> String {
        match next_sequence(self.executor.as_ref(), prefix) {
            Ok(value) => format!("{prefix}-{value}"),
            Err(_) => format!("{prefix}-fallback"),
        }
    }
}

struct CheckpointHandler {
    job_type: &'static str,
    workflows: ExecutorWorkflowDefinitionStore,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn Clock>,
}

crate::async_store! {
impl JobHandler for CheckpointHandler {
    fn job_type(&self) -> &'static str {
        self.job_type
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        checkpoint_grapheme(
            &self.workflows,
            self.ids.as_ref(),
            self.clock.as_ref(),
            job,
        )
        .await?;
        Ok(JobExecutionOutcome::Success {
            output_provenance: None,
            execution_id: None,
            diagnostics: Some(job.payload_ref.clone()),
        })
    }
}
}

fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Enqueued => "enqueued",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::DeadLetter => "dead_letter",
        JobState::Canceled => "canceled",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedJob {
    pub job_id: String,
    pub state: String,
}

/// SQLite-backed kernel for a single Durable Object.
pub struct DurableSqliteKernel {
    runtime: DurableObjectRuntime,
    workflows: ExecutorWorkflowDefinitionStore,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn Clock>,
}

impl DurableSqliteKernel {
    pub fn open(executor: Arc<dyn SqlExecutor>) -> Result<Self> {
        apply_schema(executor.as_ref())?;
        let workflows = ExecutorWorkflowDefinitionStore::new(executor.clone());
        let ids: Arc<dyn IdGenerator> = Arc::new(SequenceIds {
            executor: executor.clone(),
        });
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let metrics: Arc<dyn RuntimeMetrics> = Arc::new(NoopRuntimeMetrics);
        let tracing: Arc<dyn RuntimeTracing> = Arc::new(NoopRuntimeTracing);
        let runtime = StoreRuntime::from_stores(
            ExecutorJobStore::new(executor.clone()),
            InMemoryRecurringStore::default(),
            InMemoryOutboxStore::default(),
            ExecutorJobAttemptStore::new(executor.clone()),
            InMemoryDurableWaitStore::default(),
            InMemoryJobContinuationStore::default(),
            ExecutorInboundTriggerStore::new(executor.clone()),
            ExecutorResourceLeaseStore::new(executor),
            clock.clone(),
            ids.clone(),
            metrics,
            tracing,
        );
        for job_type in [DEFAULT_JOB_TYPE, GRAPHEME_JOB_TYPE] {
            runtime.register_handler(CheckpointHandler {
                job_type,
                workflows: workflows.clone(),
                ids: ids.clone(),
                clock: clock.clone(),
            })?;
        }
        Ok(Self {
            runtime,
            workflows,
            ids,
            clock,
        })
    }

    pub fn accept_job(&self, idempotency_key: &str, payload: &str) -> Result<InboundAccept> {
        let job_type = if payload.starts_with(GRAPHEME_JSON_PREFIX) {
            GRAPHEME_JOB_TYPE
        } else {
            DEFAULT_JOB_TYPE
        };
        self.accept_job_on_queue(idempotency_key, payload, DEFAULT_QUEUE, job_type)
    }

    pub fn accept_job_on_queue(
        &self,
        idempotency_key: &str,
        payload: &str,
        queue: &str,
        job_type: &str,
    ) -> Result<InboundAccept> {
        let trigger = InboundJobTrigger {
            protocol: InboundProtocol::HttpWebhook,
            idempotency_key: idempotency_key.to_string(),
            job_type: job_type.to_string(),
            payload_ref: payload.to_string(),
            queue: queue.to_string(),
            priority: 0,
            max_attempts: 3,
            backoff: BackoffPolicy::default(),
            correlation_id: None,
            placement: Default::default(),
        };
        ready(accept_inbound_trigger(
            &self.runtime.job_store,
            &self.runtime.inbound_trigger_store,
            self.clock.as_ref(),
            self.ids.as_ref(),
            trigger,
        ))
    }

    pub fn process_once(&self, queue: &str, worker_id: &str) -> Result<Option<ProcessedJob>> {
        let Some(job_id) = ready(self.runtime.process_once_now(queue, worker_id))? else {
            return Ok(None);
        };
        let job = ready(self.runtime.job_store.get(&job_id))?
            .ok_or_else(|| sql_err("process job", "leased job disappeared"))?;
        Ok(Some(ProcessedJob {
            job_id,
            state: state_name(job.state).to_string(),
        }))
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        ready(self.runtime.job_store.get(job_id))
    }

    pub fn lease_next(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<Job>> {
        ready(self.runtime.job_store.lease_due(
            queue,
            worker_id,
            now,
            LEASE_SECONDS,
            &WorkerCapabilities::any(),
        ))
    }

    pub fn heartbeat(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<bool> {
        self.runtime
            .job_store
            .heartbeat_if_owner(job_id, worker_id, now, lease_seconds)
    }

    pub fn attempts_for(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        ready(self.runtime.list_job_attempts(job_id))
    }

    pub fn acquire_lease(
        &self,
        resource: &str,
        owner: &str,
        ttl_seconds: i64,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease> {
        ready(self.runtime.lease_store.acquire(
            ResourceKey(resource.to_string()),
            OwnerId(owner.to_string()),
            chrono::Duration::seconds(ttl_seconds),
            now,
            force,
        ))
    }

    pub fn renew_lease(
        &self,
        resource: &str,
        owner: &str,
        fencing_token: u64,
        ttl_seconds: i64,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease> {
        ready(self.runtime.lease_store.renew(
            &ResourceKey(resource.to_string()),
            &OwnerId(owner.to_string()),
            FencingToken(fencing_token),
            chrono::Duration::seconds(ttl_seconds),
            now,
        ))
    }

    pub fn save_grapheme_state(
        &self,
        workflow_id: &str,
        source: &str,
        state_json: &str,
    ) -> Result<()> {
        ready(save_grapheme_state(
            &self.workflows,
            self.ids.as_ref(),
            self.clock.as_ref(),
            workflow_id,
            source,
            state_json,
        ))
    }

    pub fn load_grapheme_state(&self, workflow_id: &str) -> Result<Option<String>> {
        self.workflows.latest_graph_state(workflow_id)
    }
}

impl DurableSqliteKernel {
    pub fn is_duplicate(accept: &InboundAccept) -> bool {
        accept.disposition == InboundDisposition::Duplicate
    }
}

async fn checkpoint_grapheme(
    workflows: &ExecutorWorkflowDefinitionStore,
    ids: &dyn IdGenerator,
    clock: &dyn Clock,
    job: &Job,
) -> Result<()> {
    let Some(json) = job.payload_ref.strip_prefix(GRAPHEME_JSON_PREFIX) else {
        return Ok(());
    };
    let value: Value = serde_json::from_str(json)
        .map_err(|err| StasisError::PortFailure(format!("grapheme payload: {err}")))?;
    let source = value
        .get("source")
        .and_then(|item| item.as_str())
        .unwrap_or("");
    let state = value.get("state_current").cloned().unwrap_or(Value::Null);
    let workflow_id = value
        .get("workflow_id")
        .and_then(|item| item.as_str())
        .unwrap_or(job.id.as_str());
    save_grapheme_state(
        workflows,
        ids,
        clock,
        workflow_id,
        source,
        &state.to_string(),
    )
    .await
}

async fn save_grapheme_state(
    workflows: &ExecutorWorkflowDefinitionStore,
    ids: &dyn IdGenerator,
    clock: &dyn Clock,
    workflow_id: &str,
    source: &str,
    state_json: &str,
) -> Result<()> {
    let _: Value =
        serde_json::from_str(state_json).map_err(|err| sql_err("grapheme state", err))?;
    let now = clock.now();
    let revision_id = ids.next_id("rev");
    let existing = workflows.get_definition(workflow_id).await?;
    let definition = WorkflowDefinition {
        workflow_id: workflow_id.to_string(),
        queue: existing
            .as_ref()
            .map(|definition| definition.queue.clone())
            .unwrap_or_else(|| DEFAULT_QUEUE.to_string()),
        latest_revision_id: revision_id.clone(),
        created_at: existing
            .as_ref()
            .map(|definition| definition.created_at)
            .unwrap_or(now),
        updated_at: now,
    };
    workflows.upsert_definition(definition).await?;
    workflows
        .insert_revision(WorkflowRevision {
            workflow_id: workflow_id.to_string(),
            revision_id,
            source: source.to_string(),
            graph_state_json: state_json.to_string(),
            compiler_metadata_json: "{}".to_string(),
            graph_modules_csv: String::new(),
            graph_function_steps_csv: String::new(),
            graph_function_inputs_json: "{}".to_string(),
            reflected_at_utc: now,
            executable_count: 0,
            reflection_receipt_json: "{}".to_string(),
        })
        .await?;
    Ok(())
}
