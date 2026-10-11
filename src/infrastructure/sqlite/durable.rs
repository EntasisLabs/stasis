//! Durable Object session over the shared SQLite schema.
//!
//! Covers the demo path: inbound idempotency, job lease and heartbeat, attempt
//! history, and the Grapheme workflow graph state a guest needs after the
//! isolate is evicted. Recurring jobs, the outbox, durable waits, and
//! continuations stay in memory.
//!
//! The four SQLite stores (`ExecutorJobStore`, `ExecutorJobAttemptStore`,
//! `ExecutorInboundTriggerStore`, `ExecutorResourceLeaseStore`) are the ports
//! `StoreRuntime::from_stores` takes for jobs, attempts, inbound keys, and
//! leases. They implement today's `#[async_trait]` traits. After the
//! store-backed engine lands, switch those impls to `stasis::async_store!`
//! and pass them in with the in-memory fillers. `ExecutorWorkflowDefinitionStore`
//! stays beside that engine. See `docs-book/src/workers-durable-sqlite.md`.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::application::runtime::inbound_trigger::accept_inbound_trigger;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::inbound_trigger::{
    InboundAccept, InboundDisposition, InboundJobTrigger, InboundProtocol,
};
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::placement::WorkerCapabilities;
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedJob {
    pub job_id: String,
    pub state: String,
}

/// SQLite-backed kernel for a single Durable Object.
pub struct DurableSqliteKernel {
    jobs: ExecutorJobStore,
    attempts: ExecutorJobAttemptStore,
    inbound: ExecutorInboundTriggerStore,
    leases: ExecutorResourceLeaseStore,
    workflows: ExecutorWorkflowDefinitionStore,
    ids: SequenceIds,
    clock: SystemClock,
}

impl DurableSqliteKernel {
    pub fn open(executor: Arc<dyn SqlExecutor>) -> Result<Self> {
        apply_schema(executor.as_ref())?;
        Ok(Self {
            jobs: ExecutorJobStore::new(executor.clone()),
            attempts: ExecutorJobAttemptStore::new(executor.clone()),
            inbound: ExecutorInboundTriggerStore::new(executor.clone()),
            leases: ExecutorResourceLeaseStore::new(executor.clone()),
            workflows: ExecutorWorkflowDefinitionStore::new(executor.clone()),
            ids: SequenceIds { executor },
            clock: SystemClock,
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
            &self.jobs,
            &self.inbound,
            &self.clock,
            &self.ids,
            trigger,
        ))
    }

    pub fn process_once(&self, queue: &str, worker_id: &str) -> Result<Option<ProcessedJob>> {
        let now = self.clock.now();
        let Some(mut job) = ready(self.jobs.lease_due(
            queue,
            worker_id,
            now,
            LEASE_SECONDS,
            &WorkerCapabilities::any(),
        ))?
        else {
            return Ok(None);
        };
        self.jobs
            .heartbeat_if_owner(&job.id, worker_id, now, LEASE_SECONDS)?;
        job.state = JobState::Running;
        job.attempts = job.attempts.saturating_add(1);
        job.started_at = Some(now);
        let job_id = job.id.clone();
        ready(self.jobs.save(job.clone()))?;
        self.checkpoint_grapheme(&job)?;
        let finished = self.clock.now();
        let attempt = JobAttempt {
            attempt_id: self.ids.next_id("attempt"),
            job_id: job_id.clone(),
            attempt_number: job.attempts,
            worker_id: worker_id.to_string(),
            started_at: now,
            finished_at: finished,
            outcome: JobAttemptOutcome::Succeeded,
            error_message: None,
            output_provenance: None,
            execution_id: None,
            guardrail_code: None,
            policy_reason: None,
            duration_ms: u64::try_from((finished - now).num_milliseconds().max(0)).ok(),
            diagnostics: Some(job.payload_ref.clone()),
        };
        ready(self.attempts.insert(attempt))?;
        job.state = JobState::Succeeded;
        job.finished_at = Some(finished);
        job.lease_owner = None;
        job.lease_expires_at = None;
        ready(self.jobs.save(job))?;
        Ok(Some(ProcessedJob {
            job_id,
            state: "succeeded".to_string(),
        }))
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        ready(self.jobs.get(job_id))
    }

    pub fn lease_next(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<Job>> {
        ready(self.jobs.lease_due(
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
        self.jobs
            .heartbeat_if_owner(job_id, worker_id, now, lease_seconds)
    }

    pub fn attempts_for(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        ready(self.attempts.list_by_job_id(job_id))
    }

    pub fn acquire_lease(
        &self,
        resource: &str,
        owner: &str,
        ttl_seconds: i64,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease> {
        ready(self.leases.acquire(
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
        ready(self.leases.renew(
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
        let _: Value =
            serde_json::from_str(state_json).map_err(|err| sql_err("grapheme state", err))?;
        let now = self.clock.now();
        let revision_id = self.ids.next_id("rev");
        let existing = ready(self.workflows.get_definition(workflow_id))?;
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
        ready(self.workflows.upsert_definition(definition))?;
        ready(self.workflows.insert_revision(WorkflowRevision {
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
        }))?;
        Ok(())
    }

    pub fn load_grapheme_state(&self, workflow_id: &str) -> Result<Option<String>> {
        self.workflows.latest_graph_state(workflow_id)
    }

    fn checkpoint_grapheme(&self, job: &Job) -> Result<()> {
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
        self.save_grapheme_state(workflow_id, source, &state.to_string())
    }
}

impl DurableSqliteKernel {
    pub fn is_duplicate(accept: &InboundAccept) -> bool {
        accept.disposition == InboundDisposition::Duplicate
    }
}
