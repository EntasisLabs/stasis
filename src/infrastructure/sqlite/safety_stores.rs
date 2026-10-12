//! Thin durable adapters. All reference transitions run inside one SQL transaction.
use super::{
    atomic_state::{AtomicState, ready},
    codec::encode_time,
    executor::SqlExecutor,
    row_engine::Scope,
};
use crate::domain::errors::Result;
use crate::domain::runtime::{
    durable_wait::{DurableSignalRecord, DurableWaitRecord, DurableWaitStatus},
    job::{Job, JobState},
    job_attempt::JobAttempt,
    job_continuation::JobContinuation,
    outbox::OutboxEvent,
    placement::WorkerCapabilities,
    recurring::RecurringDefinition,
};
use crate::ports::outbound::runtime::{
    clock::Clock, durable_wait_store::DurableWaitStore, job_attempt_store::JobAttemptStore,
    job_continuation_store::JobContinuationStore, job_store::JobStore, outbox_store::OutboxStore,
    recurring_store::RecurringStore,
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
#[derive(Clone)]
pub struct ExecutorJobStore {
    state: AtomicState,
}
impl ExecutorJobStore {
    pub fn validate(&self) -> Result<()> {
        self.state.run(Scope::Empty, |_| Ok(()))
    }
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
crate::async_store! { impl JobStore for ExecutorJobStore {
async fn insert(&self, job: Job) -> Result<()> { self.state.run(Scope::Job(job.id.clone()), |v| ready(JobStore::insert(&v.jobs, job))) }
async fn pending_lifecycle(&self) -> Result<Vec<(String, crate::domain::runtime::execution::ExecutionReceipt)>> { self.state.run(Scope::LifecyclePending, |v| ready(JobStore::pending_lifecycle(&v.jobs))) }
async fn mark_lifecycle_delivered(&self, key: &str, digest: &str) -> Result<()> { self.state.run(Scope::LifecycleKey(key.to_string()), |v| ready(JobStore::mark_lifecycle_delivered(&v.jobs, key, digest))) }
async fn commit_execution(
        &self,
        snapshot: &Job,
        outcome: crate::domain::runtime::execution::JobExecutionOutcome,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run_completion(snapshot, |v| ready(JobStore::commit_execution(&v.jobs, snapshot, outcome))) }
async fn recover_execution(
        &self,
        snapshot: &Job,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run(Scope::Commit { job_id: snapshot.id.clone(), epoch: snapshot.execution_epoch }, |v| ready(JobStore::recover_execution(&v.jobs, snapshot))) }
async fn heartbeat_granted(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        now: DateTime<Utc>,
        lease_seconds: i64,
        progress: Option<String>,
    ) -> Result<()> { self.state.run(Scope::Job(grant.job_id.clone()), |v| ready(JobStore::heartbeat_granted(&v.jobs, grant, now, lease_seconds, progress))) }
async fn cancel_execution(&self, job_id: &str) -> Result<Option<Job>> { self.state.run(Scope::Cancel(job_id.to_string()), |v| ready(JobStore::cancel_execution(&v.jobs, job_id))) }
async fn acknowledge_cleanup(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        evidence_json: String,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run(Scope::Cleanup { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::acknowledge_cleanup(&v.jobs, grant, evidence_json))) }
async fn publish_granted(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        event: crate::domain::runtime::outbox::OutboxEvent,
    ) -> Result<()> { self.state.run(Scope::Stage { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::publish_granted(&v.jobs, grant, event))) }
async fn stage_child(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        child: Job,
    ) -> Result<()> { self.state.run(Scope::Stage { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::stage_child(&v.jobs, grant, child))) }
async fn stage_continuation(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        continuation: crate::domain::runtime::job_continuation::JobContinuation,
    ) -> Result<()> { self.state.run(Scope::Stage { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::stage_continuation(&v.jobs, grant, continuation))) }
async fn settle_continuation(
        &self,
        parent: &Job,
        record: crate::domain::runtime::job_continuation::JobContinuation,
        child: Option<Job>,
        detail: Option<String>,
    ) -> Result<bool> { self.state.run(Scope::Settle { parent_id: parent.id.clone(), continuation_id: record.id.clone(), child_id: child.as_ref().map(|job| job.id.clone()) }, |v| ready(JobStore::settle_continuation(&v.jobs, parent, record, child, detail))) }
async fn poll_wait(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        wait: crate::domain::runtime::durable_wait::DurableWaitRecord,
    ) -> Result<crate::domain::runtime::durable_wait::DurableWaitRecord> { self.state.run(Scope::Poll { job_id: grant.job_id.clone(), signal_type: wait.signal_type.clone(), correlation_key: wait.correlation_key.clone() }, |v| ready(JobStore::poll_wait(&v.jobs, grant, wait))) }
async fn fail_execution(&self, job_id: &str) -> Result<Option<Job>> { self.state.run(Scope::Fail(job_id.to_string()), |v| ready(JobStore::fail_execution(&v.jobs, job_id))) }
async fn execution_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<crate::domain::runtime::execution::ExecutionReceipt>> { self.state.run(Scope::Receipt { job_id: job_id.to_string(), epoch }, |v| ready(JobStore::execution_receipt(&v.jobs, job_id, epoch))) }
async fn cleanup_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<crate::domain::runtime::execution::CleanupRecord>> { self.state.run(Scope::Cleanup { job_id: job_id.to_string(), epoch }, |v| ready(JobStore::cleanup_receipt(&v.jobs, job_id, epoch))) }
async fn observe_cleanup(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        observation: String,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run(Scope::Cleanup { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::observe_cleanup(&v.jobs, grant, observation))) }
async fn wake_incarnation(&self, job_id: &str, incarnation: u64) -> Result<()> { self.state.run(Scope::Job(job_id.to_string()), |v| ready(JobStore::wake_incarnation(&v.jobs, job_id, incarnation))) }
async fn wake_execution(&self, job_id: &str) -> Result<()> { self.state.run(Scope::Job(job_id.to_string()), |v| ready(JobStore::wake_execution(&v.jobs, job_id))) }
async fn save(&self, job: Job) -> Result<()> { self.state.run(Scope::Job(job.id.clone()), |v| ready(JobStore::save(&v.jobs, job))) }
async fn get(&self, id: &str) -> Result<Option<Job>> { self.state.run(Scope::Job(id.to_string()), |v| ready(JobStore::get(&v.jobs, id))) }
async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
        worker: &WorkerCapabilities,
    ) -> Result<Option<Job>> { self.state.run(Scope::Due { queue: queue.to_string(), now_ns: encode_time(now) }, |v| ready(JobStore::lease_due(&v.jobs, queue, worker_id, now, lease_seconds, worker))) }
async fn heartbeat(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<()> { self.state.run(Scope::Job(job_id.to_string()), |v| ready(JobStore::heartbeat(&v.jobs, job_id, worker_id, now, lease_seconds))) }
async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> { self.state.run(Scope::ByState(super::row_engine::job_state_name(state).to_string()), |v| ready(JobStore::list_by_state(&v.jobs, state))) }
async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> { self.state.run(Scope::Expired { now_ns: encode_time(now) }, |v| ready(JobStore::list_expired_leases(&v.jobs, now))) }
async fn delete(&self, id: &str) -> Result<bool> { self.state.run(Scope::Delete(id.to_string()), |v| ready(JobStore::delete(&v.jobs, id))) }
async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> { self.state.run(Scope::PruneJobs { cutoff_ns: encode_time(cutoff) }, |v| ready(JobStore::prune_terminal_before(&v.jobs, cutoff))) }
async fn prune_retained_before(&self, receipt_cutoff: DateTime<Utc>, cleanup_cutoff: DateTime<Utc>) -> Result<(usize, usize)> { self.state.prune_retained(encode_time(receipt_cutoff), encode_time(cleanup_cutoff)) }
} }
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteJobStore {
    state: AtomicState,
}
#[cfg(feature = "sqlite")]
impl SqliteJobStore {
    pub fn validate(&self) -> Result<()> {
        self.state.run(Scope::Empty, |_| Ok(()))
    }
    pub fn new(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
    pub fn session(&self) -> super::session::SqliteSession {
        self.state.session().clone()
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
#[cfg(feature = "sqlite")]
crate::async_store! { impl JobStore for SqliteJobStore {
async fn insert(&self, job: Job) -> Result<()> { self.state.run(Scope::Job(job.id.clone()), |v| ready(JobStore::insert(&v.jobs, job))) }
async fn pending_lifecycle(&self) -> Result<Vec<(String, crate::domain::runtime::execution::ExecutionReceipt)>> { self.state.run(Scope::LifecyclePending, |v| ready(JobStore::pending_lifecycle(&v.jobs))) }
async fn mark_lifecycle_delivered(&self, key: &str, digest: &str) -> Result<()> { self.state.run(Scope::LifecycleKey(key.to_string()), |v| ready(JobStore::mark_lifecycle_delivered(&v.jobs, key, digest))) }
async fn commit_execution(
        &self,
        snapshot: &Job,
        outcome: crate::domain::runtime::execution::JobExecutionOutcome,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run_completion(snapshot, |v| ready(JobStore::commit_execution(&v.jobs, snapshot, outcome))) }
async fn recover_execution(
        &self,
        snapshot: &Job,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run(Scope::Commit { job_id: snapshot.id.clone(), epoch: snapshot.execution_epoch }, |v| ready(JobStore::recover_execution(&v.jobs, snapshot))) }
async fn heartbeat_granted(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        now: DateTime<Utc>,
        lease_seconds: i64,
        progress: Option<String>,
    ) -> Result<()> { self.state.run(Scope::Job(grant.job_id.clone()), |v| ready(JobStore::heartbeat_granted(&v.jobs, grant, now, lease_seconds, progress))) }
async fn cancel_execution(&self, job_id: &str) -> Result<Option<Job>> { self.state.run(Scope::Cancel(job_id.to_string()), |v| ready(JobStore::cancel_execution(&v.jobs, job_id))) }
async fn acknowledge_cleanup(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        evidence_json: String,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run(Scope::Cleanup { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::acknowledge_cleanup(&v.jobs, grant, evidence_json))) }
async fn publish_granted(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        event: crate::domain::runtime::outbox::OutboxEvent,
    ) -> Result<()> { self.state.run(Scope::Stage { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::publish_granted(&v.jobs, grant, event))) }
async fn stage_child(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        child: Job,
    ) -> Result<()> { self.state.run(Scope::Stage { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::stage_child(&v.jobs, grant, child))) }
async fn stage_continuation(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        continuation: crate::domain::runtime::job_continuation::JobContinuation,
    ) -> Result<()> { self.state.run(Scope::Stage { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::stage_continuation(&v.jobs, grant, continuation))) }
async fn settle_continuation(
        &self,
        parent: &Job,
        record: crate::domain::runtime::job_continuation::JobContinuation,
        child: Option<Job>,
        detail: Option<String>,
    ) -> Result<bool> { self.state.run(Scope::Settle { parent_id: parent.id.clone(), continuation_id: record.id.clone(), child_id: child.as_ref().map(|job| job.id.clone()) }, |v| ready(JobStore::settle_continuation(&v.jobs, parent, record, child, detail))) }
async fn poll_wait(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        wait: crate::domain::runtime::durable_wait::DurableWaitRecord,
    ) -> Result<crate::domain::runtime::durable_wait::DurableWaitRecord> { self.state.run(Scope::Poll { job_id: grant.job_id.clone(), signal_type: wait.signal_type.clone(), correlation_key: wait.correlation_key.clone() }, |v| ready(JobStore::poll_wait(&v.jobs, grant, wait))) }
async fn fail_execution(&self, job_id: &str) -> Result<Option<Job>> { self.state.run(Scope::Fail(job_id.to_string()), |v| ready(JobStore::fail_execution(&v.jobs, job_id))) }
async fn execution_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<crate::domain::runtime::execution::ExecutionReceipt>> { self.state.run(Scope::Receipt { job_id: job_id.to_string(), epoch }, |v| ready(JobStore::execution_receipt(&v.jobs, job_id, epoch))) }
async fn cleanup_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<crate::domain::runtime::execution::CleanupRecord>> { self.state.run(Scope::Cleanup { job_id: job_id.to_string(), epoch }, |v| ready(JobStore::cleanup_receipt(&v.jobs, job_id, epoch))) }
async fn observe_cleanup(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        observation: String,
    ) -> Result<crate::domain::runtime::execution::CommitResult> { self.state.run(Scope::Cleanup { job_id: grant.job_id.clone(), epoch: grant.epoch }, |v| ready(JobStore::observe_cleanup(&v.jobs, grant, observation))) }
async fn wake_incarnation(&self, job_id: &str, incarnation: u64) -> Result<()> { self.state.run(Scope::Job(job_id.to_string()), |v| ready(JobStore::wake_incarnation(&v.jobs, job_id, incarnation))) }
async fn wake_execution(&self, job_id: &str) -> Result<()> { self.state.run(Scope::Job(job_id.to_string()), |v| ready(JobStore::wake_execution(&v.jobs, job_id))) }
async fn save(&self, job: Job) -> Result<()> { self.state.run(Scope::Job(job.id.clone()), |v| ready(JobStore::save(&v.jobs, job))) }
async fn get(&self, id: &str) -> Result<Option<Job>> { self.state.run(Scope::Job(id.to_string()), |v| ready(JobStore::get(&v.jobs, id))) }
async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
        worker: &WorkerCapabilities,
    ) -> Result<Option<Job>> { self.state.run(Scope::Due { queue: queue.to_string(), now_ns: encode_time(now) }, |v| ready(JobStore::lease_due(&v.jobs, queue, worker_id, now, lease_seconds, worker))) }
async fn heartbeat(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<()> { self.state.run(Scope::Job(job_id.to_string()), |v| ready(JobStore::heartbeat(&v.jobs, job_id, worker_id, now, lease_seconds))) }
async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> { self.state.run(Scope::ByState(super::row_engine::job_state_name(state).to_string()), |v| ready(JobStore::list_by_state(&v.jobs, state))) }
async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> { self.state.run(Scope::Expired { now_ns: encode_time(now) }, |v| ready(JobStore::list_expired_leases(&v.jobs, now))) }
async fn delete(&self, id: &str) -> Result<bool> { self.state.run(Scope::Delete(id.to_string()), |v| ready(JobStore::delete(&v.jobs, id))) }
async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> { self.state.run(Scope::PruneJobs { cutoff_ns: encode_time(cutoff) }, |v| ready(JobStore::prune_terminal_before(&v.jobs, cutoff))) }
async fn prune_retained_before(&self, receipt_cutoff: DateTime<Utc>, cleanup_cutoff: DateTime<Utc>) -> Result<(usize, usize)> { self.state.prune_retained(encode_time(receipt_cutoff), encode_time(cleanup_cutoff)) }
} }
#[derive(Clone)]
pub struct ExecutorJobAttemptStore {
    state: AtomicState,
}
impl ExecutorJobAttemptStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
crate::async_store! { impl JobAttemptStore for ExecutorJobAttemptStore {
async fn insert(&self, attempt: JobAttempt) -> Result<()> { self.state.run(Scope::AttemptsJob(attempt.job_id.clone()), |v| ready(JobAttemptStore::insert(&v.jobs.native_attempt_store(), attempt))) }
async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> { self.state.run(Scope::AttemptsJob(job_id.to_string()), |v| ready(JobAttemptStore::list_by_job_id(&v.jobs.native_attempt_store(), job_id))) }
async fn list_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>> { self.state.run(Scope::AttemptsGuardrail(guardrail_code.to_string()), |v| ready(JobAttemptStore::list_by_guardrail_code(&v.jobs.native_attempt_store(), guardrail_code))) }
async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> { self.state.run(Scope::AttemptsExecution(execution_id.to_string()), |v| ready(JobAttemptStore::list_by_execution_id(&v.jobs.native_attempt_store(), execution_id))) }
async fn prune_finished_before(&self, cutoff: DateTime<Utc>) -> Result<usize> { self.state.run(Scope::PruneAttempts { cutoff_ns: encode_time(cutoff) }, |v| ready(JobAttemptStore::prune_finished_before(&v.jobs.native_attempt_store(), cutoff))) }
} }
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteJobAttemptStore {
    state: AtomicState,
}
#[cfg(feature = "sqlite")]
impl SqliteJobAttemptStore {
    pub fn new(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
    pub fn session(&self) -> super::session::SqliteSession {
        self.state.session().clone()
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
#[cfg(feature = "sqlite")]
crate::async_store! { impl JobAttemptStore for SqliteJobAttemptStore {
async fn insert(&self, attempt: JobAttempt) -> Result<()> { self.state.run(Scope::AttemptsJob(attempt.job_id.clone()), |v| ready(JobAttemptStore::insert(&v.jobs.native_attempt_store(), attempt))) }
async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> { self.state.run(Scope::AttemptsJob(job_id.to_string()), |v| ready(JobAttemptStore::list_by_job_id(&v.jobs.native_attempt_store(), job_id))) }
async fn list_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>> { self.state.run(Scope::AttemptsGuardrail(guardrail_code.to_string()), |v| ready(JobAttemptStore::list_by_guardrail_code(&v.jobs.native_attempt_store(), guardrail_code))) }
async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> { self.state.run(Scope::AttemptsExecution(execution_id.to_string()), |v| ready(JobAttemptStore::list_by_execution_id(&v.jobs.native_attempt_store(), execution_id))) }
async fn prune_finished_before(&self, cutoff: DateTime<Utc>) -> Result<usize> { self.state.run(Scope::PruneAttempts { cutoff_ns: encode_time(cutoff) }, |v| ready(JobAttemptStore::prune_finished_before(&v.jobs.native_attempt_store(), cutoff))) }
} }
#[derive(Clone)]
pub struct ExecutorOutboxStore {
    state: AtomicState,
}
impl ExecutorOutboxStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
crate::async_store! { impl OutboxStore for ExecutorOutboxStore {
async fn insert(&self, event: OutboxEvent) -> Result<()> { self.state.run(Scope::Event(event.event_id.clone()), |v| ready(OutboxStore::insert(&v.jobs.native_outbox_store(), event))) }
async fn save(&self, event: OutboxEvent) -> Result<()> { self.state.run(Scope::Event(event.event_id.clone()), |v| ready(OutboxStore::save(&v.jobs.native_outbox_store(), event))) }
async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>> { self.state.run(Scope::Event(event_id.to_string()), |v| ready(OutboxStore::get(&v.jobs.native_outbox_store(), event_id))) }
async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsPending { limit: u64::try_from(limit).unwrap_or(u64::MAX) }, |v| ready(OutboxStore::list_pending(&v.jobs.native_outbox_store(), limit))) }
async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsJob(job_id.to_string()), |v| ready(OutboxStore::list_by_job_id(&v.jobs.native_outbox_store(), job_id))) }
async fn list_by_thread_id(&self, thread_id: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsThread(thread_id.to_string()), |v| ready(OutboxStore::list_by_thread_id(&v.jobs.native_outbox_store(), thread_id))) }
async fn list_by_thread_prefix(&self, thread_prefix: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsPrefix(thread_prefix.to_string()), |v| ready(OutboxStore::list_by_thread_prefix(&v.jobs.native_outbox_store(), thread_prefix))) }
async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsExecution(execution_id.to_string()), |v| ready(OutboxStore::list_by_execution_id(&v.jobs.native_outbox_store(), execution_id))) }
async fn prune_non_pending_before(&self, cutoff: DateTime<Utc>) -> Result<usize> { self.state.run(Scope::PruneEvents { cutoff_ns: encode_time(cutoff) }, |v| ready(OutboxStore::prune_non_pending_before(&v.jobs.native_outbox_store(), cutoff))) }
} }
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteOutboxStore {
    state: AtomicState,
}
#[cfg(feature = "sqlite")]
impl SqliteOutboxStore {
    pub fn new(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
    pub fn session(&self) -> super::session::SqliteSession {
        self.state.session().clone()
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
#[cfg(feature = "sqlite")]
crate::async_store! { impl OutboxStore for SqliteOutboxStore {
async fn insert(&self, event: OutboxEvent) -> Result<()> { self.state.run(Scope::Event(event.event_id.clone()), |v| ready(OutboxStore::insert(&v.jobs.native_outbox_store(), event))) }
async fn save(&self, event: OutboxEvent) -> Result<()> { self.state.run(Scope::Event(event.event_id.clone()), |v| ready(OutboxStore::save(&v.jobs.native_outbox_store(), event))) }
async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>> { self.state.run(Scope::Event(event_id.to_string()), |v| ready(OutboxStore::get(&v.jobs.native_outbox_store(), event_id))) }
async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsPending { limit: u64::try_from(limit).unwrap_or(u64::MAX) }, |v| ready(OutboxStore::list_pending(&v.jobs.native_outbox_store(), limit))) }
async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsJob(job_id.to_string()), |v| ready(OutboxStore::list_by_job_id(&v.jobs.native_outbox_store(), job_id))) }
async fn list_by_thread_id(&self, thread_id: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsThread(thread_id.to_string()), |v| ready(OutboxStore::list_by_thread_id(&v.jobs.native_outbox_store(), thread_id))) }
async fn list_by_thread_prefix(&self, thread_prefix: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsPrefix(thread_prefix.to_string()), |v| ready(OutboxStore::list_by_thread_prefix(&v.jobs.native_outbox_store(), thread_prefix))) }
async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<OutboxEvent>> { self.state.run(Scope::EventsExecution(execution_id.to_string()), |v| ready(OutboxStore::list_by_execution_id(&v.jobs.native_outbox_store(), execution_id))) }
async fn prune_non_pending_before(&self, cutoff: DateTime<Utc>) -> Result<usize> { self.state.run(Scope::PruneEvents { cutoff_ns: encode_time(cutoff) }, |v| ready(OutboxStore::prune_non_pending_before(&v.jobs.native_outbox_store(), cutoff))) }
} }
#[derive(Clone)]
pub struct ExecutorDurableWaitStore {
    state: AtomicState,
}
impl ExecutorDurableWaitStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
crate::async_store! { impl DurableWaitStore for ExecutorDurableWaitStore {
async fn insert_wait(&self, record: DurableWaitRecord) -> Result<()> { self.state.run(Scope::Wait(record.wait_id.clone()), |v| ready(DurableWaitStore::insert_wait(&v.jobs.native_wait_store(), record))) }
async fn get_wait(&self, wait_id: &str) -> Result<Option<DurableWaitRecord>> { self.state.run(Scope::Wait(wait_id.to_string()), |v| ready(DurableWaitStore::get_wait(&v.jobs.native_wait_store(), wait_id))) }
async fn get_pending_wait(
        &self,
        job_id: &str,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Option<DurableWaitRecord>> { self.state.run(Scope::WaitsJobSignal { job_id: job_id.to_string(), signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::get_pending_wait(&v.jobs.native_wait_store(), job_id, signal_type, correlation_key))) }
async fn list_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> { self.state.run(Scope::WaitsSignal { signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::list_by_signal(&v.jobs.native_wait_store(), signal_type, correlation_key))) }
async fn list_pending_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> { self.state.run(Scope::WaitsSignal { signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::list_pending_by_signal(&v.jobs.native_wait_store(), signal_type, correlation_key))) }
async fn list_pending_by_job(&self, job_id: &str) -> Result<Vec<DurableWaitRecord>> { self.state.run(Scope::WaitsJob(job_id.to_string()), |v| ready(DurableWaitStore::list_pending_by_job(&v.jobs.native_wait_store(), job_id))) }
async fn save_wait(&self, record: DurableWaitRecord) -> Result<()> { self.state.run(Scope::Wait(record.wait_id.clone()), |v| ready(DurableWaitStore::save_wait(&v.jobs.native_wait_store(), record))) }
async fn insert_signal(&self, record: DurableSignalRecord) -> Result<bool> { self.state.run(Scope::Signal(record.signal_id.clone()), |v| ready(DurableWaitStore::insert_signal(&v.jobs.native_wait_store(), record))) }
async fn get_signal(&self, signal_id: &str) -> Result<Option<DurableSignalRecord>> { self.state.run(Scope::Signal(signal_id.to_string()), |v| ready(DurableWaitStore::get_signal(&v.jobs.native_wait_store(), signal_id))) }
async fn take_unconsumed_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
        consumed_ids: &[String],
    ) -> Result<Option<DurableSignalRecord>> { self.state.run(Scope::SignalsKey { signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::take_unconsumed_signal(&v.jobs.native_wait_store(), signal_type, correlation_key, consumed_ids))) }
async fn complete_wait(
        &self,
        wait_id: &str,
        status: DurableWaitStatus,
        signal_payload: Option<String>,
        signal_id: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> { self.state.run(Scope::Wait(wait_id.to_string()), |v| ready(DurableWaitStore::complete_wait(&v.jobs.native_wait_store(), wait_id, status, signal_payload, signal_id, updated_at))) }
} }
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteDurableWaitStore {
    state: AtomicState,
}
#[cfg(feature = "sqlite")]
impl SqliteDurableWaitStore {
    pub fn new(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
    pub fn session(&self) -> super::session::SqliteSession {
        self.state.session().clone()
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
#[cfg(feature = "sqlite")]
crate::async_store! { impl DurableWaitStore for SqliteDurableWaitStore {
async fn insert_wait(&self, record: DurableWaitRecord) -> Result<()> { self.state.run(Scope::Wait(record.wait_id.clone()), |v| ready(DurableWaitStore::insert_wait(&v.jobs.native_wait_store(), record))) }
async fn get_wait(&self, wait_id: &str) -> Result<Option<DurableWaitRecord>> { self.state.run(Scope::Wait(wait_id.to_string()), |v| ready(DurableWaitStore::get_wait(&v.jobs.native_wait_store(), wait_id))) }
async fn get_pending_wait(
        &self,
        job_id: &str,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Option<DurableWaitRecord>> { self.state.run(Scope::WaitsJobSignal { job_id: job_id.to_string(), signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::get_pending_wait(&v.jobs.native_wait_store(), job_id, signal_type, correlation_key))) }
async fn list_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> { self.state.run(Scope::WaitsSignal { signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::list_by_signal(&v.jobs.native_wait_store(), signal_type, correlation_key))) }
async fn list_pending_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> { self.state.run(Scope::WaitsSignal { signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::list_pending_by_signal(&v.jobs.native_wait_store(), signal_type, correlation_key))) }
async fn list_pending_by_job(&self, job_id: &str) -> Result<Vec<DurableWaitRecord>> { self.state.run(Scope::WaitsJob(job_id.to_string()), |v| ready(DurableWaitStore::list_pending_by_job(&v.jobs.native_wait_store(), job_id))) }
async fn save_wait(&self, record: DurableWaitRecord) -> Result<()> { self.state.run(Scope::Wait(record.wait_id.clone()), |v| ready(DurableWaitStore::save_wait(&v.jobs.native_wait_store(), record))) }
async fn insert_signal(&self, record: DurableSignalRecord) -> Result<bool> { self.state.run(Scope::Signal(record.signal_id.clone()), |v| ready(DurableWaitStore::insert_signal(&v.jobs.native_wait_store(), record))) }
async fn get_signal(&self, signal_id: &str) -> Result<Option<DurableSignalRecord>> { self.state.run(Scope::Signal(signal_id.to_string()), |v| ready(DurableWaitStore::get_signal(&v.jobs.native_wait_store(), signal_id))) }
async fn take_unconsumed_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
        consumed_ids: &[String],
    ) -> Result<Option<DurableSignalRecord>> { self.state.run(Scope::SignalsKey { signal_type: signal_type.to_string(), correlation_key: correlation_key.to_string() }, |v| ready(DurableWaitStore::take_unconsumed_signal(&v.jobs.native_wait_store(), signal_type, correlation_key, consumed_ids))) }
async fn complete_wait(
        &self,
        wait_id: &str,
        status: DurableWaitStatus,
        signal_payload: Option<String>,
        signal_id: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> { self.state.run(Scope::Wait(wait_id.to_string()), |v| ready(DurableWaitStore::complete_wait(&v.jobs.native_wait_store(), wait_id, status, signal_payload, signal_id, updated_at))) }
} }
#[derive(Clone)]
pub struct ExecutorJobContinuationStore {
    state: AtomicState,
}
impl ExecutorJobContinuationStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
crate::async_store! { impl JobContinuationStore for ExecutorJobContinuationStore {
async fn insert(&self, record: JobContinuation) -> Result<()> { self.state.run(Scope::Continuation(record.id.clone()), |v| ready(JobContinuationStore::insert(&v.jobs.native_continuation_store(), record))) }
async fn get(&self, id: &str) -> Result<Option<JobContinuation>> { self.state.run(Scope::Continuation(id.to_string()), |v| ready(JobContinuationStore::get(&v.jobs.native_continuation_store(), id))) }
async fn save(&self, record: JobContinuation) -> Result<()> { self.state.run(Scope::Continuation(record.id.clone()), |v| ready(JobContinuationStore::save(&v.jobs.native_continuation_store(), record))) }
async fn list_pending_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobContinuation>> { self.state.run(Scope::ContinuationsParent(parent_job_id.to_string()), |v| ready(JobContinuationStore::list_pending_by_parent(&v.jobs.native_continuation_store(), parent_job_id))) }
async fn try_claim(&self, id: &str, claim_token: &str) -> Result<bool> { self.state.run(Scope::Continuation(id.to_string()), |v| ready(JobContinuationStore::try_claim(&v.jobs.native_continuation_store(), id, claim_token))) }
async fn get_by_child(&self, child_job_id: &str) -> Result<Option<JobContinuation>> { self.state.run(Scope::ContinuationChild(child_job_id.to_string()), |v| ready(JobContinuationStore::get_by_child(&v.jobs.native_continuation_store(), child_job_id))) }
} }
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteJobContinuationStore {
    state: AtomicState,
}
#[cfg(feature = "sqlite")]
impl SqliteJobContinuationStore {
    pub fn new(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
    pub fn session(&self) -> super::session::SqliteSession {
        self.state.session().clone()
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
#[cfg(feature = "sqlite")]
crate::async_store! { impl JobContinuationStore for SqliteJobContinuationStore {
async fn insert(&self, record: JobContinuation) -> Result<()> { self.state.run(Scope::Continuation(record.id.clone()), |v| ready(JobContinuationStore::insert(&v.jobs.native_continuation_store(), record))) }
async fn get(&self, id: &str) -> Result<Option<JobContinuation>> { self.state.run(Scope::Continuation(id.to_string()), |v| ready(JobContinuationStore::get(&v.jobs.native_continuation_store(), id))) }
async fn save(&self, record: JobContinuation) -> Result<()> { self.state.run(Scope::Continuation(record.id.clone()), |v| ready(JobContinuationStore::save(&v.jobs.native_continuation_store(), record))) }
async fn list_pending_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobContinuation>> { self.state.run(Scope::ContinuationsParent(parent_job_id.to_string()), |v| ready(JobContinuationStore::list_pending_by_parent(&v.jobs.native_continuation_store(), parent_job_id))) }
async fn try_claim(&self, id: &str, claim_token: &str) -> Result<bool> { self.state.run(Scope::Continuation(id.to_string()), |v| ready(JobContinuationStore::try_claim(&v.jobs.native_continuation_store(), id, claim_token))) }
async fn get_by_child(&self, child_job_id: &str) -> Result<Option<JobContinuation>> { self.state.run(Scope::ContinuationChild(child_job_id.to_string()), |v| ready(JobContinuationStore::get_by_child(&v.jobs.native_continuation_store(), child_job_id))) }
} }
#[derive(Clone)]
pub struct ExecutorRecurringStore {
    state: AtomicState,
}
impl ExecutorRecurringStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
crate::async_store! { impl RecurringStore for ExecutorRecurringStore {
async fn insert(&self, definition: RecurringDefinition) -> Result<()> { self.state.run(Scope::Recurring(definition.id.clone()), |v| ready(RecurringStore::insert(&v.recurring, definition))) }
async fn save(&self, definition: RecurringDefinition) -> Result<()> { self.state.run(Scope::Recurring(definition.id.clone()), |v| ready(RecurringStore::save(&v.recurring, definition))) }
async fn lease_due(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
        lease_seconds: i64,
    ) -> Result<Vec<RecurringDefinition>> { self.state.run(Scope::RecurringDue { now_ns: encode_time(now) }, |v| ready(RecurringStore::lease_due(&v.recurring, now, scheduler_id, lease_seconds))) }
async fn list(&self) -> Result<Vec<RecurringDefinition>> { self.state.run(Scope::RecurringAll, |v| ready(RecurringStore::list(&v.recurring))) }
} }
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteRecurringStore {
    state: AtomicState,
}
#[cfg(feature = "sqlite")]
impl SqliteRecurringStore {
    pub fn new(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.state = self.state.with_clock(clock);
        self
    }
}
#[cfg(feature = "sqlite")]
crate::async_store! { impl RecurringStore for SqliteRecurringStore {
async fn insert(&self, definition: RecurringDefinition) -> Result<()> { self.state.run(Scope::Recurring(definition.id.clone()), |v| ready(RecurringStore::insert(&v.recurring, definition))) }
async fn save(&self, definition: RecurringDefinition) -> Result<()> { self.state.run(Scope::Recurring(definition.id.clone()), |v| ready(RecurringStore::save(&v.recurring, definition))) }
async fn lease_due(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
        lease_seconds: i64,
    ) -> Result<Vec<RecurringDefinition>> { self.state.run(Scope::RecurringDue { now_ns: encode_time(now) }, |v| ready(RecurringStore::lease_due(&v.recurring, now, scheduler_id, lease_seconds))) }
async fn list(&self) -> Result<Vec<RecurringDefinition>> { self.state.run(Scope::RecurringAll, |v| ready(RecurringStore::list(&v.recurring))) }
} }
