use chrono::Utc;

use crate::application::runtime::runtime_factory::RuntimeComposition;
use crate::domain::errors::Result;
use crate::domain::runtime::job::{JobState, NewJob};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;

/// Snapshot of high-level runtime queue, outbox, and recurring workload counts.
#[derive(Clone, Debug, Default)]
pub struct RuntimeStatsSnapshot {
    pub enqueued_jobs: usize,
    pub running_jobs: usize,
    pub succeeded_jobs: usize,
    pub failed_jobs: usize,
    pub dead_letter_jobs: usize,
    pub pending_outbox_events: usize,
    pub recurring_definitions: usize,
}

/// Backend-agnostic facade for runtime queue, outbox, and recurring operations.
#[derive(Clone)]
pub struct RuntimeSdk {
    runtime: RuntimeComposition,
}

impl RuntimeSdk {
    /// Creates a new facade over a pre-built runtime composition.
    pub fn new(runtime: RuntimeComposition) -> Self {
        Self { runtime }
    }

    /// Returns a shared reference to the underlying runtime composition.
    pub fn runtime(&self) -> &RuntimeComposition {
        &self.runtime
    }

    /// Consumes this facade and returns the owned runtime composition.
    pub fn into_runtime(self) -> RuntimeComposition {
        self.runtime
    }

    /// Enqueues a single runtime job.
    pub async fn enqueue(&self, job: NewJob) -> Result<()> {
        match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.enqueue(job).await,
            RuntimeComposition::Surreal(rt) => rt.enqueue(job).await,
        }
    }

    /// Registers a recurring job definition.
    pub async fn register_recurring(&self, definition: RecurringDefinition) -> Result<()> {
        match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.register_recurring(definition).await,
            RuntimeComposition::Surreal(rt) => rt.register_recurring(definition).await,
        }
    }

    /// Attempts to process one job from a queue using the provided worker id.
    pub async fn process_once(&self, queue: &str, worker_id: &str) -> Result<Option<String>> {
        let now = Utc::now();
        match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.process_once(queue, worker_id, now).await,
            RuntimeComposition::Surreal(rt) => rt.process_once(queue, worker_id, now).await,
        }
    }

    /// Publishes pending outbox events up to `limit`.
    pub async fn publish_pending_events(&self, limit: usize) -> Result<usize> {
        let now = Utc::now();
        match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.publish_pending_events(limit, now).await,
            RuntimeComposition::Surreal(rt) => rt.publish_pending_events(limit, now).await,
        }
    }

    /// Materializes any due recurring jobs at the current wall-clock time.
    pub async fn materialize_recurring_now(&self, scheduler_id: &str) -> Result<usize> {
        match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.materialize_recurring_now(scheduler_id).await,
            RuntimeComposition::Surreal(rt) => rt.materialize_recurring_now(scheduler_id).await,
        }
    }

    /// Aggregates common runtime counts into a single snapshot.
    pub async fn stats_snapshot(&self, pending_limit: usize) -> Result<RuntimeStatsSnapshot> {
        Ok(RuntimeStatsSnapshot {
            enqueued_jobs: self.job_count_by_state(JobState::Enqueued).await?,
            running_jobs: self.job_count_by_state(JobState::Running).await?,
            succeeded_jobs: self.job_count_by_state(JobState::Succeeded).await?,
            failed_jobs: self.job_count_by_state(JobState::Failed).await?,
            dead_letter_jobs: self.job_count_by_state(JobState::DeadLetter).await?,
            pending_outbox_events: self.pending_outbox_count(pending_limit).await?,
            recurring_definitions: self.recurring_count().await?,
        })
    }

    /// Counts jobs currently in the specified state.
    pub async fn job_count_by_state(&self, state: JobState) -> Result<usize> {
        let jobs = match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.job_store.list_by_state(state).await?,
            RuntimeComposition::Surreal(rt) => rt.job_store.list_by_state(state).await?,
        };
        Ok(jobs.len())
    }

    /// Counts pending outbox events, bounded by `limit`.
    pub async fn pending_outbox_count(&self, limit: usize) -> Result<usize> {
        let pending = match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.outbox_store.list_pending(limit).await?,
            RuntimeComposition::Surreal(rt) => rt.outbox_store.list_pending(limit).await?,
        };
        Ok(pending.len())
    }

    /// Counts registered recurring definitions.
    pub async fn recurring_count(&self) -> Result<usize> {
        let definitions = match &self.runtime {
            RuntimeComposition::InMemory(rt) => rt.recurring_store.list().await?,
            RuntimeComposition::Surreal(rt) => rt.recurring_store.list().await?,
        };
        Ok(definitions.len())
    }
}