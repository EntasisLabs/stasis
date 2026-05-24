use chrono::Utc;

use crate::application::runtime::runtime_factory::RuntimeBackend;
use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
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

/// Preferred public runtime naming.
pub type StasisRuntime = RuntimeSdk;

impl RuntimeSdk {
    /// Creates a new facade over a pre-built runtime composition.
    pub fn new(runtime: RuntimeComposition) -> Self {
        Self { runtime }
    }

    /// Builds an in-memory runtime facade with default wiring.
    pub async fn in_memory() -> Result<Self> {
        Self::from_builder(StasisRuntimeBuilder::new(RuntimeBackend::InMemory)).await
    }

    /// Builds a surreal-mem runtime facade with default wiring.
    pub async fn surreal_mem(
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self> {
        Self::from_builder(StasisRuntimeBuilder::new(RuntimeBackend::SurrealMem {
            namespace: namespace.into(),
            database: database.into(),
        }))
        .await
    }

    /// Builds a remote websocket surreal runtime facade with default wiring.
    pub async fn surreal_ws(
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self> {
        Self::from_builder(StasisRuntimeBuilder::new(RuntimeBackend::SurrealWs {
            endpoint: endpoint.into(),
            namespace: namespace.into(),
            database: database.into(),
        }))
        .await
    }

    /// Builds an embedded surreal-kv runtime facade with default wiring.
    pub async fn surreal_kv(
        path: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self> {
        Self::from_builder(StasisRuntimeBuilder::new(RuntimeBackend::SurrealKv {
            path: path.into(),
            namespace: namespace.into(),
            database: database.into(),
        }))
        .await
    }

    /// Builds a runtime facade from a fully configured runtime builder.
    pub async fn from_builder(builder: StasisRuntimeBuilder) -> Result<Self> {
        let runtime = builder.build().await?;
        Ok(Self::new(runtime))
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::application::runtime::runtime_factory::RuntimeComposition;

    use super::RuntimeSdk;

    #[tokio::test]
    async fn runtime_sdk_in_memory_constructor_builds() {
        let runtime = RuntimeSdk::in_memory()
            .await
            .expect("in-memory runtime should build");
        let stats = runtime
            .stats_snapshot(10)
            .await
            .expect("stats snapshot should succeed");
        assert_eq!(stats.enqueued_jobs, 0);
    }

    #[tokio::test]
    async fn runtime_sdk_surreal_mem_constructor_builds() {
        let runtime = RuntimeSdk::surreal_mem("stasis", "runtime")
            .await
            .expect("surreal-mem runtime should build");
        assert!(matches!(runtime.runtime(), RuntimeComposition::Surreal(_)));
    }

    #[tokio::test]
    async fn runtime_sdk_surreal_ws_constructor_rejects_invalid_endpoint() {
        let result = RuntimeSdk::surreal_ws("not-a-valid-endpoint", "stasis", "runtime").await;
        assert!(result.is_err(), "invalid websocket endpoint should fail");
        let err = result.err().expect("result should contain an error");
        assert!(err.to_string().contains("connect surreal db"));
    }

    #[tokio::test]
    async fn runtime_sdk_surreal_kv_constructor_builds() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        let path = env::temp_dir().join(format!("stasis-surrealkv-{nanos}"));
        let path_str = path.to_string_lossy().into_owned();

        let runtime = RuntimeSdk::surreal_kv(path_str, "stasis", "runtime")
            .await
            .expect("surreal-kv runtime should build");
        assert!(matches!(runtime.runtime(), RuntimeComposition::Surreal(_)));

        drop(runtime);
        let _ = fs::remove_dir_all(path);
    }
}