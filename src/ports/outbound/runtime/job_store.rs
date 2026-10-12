use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::job::{Job, JobState};
use crate::domain::runtime::placement::WorkerCapabilities;

use crate::ports::outbound::runtime::store_bounds::StoreBound;

crate::async_store! {
pub trait JobStore: StoreBound {
    async fn insert(&self, job: Job) -> Result<()>;
    /// Retained receipts are independent of current job existence and incarnation.
    async fn pending_lifecycle(&self) -> Result<Vec<(String, crate::domain::runtime::execution::ExecutionReceipt)>> {
        Err(crate::domain::errors::StasisError::PortFailure("native lifecycle receipts unsupported".into()))
    }
    async fn mark_lifecycle_delivered(&self, key: &str, digest: &str) -> Result<()> {
        let _ = (key, digest);
        Err(crate::domain::errors::StasisError::PortFailure("native lifecycle acknowledgement unsupported".into()))
    }
    /// Native atomic publication. Third-party stores must implement this; never fall back to save.
    async fn commit_execution(
        &self,
        snapshot: &Job,
        outcome: crate::domain::runtime::execution::JobExecutionOutcome,
    ) -> Result<crate::domain::runtime::execution::CommitResult> {
        let _ = (snapshot, outcome);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native atomic execution commit unsupported".into(),
        ))
    }
    /// Native recovery has a FIXED failure outcome; it cannot accept a worker's success.
    async fn recover_execution(
        &self,
        snapshot: &Job,
    ) -> Result<crate::domain::runtime::execution::CommitResult> {
        let _ = snapshot;
        Err(crate::domain::errors::StasisError::PortFailure(
            "native recovery unsupported".into(),
        ))
    }
    async fn heartbeat_granted(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        now: DateTime<Utc>,
        lease_seconds: i64,
        progress: Option<String>,
    ) -> Result<()> {
        let _ = (grant, now, lease_seconds, progress);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native granted update unsupported".into(),
        ))
    }
    async fn cancel_execution(&self, job_id: &str) -> Result<Option<Job>> {
        let _ = job_id;
        Err(crate::domain::errors::StasisError::PortFailure(
            "native atomic cancellation unsupported".into(),
        ))
    }
    async fn acknowledge_cleanup(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        evidence_json: String,
    ) -> Result<crate::domain::runtime::execution::CommitResult> {
        let _ = (grant, evidence_json);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native cleanup authority unsupported".into(),
        ))
    }
    async fn publish_granted(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        event: crate::domain::runtime::outbox::OutboxEvent,
    ) -> Result<()> {
        let _ = (grant, event);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native granted publication unsupported".into(),
        ))
    }
    async fn stage_child(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        child: Job,
    ) -> Result<()> {
        let _ = (grant, child);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native staged child unsupported".into(),
        ))
    }
    async fn stage_continuation(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        continuation: crate::domain::runtime::job_continuation::JobContinuation,
    ) -> Result<()> {
        let _ = (grant, continuation);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native staged continuation unsupported".into(),
        ))
    }
    async fn settle_continuation(
        &self,
        parent: &Job,
        record: crate::domain::runtime::job_continuation::JobContinuation,
        child: Option<Job>,
        detail: Option<String>,
    ) -> Result<bool> {
        let _ = (parent, record, child, detail);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native atomic continuation unsupported".into(),
        ))
    }
    async fn poll_wait(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        wait: crate::domain::runtime::durable_wait::DurableWaitRecord,
    ) -> Result<crate::domain::runtime::durable_wait::DurableWaitRecord> {
        let _ = (grant, wait);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native granted wait unsupported".into(),
        ))
    }
    async fn fail_execution(&self, job_id: &str) -> Result<Option<Job>> {
        let _ = job_id;
        Err(crate::domain::errors::StasisError::PortFailure(
            "native atomic failure unsupported".into(),
        ))
    }
    /// Read the retained native commit receipt, including after job deletion.
    async fn execution_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<crate::domain::runtime::execution::ExecutionReceipt>> {
        let _ = (job_id, epoch);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native receipt inspection unsupported".into(),
        ))
    }
    async fn cleanup_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<crate::domain::runtime::execution::CleanupRecord>> {
        let _ = (job_id, epoch);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native cleanup inspection unsupported".into(),
        ))
    }
    async fn observe_cleanup(
        &self,
        grant: &crate::domain::runtime::execution::ExecutionGrant,
        observation: String,
    ) -> Result<crate::domain::runtime::execution::CommitResult> {
        let _ = (grant, observation);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native cleanup observations unsupported".into(),
        ))
    }
    /// Wake only the stable incarnation captured by the wait dispatcher.
    async fn wake_incarnation(&self, job_id: &str, incarnation: u64) -> Result<()> {
        let _ = (job_id, incarnation);
        Err(crate::domain::errors::StasisError::PortFailure(
            "native incarnation wakeup unsupported".into(),
        ))
    }
    /// Atomic signal wakeup: active grant/version must remain unchanged.
    async fn wake_execution(&self, job_id: &str) -> Result<()> {
        let _ = job_id;
        Err(crate::domain::errors::StasisError::PortFailure(
            "native atomic wakeup unsupported".into(),
        ))
    }
    async fn save(&self, job: Job) -> Result<()>;
    async fn get(&self, id: &str) -> Result<Option<Job>>;
    async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
        worker: &WorkerCapabilities,
    ) -> Result<Option<Job>>;
    async fn heartbeat(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<()>;
    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>>;
    async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>>;
    async fn delete(&self, id: &str) -> Result<bool>;
    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize>;
}

}
