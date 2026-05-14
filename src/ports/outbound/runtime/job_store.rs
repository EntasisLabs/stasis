use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::job::{Job, JobState};

#[async_trait]
pub trait JobStore: Send + Sync {
    async fn insert(&self, job: Job) -> Result<()>;
    async fn save(&self, job: Job) -> Result<()>;
    async fn get(&self, id: &str) -> Result<Option<Job>>;
    async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<Option<Job>>;
    async fn heartbeat(&self, job_id: &str, worker_id: &str, now: DateTime<Utc>) -> Result<()>;
    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>>;
    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize>;
}
