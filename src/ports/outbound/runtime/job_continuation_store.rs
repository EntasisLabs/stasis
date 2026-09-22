use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::job_continuation::JobContinuation;

#[async_trait]
pub trait JobContinuationStore: Send + Sync {
    async fn insert(&self, record: JobContinuation) -> Result<()>;
    async fn get(&self, id: &str) -> Result<Option<JobContinuation>>;
    async fn save(&self, record: JobContinuation) -> Result<()>;
    async fn list_pending_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobContinuation>>;
    /// Atomically move `pending` → `settling` when `claim_token` wins.
    async fn try_claim(&self, id: &str, claim_token: &str) -> Result<bool>;
    async fn get_by_child(&self, child_job_id: &str) -> Result<Option<JobContinuation>>;
}
