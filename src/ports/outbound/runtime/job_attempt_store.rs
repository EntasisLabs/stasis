use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::job_attempt::JobAttempt;

#[async_trait]
pub trait JobAttemptStore: Send + Sync {
    async fn insert(&self, attempt: JobAttempt) -> Result<()>;
    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>>;
}
