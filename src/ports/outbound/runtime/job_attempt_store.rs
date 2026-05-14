use async_trait::async_trait;

use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::job_attempt::JobAttempt;

#[async_trait]
pub trait JobAttemptStore: Send + Sync {
    async fn insert(&self, attempt: JobAttempt) -> Result<()>;
    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>>;
    async fn list_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>>;
    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>>;
    async fn prune_finished_before(&self, cutoff: DateTime<Utc>) -> Result<usize>;
}
