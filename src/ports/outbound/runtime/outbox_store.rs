use async_trait::async_trait;

use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::outbox::OutboxEvent;

#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn insert(&self, event: OutboxEvent) -> Result<()>;
    async fn save(&self, event: OutboxEvent) -> Result<()>;
    async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>>;
    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>>;
    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<OutboxEvent>>;
    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<OutboxEvent>>;
    async fn prune_non_pending_before(&self, cutoff: DateTime<Utc>) -> Result<usize>;
}
