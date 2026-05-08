use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::outbox::OutboxEvent;

#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn insert(&self, event: OutboxEvent) -> Result<()>;
    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>>;
    async fn mark_published(&self, event_id: &str, published_at: DateTime<Utc>) -> Result<()>;
}
