use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::outbox::OutboxEvent;

#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn insert(&self, event: OutboxEvent) -> Result<()>;
    async fn save(&self, event: OutboxEvent) -> Result<()>;
    async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>>;
    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>>;
}
