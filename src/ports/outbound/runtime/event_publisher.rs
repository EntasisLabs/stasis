use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::outbox::OutboxEvent;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: &OutboxEvent) -> Result<()>;
}
