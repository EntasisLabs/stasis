use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::domain::runtime::outbox::OutboxEvent;

#[async_trait]
pub trait EndpointTransportPublisher: Send + Sync {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool;
    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> Result<()>;
}
