use async_trait::async_trait;

use crate::domain::agent::envelope::EncodedAgentMessage;
use crate::domain::errors::Result;
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};

/// Vendor-neutral transport for encoded agent messages (ADR-0007).
#[async_trait]
pub trait AgentTransport: Send + Sync {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool;
    async fn publish(
        &self,
        endpoint: &DeliveryEndpoint,
        message: &EncodedAgentMessage,
    ) -> Result<()>;
}
