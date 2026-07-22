use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::agent::envelope::EncodedAgentMessage;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::ports::outbound::agent::transport::AgentTransport;

/// In-process agent transport that records published messages (tests + local demos).
#[derive(Clone, Default)]
pub struct InMemoryAgentTransport {
    published: Arc<Mutex<Vec<(String, EncodedAgentMessage)>>>,
}

impl InMemoryAgentTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn published(&self) -> Result<Vec<(String, EncodedAgentMessage)>> {
        self.published
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| StasisError::PortFailure("agent transport lock poisoned".into()))
    }
}

#[async_trait]
impl AgentTransport for InMemoryAgentTransport {
    fn supports(&self, _protocol: &DeliveryProtocol) -> bool {
        true
    }

    async fn publish(
        &self,
        endpoint: &DeliveryEndpoint,
        message: &EncodedAgentMessage,
    ) -> Result<()> {
        let mut published = self
            .published
            .lock()
            .map_err(|_| StasisError::PortFailure("agent transport lock poisoned".into()))?;
        published.push((endpoint.endpoint_id.clone(), message.clone()));
        Ok(())
    }
}
