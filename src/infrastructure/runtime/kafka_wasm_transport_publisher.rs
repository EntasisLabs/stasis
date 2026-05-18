use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::domain::runtime::outbox::OutboxEvent;
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;

#[derive(Clone, Default)]
pub struct WasmKafkaTransportPublisher;

impl WasmKafkaTransportPublisher {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl EndpointTransportPublisher for WasmKafkaTransportPublisher {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::Kafka)
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        _event: &OutboxEvent,
    ) -> Result<()> {
        Err(StasisError::PortFailure(format!(
            "WASM Kafka transport placeholder is enabled but not yet bound to rfkafka_wasi for endpoint_id={} (target={})",
            endpoint.endpoint_id, endpoint.target
        )))
    }
}
