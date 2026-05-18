use async_trait::async_trait;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use serde::Deserialize;
use serde_json::json;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::domain::runtime::outbox::OutboxEvent;
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;

#[derive(Clone, Debug, Deserialize, Default, Eq, PartialEq)]
struct RabbitMqEndpointMetadata {
    exchange: Option<String>,
    routing_key: Option<String>,
    persistent: Option<bool>,
}

#[derive(Clone, Default)]
pub struct LapinRabbitMqTransportPublisher;

impl LapinRabbitMqTransportPublisher {
    pub fn new() -> Self {
        Self
    }

    fn parse_metadata(metadata: Option<&str>) -> Result<RabbitMqEndpointMetadata> {
        let Some(raw) = metadata else {
            return Ok(RabbitMqEndpointMetadata::default());
        };

        serde_json::from_str::<RabbitMqEndpointMetadata>(raw).map_err(|e| {
            StasisError::PortFailure(format!(
                "invalid RabbitMQ endpoint metadata JSON (exchange/routing_key/persistent): {e}"
            ))
        })
    }

    fn build_payload(event: &OutboxEvent) -> Result<Vec<u8>> {
        let event_type = match event.event.event_type {
            crate::domain::runtime::outbox::RuntimeEventType::JobSucceeded => "job_succeeded",
            crate::domain::runtime::outbox::RuntimeEventType::JobRetryScheduled => {
                "job_retry_scheduled"
            }
            crate::domain::runtime::outbox::RuntimeEventType::JobDeadLettered => {
                "job_dead_lettered"
            }
        };

        let payload = json!({
            "event_id": event.event_id,
            "event_type": event_type,
            "job_id": event.event.job_id,
            "thread_id": event.event.thread_id,
            "correlation_id": event.event.correlation_id,
            "causation_id": event.event.causation_id,
            "trace_id": event.event.trace_id,
            "sttp_input_node_id": event.event.sttp_input_node_id,
            "sttp_output_node_id": event.event.sttp_output_node_id,
            "execution_id": event.event.execution_id,
            "occurred_at": event.event.occurred_at.to_rfc3339(),
            "message": event.event.message,
        });

        serde_json::to_vec(&payload).map_err(|e| {
            StasisError::PortFailure(format!("serialize outbox event for RabbitMQ publish: {e}"))
        })
    }
}

#[async_trait]
impl EndpointTransportPublisher for LapinRabbitMqTransportPublisher {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::RabbitMq)
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> Result<()> {
        let metadata = Self::parse_metadata(endpoint.metadata.as_deref())?;
        let exchange = metadata.exchange.unwrap_or_default();
        let routing_key = metadata
            .routing_key
            .unwrap_or_else(|| "stasis.runtime.events".to_string());

        let connection = Connection::connect(&endpoint.target, ConnectionProperties::default())
            .await
            .map_err(|e| {
                StasisError::PortFailure(format!(
                    "connect RabbitMQ for endpoint_id={}: {e}",
                    endpoint.endpoint_id
                ))
            })?;

        let channel = connection.create_channel().await.map_err(|e| {
            StasisError::PortFailure(format!(
                "create RabbitMQ channel for endpoint_id={}: {e}",
                endpoint.endpoint_id
            ))
        })?;

        let payload = Self::build_payload(event)?;
        let mut properties = BasicProperties::default().with_content_type("application/json".into());
        if metadata.persistent.unwrap_or(true) {
            properties = properties.with_delivery_mode(2);
        }

        let confirm = channel
            .basic_publish(
                &exchange,
                &routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .map_err(|e| {
                StasisError::PortFailure(format!(
                    "publish RabbitMQ event for endpoint_id={}: {e}",
                    endpoint.endpoint_id
                ))
            })?;

        confirm.await.map_err(|e| {
            StasisError::PortFailure(format!(
                "confirm RabbitMQ publish for endpoint_id={}: {e}",
                endpoint.endpoint_id
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{LapinRabbitMqTransportPublisher, RabbitMqEndpointMetadata};

    #[test]
    fn parses_rabbitmq_metadata_fields() {
        let metadata = LapinRabbitMqTransportPublisher::parse_metadata(Some(
            r#"{"exchange":"stasis.exchange","routing_key":"events.success","persistent":true}"#,
        ))
        .expect("metadata should parse");

        assert_eq!(metadata.exchange.as_deref(), Some("stasis.exchange"));
        assert_eq!(metadata.routing_key.as_deref(), Some("events.success"));
        assert_eq!(metadata.persistent, Some(true));
    }

    #[test]
    fn invalid_rabbitmq_metadata_fails() {
        let result = LapinRabbitMqTransportPublisher::parse_metadata(Some("{bad-json"));
        assert!(result.is_err());
    }

    #[test]
    fn empty_rabbitmq_metadata_uses_defaults() {
        let metadata = LapinRabbitMqTransportPublisher::parse_metadata(None)
            .expect("metadata should default");
        assert_eq!(metadata, RabbitMqEndpointMetadata::default());
    }
}
