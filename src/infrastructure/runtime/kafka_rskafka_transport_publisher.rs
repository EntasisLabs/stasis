use async_trait::async_trait;
use rskafka::client::{
    partition::{Compression, UnknownTopicHandling},
    ClientBuilder,
};
use rskafka::record::Record;
use serde::Deserialize;
use serde_json::json;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::domain::runtime::outbox::OutboxEvent;
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;

#[derive(Clone, Debug, Deserialize, Default, Eq, PartialEq)]
struct KafkaEndpointMetadata {
    topic: Option<String>,
    key: Option<String>,
    partition: Option<i32>,
}

#[derive(Clone, Debug)]
struct KafkaTarget {
    bootstrap_brokers: Vec<String>,
    topic: Option<String>,
}

#[derive(Clone, Default)]
pub struct RskafkaTransportPublisher;

impl RskafkaTransportPublisher {
    pub fn new() -> Self {
        Self
    }

    fn parse_metadata(metadata: Option<&str>) -> Result<KafkaEndpointMetadata> {
        let Some(raw) = metadata else {
            return Ok(KafkaEndpointMetadata::default());
        };

        serde_json::from_str::<KafkaEndpointMetadata>(raw).map_err(|e| {
            StasisError::PortFailure(format!(
                "invalid Kafka endpoint metadata JSON (topic/key): {e}"
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
            StasisError::PortFailure(format!("serialize outbox event for Kafka publish: {e}"))
        })
    }

    fn parse_target(target: &str) -> Result<KafkaTarget> {
        let raw = target.trim();
        if raw.is_empty() {
            return Err(StasisError::PortFailure(
                "Kafka endpoint target must not be empty".to_string(),
            ));
        }

        let stripped = raw.strip_prefix("kafka://").unwrap_or(raw);
        let (brokers_raw, topic_raw) = if let Some((left, right)) = stripped.split_once('/') {
            (left, Some(right))
        } else {
            (stripped, None)
        };

        let bootstrap_brokers = brokers_raw
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        if bootstrap_brokers.is_empty() {
            return Err(StasisError::PortFailure(format!(
                "Kafka endpoint target has no bootstrap brokers: {target}"
            )));
        }

        let topic = topic_raw
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string);

        Ok(KafkaTarget {
            bootstrap_brokers,
            topic,
        })
    }
}

#[async_trait]
impl EndpointTransportPublisher for RskafkaTransportPublisher {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::Kafka)
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> Result<()> {
        let metadata = Self::parse_metadata(endpoint.metadata.as_deref())?;
        let parsed_target = Self::parse_target(&endpoint.target)?;
        let payload = Self::build_payload(event)?;

        let topic = metadata
            .topic
            .or(parsed_target.topic)
            .unwrap_or_else(|| "stasis.runtime.events".to_string());
        let partition = metadata.partition.unwrap_or(0);

        let key = metadata
            .key
            .map(|value| value.into_bytes())
            .unwrap_or_else(|| event.event.correlation_id.clone().into_bytes());

        let client = ClientBuilder::new(parsed_target.bootstrap_brokers)
            .build()
            .await
            .map_err(|e| {
                StasisError::PortFailure(format!(
                    "create Kafka client for endpoint_id={}: {e}",
                    endpoint.endpoint_id
                ))
            })?;

        let partition_client = client
            .partition_client(topic, partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| {
                StasisError::PortFailure(format!(
                    "create Kafka partition client for endpoint_id={}: {e}",
                    endpoint.endpoint_id
                ))
            })?;

        partition_client
            .produce(
                vec![Record {
                    key: Some(key),
                    value: Some(payload),
                    headers: Default::default(),
                    timestamp: event.event.occurred_at,
                }],
                Compression::NoCompression,
            )
            .await
            .map_err(|e| {
                StasisError::PortFailure(format!(
                    "publish Kafka event for endpoint_id={}: {e}",
                    endpoint.endpoint_id
                ))
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{KafkaEndpointMetadata, RskafkaTransportPublisher};

    #[test]
    fn parses_kafka_target_with_topic_suffix() {
        let parsed = RskafkaTransportPublisher::parse_target("kafka://b1:9092,b2:9092/events.topic")
            .expect("target should parse");
        assert_eq!(parsed.bootstrap_brokers, vec!["b1:9092", "b2:9092"]);
        assert_eq!(parsed.topic.as_deref(), Some("events.topic"));
    }

    #[test]
    fn parses_kafka_target_without_scheme_or_topic() {
        let parsed = RskafkaTransportPublisher::parse_target("b1:9092,b2:9092")
            .expect("target should parse");
        assert_eq!(parsed.bootstrap_brokers, vec!["b1:9092", "b2:9092"]);
        assert_eq!(parsed.topic, None);
    }

    #[test]
    fn rejects_kafka_target_without_brokers() {
        let result = RskafkaTransportPublisher::parse_target("kafka:///topic");
        assert!(result.is_err());
    }

    #[test]
    fn parses_kafka_metadata_fields() {
        let metadata = RskafkaTransportPublisher::parse_metadata(Some(
            r#"{"topic":"custom.topic","key":"tenant-1","partition":3}"#,
        ))
        .expect("metadata should parse");

        assert_eq!(metadata.topic.as_deref(), Some("custom.topic"));
        assert_eq!(metadata.key.as_deref(), Some("tenant-1"));
        assert_eq!(metadata.partition, Some(3));
    }

    #[test]
    fn invalid_kafka_metadata_fails() {
        let result = RskafkaTransportPublisher::parse_metadata(Some("not-json"));
        assert!(result.is_err());
    }

    #[test]
    fn empty_kafka_metadata_uses_defaults() {
        let metadata = RskafkaTransportPublisher::parse_metadata(None)
            .expect("metadata should default");
        assert_eq!(metadata, KafkaEndpointMetadata::default());
    }
}
