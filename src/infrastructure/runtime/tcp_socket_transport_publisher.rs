use async_trait::async_trait;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::domain::runtime::outbox::{OutboxEvent, RuntimeEventType};
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;

#[derive(Clone, Default)]
pub struct TcpSocketTransportPublisher;

#[derive(Debug, Serialize)]
struct TcpRuntimeEvent {
    event_id: String,
    event_type: &'static str,
    job_id: String,
    thread_id: Option<String>,
    correlation_id: String,
    trace_id: String,
    occurred_at: String,
    message: Option<String>,
}

impl From<&OutboxEvent> for TcpRuntimeEvent {
    fn from(value: &OutboxEvent) -> Self {
        let event_type = match value.event.event_type {
            RuntimeEventType::JobSucceeded => "job_succeeded",
            RuntimeEventType::JobRetryScheduled => "job_retry_scheduled",
            RuntimeEventType::JobDeadLettered => "job_dead_lettered",
        };

        Self {
            event_id: value.event_id.clone(),
            event_type,
            job_id: value.event.job_id.clone(),
            thread_id: value.event.thread_id.clone(),
            correlation_id: value.event.correlation_id.clone(),
            trace_id: value.event.trace_id.clone(),
            occurred_at: value.event.occurred_at.to_rfc3339(),
            message: value.event.message.clone(),
        }
    }
}

fn parse_tcp_target(target: &str) -> Result<String> {
    if let Some(rest) = target.strip_prefix("tcp://") {
        if rest.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "tcp target must include host:port".to_string(),
            ));
        }
        return Ok(rest.to_string());
    }

    if target.trim().is_empty() {
        return Err(StasisError::PortFailure(
            "tcp target must not be empty".to_string(),
        ));
    }

    Ok(target.to_string())
}

#[async_trait]
impl EndpointTransportPublisher for TcpSocketTransportPublisher {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::Tcp)
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> Result<()> {
        let addr = parse_tcp_target(&endpoint.target)?;
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
            StasisError::PortFailure(format!(
                "tcp connect failed endpoint_id={} error={e}",
                endpoint.endpoint_id
            ))
        })?;

        let payload = serde_json::to_vec(&TcpRuntimeEvent::from(event))
            .map_err(|e| StasisError::PortFailure(format!("tcp payload encode failed: {e}")))?;

        stream.write_all(&payload).await.map_err(|e| {
            StasisError::PortFailure(format!(
                "tcp publish write failed endpoint_id={} error={e}",
                endpoint.endpoint_id
            ))
        })?;
        stream.write_all(b"\n").await.map_err(|e| {
            StasisError::PortFailure(format!(
                "tcp publish newline failed endpoint_id={} error={e}",
                endpoint.endpoint_id
            ))
        })?;
        stream.shutdown().await.map_err(|e| {
            StasisError::PortFailure(format!(
                "tcp shutdown failed endpoint_id={} error={e}",
                endpoint.endpoint_id
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
    use crate::domain::runtime::outbox::{
        OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType,
    };
    use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;

    use super::TcpSocketTransportPublisher;

    fn sample_event() -> OutboxEvent {
        OutboxEvent {
            event_id: "evt-1".to_string(),
            status: OutboxStatus::Pending,
            publish_attempts: 0,
            published_at: None,
            next_attempt_at: None,
            last_publish_error: None,
            event: RuntimeEvent {
                event_type: RuntimeEventType::JobSucceeded,
                job_id: "job-1".to_string(),
                thread_id: None,
                correlation_id: "corr-1".to_string(),
                causation_id: "cause-1".to_string(),
                trace_id: "trace-1".to_string(),
                sttp_input_node_id: "sttp:in:1".to_string(),
                sttp_output_node_id: None,
                execution_id: None,
                input_memory_query_id: None,
                input_memory_query_fingerprint: None,
                output_memory_node_id: None,
                retrieval_path: None,
                occurred_at: Utc::now(),
                message: Some("ok".to_string()),
            },
        }
    }

    #[tokio::test]
    async fn publishes_json_line_to_tcp_target() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should succeed");
        let addr = listener.local_addr().expect("addr should be available");

        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept should succeed");
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.expect("read should succeed");
            String::from_utf8_lossy(&buf[..n]).to_string()
        });

        let endpoint = DeliveryEndpoint {
            endpoint_id: "endpoint.tcp.1".to_string(),
            name: "TCP".to_string(),
            protocol: DeliveryProtocol::Tcp,
            target: format!("tcp://{}", addr),
            metadata: None,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        TcpSocketTransportPublisher
            .publish_to_endpoint(&endpoint, &sample_event())
            .await
            .expect("publish should succeed");

        let payload = handle.await.expect("join should succeed");
        assert!(payload.contains("\"event_id\":\"evt-1\""));
        assert!(payload.contains("\"event_type\":\"job_succeeded\""));
    }
}
