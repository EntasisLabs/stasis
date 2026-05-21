use async_trait::async_trait;
use serde::Serialize;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, DeliveryProtocol};
use crate::domain::runtime::outbox::{OutboxEvent, RuntimeEventType};
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
use crate::ports::outbound::runtime::event_publisher::EventPublisher;

#[derive(Clone)]
pub struct HttpWebhookEventPublisher {
    client: reqwest::Client,
    endpoint_url: String,
    authorization_bearer: Option<String>,
}

impl HttpWebhookEventPublisher {
    pub fn new(endpoint_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint_url: endpoint_url.into(),
            authorization_bearer: None,
        }
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.authorization_bearer = Some(token.into());
        self
    }

    async fn publish_to_url(&self, endpoint_url: &str, event: &OutboxEvent) -> Result<()> {
        let payload = WebhookRuntimeEvent::from(event);
        let mut request = self.client.post(endpoint_url).json(&payload);

        if let Some(token) = &self.authorization_bearer {
            request = request.bearer_auth(token);
        }

        let response = request.send().await.map_err(|e| {
            StasisError::PortFailure(format!("publish webhook request failed: {e}"))
        })?;

        if !response.status().is_success() {
            return Err(StasisError::PortFailure(format!(
                "publish webhook returned non-success status: {}",
                response.status()
            )));
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct HttpWebhookTransportPublisher {
    client: reqwest::Client,
    authorization_bearer: Option<String>,
}

impl HttpWebhookTransportPublisher {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            authorization_bearer: None,
        }
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.authorization_bearer = Some(token.into());
        self
    }
}

impl Default for HttpWebhookTransportPublisher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EndpointTransportPublisher for HttpWebhookTransportPublisher {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::HttpWebhook)
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> Result<()> {
        let publisher = HttpWebhookEventPublisher {
            client: self.client.clone(),
            endpoint_url: endpoint.target.clone(),
            authorization_bearer: self.authorization_bearer.clone(),
        };

        publisher.publish_to_url(&endpoint.target, event).await
    }
}

#[derive(Debug, Serialize)]
struct WebhookRuntimeEvent {
    event_id: String,
    event_type: &'static str,
    job_id: String,
    thread_id: Option<String>,
    correlation_id: String,
    causation_id: String,
    trace_id: String,
    sttp_input_node_id: String,
    sttp_output_node_id: Option<String>,
    execution_id: Option<String>,
    occurred_at: String,
    message: Option<String>,
}

impl From<&OutboxEvent> for WebhookRuntimeEvent {
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
            causation_id: value.event.causation_id.clone(),
            trace_id: value.event.trace_id.clone(),
            sttp_input_node_id: value.event.sttp_input_node_id.clone(),
            sttp_output_node_id: value.event.sttp_output_node_id.clone(),
            execution_id: value.event.execution_id.clone(),
            occurred_at: value.event.occurred_at.to_rfc3339(),
            message: value.event.message.clone(),
        }
    }
}

#[async_trait]
impl EventPublisher for HttpWebhookEventPublisher {
    async fn publish(&self, event: &OutboxEvent) -> Result<()> {
        self.publish_to_url(&self.endpoint_url, event).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::domain::runtime::outbox::{
        OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType,
    };

    use super::WebhookRuntimeEvent;

    #[test]
    fn maps_outbox_event_to_webhook_payload() {
        let event = OutboxEvent {
            event_id: "evt-1".to_string(),
            status: OutboxStatus::Pending,
            publish_attempts: 0,
            published_at: None,
            next_attempt_at: None,
            last_publish_error: None,
            event: RuntimeEvent {
                event_type: RuntimeEventType::JobSucceeded,
                job_id: "job-1".to_string(),
                thread_id: Some("thread-1".to_string()),
                correlation_id: "corr-1".to_string(),
                causation_id: "cause-1".to_string(),
                trace_id: "trace-1".to_string(),
                sttp_input_node_id: "sttp:in:1".to_string(),
                sttp_output_node_id: Some("sttp:out:1".to_string()),
                execution_id: Some("exec-1".to_string()),
                input_memory_query_id: None,
                input_memory_query_fingerprint: None,
                output_memory_node_id: None,
                retrieval_path: None,
                occurred_at: Utc::now(),
                message: Some("ok".to_string()),
            },
        };

        let payload = WebhookRuntimeEvent::from(&event);
        assert_eq!(payload.event_id, "evt-1");
        assert_eq!(payload.event_type, "job_succeeded");
        assert_eq!(payload.job_id, "job-1");
        assert_eq!(payload.execution_id.as_deref(), Some("exec-1"));
    }
}
