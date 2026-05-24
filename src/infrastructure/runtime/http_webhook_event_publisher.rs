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
        let endpoint_url = validate_webhook_target(endpoint_url)?;
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

fn validate_webhook_target(target: &str) -> Result<reqwest::Url> {
    let trimmed = target.trim();
    if trimmed.is_empty() {
        return Err(StasisError::PortFailure(
            "webhook target policy rejected: target must not be empty".to_string(),
        ));
    }

    let url = reqwest::Url::parse(trimmed).map_err(|e| {
        StasisError::PortFailure(format!(
            "webhook target policy rejected: target must be an absolute URL ({e})"
        ))
    })?;

    if !matches!(url.scheme(), "http" | "https") {
        return Err(StasisError::PortFailure(format!(
            "webhook target policy rejected: unsupported scheme '{}'",
            url.scheme()
        )));
    }

    if url.host_str().is_none() {
        return Err(StasisError::PortFailure(
            "webhook target policy rejected: target must include host".to_string(),
        ));
    }

    Ok(url)
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use chrono::Utc;

    use crate::domain::errors::StasisError;
    use crate::domain::runtime::outbox::{
        OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType,
    };
    use crate::ports::outbound::runtime::event_publisher::EventPublisher;

    use super::{HttpWebhookEventPublisher, WebhookRuntimeEvent, validate_webhook_target};

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
        }
    }

    async fn spawn_webhook_server(
        expected_auth: Option<&'static str>,
        success_status: &'static str,
    ) -> (String, tokio::task::JoinHandle<Option<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");

        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("socket should accept");

            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = socket.read(&mut buf).await.expect("socket should read");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request_text = String::from_utf8_lossy(&request);
            let auth_header = request_text.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("authorization") {
                    Some(value.trim().to_string())
                } else {
                    None
                }
            });

            let status_line = if let Some(expected) = expected_auth {
                if auth_header.as_deref() == Some(expected) {
                    "200 OK"
                } else {
                    "401 Unauthorized"
                }
            } else {
                success_status
            };

            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("response should write");

            auth_header
        });

        (format!("http://{addr}"), server_task)
    }

    #[test]
    fn maps_outbox_event_to_webhook_payload() {
        let event = sample_event();

        let payload = WebhookRuntimeEvent::from(&event);
        assert_eq!(payload.event_id, "evt-1");
        assert_eq!(payload.event_type, "job_succeeded");
        assert_eq!(payload.job_id, "job-1");
        assert_eq!(payload.execution_id.as_deref(), Some("exec-1"));
    }

    #[tokio::test]
    async fn publish_includes_bearer_header_when_configured() {
        let (endpoint_url, server_task) = spawn_webhook_server(Some("Bearer test-token"), "200 OK").await;
        let event = sample_event();
        let publisher = HttpWebhookEventPublisher::new(endpoint_url).with_bearer_token("test-token");

        let result = publisher.publish(&event).await;
        assert!(result.is_ok(), "publish should succeed with valid auth header");

        let auth_header = server_task.await.expect("server task should complete");
        assert_eq!(auth_header.as_deref(), Some("Bearer test-token"));
    }

    #[tokio::test]
    async fn publish_fails_closed_when_auth_is_required_but_missing() {
        let (endpoint_url, _server_task) =
            spawn_webhook_server(Some("Bearer required-token"), "200 OK").await;
        let event = sample_event();
        let publisher = HttpWebhookEventPublisher::new(endpoint_url);

        let result = publisher.publish(&event).await;
        let Err(StasisError::PortFailure(message)) = result else {
            panic!("expected publish failure for missing auth header");
        };
        assert!(
            message.contains("non-success status: 401 Unauthorized"),
            "unexpected error message: {message}"
        );
    }

    #[tokio::test]
    async fn publish_fails_on_non_success_status() {
        let (endpoint_url, _server_task) = spawn_webhook_server(None, "503 Service Unavailable").await;
        let event = sample_event();
        let publisher = HttpWebhookEventPublisher::new(endpoint_url);

        let result = publisher.publish(&event).await;
        let Err(StasisError::PortFailure(message)) = result else {
            panic!("expected publish failure for non-success status");
        };
        assert!(
            message.contains("non-success status: 503 Service Unavailable"),
            "unexpected error message: {message}"
        );
    }

    #[tokio::test]
    async fn publish_fails_when_endpoint_is_unreachable() {
        let event = sample_event();
        let publisher = HttpWebhookEventPublisher::new("http://127.0.0.1:1");

        let result = publisher.publish(&event).await;
        let Err(StasisError::PortFailure(message)) = result else {
            panic!("expected publish failure for unreachable endpoint");
        };
        assert!(
            message.contains("publish webhook request failed"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn target_policy_accepts_http_and_https_absolute_urls() {
        assert!(validate_webhook_target("https://example.com/hook").is_ok());
        assert!(validate_webhook_target("http://example.com/hook").is_ok());
    }

    #[test]
    fn target_policy_rejects_non_http_schemes() {
        let result = validate_webhook_target("tcp://example.com:9000");
        let Err(StasisError::PortFailure(message)) = result else {
            panic!("expected policy rejection for non-http scheme");
        };
        assert!(
            message.contains("unsupported scheme 'tcp'"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn target_policy_rejects_non_absolute_urls() {
        let result = validate_webhook_target("/relative/hook");
        let Err(StasisError::PortFailure(message)) = result else {
            panic!("expected policy rejection for non-absolute URL");
        };
        assert!(
            message.contains("target must be an absolute URL"),
            "unexpected error message: {message}"
        );
    }
}
