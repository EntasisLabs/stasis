use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::DeliveryEndpoint;
use crate::domain::runtime::outbox::OutboxEvent;
use crate::infrastructure::runtime::endpoint_routing_policy::AllowAllEndpointRoutingPolicy;
use crate::infrastructure::runtime::http_webhook_event_publisher::HttpWebhookTransportPublisher;
use crate::infrastructure::runtime::tcp_socket_transport_publisher::TcpSocketTransportPublisher;
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
use crate::ports::outbound::runtime::endpoint_routing_policy::EndpointRoutingPolicy;
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
use crate::ports::outbound::runtime::event_publisher::EventPublisher;

#[derive(Clone)]
pub struct EndpointRoutingEventPublisher {
    endpoint_store: Arc<dyn DeliveryEndpointStore>,
    status_store: Option<Arc<dyn EndpointDeliveryStatusStore>>,
    transports: Vec<Arc<dyn EndpointTransportPublisher>>,
    routing_policy: Arc<dyn EndpointRoutingPolicy>,
    fail_on_unsupported_protocol: bool,
    fail_fast_on_publish_error: bool,
}

impl EndpointRoutingEventPublisher {
    pub fn new(endpoint_store: Arc<dyn DeliveryEndpointStore>) -> Self {
        Self {
            endpoint_store,
            status_store: None,
            transports: Vec::new(),
            routing_policy: Arc::new(AllowAllEndpointRoutingPolicy),
            fail_on_unsupported_protocol: true,
            fail_fast_on_publish_error: true,
        }
    }

    pub fn with_transport<P: EndpointTransportPublisher + 'static>(mut self, transport: P) -> Self {
        self.transports.push(Arc::new(transport));
        self
    }

    pub fn with_transport_arc(mut self, transport: Arc<dyn EndpointTransportPublisher>) -> Self {
        self.transports.push(transport);
        self
    }

    pub fn with_http_webhook_transport(self) -> Self {
        self.with_transport(HttpWebhookTransportPublisher::new())
    }

    pub fn with_tcp_socket_transport(self) -> Self {
        self.with_transport(TcpSocketTransportPublisher)
    }

    pub fn fail_on_unsupported_protocol(mut self, value: bool) -> Self {
        self.fail_on_unsupported_protocol = value;
        self
    }

    pub fn fail_fast_on_publish_error(mut self, value: bool) -> Self {
        self.fail_fast_on_publish_error = value;
        self
    }

    pub fn with_routing_policy<P: EndpointRoutingPolicy + 'static>(mut self, policy: P) -> Self {
        self.routing_policy = Arc::new(policy);
        self
    }

    pub fn with_routing_policy_arc(mut self, policy: Arc<dyn EndpointRoutingPolicy>) -> Self {
        self.routing_policy = policy;
        self
    }

    pub fn with_status_store<S: EndpointDeliveryStatusStore + 'static>(mut self, store: S) -> Self {
        self.status_store = Some(Arc::new(store));
        self
    }

    pub fn with_status_store_arc(mut self, store: Arc<dyn EndpointDeliveryStatusStore>) -> Self {
        self.status_store = Some(store);
        self
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> Result<()> {
        let Some(transport) = self
            .transports
            .iter()
            .find(|transport| transport.supports(&endpoint.protocol))
        else {
            if self.fail_on_unsupported_protocol {
                return Err(StasisError::PortFailure(format!(
                    "no transport publisher registered for endpoint protocol on endpoint_id={}",
                    endpoint.endpoint_id
                )));
            }

            return Ok(());
        };

        transport.publish_to_endpoint(endpoint, event).await
    }
}

#[async_trait]
impl EventPublisher for EndpointRoutingEventPublisher {
    async fn publish(&self, event: &OutboxEvent) -> Result<()> {
        let endpoints = self.endpoint_store.list().await?;
        let mut first_error: Option<StasisError> = None;

        for endpoint in endpoints
            .iter()
            .filter(|endpoint| endpoint.enabled)
            .filter(|endpoint| self.routing_policy.should_route(endpoint, event))
        {
            if let Err(err) = self.publish_to_endpoint(endpoint, event).await {
                let err_message = err.to_string();
                if let Some(store) = &self.status_store {
                    let _ = store
                        .record_failure(
                            &endpoint.endpoint_id,
                            &event.event_id,
                            &err_message,
                            event.event.occurred_at,
                        )
                        .await;
                }

                if self.fail_fast_on_publish_error {
                    return Err(err);
                }

                if first_error.is_none() {
                    first_error = Some(err);
                }
                continue;
            }

            if let Some(store) = &self.status_store {
                store
                    .record_success(
                        &endpoint.endpoint_id,
                        &event.event_id,
                        event.event.occurred_at,
                    )
                    .await?;
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use chrono::Utc;

    use crate::domain::runtime::delivery_endpoint::{
        DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
    };
    use crate::domain::runtime::outbox::{
        OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType,
    };
    use crate::infrastructure::runtime::endpoint_routing_policy::{
        EndpointRouteRule, RuleBasedEndpointRoutingPolicy,
    };
    use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
    use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
    use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
    use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
    use crate::ports::outbound::runtime::event_publisher::EventPublisher;

    use super::EndpointRoutingEventPublisher;

    #[derive(Clone)]
    struct RecordingTransport {
        protocol: DeliveryProtocol,
        calls: Arc<RwLock<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl EndpointTransportPublisher for RecordingTransport {
        fn supports(&self, protocol: &DeliveryProtocol) -> bool {
            protocol == &self.protocol
        }

        async fn publish_to_endpoint(
            &self,
            endpoint: &DeliveryEndpoint,
            _event: &OutboxEvent,
        ) -> crate::domain::errors::Result<()> {
            let mut calls = self.calls.write().map_err(|_| {
                crate::domain::errors::StasisError::PortFailure("calls lock poisoned".to_string())
            })?;
            calls.push(endpoint.endpoint_id.clone());
            Ok(())
        }
    }

    #[derive(Clone)]
    struct SelectiveFailTransport {
        protocol: DeliveryProtocol,
        fail_endpoint_id: String,
        calls: Arc<RwLock<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl EndpointTransportPublisher for SelectiveFailTransport {
        fn supports(&self, protocol: &DeliveryProtocol) -> bool {
            protocol == &self.protocol
        }

        async fn publish_to_endpoint(
            &self,
            endpoint: &DeliveryEndpoint,
            _event: &OutboxEvent,
        ) -> crate::domain::errors::Result<()> {
            let mut calls = self.calls.write().map_err(|_| {
                crate::domain::errors::StasisError::PortFailure("calls lock poisoned".to_string())
            })?;
            calls.push(endpoint.endpoint_id.clone());
            if endpoint.endpoint_id == self.fail_endpoint_id {
                return Err(crate::domain::errors::StasisError::PortFailure(
                    "synthetic endpoint failure".to_string(),
                ));
            }

            Ok(())
        }
    }

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
                sttp_output_node_id: Some("sttp:out:1".to_string()),
                execution_id: None,
                input_memory_query_id: None,
                input_memory_query_fingerprint: None,
                output_memory_node_id: None,
                retrieval_path: None,
                occurred_at: Utc::now(),
                message: None,
            },
        }
    }

    #[tokio::test]
    async fn routes_to_enabled_supported_endpoints() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.webhook.1".to_string(),
                name: "Webhook".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/hook".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.kafka.1".to_string(),
                name: "Kafka".to_string(),
                protocol: DeliveryProtocol::Kafka,
                target: "kafka://broker:9092/topic".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let _ = store
            .set_enabled("endpoint.kafka.1", false)
            .await
            .expect("disable should succeed");

        let calls = Arc::new(RwLock::new(Vec::new()));
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store)).with_transport(
            RecordingTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                calls: Arc::clone(&calls),
            },
        );

        publisher
            .publish(&sample_event())
            .await
            .expect("publish should succeed");

        let calls = calls.read().expect("calls read lock should succeed");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], "endpoint.webhook.1");
    }

    #[tokio::test]
    async fn errors_when_enabled_endpoint_protocol_has_no_transport() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.tcp.1".to_string(),
                name: "TCP".to_string(),
                protocol: DeliveryProtocol::Tcp,
                target: "tcp://localhost:9000".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let calls = Arc::new(RwLock::new(Vec::new()));
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store)).with_transport(
            RecordingTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                calls,
            },
        );

        let result = publisher.publish(&sample_event()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn routes_only_when_policy_rule_matches() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.webhook.success".to_string(),
                name: "Webhook Success".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/success".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let calls = Arc::new(RwLock::new(Vec::new()));
        let policy = RuleBasedEndpointRoutingPolicy::new(vec![EndpointRouteRule {
            endpoint_ids: Some(vec!["endpoint.webhook.success".to_string()]),
            event_types: Some(vec![RuntimeEventType::JobSucceeded]),
            correlation_id_prefix: Some("corr-".to_string()),
            trace_id_prefix: None,
        }]);

        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store))
            .with_transport(RecordingTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                calls: Arc::clone(&calls),
            })
            .with_routing_policy(policy);

        publisher
            .publish(&sample_event())
            .await
            .expect("publish should succeed");

        let calls = calls.read().expect("calls read lock should succeed");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], "endpoint.webhook.success");
    }

    #[tokio::test]
    async fn records_delivery_status_on_success() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.webhook.status.success".to_string(),
                name: "Webhook Status Success".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/status-success".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store))
            .with_transport(RecordingTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                calls: Arc::new(RwLock::new(Vec::new())),
            })
            .with_status_store(status_store.clone());

        publisher
            .publish(&sample_event())
            .await
            .expect("publish should succeed");

        let status = status_store
            .get("endpoint.webhook.status.success")
            .await
            .expect("status get should succeed")
            .expect("status should exist");

        assert_eq!(status.success_count, 1);
        assert_eq!(status.failure_count, 0);
        assert_eq!(status.last_event_id.as_deref(), Some("evt-1"));
        assert_eq!(status.last_error, None);
    }

    #[tokio::test]
    async fn records_delivery_status_on_failure() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.tcp.status.failure".to_string(),
                name: "TCP Status Failure".to_string(),
                protocol: DeliveryProtocol::Tcp,
                target: "tcp://localhost:9000".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store))
            .with_transport(RecordingTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                calls: Arc::new(RwLock::new(Vec::new())),
            })
            .with_status_store(status_store.clone());

        let result = publisher.publish(&sample_event()).await;
        assert!(result.is_err());

        let status = status_store
            .get("endpoint.tcp.status.failure")
            .await
            .expect("status get should succeed")
            .expect("status should exist");

        assert_eq!(status.success_count, 0);
        assert_eq!(status.failure_count, 1);
        assert_eq!(status.last_event_id.as_deref(), Some("evt-1"));
        assert!(status.last_error.is_some());
    }

    #[tokio::test]
    async fn fails_fast_on_first_endpoint_error_and_skips_remaining_endpoints() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.a.fail".to_string(),
                name: "Webhook Fail First".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/fail-first".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.z.skip".to_string(),
                name: "Webhook Should Skip".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/skip".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let calls = Arc::new(RwLock::new(Vec::new()));
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store))
            .with_transport(SelectiveFailTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                fail_endpoint_id: "endpoint.a.fail".to_string(),
                calls: Arc::clone(&calls),
            })
            .with_status_store(status_store.clone());

        let result = publisher.publish(&sample_event()).await;
        assert!(result.is_err());

        let calls = calls.read().expect("calls read lock should succeed");
        assert_eq!(calls.as_slice(), ["endpoint.a.fail"]);

        let first_status = status_store
            .get("endpoint.a.fail")
            .await
            .expect("status get should succeed")
            .expect("failing endpoint status should exist");
        assert_eq!(first_status.failure_count, 1);

        let second_status = status_store
            .get("endpoint.z.skip")
            .await
            .expect("status get should succeed");
        assert!(second_status.is_none());
    }

    #[tokio::test]
    async fn continue_on_publish_error_attempts_remaining_endpoints_and_records_mixed_status() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.a.fail".to_string(),
                name: "Webhook Fail First".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/fail-first".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.z.success".to_string(),
                name: "Webhook Success".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/success".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let calls = Arc::new(RwLock::new(Vec::new()));
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store))
            .with_transport(SelectiveFailTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                fail_endpoint_id: "endpoint.a.fail".to_string(),
                calls: Arc::clone(&calls),
            })
            .with_status_store(status_store.clone())
            .fail_fast_on_publish_error(false);

        let result = publisher.publish(&sample_event()).await;
        assert!(result.is_err());

        let calls = calls.read().expect("calls read lock should succeed");
        assert_eq!(calls.as_slice(), ["endpoint.a.fail", "endpoint.z.success"]);

        let failed_status = status_store
            .get("endpoint.a.fail")
            .await
            .expect("status get should succeed")
            .expect("failed endpoint status should exist");
        assert_eq!(failed_status.failure_count, 1);
        assert_eq!(failed_status.success_count, 0);

        let success_status = status_store
            .get("endpoint.z.success")
            .await
            .expect("status get should succeed")
            .expect("successful endpoint status should exist");
        assert_eq!(success_status.failure_count, 0);
        assert_eq!(success_status.success_count, 1);
    }

    #[tokio::test]
    async fn continue_on_publish_error_does_not_starve_healthy_endpoint_across_repeated_events() {
        let store = InMemoryDeliveryEndpointStore::default();
        let now = Utc::now();

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.a.fail".to_string(),
                name: "Webhook Fail First".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/fail-first".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.z.success".to_string(),
                name: "Webhook Success".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/success".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let calls = Arc::new(RwLock::new(Vec::new()));
        let publisher = EndpointRoutingEventPublisher::new(Arc::new(store))
            .with_transport(SelectiveFailTransport {
                protocol: DeliveryProtocol::HttpWebhook,
                fail_endpoint_id: "endpoint.a.fail".to_string(),
                calls: Arc::clone(&calls),
            })
            .with_status_store(status_store.clone())
            .fail_fast_on_publish_error(false);

        let first_result = publisher.publish(&sample_event()).await;
        assert!(first_result.is_err());

        let mut second_event = sample_event();
        second_event.event_id = "evt-2".to_string();
        let second_result = publisher.publish(&second_event).await;
        assert!(second_result.is_err());

        let calls = calls.read().expect("calls read lock should succeed");
        assert_eq!(
            calls.as_slice(),
            [
                "endpoint.a.fail",
                "endpoint.z.success",
                "endpoint.a.fail",
                "endpoint.z.success",
            ]
        );

        let failed_status = status_store
            .get("endpoint.a.fail")
            .await
            .expect("status get should succeed")
            .expect("failed endpoint status should exist");
        assert_eq!(failed_status.failure_count, 2);
        assert_eq!(failed_status.success_count, 0);

        let success_status = status_store
            .get("endpoint.z.success")
            .await
            .expect("status get should succeed")
            .expect("successful endpoint status should exist");
        assert_eq!(success_status.failure_count, 0);
        assert_eq!(success_status.success_count, 2);
    }
}
