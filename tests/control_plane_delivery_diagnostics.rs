use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use stasis::application::dto::ListEndpointDiagnosticsReadModelRequest;
use stasis::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use stasis::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::domain::errors::{Result, StasisError};
use stasis::domain::runtime::delivery_endpoint::{
    DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
};
use stasis::domain::runtime::job::{BackoffPolicy, NewJob};
use stasis::domain::runtime::outbox::{OutboxEvent, OutboxPublishPolicy, OutboxStatus};
use stasis::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
use stasis::infrastructure::runtime::endpoint_routing_event_publisher::EndpointRoutingEventPublisher;
use stasis::infrastructure::runtime::surreal_cluster_node_store::SurrealClusterNodeStore;
use stasis::infrastructure::runtime::surreal_delivery_endpoint_store::SurrealDeliveryEndpointStore;
use stasis::infrastructure::runtime::surreal_endpoint_delivery_status_store::SurrealEndpointDeliveryStatusStore;
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;
use stasis::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use stasis::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
use stasis::sdk::control_plane_sdk::ControlPlaneSdk;

#[derive(Clone)]
struct SuccessHandler;

#[async_trait]
impl JobHandler for SuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.success"
    }

    async fn execute(
        &self,
        _job: &stasis::domain::runtime::job::Job,
    ) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:surreal-e2e".to_string(),
            execution_id: Some("exec:surreal-e2e".to_string()),
            diagnostics: None,
        })
    }
}

#[derive(Clone)]
struct RecordingTransport {
    calls: Arc<RwLock<Vec<String>>>,
}

#[async_trait]
impl EndpointTransportPublisher for RecordingTransport {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::HttpWebhook)
    }

    async fn publish_to_endpoint(
        &self,
        endpoint: &DeliveryEndpoint,
        _event: &OutboxEvent,
    ) -> Result<()> {
        let mut calls = self
            .calls
            .write()
            .map_err(|_| StasisError::PortFailure("calls lock poisoned".to_string()))?;
        calls.push(endpoint.endpoint_id.clone());
        Ok(())
    }
}

#[derive(Clone)]
struct FailOnceTransport {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EndpointTransportPublisher for FailOnceTransport {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::HttpWebhook)
    }

    async fn publish_to_endpoint(
        &self,
        _endpoint: &DeliveryEndpoint,
        _event: &OutboxEvent,
    ) -> Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 1 {
            return Err(StasisError::PortFailure(
                "synthetic endpoint transport failure".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Clone)]
struct FailAlwaysTransport {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EndpointTransportPublisher for FailAlwaysTransport {
    fn supports(&self, protocol: &DeliveryProtocol) -> bool {
        matches!(protocol, DeliveryProtocol::HttpWebhook)
    }

    async fn publish_to_endpoint(
        &self,
        _endpoint: &DeliveryEndpoint,
        _event: &OutboxEvent,
    ) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(StasisError::PortFailure(
            "synthetic endpoint transport failure (always)".to_string(),
        ))
    }
}

#[tokio::test]
async fn surreal_runtime_persists_endpoint_delivery_status_and_control_plane_queries_it() {
    let now = Utc::now();
    let backend = RuntimeBackend::SurrealMem {
        namespace: format!("ns_{}", now.timestamp_nanos_opt().unwrap_or_default()),
        database: "stasis_delivery_diag".to_string(),
    };

    let calls = Arc::new(RwLock::new(Vec::<String>::new()));
    let runtime = StasisRuntimeBuilder::new(backend)
        .with_extra_handler(SuccessHandler)
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .without_orchestration_pattern_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::Surreal(rt) = runtime else {
        panic!("expected surreal runtime composition");
    };

    let db = rt.job_store.db();
    let endpoint_store = SurrealDeliveryEndpointStore::new(db.clone());
    let status_store = Arc::new(SurrealEndpointDeliveryStatusStore::new(db));

    let publisher = EndpointRoutingEventPublisher::new(Arc::new(endpoint_store.clone()))
        .with_transport(RecordingTransport {
            calls: Arc::clone(&calls),
        })
        .with_status_store_arc(status_store.clone());
    rt.register_event_publisher(publisher)
        .expect("publisher should register");

    endpoint_store
        .insert(NewDeliveryEndpoint {
            endpoint_id: "endpoint.surreal.webhook".to_string(),
            name: "Surreal Webhook".to_string(),
            protocol: DeliveryProtocol::HttpWebhook,
            target: "https://example.com/surreal-hook".to_string(),
            metadata: None,
            created_at: now,
        })
        .await
        .expect("endpoint should insert");

    rt.enqueue(NewJob {
        id: "job-surreal-routing-status".to_string(),
        queue: "default".to_string(),
        job_type: "test.success".to_string(),
        payload_ref: "sttp:in:surreal-e2e".to_string(),
        priority: 100,
        max_attempts: 1,
        idempotency_key: "idem-surreal-routing-status".to_string(),
        correlation_id: "corr-surreal-routing-status".to_string(),
        causation_id: "cause-surreal-routing-status".to_string(),
        trace_id: "trace-surreal-routing-status".to_string(),
        sttp_input_node_id: "sttp:in:surreal-e2e".to_string(),
        scheduled_at: now - Duration::seconds(1),
        backoff_policy: BackoffPolicy::default(),
    })
    .await
    .expect("job should enqueue");

    let processed = rt
        .process_once("default", "worker-surreal", now)
        .await
        .expect("process should succeed");
    assert_eq!(processed.as_deref(), Some("job-surreal-routing-status"));

    let published = rt
        .publish_pending_events(10, now)
        .await
        .expect("publish should succeed");
    let outbox_events = rt
        .outbox_store
        .list_by_job_id("job-surreal-routing-status")
        .await
        .expect("outbox list should succeed");
    assert_eq!(
        published,
        1,
        "expected one published event, outbox={:?}",
        outbox_events
            .iter()
            .map(|evt| (&evt.event_id, &evt.status, &evt.last_publish_error))
            .collect::<Vec<_>>()
    );

    let (call_len, first_call) = {
        let calls = calls.read().expect("calls read lock should succeed");
        (calls.len(), calls.first().cloned())
    };
    assert_eq!(call_len, 1);
    assert_eq!(first_call.as_deref(), Some("endpoint.surreal.webhook"));

    let control_plane = ControlPlaneSdk::new_with_status_store(
        CompositeControlPlaneStore::new(
            endpoint_store,
            SurrealClusterNodeStore::new(rt.job_store.db()),
        ),
        status_store,
    );

    let statuses = control_plane
        .list_endpoint_delivery_statuses()
        .await
        .expect("status list should succeed");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].endpoint_id, "endpoint.surreal.webhook");
    assert_eq!(statuses[0].success_count, 1);
    assert_eq!(statuses[0].failure_count, 0);

    let read_model_rows = control_plane
        .list_endpoint_diagnostics_read_model(ListEndpointDiagnosticsReadModelRequest {
            include_disabled: true,
            ..Default::default()
        })
        .await
        .expect("read model query should succeed");

    assert_eq!(read_model_rows.len(), 1);
    assert_eq!(read_model_rows[0].endpoint_id, "endpoint.surreal.webhook");
    assert_eq!(read_model_rows[0].success_count, 1);
    assert!(!read_model_rows[0].unhealthy);
}

#[tokio::test]
async fn surreal_runtime_records_retry_backoff_then_recovery_for_endpoint_delivery() {
    let now = Utc::now();
    let backend = RuntimeBackend::SurrealMem {
        namespace: format!("ns_{}", now.timestamp_nanos_opt().unwrap_or_default()),
        database: "stasis_delivery_backoff".to_string(),
    };

    let runtime = StasisRuntimeBuilder::new(backend)
        .with_extra_handler(SuccessHandler)
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .without_orchestration_pattern_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::Surreal(rt) = runtime else {
        panic!("expected surreal runtime composition");
    };

    rt.configure_outbox_publish_policy(OutboxPublishPolicy {
        max_attempts: 3,
        base_delay_seconds: 1,
        max_delay_seconds: 4,
    })
    .expect("publish policy should configure");

    let db = rt.job_store.db();
    let endpoint_store = SurrealDeliveryEndpointStore::new(db.clone());
    let status_store = Arc::new(SurrealEndpointDeliveryStatusStore::new(db));
    let transport_calls = Arc::new(AtomicUsize::new(0));

    let publisher = EndpointRoutingEventPublisher::new(Arc::new(endpoint_store.clone()))
        .with_transport(FailOnceTransport {
            calls: Arc::clone(&transport_calls),
        })
        .with_status_store_arc(status_store.clone());
    rt.register_event_publisher(publisher)
        .expect("publisher should register");

    endpoint_store
        .insert(NewDeliveryEndpoint {
            endpoint_id: "endpoint.surreal.webhook.backoff".to_string(),
            name: "Surreal Webhook Backoff".to_string(),
            protocol: DeliveryProtocol::HttpWebhook,
            target: "https://example.com/surreal-hook-backoff".to_string(),
            metadata: None,
            created_at: now,
        })
        .await
        .expect("endpoint should insert");

    rt.enqueue(NewJob {
        id: "job-surreal-routing-backoff".to_string(),
        queue: "default".to_string(),
        job_type: "test.success".to_string(),
        payload_ref: "sttp:in:surreal-backoff".to_string(),
        priority: 100,
        max_attempts: 1,
        idempotency_key: "idem-surreal-routing-backoff".to_string(),
        correlation_id: "corr-surreal-routing-backoff".to_string(),
        causation_id: "cause-surreal-routing-backoff".to_string(),
        trace_id: "trace-surreal-routing-backoff".to_string(),
        sttp_input_node_id: "sttp:in:surreal-backoff".to_string(),
        scheduled_at: now - Duration::seconds(1),
        backoff_policy: BackoffPolicy::default(),
    })
    .await
    .expect("job should enqueue");

    rt.process_once("default", "worker-surreal", now)
        .await
        .expect("process should succeed");

    let first_publish = rt
        .publish_pending_events(10, now)
        .await
        .expect("publish sweep should succeed");
    assert_eq!(first_publish, 0);

    let after_first = rt
        .outbox_store
        .list_by_job_id("job-surreal-routing-backoff")
        .await
        .expect("outbox list should succeed");
    assert_eq!(after_first.len(), 1);
    assert_eq!(after_first[0].status, OutboxStatus::Pending);
    assert_eq!(after_first[0].publish_attempts, 1);
    assert!(
        after_first[0]
            .last_publish_error
            .as_deref()
            .unwrap_or_default()
            .contains("synthetic endpoint transport failure")
    );
    let retry_at = after_first[0]
        .next_attempt_at
        .expect("next_attempt_at should be set after failure");
    assert_eq!(retry_at, now + Duration::seconds(1));

    let premature_publish = rt
        .publish_pending_events(10, now)
        .await
        .expect("premature publish sweep should succeed");
    assert_eq!(premature_publish, 0);

    let second_publish = rt
        .publish_pending_events(10, retry_at)
        .await
        .expect("retry publish sweep should succeed");
    assert_eq!(second_publish, 1);

    let after_second = rt
        .outbox_store
        .list_by_job_id("job-surreal-routing-backoff")
        .await
        .expect("outbox list should succeed");
    assert_eq!(after_second.len(), 1);
    assert_eq!(after_second[0].status, OutboxStatus::Published);
    assert_eq!(after_second[0].publish_attempts, 2);
    assert_eq!(after_second[0].last_publish_error, None);
    assert_eq!(after_second[0].next_attempt_at, None);
    assert_eq!(transport_calls.load(Ordering::SeqCst), 2);

    let control_plane = ControlPlaneSdk::new_with_status_store(
        CompositeControlPlaneStore::new(
            endpoint_store,
            SurrealClusterNodeStore::new(rt.job_store.db()),
        ),
        status_store,
    );

    let statuses = control_plane
        .list_endpoint_delivery_statuses()
        .await
        .expect("status list should succeed");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].endpoint_id, "endpoint.surreal.webhook.backoff");
    assert_eq!(statuses[0].success_count, 1);
    assert_eq!(statuses[0].failure_count, 1);
    assert_eq!(statuses[0].last_error, None);

    let read_model_rows = control_plane
        .list_endpoint_diagnostics_read_model(ListEndpointDiagnosticsReadModelRequest {
            include_disabled: true,
            ..Default::default()
        })
        .await
        .expect("read model query should succeed");
    assert_eq!(read_model_rows.len(), 1);
    assert_eq!(read_model_rows[0].endpoint_id, "endpoint.surreal.webhook.backoff");
    assert_eq!(read_model_rows[0].success_count, 1);
    assert_eq!(read_model_rows[0].failure_count, 1);
}

#[tokio::test]
async fn surreal_runtime_marks_outbox_failed_after_max_publish_attempts() {
    let now = Utc::now();
    let backend = RuntimeBackend::SurrealMem {
        namespace: format!("ns_{}", now.timestamp_nanos_opt().unwrap_or_default()),
        database: "stasis_delivery_terminal_failure".to_string(),
    };

    let runtime = StasisRuntimeBuilder::new(backend)
        .with_extra_handler(SuccessHandler)
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .without_orchestration_pattern_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::Surreal(rt) = runtime else {
        panic!("expected surreal runtime composition");
    };

    rt.configure_outbox_publish_policy(OutboxPublishPolicy {
        max_attempts: 2,
        base_delay_seconds: 1,
        max_delay_seconds: 4,
    })
    .expect("publish policy should configure");

    let db = rt.job_store.db();
    let endpoint_store = SurrealDeliveryEndpointStore::new(db.clone());
    let status_store = Arc::new(SurrealEndpointDeliveryStatusStore::new(db));
    let transport_calls = Arc::new(AtomicUsize::new(0));

    let publisher = EndpointRoutingEventPublisher::new(Arc::new(endpoint_store.clone()))
        .with_transport(FailAlwaysTransport {
            calls: Arc::clone(&transport_calls),
        })
        .with_status_store_arc(status_store.clone());
    rt.register_event_publisher(publisher)
        .expect("publisher should register");

    endpoint_store
        .insert(NewDeliveryEndpoint {
            endpoint_id: "endpoint.surreal.webhook.fail-forever".to_string(),
            name: "Surreal Webhook Fail Forever".to_string(),
            protocol: DeliveryProtocol::HttpWebhook,
            target: "https://example.com/surreal-hook-fail-forever".to_string(),
            metadata: None,
            created_at: now,
        })
        .await
        .expect("endpoint should insert");

    rt.enqueue(NewJob {
        id: "job-surreal-routing-terminal-failure".to_string(),
        queue: "default".to_string(),
        job_type: "test.success".to_string(),
        payload_ref: "sttp:in:surreal-terminal-failure".to_string(),
        priority: 100,
        max_attempts: 1,
        idempotency_key: "idem-surreal-routing-terminal-failure".to_string(),
        correlation_id: "corr-surreal-routing-terminal-failure".to_string(),
        causation_id: "cause-surreal-routing-terminal-failure".to_string(),
        trace_id: "trace-surreal-routing-terminal-failure".to_string(),
        sttp_input_node_id: "sttp:in:surreal-terminal-failure".to_string(),
        scheduled_at: now - Duration::seconds(1),
        backoff_policy: BackoffPolicy::default(),
    })
    .await
    .expect("job should enqueue");

    rt.process_once("default", "worker-surreal", now)
        .await
        .expect("process should succeed");

    let first_publish = rt
        .publish_pending_events(10, now)
        .await
        .expect("first publish sweep should succeed");
    assert_eq!(first_publish, 0);

    let after_first = rt
        .outbox_store
        .list_by_job_id("job-surreal-routing-terminal-failure")
        .await
        .expect("outbox list should succeed");
    assert_eq!(after_first.len(), 1);
    assert_eq!(after_first[0].status, OutboxStatus::Pending);
    assert_eq!(after_first[0].publish_attempts, 1);
    assert_eq!(after_first[0].next_attempt_at, Some(now + Duration::seconds(1)));

    let second_publish = rt
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("second publish sweep should succeed");
    assert_eq!(second_publish, 0);

    let after_second = rt
        .outbox_store
        .list_by_job_id("job-surreal-routing-terminal-failure")
        .await
        .expect("outbox list should succeed");
    assert_eq!(after_second.len(), 1);
    assert_eq!(after_second[0].status, OutboxStatus::Failed);
    assert_eq!(after_second[0].publish_attempts, 2);
    assert_eq!(after_second[0].next_attempt_at, None);
    assert!(after_second[0].last_publish_error.is_some());
    assert_eq!(transport_calls.load(Ordering::SeqCst), 2);

    let control_plane = ControlPlaneSdk::new_with_status_store(
        CompositeControlPlaneStore::new(
            endpoint_store,
            SurrealClusterNodeStore::new(rt.job_store.db()),
        ),
        status_store,
    );

    let statuses = control_plane
        .list_endpoint_delivery_statuses()
        .await
        .expect("status list should succeed");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].endpoint_id, "endpoint.surreal.webhook.fail-forever");
    assert_eq!(statuses[0].success_count, 0);
    assert_eq!(statuses[0].failure_count, 2);
    assert!(statuses[0].last_error.is_some());
}
