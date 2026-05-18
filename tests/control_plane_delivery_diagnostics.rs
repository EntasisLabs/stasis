use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;
use stasis::prelude::{
    BackoffPolicy, ControlPlaneSdk, DeliveryEndpoint, DeliveryEndpointStore, DeliveryProtocol,
    EndpointRoutingEventPublisher, EndpointTransportPublisher, JobExecutionOutcome, JobHandler,
    ListEndpointDiagnosticsReadModelRequest, NewDeliveryEndpoint, NewJob, OutboxEvent,
    RuntimeBackend, RuntimeComposition, StasisRuntimeBuilder, SurrealClusterNodeStore,
    SurrealDeliveryEndpointStore, SurrealEndpointDeliveryStatusStore,
};
use stasis::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;

#[derive(Clone)]
struct SuccessHandler;

#[async_trait]
impl JobHandler for SuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.success"
    }

    async fn execute(&self, _job: &stasis::domain::runtime::job::Job) -> stasis::prelude::Result<JobExecutionOutcome> {
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
    ) -> stasis::prelude::Result<()> {
        let mut calls = self
            .calls
            .write()
            .map_err(|_| stasis::prelude::StasisError::PortFailure("calls lock poisoned".to_string()))?;
        calls.push(endpoint.endpoint_id.clone());
        Ok(())
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
        published, 1,
        "expected one published event, outbox={:?}",
        outbox_events
            .iter()
            .map(|evt| (&evt.event_id, &evt.status, &evt.last_publish_error))
            .collect::<Vec<_>>()
    );

    let calls = calls.read().expect("calls read lock should succeed");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], "endpoint.surreal.webhook");
    drop(calls);

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
