use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use stasis::application::runtime::in_memory_runtime::InMemoryRuntime;
use stasis::application::telemetry::request_context::{scope_inbound_trace, trace_id_for_enqueue};
use stasis::dashboard::{router, DashboardState, RuntimeDashboardQueryService};
use stasis::domain::runtime::job::JobState;
use stasis::domain::runtime::recurring::RecurringDefinition;
use stasis::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
use stasis::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
use stasis::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use stasis::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::runtime_tracing::TraceContext;
use stasis::sdk::control_plane_sdk::ControlPlaneSdk;
use tower::ServiceExt;

const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[tokio::test]
async fn dashboard_materialize_propagates_traceparent_to_enqueued_jobs() {
    unsafe {
        std::env::set_var("STASIS_OTEL_TRACE_PROPAGATION", "w3c");
    }

    let runtime = Arc::new(InMemoryRuntime::new());
    let now = Utc::now();
    runtime
        .register_recurring(RecurringDefinition {
            id: "recur.dashboard".to_string(),
            queue: "default".to_string(),
            job_type: "workflow.stasis.prompt".to_string(),
            payload_template_ref: "sttp:in:dashboard".to_string(),
            cron_expr: "0/1 * * * * * *".to_string(),
            timezone: "UTC".to_string(),
            jitter_seconds: 0,
            enabled: true,
            max_attempts: 1,
            next_run_at: now,
            last_run_at: None,
            lease_owner: None,
            lease_expires_at: None,
        })
        .await
        .expect("recurring should register");

    let endpoint_store = InMemoryDeliveryEndpointStore::default();
    let cluster_store = InMemoryClusterNodeStore::default();
    let status_store = Arc::new(InMemoryEndpointDeliveryStatusStore::default());
    let control_store = CompositeControlPlaneStore::new(endpoint_store, cluster_store);
    let control_plane =
        ControlPlaneSdk::new_with_status_store(control_store, status_store.clone());

    let service = Arc::new(RuntimeDashboardQueryService::new(runtime.clone(), control_plane));
    let app = router(DashboardState::new(service));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/action/scheduler/materialize")
                .header("traceparent", TRACEPARENT)
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("request should complete");

    assert_eq!(response.status(), StatusCode::OK);

    let jobs = runtime
        .job_store
        .list_by_state(JobState::Enqueued)
        .await
        .expect("jobs should list");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");

    unsafe {
        std::env::remove_var("STASIS_OTEL_TRACE_PROPAGATION");
    }
}

#[tokio::test]
async fn trace_id_for_enqueue_uses_inbound_trace_context() {
    unsafe {
        std::env::set_var("STASIS_OTEL_TRACE_PROPAGATION", "w3c");
    }

    let trace = TraceContext {
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        span_id: "00f067aa0ba902b7".to_string(),
        trace_flags: 1,
    };

    scope_inbound_trace(trace, async {
        let trace_id = trace_id_for_enqueue(|| "legacy-default".to_string());
        assert_eq!(trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    })
    .await;

    unsafe {
        std::env::remove_var("STASIS_OTEL_TRACE_PROPAGATION");
    }
}
