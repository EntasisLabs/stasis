use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{Duration, Utc};
use stasis::application::dto::{
    HeartbeatClusterNodeRequest, RegisterClusterNodeRequest, RegisterDeliveryEndpointRequest,
};
use stasis::application::composition::surreal_backend_config::{
    resolve_surreal_auth_from_env, resolve_surreal_database_from_env, resolve_surreal_namespace_from_env,
};
use stasis::application::runtime::runtime_factory::{
    RuntimeBackend, RuntimeComposition, RuntimeFactory, SurrealAuth,
};
use stasis::application::runtime::in_memory_runtime::{
    InMemoryRuntime, JobExecutionOutcome, JobHandler,
};
use stasis::dashboard::{DashboardState, RuntimeDashboardQueryService, router};
use stasis::domain::errors::Result;
use stasis::domain::runtime::cluster_node::ClusterNodeRole;
use stasis::domain::runtime::job::Job;
use stasis::domain::runtime::delivery_endpoint::DeliveryProtocol;
use stasis::domain::runtime::job::{BackoffPolicy, NewJob};
use stasis::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
use stasis::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
use stasis::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use stasis::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
use stasis::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
use stasis::sdk::control_plane_sdk::ControlPlaneSdk;

#[derive(Clone)]
struct DemoSuccessHandler;

#[async_trait::async_trait]
impl JobHandler for DemoSuccessHandler {
    fn job_type(&self) -> &'static str {
        "demo.success"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:demo-success".to_string(),
            execution_id: Some("exec-demo-success".to_string()),
            diagnostics: None,
        })
    }
}

#[derive(Clone)]
struct DemoFatalHandler;

#[async_trait::async_trait]
impl JobHandler for DemoFatalHandler {
    fn job_type(&self) -> &'static str {
        "demo.fatal"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::FatalFailure {
            message: "demo fatal crash".to_string(),
            execution_id: Some("exec-demo-fatal".to_string()),
            diagnostics: Some("{\"guardrail_code\":\"DEMO_FATAL\"}".to_string()),
        })
    }
}

async fn seed_runtime_data(runtime: &InMemoryRuntime) {
    runtime
        .register_handler(DemoSuccessHandler)
        .expect("register demo success handler");
    runtime
        .register_handler(DemoFatalHandler)
        .expect("register demo fatal handler");

    let now = Utc::now();
    let backoff = BackoffPolicy {
        base_delay_seconds: 1,
        max_delay_seconds: 4,
    };

    runtime
        .enqueue(NewJob {
            id: "job-demo-success-1".to_string(),
            queue: "default".to_string(),
            job_type: "demo.success".to_string(),
            payload_ref: "sttp:in:demo-1".to_string(),
            priority: 100,
            max_attempts: 2,
            idempotency_key: "idem-demo-success-1".to_string(),
            correlation_id: "corr-demo-success-1".to_string(),
            causation_id: "cause-demo-success-1".to_string(),
            trace_id: "trace-demo-success-1".to_string(),
            sttp_input_node_id: "sttp:in:demo-1".to_string(),
            scheduled_at: now,
            backoff_policy: backoff.clone(),
        })
        .await
        .expect("enqueue success demo job");

    runtime
        .enqueue(NewJob {
            id: "job-demo-fatal-1".to_string(),
            queue: "default".to_string(),
            job_type: "demo.fatal".to_string(),
            payload_ref: "sttp:in:demo-2".to_string(),
            priority: 90,
            max_attempts: 1,
            idempotency_key: "idem-demo-fatal-1".to_string(),
            correlation_id: "corr-demo-fatal-1".to_string(),
            causation_id: "cause-demo-fatal-1".to_string(),
            trace_id: "trace-demo-fatal-1".to_string(),
            sttp_input_node_id: "sttp:in:demo-2".to_string(),
            scheduled_at: now,
            backoff_policy: backoff.clone(),
        })
        .await
        .expect("enqueue fatal demo job");

    runtime
        .enqueue(NewJob {
            id: "job-demo-pending-1".to_string(),
            queue: "default".to_string(),
            job_type: "demo.success".to_string(),
            payload_ref: "sttp:in:demo-3".to_string(),
            priority: 80,
            max_attempts: 3,
            idempotency_key: "idem-demo-pending-1".to_string(),
            correlation_id: "corr-demo-pending-1".to_string(),
            causation_id: "cause-demo-pending-1".to_string(),
            trace_id: "trace-demo-pending-1".to_string(),
            sttp_input_node_id: "sttp:in:demo-3".to_string(),
            scheduled_at: now + Duration::minutes(5),
            backoff_policy: backoff,
        })
        .await
        .expect("enqueue pending demo job");

    runtime
        .process_once("default", "worker-demo-a", now)
        .await
        .expect("process first demo job");
    runtime
        .process_once("default", "worker-demo-b", now + Duration::seconds(1))
        .await
        .expect("process second demo job");
}

async fn seed_control_plane_data(
    control_plane: &ControlPlaneSdk<
        CompositeControlPlaneStore<InMemoryDeliveryEndpointStore, InMemoryClusterNodeStore>,
    >,
    endpoint_status_store: Arc<InMemoryEndpointDeliveryStatusStore>,
) {
    let now = Utc::now();

    control_plane
        .register_delivery_endpoint(RegisterDeliveryEndpointRequest {
            endpoint_id: "endpoint.webhook.ops".to_string(),
            name: "Ops Webhook".to_string(),
            protocol: DeliveryProtocol::HttpWebhook,
            target: "https://ops.example/hook".to_string(),
            metadata: None,
        })
        .await
        .expect("register webhook endpoint");

    control_plane
        .register_delivery_endpoint(RegisterDeliveryEndpointRequest {
            endpoint_id: "endpoint.kafka.audit".to_string(),
            name: "Audit Kafka".to_string(),
            protocol: DeliveryProtocol::Kafka,
            target: "kafka://broker:9092/audit".to_string(),
            metadata: None,
        })
        .await
        .expect("register kafka endpoint");

    endpoint_status_store
        .record_success("endpoint.webhook.ops", "evt-demo-1", now)
        .await
        .expect("record endpoint success");
    endpoint_status_store
        .record_failure(
            "endpoint.kafka.audit",
            "evt-demo-2",
            "delivery timeout",
            now - Duration::seconds(10),
        )
        .await
        .expect("record endpoint failure");

    control_plane
        .register_cluster_node(RegisterClusterNodeRequest {
            node_id: "worker-12".to_string(),
            role: ClusterNodeRole::Worker,
            region: "eu-west-1".to_string(),
            queue_ownership: vec!["default".to_string(), "billing".to_string()],
            capability_tags: vec!["cpu".to_string()],
            heartbeat_at: now,
            lease_ttl_seconds: 45,
            queue_ownership_mode: None,
            metadata: Some("v1.0.0".to_string()),
        })
        .await
        .expect("register worker node");

    control_plane
        .register_cluster_node(RegisterClusterNodeRequest {
            node_id: "scheduler-2".to_string(),
            role: ClusterNodeRole::Scheduler,
            region: "eu-west-1".to_string(),
            queue_ownership: vec!["priority".to_string()],
            capability_tags: vec!["orchestration".to_string()],
            heartbeat_at: now - Duration::seconds(40),
            lease_ttl_seconds: 60,
            queue_ownership_mode: None,
            metadata: Some("rolling".to_string()),
        })
        .await
        .expect("register scheduler node");

    control_plane
        .heartbeat_cluster_node(HeartbeatClusterNodeRequest {
            node_id: "worker-12".to_string(),
            heartbeat_at: now,
            lease_ttl_seconds: 45,
            queue_ownership_mode: None,
            queue_ownership: None,
            capability_tags: None,
            metadata: None,
        })
        .await
        .expect("heartbeat worker node");
}

#[tokio::main]
async fn main() {
    let seed_demo_data = std::env::var("STASIS_DASHBOARD_DEMO_SEED")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);

    let backend = resolve_dashboard_runtime_backend();
    let runtime = RuntimeFactory::build(backend)
        .await
        .expect("build dashboard runtime composition");

    let service: Arc<RuntimeDashboardQueryService> = match runtime {
        RuntimeComposition::InMemory(runtime) => {
            let runtime = Arc::new(runtime);
            if seed_demo_data {
                seed_runtime_data(runtime.as_ref()).await;
            }

            let endpoint_store = InMemoryDeliveryEndpointStore::default();
            let cluster_store = InMemoryClusterNodeStore::default();
            let endpoint_status_store = Arc::new(InMemoryEndpointDeliveryStatusStore::default());
            let control_store = CompositeControlPlaneStore::new(endpoint_store, cluster_store);
            let control_plane =
                ControlPlaneSdk::new_with_status_store(control_store, endpoint_status_store.clone());

            if seed_demo_data {
                seed_control_plane_data(&control_plane, endpoint_status_store).await;
            }

            Arc::new(RuntimeDashboardQueryService::new(runtime, control_plane))
        }
        RuntimeComposition::Surreal(runtime) => {
            if seed_demo_data {
                println!(
                    "dashboard demo seed mode is ignored for surreal runtime backends"
                );
            }

            Arc::new(RuntimeDashboardQueryService::from_runtime_composition(
                RuntimeComposition::Surreal(runtime),
            ))
        }
    };

    let mut state = DashboardState::new(service);
    if let Some(token) = std::env::var("STASIS_DASHBOARD_ACTION_AUTH_BEARER")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        state = state.with_action_auth_bearer_token(token);
    }
    if let Some(required_role) = std::env::var("STASIS_DASHBOARD_ACTION_REQUIRED_ROLE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        state = state.with_action_required_role(required_role);
    }
    if let Some(role_claim_header) = std::env::var("STASIS_DASHBOARD_ACTION_ROLE_CLAIM_HEADER")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        state = state.with_action_role_claim_header(role_claim_header);
    }
    let app = router(state);

    let addr: SocketAddr = std::env::var("STASIS_DASHBOARD_ADDR")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(|| {
            "127.0.0.1:3007"
                .parse()
                .expect("valid dashboard bind address")
        });

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind dashboard listener");

    println!("stasis dashboard listening on http://{}", addr);
    if seed_demo_data {
        println!("dashboard demo seed mode enabled via STASIS_DASHBOARD_DEMO_SEED");
    }

    axum::serve(listener, app)
        .await
        .expect("run dashboard server");
}

fn resolve_dashboard_runtime_backend() -> RuntimeBackend {
    let backend = std::env::var("STASIS_DASHBOARD_RUNTIME_BACKEND")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "in-memory".to_string());

    match backend.as_str() {
        "in-memory" | "inmemory" => RuntimeBackend::InMemory,
        "surreal-mem" | "mem" => apply_surreal_auth(RuntimeBackend::surreal_mem(
            dashboard_surreal_namespace(),
            dashboard_surreal_database(),
        )),
        "surreal-ws" | "ws" => apply_surreal_auth(RuntimeBackend::surreal_ws(
            std::env::var("STASIS_DASHBOARD_SURREAL_ENDPOINT")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .expect(
                    "STASIS_DASHBOARD_SURREAL_ENDPOINT is required when STASIS_DASHBOARD_RUNTIME_BACKEND=surreal-ws",
                ),
            dashboard_surreal_namespace(),
            dashboard_surreal_database(),
        )),
        "surreal-kv" | "kv" => apply_surreal_auth(RuntimeBackend::surreal_kv(
            std::env::var("STASIS_DASHBOARD_SURREAL_KV_PATH")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .expect(
                    "STASIS_DASHBOARD_SURREAL_KV_PATH is required when STASIS_DASHBOARD_RUNTIME_BACKEND=surreal-kv",
                ),
            dashboard_surreal_namespace(),
            dashboard_surreal_database(),
        )),
        other => {
            println!(
                "unknown STASIS_DASHBOARD_RUNTIME_BACKEND='{}', falling back to in-memory",
                other
            );
            RuntimeBackend::InMemory
        }
    }
}

fn dashboard_surreal_namespace() -> String {
    resolve_surreal_namespace_from_env("STASIS_DASHBOARD_SURREAL_NAMESPACE", None, "stasis")
}

fn dashboard_surreal_database() -> String {
    resolve_surreal_database_from_env("STASIS_DASHBOARD_SURREAL_DATABASE", None, "runtime")
}

fn dashboard_surreal_auth() -> Option<SurrealAuth> {
    resolve_surreal_auth_from_env(
        "STASIS_DASHBOARD_SURREAL_USERNAME",
        "STASIS_DASHBOARD_SURREAL_PASSWORD",
        None,
        None,
    )
}

fn apply_surreal_auth(backend: RuntimeBackend) -> RuntimeBackend {
    match dashboard_surreal_auth() {
        Some(auth) => backend.with_surreal_auth(auth),
        None => backend,
    }
}
