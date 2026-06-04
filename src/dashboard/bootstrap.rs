use std::sync::Arc;

use chrono::{Duration, Utc};

use crate::application::composition::surreal_backend_config::{
    resolve_surreal_auth_from_env, resolve_surreal_database_from_env, resolve_surreal_namespace_from_env,
};
use crate::application::config::env::{required, truthy, with_default};
use crate::application::dto::{
    HeartbeatClusterNodeRequest, RegisterClusterNodeRequest, RegisterDeliveryEndpointRequest,
};
use crate::application::runtime::in_memory_runtime::{
    InMemoryRuntime, JobExecutionOutcome, JobHandler,
};
use crate::application::runtime::runtime_factory::{
    RuntimeBackend, RuntimeComposition, SurrealAuth,
};
use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use crate::dashboard::service::RuntimeDashboardQueryService;
use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterNodeRole;
use crate::domain::runtime::delivery_endpoint::DeliveryProtocol;
use crate::domain::runtime::job::{BackoffPolicy, Job, NewJob};
use crate::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
use crate::sdk::control_plane_sdk::ControlPlaneSdk;

type InMemoryControlPlane = ControlPlaneSdk<
    CompositeControlPlaneStore<InMemoryDeliveryEndpointStore, InMemoryClusterNodeStore>,
>;

#[derive(Clone, Debug)]
pub struct DashboardBootstrapOptions {
    pub seed_demo: bool,
}

impl Default for DashboardBootstrapOptions {
    fn default() -> Self {
        Self {
            seed_demo: truthy("STASIS_DASHBOARD_DEMO_SEED"),
        }
    }
}

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

pub fn resolve_dashboard_runtime_backend() -> RuntimeBackend {
    let backend = with_default("STASIS_DASHBOARD_RUNTIME_BACKEND", "in-memory")
        .trim()
        .to_ascii_lowercase();

    match backend.as_str() {
        "in-memory" | "inmemory" => RuntimeBackend::InMemory,
        "surreal-mem" | "mem" => apply_surreal_auth(RuntimeBackend::surreal_mem(
            dashboard_surreal_namespace(),
            dashboard_surreal_database(),
        )),
        "surreal-ws" | "ws" => apply_surreal_auth(RuntimeBackend::surreal_ws(
            required("STASIS_DASHBOARD_SURREAL_ENDPOINT").unwrap_or_else(|_| {
                panic!(
                    "STASIS_DASHBOARD_SURREAL_ENDPOINT is required when STASIS_DASHBOARD_RUNTIME_BACKEND=surreal-ws"
                )
            }),
            dashboard_surreal_namespace(),
            dashboard_surreal_database(),
        )),
        "surreal-kv" | "kv" => apply_surreal_auth(RuntimeBackend::surreal_kv(
            required("STASIS_DASHBOARD_SURREAL_KV_PATH").unwrap_or_else(|_| {
                panic!(
                    "STASIS_DASHBOARD_SURREAL_KV_PATH is required when STASIS_DASHBOARD_RUNTIME_BACKEND=surreal-kv"
                )
            }),
            dashboard_surreal_namespace(),
            dashboard_surreal_database(),
        )),
        other => {
            eprintln!(
                "unknown STASIS_DASHBOARD_RUNTIME_BACKEND='{}', falling back to in-memory",
                other
            );
            RuntimeBackend::InMemory
        }
    }
}

pub async fn build_dashboard_query_service(
    options: DashboardBootstrapOptions,
) -> Result<Arc<RuntimeDashboardQueryService>> {
    let backend = resolve_dashboard_runtime_backend();

    match backend {
        RuntimeBackend::InMemory => {
            build_in_memory_dashboard_query_service(options, RuntimeBackend::InMemory).await
        }
        other => build_surreal_dashboard_query_service(options, other).await,
    }
}

async fn build_in_memory_dashboard_query_service(
    options: DashboardBootstrapOptions,
    backend: RuntimeBackend,
) -> Result<Arc<RuntimeDashboardQueryService>> {
    let endpoint_store = InMemoryDeliveryEndpointStore::default();
    let cluster_store = InMemoryClusterNodeStore::default();
    let endpoint_status_store = Arc::new(InMemoryEndpointDeliveryStatusStore::default());

    let mut builder = StasisRuntimeBuilder::new(backend)
        .with_cluster_node_store(Arc::new(cluster_store.clone()))
        .with_delivery_endpoint_store(Arc::new(endpoint_store.clone()))
        .with_endpoint_delivery_status_store(endpoint_status_store.clone());

    builder = apply_dashboard_builder_options(builder, &options)?;

    let runtime = builder.build().await?;
    let RuntimeComposition::InMemory(runtime) = runtime else {
        return Err(crate::domain::errors::StasisError::PortFailure(
            "expected in-memory runtime composition".to_string(),
        ));
    };

    if options.seed_demo {
        seed_demo_jobs(&runtime).await;
    }

    let control_store = CompositeControlPlaneStore::new(endpoint_store, cluster_store);
    let control_plane =
        ControlPlaneSdk::new_with_status_store(control_store, endpoint_status_store.clone());

    if options.seed_demo {
        seed_control_plane_data(&control_plane, endpoint_status_store).await;
    }

    Ok(Arc::new(RuntimeDashboardQueryService::from_in_memory_composition(
        runtime,
        control_plane,
    )))
}

async fn build_surreal_dashboard_query_service(
    options: DashboardBootstrapOptions,
    backend: RuntimeBackend,
) -> Result<Arc<RuntimeDashboardQueryService>> {
    if options.seed_demo {
        eprintln!("dashboard demo seed mode is ignored for surreal runtime backends");
    }

    let builder = apply_dashboard_builder_options(StasisRuntimeBuilder::new(backend), &options)?;
    let runtime = builder.build().await?;

    Ok(Arc::new(RuntimeDashboardQueryService::from_runtime_composition(
        runtime,
    )))
}

fn apply_dashboard_builder_options(
    mut builder: StasisRuntimeBuilder,
    options: &DashboardBootstrapOptions,
) -> Result<StasisRuntimeBuilder> {
    if dashboard_locus_memory_enabled() {
        builder = builder.with_locus_memory();
    }

    if truthy("STASIS_DASHBOARD_LOGGING_CHAT") {
        builder = builder.with_logging_chat_middleware();
    }

    if options.seed_demo {
        builder = builder
            .with_extra_handler(DemoSuccessHandler)
            .with_extra_handler(DemoFatalHandler);
    }

    #[cfg(feature = "otel")]
    {
        use crate::infrastructure::telemetry::otel::otel_enabled;

        if otel_enabled() {
            builder = builder.with_otel_from_env()?;
        }
    }

    Ok(builder)
}

fn dashboard_locus_memory_enabled() -> bool {
    truthy("STASIS_DASHBOARD_LOCUS_MEMORY")
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

async fn seed_demo_jobs(runtime: &InMemoryRuntime) {
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
    control_plane: &InMemoryControlPlane,
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
