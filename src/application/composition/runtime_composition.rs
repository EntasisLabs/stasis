use std::sync::Arc;

use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

use crate::application::runtime::in_memory_runtime::InMemoryRuntime;
use crate::application::runtime::surreal_runtime::SurrealRuntime;
use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::llm::genai_chat_client::GenaiChatClient;
use crate::infrastructure::memory::locus_context_reader::LocusContextReader;
use crate::infrastructure::memory::locus_context_writer::LocusContextWriter;
use crate::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
use crate::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
use crate::infrastructure::memory::surreal_identity_memory_store::SurrealIdentityMemoryStore;
use crate::infrastructure::runtime::endpoint_routing_event_publisher::EndpointRoutingEventPublisher;
use crate::infrastructure::runtime::grapheme_sdk_workflow_engine::GraphemeSdkWorkflowEngine;
use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
use crate::infrastructure::runtime::in_memory_thread_store::InMemoryThreadStore;
use crate::infrastructure::runtime::surreal_cluster_node_store::SurrealClusterNodeStore;
use crate::infrastructure::runtime::surreal_delivery_endpoint_store::SurrealDeliveryEndpointStore;
use crate::infrastructure::runtime::surreal_endpoint_delivery_status_store::SurrealEndpointDeliveryStatusStore;
use crate::infrastructure::runtime::surreal_thread_store::SurrealThreadStore;
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use crate::ports::outbound::memory::memory_operations::MemoryOperations;
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
use crate::ports::outbound::runtime::endpoint_routing_policy::EndpointRoutingPolicy;
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;
use crate::ports::outbound::runtime::thread_store::ThreadStore;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

#[derive(Clone, Debug)]
pub enum RuntimeBackend {
    InMemory,
    SurrealMem {
        namespace: String,
        database: String,
        auth: Option<SurrealAuth>,
    },
    SurrealWs {
        endpoint: String,
        namespace: String,
        database: String,
        auth: Option<SurrealAuth>,
    },
    SurrealKv {
        path: String,
        namespace: String,
        database: String,
        auth: Option<SurrealAuth>,
    },
}

pub use crate::application::composition::surreal_backend_config::SurrealAuth;

impl RuntimeBackend {
    pub fn surreal_mem(namespace: impl Into<String>, database: impl Into<String>) -> Self {
        Self::SurrealMem {
            namespace: namespace.into(),
            database: database.into(),
            auth: None,
        }
    }

    pub fn surreal_ws(
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self::SurrealWs {
            endpoint: endpoint.into(),
            namespace: namespace.into(),
            database: database.into(),
            auth: None,
        }
    }

    pub fn surreal_kv(
        path: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self::SurrealKv {
            path: path.into(),
            namespace: namespace.into(),
            database: database.into(),
            auth: None,
        }
    }

    pub fn with_surreal_auth(mut self, auth: SurrealAuth) -> Self {
        match &mut self {
            Self::SurrealMem { auth: slot, .. }
            | Self::SurrealWs { auth: slot, .. }
            | Self::SurrealKv { auth: slot, .. } => *slot = Some(auth),
            Self::InMemory => {}
        }
        self
    }
}

#[derive(Clone)]
pub enum RuntimeComposition {
    InMemory(InMemoryRuntime),
    Surreal(SurrealRuntime),
}

impl RuntimeComposition {
    pub fn replace_telemetry(
        &mut self,
        metrics: Arc<dyn RuntimeMetrics>,
        tracing: Arc<dyn RuntimeTracing>,
    ) {
        match self {
            Self::InMemory(runtime) => runtime.replace_telemetry(metrics, tracing),
            Self::Surreal(runtime) => runtime.replace_telemetry(metrics, tracing),
        }
    }
}

pub struct RuntimeFactory;

impl RuntimeFactory {
    async fn connect_surreal_any(
        endpoint: &str,
        namespace: String,
        database: String,
        auth: Option<SurrealAuth>,
    ) -> Result<RuntimeComposition> {
        let db = Surreal::<Any>::init();
        db.connect(endpoint)
            .await
            .map_err(|e| StasisError::PortFailure(format!("connect surreal db ({endpoint}): {e}")))?;

        if let Some(auth) = auth {
            db.signin(Root {
                username: auth.username,
                password: auth.password,
            })
            .await
            .map_err(|e| StasisError::PortFailure(format!("signin surreal db: {e}")))?;
        }

        db.use_ns(namespace).use_db(database).await.map_err(|e| {
            StasisError::PortFailure(format!("select surreal namespace/database: {e}"))
        })?;

        SurrealIdentityMemoryStore::ensure_schema_for_db(&db).await?;

        Ok(RuntimeComposition::Surreal(SurrealRuntime::new(db)))
    }

    pub async fn build(config: RuntimeBackend) -> Result<RuntimeComposition> {
        match config {
            RuntimeBackend::InMemory => Ok(RuntimeComposition::InMemory(InMemoryRuntime::new())),
            RuntimeBackend::SurrealMem {
                namespace,
                database,
                auth,
            } => Self::connect_surreal_any("mem://", namespace, database, auth).await,
            RuntimeBackend::SurrealWs {
                endpoint,
                namespace,
                database,
                auth,
            } => Self::connect_surreal_any(&endpoint, namespace, database, auth).await,
            RuntimeBackend::SurrealKv {
                path,
                namespace,
                database,
                auth,
            } => {
                let endpoint = if path.starts_with("surrealkv://") {
                    path
                } else {
                    format!("surrealkv://{path}")
                };
                Self::connect_surreal_any(&endpoint, namespace, database, auth).await
            }
        }
    }

    pub fn from_db(db: Surreal<Any>) -> RuntimeComposition {
        RuntimeComposition::Surreal(SurrealRuntime::new(db))
    }

    pub fn default_chat_client() -> Arc<dyn AiChatClient> {
        Arc::new(GenaiChatClient::from_env())
    }

    pub fn default_workflow_engine() -> Arc<dyn WorkflowEngine> {
        Arc::new(GraphemeSdkWorkflowEngine::new())
    }

    pub async fn ensure_locus_memory_adapters(
        enable_locus_memory: bool,
        mut memory_context_reader: Option<Arc<dyn MemoryContextReader>>,
        mut memory_context_writer: Option<Arc<dyn MemoryContextWriter>>,
        mut memory_operations: Option<Arc<dyn MemoryOperations>>,
    ) -> Result<(
        Option<Arc<dyn MemoryContextReader>>,
        Option<Arc<dyn MemoryContextWriter>>,
        Option<Arc<dyn MemoryOperations>>,
    )> {
        if enable_locus_memory
            && (memory_context_reader.is_none()
                || memory_context_writer.is_none()
                || memory_operations.is_none())
        {
            let store = LocusNodeStoreFactory::in_memory().await?;
            if memory_context_reader.is_none() {
                memory_context_reader = Some(Arc::new(LocusContextReader::new(store.clone())));
            }
            if memory_context_writer.is_none() {
                memory_context_writer = Some(Arc::new(LocusContextWriter::new(store.clone())));
            }
            if memory_operations.is_none() {
                memory_operations = Some(Arc::new(LocusMemoryOperations::new(store, None)));
            }
        }

        Ok((
            memory_context_reader,
            memory_context_writer,
            memory_operations,
        ))
    }

    pub fn resolve_thread_store(
        runtime: &RuntimeComposition,
        configured: Option<Arc<dyn ThreadStore>>,
    ) -> Arc<dyn ThreadStore> {
        if let Some(store) = configured {
            return store;
        }

        match runtime {
            RuntimeComposition::InMemory(_) => Arc::new(InMemoryThreadStore::default()),
            RuntimeComposition::Surreal(rt) => Arc::new(SurrealThreadStore::new(rt.job_store.db())),
        }
    }

    pub fn resolve_cluster_node_store(
        runtime: &RuntimeComposition,
        configured: Option<Arc<dyn ClusterNodeStore>>,
    ) -> Arc<dyn ClusterNodeStore> {
        if let Some(store) = configured {
            return store;
        }

        match runtime {
            RuntimeComposition::InMemory(_) => Arc::new(InMemoryClusterNodeStore::default()),
            RuntimeComposition::Surreal(rt) => {
                Arc::new(SurrealClusterNodeStore::new(rt.job_store.db()))
            }
        }
    }

    pub fn resolve_delivery_endpoint_store(
        runtime: &RuntimeComposition,
        configured: Option<Arc<dyn DeliveryEndpointStore>>,
    ) -> Arc<dyn DeliveryEndpointStore> {
        if let Some(store) = configured {
            return store;
        }

        match runtime {
            RuntimeComposition::InMemory(_) => Arc::new(InMemoryDeliveryEndpointStore::default()),
            RuntimeComposition::Surreal(rt) => {
                Arc::new(SurrealDeliveryEndpointStore::new(rt.job_store.db()))
            }
        }
    }

    pub fn resolve_endpoint_delivery_status_store(
        runtime: &RuntimeComposition,
        configured: Option<Arc<dyn EndpointDeliveryStatusStore>>,
    ) -> Arc<dyn EndpointDeliveryStatusStore> {
        if let Some(store) = configured {
            return store;
        }

        match runtime {
            RuntimeComposition::InMemory(_) => {
                Arc::new(InMemoryEndpointDeliveryStatusStore::default())
            }
            RuntimeComposition::Surreal(rt) => {
                Arc::new(SurrealEndpointDeliveryStatusStore::new(rt.job_store.db()))
            }
        }
    }

    pub fn build_endpoint_routing_publisher(
        endpoint_store: Arc<dyn DeliveryEndpointStore>,
        status_store: Arc<dyn EndpointDeliveryStatusStore>,
        transports: &[Arc<dyn EndpointTransportPublisher>],
        routing_policy: Option<Arc<dyn EndpointRoutingPolicy>>,
    ) -> EndpointRoutingEventPublisher {
        let mut routing_publisher =
            EndpointRoutingEventPublisher::new(endpoint_store).fail_on_unsupported_protocol(false);

        if transports.is_empty() {
            routing_publisher = routing_publisher
                .with_http_webhook_transport()
                .with_tcp_socket_transport();
        } else {
            for transport in transports {
                routing_publisher = routing_publisher.with_transport_arc(transport.clone());
            }
        }

        if let Some(policy) = routing_policy {
            routing_publisher = routing_publisher.with_routing_policy_arc(policy);
        }

        routing_publisher.with_status_store_arc(status_store)
    }
}
