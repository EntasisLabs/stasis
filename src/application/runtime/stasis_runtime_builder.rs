use std::sync::Arc;

use async_trait::async_trait;

use crate::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
use crate::application::runtime::agent_session_job_handler::AgentSessionJobHandler;
use crate::application::runtime::agent_turn_job_handler::AgentTurnJobHandler;
use crate::application::runtime::chat_client_middleware::ChatClientMiddleware;
use crate::application::runtime::concurrent_pattern_job_handler::ConcurrentPatternJobHandler;
use crate::application::runtime::coordinator_failover_job_handler::CoordinatorFailoverJobHandler;
use crate::application::runtime::default_chat_middlewares::{
    CacheChatMiddleware, LoggingChatMiddleware, TelemetryChatMiddleware,
    ToolCallInterceptionChatMiddleware,
};
use crate::application::runtime::grapheme_echo_job_handler::GraphemeEchoJobHandler;
use crate::application::runtime::grapheme_healthcheck_job_handler::GraphemeHealthcheckJobHandler;
use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use crate::application::runtime::grapheme_textops_job_handler::GraphemeTextOpsJobHandler;
use crate::application::runtime::handoff_pattern_job_handler::HandoffPatternJobHandler;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::memory_aggregate_job_handler::MemoryAggregateJobHandler;
use crate::application::runtime::memory_evict_job_handler::MemoryEvictJobHandler;
use crate::application::runtime::memory_find_job_handler::MemoryFindJobHandler;
use crate::application::runtime::memory_graph_job_handler::MemoryGraphJobHandler;
use crate::application::runtime::memory_recall_job_handler::MemoryRecallJobHandler;
use crate::application::runtime::memory_rollup_job_handler::MemoryRollupJobHandler;
use crate::application::runtime::memory_schema_job_handler::MemorySchemaJobHandler;
use crate::application::runtime::memory_transform_job_handler::MemoryTransformJobHandler;
use crate::application::runtime::orchestrator_pattern_job_handler::OrchestratorPatternJobHandler;
use crate::application::runtime::prompt_chat_job_handler::PromptChatJobHandler;
use crate::application::runtime::queue_ownership_rebalance_job_handler::QueueOwnershipRebalanceJobHandler;
use crate::application::runtime::runtime_factory::{
    RuntimeBackend, RuntimeComposition, RuntimeFactory,
};
use crate::application::runtime::sequential_pattern_job_handler::SequentialPatternJobHandler;
use crate::application::runtime::tool_loop_job_handler::ToolLoopJobHandler;
use crate::application::telemetry::operation::OperationTelemetry;
use crate::domain::errors::Result;
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::ai_chat_response_cache::AiChatResponseCache;
use crate::ports::outbound::ai_chat_tool_interceptor::AiChatToolInterceptor;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use crate::ports::outbound::memory::memory_operations::MemoryOperations;
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
use crate::ports::outbound::runtime::endpoint_routing_policy::EndpointRoutingPolicy;
use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_telemetry::RuntimeTelemetry;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;
use crate::ports::outbound::runtime::thread_store::ThreadStore;

#[derive(Clone)]
struct DelegatingJobHandler {
    inner: Arc<dyn JobHandler>,
}

#[async_trait]
impl JobHandler for DelegatingJobHandler {
    fn job_type(&self) -> &'static str {
        self.inner.job_type()
    }

    async fn execute(&self, job: &crate::domain::runtime::job::Job) -> Result<JobExecutionOutcome> {
        self.inner.execute(job).await
    }
}

#[derive(Clone)]
pub struct StasisRuntimeBuilder {
    backend: RuntimeBackend,
    chat_client: Option<Arc<dyn AiChatClient>>,
    chat_middlewares: Vec<Arc<dyn ChatClientMiddleware>>,
    memory_context_reader: Option<Arc<dyn MemoryContextReader>>,
    memory_context_writer: Option<Arc<dyn MemoryContextWriter>>,
    identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
    memory_operations: Option<Arc<dyn MemoryOperations>>,
    thread_store: Option<Arc<dyn ThreadStore>>,
    cluster_node_store: Option<Arc<dyn ClusterNodeStore>>,
    delivery_endpoint_store: Option<Arc<dyn DeliveryEndpointStore>>,
    endpoint_delivery_status_store: Option<Arc<dyn EndpointDeliveryStatusStore>>,
    endpoint_transport_publishers: Vec<Arc<dyn EndpointTransportPublisher>>,
    endpoint_routing_policy: Option<Arc<dyn EndpointRoutingPolicy>>,
    enable_endpoint_routing_delivery: bool,
    enable_locus_memory: bool,
    tool_registry: InMemoryToolRegistry,
    include_grapheme_handlers: bool,
    include_prompt_handler: bool,
    include_tool_loop_handler: bool,
    include_agent_handlers: bool,
    include_memory_operation_handlers: bool,
    include_orchestration_pattern_handlers: bool,
    include_cluster_control_handlers: bool,
    extra_handlers: Vec<Arc<dyn JobHandler>>,
    runtime_telemetry_metrics: Option<Arc<dyn RuntimeMetrics>>,
    runtime_telemetry_tracing: Option<Arc<dyn RuntimeTracing>>,
    explicit_telemetry_chat_middleware: bool,
}

macro_rules! define_arc_option_setter {
    ($fn_name:ident, $field:ident, $ty:ty) => {
        pub fn $fn_name(mut self, value: Arc<$ty>) -> Self {
            self.$field = Some(value);
            self
        }
    };
}

macro_rules! define_enable_flag_setter {
    ($fn_name:ident, $field:ident) => {
        pub fn $fn_name(mut self) -> Self {
            self.$field = true;
            self
        }
    };
}

macro_rules! define_disable_flag_setter {
    ($fn_name:ident, $field:ident) => {
        pub fn $fn_name(mut self) -> Self {
            self.$field = false;
            self
        }
    };
}

impl StasisRuntimeBuilder {
    pub fn new(backend: RuntimeBackend) -> Self {
        Self {
            backend,
            chat_client: None,
            chat_middlewares: Vec::new(),
            memory_context_reader: None,
            memory_context_writer: None,
            identity_memory_store: None,
            memory_operations: None,
            thread_store: None,
            cluster_node_store: None,
            delivery_endpoint_store: None,
            endpoint_delivery_status_store: None,
            endpoint_transport_publishers: Vec::new(),
            endpoint_routing_policy: None,
            enable_endpoint_routing_delivery: false,
            enable_locus_memory: false,
            tool_registry: InMemoryToolRegistry::default(),
            include_grapheme_handlers: true,
            include_prompt_handler: true,
            include_tool_loop_handler: true,
            include_agent_handlers: true,
            include_memory_operation_handlers: true,
            include_orchestration_pattern_handlers: true,
            include_cluster_control_handlers: true,
            extra_handlers: Vec::new(),
            runtime_telemetry_metrics: None,
            runtime_telemetry_tracing: None,
            explicit_telemetry_chat_middleware: false,
        }
    }

    define_arc_option_setter!(with_chat_client, chat_client, dyn AiChatClient);

    pub fn with_chat_middleware<M: ChatClientMiddleware + 'static>(
        mut self,
        middleware: M,
    ) -> Self {
        self.chat_middlewares.push(Arc::new(middleware));
        self
    }

    pub fn with_chat_middleware_arc(mut self, middleware: Arc<dyn ChatClientMiddleware>) -> Self {
        self.chat_middlewares.push(middleware);
        self
    }

    pub fn with_logging_chat_middleware(self) -> Self {
        self.with_chat_middleware(LoggingChatMiddleware)
    }

    pub fn with_telemetry_chat_middleware(mut self, metrics: Arc<dyn RuntimeMetrics>) -> Self {
        self.explicit_telemetry_chat_middleware = true;
        self.with_chat_middleware(TelemetryChatMiddleware::new(metrics))
    }

    pub fn with_runtime_telemetry<T: RuntimeTelemetry + 'static>(
        mut self,
        telemetry: Arc<T>,
    ) -> Self {
        self.runtime_telemetry_metrics = Some(telemetry.clone());
        self.runtime_telemetry_tracing = Some(telemetry);
        self
    }

    #[cfg(feature = "otel")]
    pub fn with_otel_from_env(self) -> Result<Self> {
        let telemetry = crate::infrastructure::telemetry::OpenTelemetryTelemetry::from_env()?;
        Ok(self.with_runtime_telemetry(telemetry))
    }

    #[cfg(not(feature = "otel"))]
    pub fn with_otel_from_env(self) -> Result<Self> {
        Err(crate::domain::errors::StasisError::PortFailure(
            "OpenTelemetry support requires the `otel` Cargo feature".to_string(),
        ))
    }

    pub fn with_cache_chat_middleware(self, cache: Arc<dyn AiChatResponseCache>) -> Self {
        self.with_chat_middleware(CacheChatMiddleware::new(cache))
    }

    pub fn with_tool_call_interception_chat_middleware(
        self,
        interceptor: Arc<dyn AiChatToolInterceptor>,
    ) -> Self {
        self.with_chat_middleware(ToolCallInterceptionChatMiddleware::new(interceptor))
    }

    define_arc_option_setter!(
        with_memory_context_reader,
        memory_context_reader,
        dyn MemoryContextReader
    );
    define_arc_option_setter!(
        with_memory_context_writer,
        memory_context_writer,
        dyn MemoryContextWriter
    );
    define_enable_flag_setter!(with_locus_memory, enable_locus_memory);
    define_arc_option_setter!(
        with_identity_memory_store,
        identity_memory_store,
        dyn IdentityMemoryStore
    );
    define_arc_option_setter!(with_memory_operations, memory_operations, dyn MemoryOperations);
    define_arc_option_setter!(with_thread_store, thread_store, dyn ThreadStore);
    define_arc_option_setter!(with_cluster_node_store, cluster_node_store, dyn ClusterNodeStore);
    define_arc_option_setter!(
        with_delivery_endpoint_store,
        delivery_endpoint_store,
        dyn DeliveryEndpointStore
    );
    define_arc_option_setter!(
        with_endpoint_delivery_status_store,
        endpoint_delivery_status_store,
        dyn EndpointDeliveryStatusStore
    );

    pub fn with_endpoint_transport_publisher<P: EndpointTransportPublisher + 'static>(
        mut self,
        transport: P,
    ) -> Self {
        self.endpoint_transport_publishers.push(Arc::new(transport));
        self
    }

    pub fn with_endpoint_transport_publisher_arc(
        mut self,
        transport: Arc<dyn EndpointTransportPublisher>,
    ) -> Self {
        self.endpoint_transport_publishers.push(transport);
        self
    }

    define_enable_flag_setter!(with_endpoint_routing_delivery, enable_endpoint_routing_delivery);

    pub fn with_endpoint_routing_policy<P: EndpointRoutingPolicy + 'static>(
        mut self,
        policy: P,
    ) -> Self {
        self.endpoint_routing_policy = Some(Arc::new(policy));
        self
    }

    define_arc_option_setter!(
        with_endpoint_routing_policy_arc,
        endpoint_routing_policy,
        dyn EndpointRoutingPolicy
    );

    pub fn with_tool<T: StasisTool + 'static>(self, tool: T) -> Result<Self> {
        self.tool_registry.register_tool(tool)?;
        Ok(self)
    }

    pub fn with_extra_handler<H: JobHandler + 'static>(mut self, handler: H) -> Self {
        self.extra_handlers.push(Arc::new(handler));
        self
    }

    define_disable_flag_setter!(without_grapheme_handlers, include_grapheme_handlers);
    define_disable_flag_setter!(without_prompt_handler, include_prompt_handler);
    define_disable_flag_setter!(without_tool_loop_handler, include_tool_loop_handler);
    define_disable_flag_setter!(without_agent_handlers, include_agent_handlers);
    define_disable_flag_setter!(
        without_memory_operation_handlers,
        include_memory_operation_handlers
    );
    define_disable_flag_setter!(
        without_orchestration_pattern_handlers,
        include_orchestration_pattern_handlers
    );
    define_disable_flag_setter!(without_cluster_control_handlers, include_cluster_control_handlers);

    pub async fn build(self) -> Result<RuntimeComposition> {
        let mut runtime = RuntimeFactory::build(self.backend).await?;
        let mut chat_middlewares = self.chat_middlewares;

        if let (Some(metrics), Some(tracing)) = (
            self.runtime_telemetry_metrics.clone(),
            self.runtime_telemetry_tracing.clone(),
        ) {
            runtime.replace_telemetry(metrics.clone(), tracing.clone());
            if !self.explicit_telemetry_chat_middleware {
                chat_middlewares.push(Arc::new(
                    TelemetryChatMiddleware::new(metrics.clone()).with_tracing(tracing),
                ));
            }
        }

        let workflow_engine = RuntimeFactory::default_workflow_engine();
        let chat_client = self
            .chat_client
            .unwrap_or_else(RuntimeFactory::default_chat_client);
        let chat_client = Self::compose_chat_client(chat_client, &chat_middlewares);
        let (memory_context_reader, memory_context_writer, memory_operations) =
            RuntimeFactory::ensure_locus_memory_adapters(
                self.enable_locus_memory,
                self.memory_context_reader,
                self.memory_context_writer,
                self.memory_operations,
            )
            .await?;
        let identity_memory_store = self.identity_memory_store;
        let default_thread_store = self.thread_store.clone();
        let configured_cluster_store = self.cluster_node_store.clone();
        let configured_endpoint_store = self.delivery_endpoint_store.clone();
        let configured_endpoint_status_store = self.endpoint_delivery_status_store.clone();
        let configured_endpoint_transports = self.endpoint_transport_publishers.clone();
        let configured_endpoint_routing_policy = self.endpoint_routing_policy.clone();

        let tool_registry = Arc::new(self.tool_registry);
        let operation_telemetry = self
            .runtime_telemetry_metrics
            .as_ref()
            .and_then(|metrics| {
                self.runtime_telemetry_tracing.as_ref().map(|tracing| {
                    OperationTelemetry::new(metrics.clone(), tracing.clone())
                })
            });

        match &runtime {
            RuntimeComposition::InMemory(rt) => {
                let thread_store =
                    RuntimeFactory::resolve_thread_store(&runtime, default_thread_store.clone());
                let cluster_store = RuntimeFactory::resolve_cluster_node_store(
                    &runtime,
                    configured_cluster_store.clone(),
                );

                if self.enable_endpoint_routing_delivery {
                    let endpoint_store = RuntimeFactory::resolve_delivery_endpoint_store(
                        &runtime,
                        configured_endpoint_store.clone(),
                    );
                    let status_store = RuntimeFactory::resolve_endpoint_delivery_status_store(
                        &runtime,
                        configured_endpoint_status_store.clone(),
                    );

                    let routing_publisher = RuntimeFactory::build_endpoint_routing_publisher(
                        endpoint_store,
                        status_store,
                        &configured_endpoint_transports,
                        configured_endpoint_routing_policy.clone(),
                    );

                    rt.register_event_publisher(routing_publisher)?;
                }

                if self.include_grapheme_handlers {
                    rt.register_handler(
                        GraphemeJobHandler::new(workflow_engine.clone())
                            .with_operation_telemetry(operation_telemetry.clone()),
                    )?;
                    rt.register_handler(GraphemeHealthcheckJobHandler::new(
                        workflow_engine.clone(),
                    ))?;
                    rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
                }

                if self.include_prompt_handler {
                    rt.register_handler(PromptChatJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                }

                if self.include_tool_loop_handler {
                    rt.register_handler(ToolLoopJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        tool_registry.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                }

                if self.include_agent_handlers {
                    rt.register_handler(AgentTurnJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        tool_registry.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                    rt.register_handler(AgentSessionJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        tool_registry.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                }

                if self.include_memory_operation_handlers {
                    if let Some(reader) = memory_context_reader.clone() {
                        rt.register_handler(
                            MemoryRecallJobHandler::new(reader.clone())
                                .with_operation_telemetry(operation_telemetry.clone()),
                        )?;
                        rt.register_handler(MemoryFindJobHandler::new(reader.clone()))?;
                        rt.register_handler(MemoryGraphJobHandler::new(reader))?;
                    }
                    if let Some(operations) = memory_operations.clone() {
                        rt.register_handler(MemoryAggregateJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryTransformJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryRollupJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryEvictJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemorySchemaJobHandler::new(operations))?;
                    }
                }

                if self.include_orchestration_pattern_handlers {
                    rt.register_handler(ConcurrentPatternJobHandler::new_with_thread_store_and_memory(
                        chat_client.clone(),
                        tool_registry.clone(),
                        Some(thread_store.clone()),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                    rt.register_handler(HandoffPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store.clone()),
                    ))?;
                    rt.register_handler(OrchestratorPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store.clone()),
                    ))?;
                    rt.register_handler(SequentialPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store),
                    ))?;
                }

                if self.include_cluster_control_handlers {
                    rt.register_handler(CoordinatorFailoverJobHandler::new(cluster_store.clone()))?;
                    rt.register_handler(QueueOwnershipRebalanceJobHandler::new(cluster_store))?;
                }

                for handler in &self.extra_handlers {
                    rt.register_handler(DelegatingJobHandler {
                        inner: handler.clone(),
                    })?;
                }
            }
            RuntimeComposition::Surreal(rt) => {
                let thread_store =
                    RuntimeFactory::resolve_thread_store(&runtime, default_thread_store.clone());
                let cluster_store = RuntimeFactory::resolve_cluster_node_store(
                    &runtime,
                    configured_cluster_store.clone(),
                );

                if self.enable_endpoint_routing_delivery {
                    let endpoint_store = RuntimeFactory::resolve_delivery_endpoint_store(
                        &runtime,
                        configured_endpoint_store.clone(),
                    );
                    let status_store = RuntimeFactory::resolve_endpoint_delivery_status_store(
                        &runtime,
                        configured_endpoint_status_store.clone(),
                    );

                    let routing_publisher = RuntimeFactory::build_endpoint_routing_publisher(
                        endpoint_store,
                        status_store,
                        &configured_endpoint_transports,
                        configured_endpoint_routing_policy.clone(),
                    );

                    rt.register_event_publisher(routing_publisher)?;
                }

                if self.include_grapheme_handlers {
                    rt.register_handler(
                        GraphemeJobHandler::new(workflow_engine.clone())
                            .with_operation_telemetry(operation_telemetry.clone()),
                    )?;
                    rt.register_handler(GraphemeHealthcheckJobHandler::new(
                        workflow_engine.clone(),
                    ))?;
                    rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
                }

                if self.include_prompt_handler {
                    rt.register_handler(PromptChatJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                }

                if self.include_tool_loop_handler {
                    rt.register_handler(ToolLoopJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        tool_registry.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                }

                if self.include_agent_handlers {
                    rt.register_handler(AgentTurnJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        tool_registry.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                    rt.register_handler(AgentSessionJobHandler::new_with_memory_and_identity(
                        chat_client.clone(),
                        tool_registry.clone(),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                }

                if self.include_memory_operation_handlers {
                    if let Some(reader) = memory_context_reader.clone() {
                        rt.register_handler(
                            MemoryRecallJobHandler::new(reader.clone())
                                .with_operation_telemetry(operation_telemetry.clone()),
                        )?;
                        rt.register_handler(MemoryFindJobHandler::new(reader.clone()))?;
                        rt.register_handler(MemoryGraphJobHandler::new(reader))?;
                    }
                    if let Some(operations) = memory_operations.clone() {
                        rt.register_handler(MemoryAggregateJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryTransformJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryRollupJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryEvictJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemorySchemaJobHandler::new(operations))?;
                    }
                }

                if self.include_orchestration_pattern_handlers {
                    rt.register_handler(ConcurrentPatternJobHandler::new_with_thread_store_and_memory(
                        chat_client.clone(),
                        tool_registry.clone(),
                        Some(thread_store.clone()),
                        memory_context_reader.clone(),
                        memory_context_writer.clone(),
                        identity_memory_store.clone(),
                    ))?;
                    rt.register_handler(HandoffPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store.clone()),
                    ))?;
                    rt.register_handler(OrchestratorPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store.clone()),
                    ))?;
                    rt.register_handler(SequentialPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store),
                    ))?;
                }

                if self.include_cluster_control_handlers {
                    rt.register_handler(CoordinatorFailoverJobHandler::new(cluster_store.clone()))?;
                    rt.register_handler(QueueOwnershipRebalanceJobHandler::new(cluster_store))?;
                }

                for handler in &self.extra_handlers {
                    rt.register_handler(DelegatingJobHandler {
                        inner: handler.clone(),
                    })?;
                }
            }
        }

        Ok(runtime)
    }

    /// Builds and returns the primary runtime facade.
    pub async fn build_stasis_runtime(self) -> Result<crate::sdk::runtime_sdk::StasisRuntime> {
        let composition = self.build().await?;
        Ok(crate::sdk::runtime_sdk::RuntimeSdk::new(composition))
    }

    fn compose_chat_client(
        chat_client: Arc<dyn AiChatClient>,
        middlewares: &[Arc<dyn ChatClientMiddleware>],
    ) -> Arc<dyn AiChatClient> {
        let mut wrapped = chat_client;
        for middleware in middlewares.iter().rev() {
            wrapped = middleware.wrap(wrapped);
        }
        wrapped
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use async_trait::async_trait;
    use chrono::Utc;

    use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
    use crate::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition};
    use crate::domain::errors::{Result, StasisError};
    use crate::domain::runtime::delivery_endpoint::{
        DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
    };
    use crate::domain::runtime::job::{BackoffPolicy, NewJob};
    use crate::domain::runtime::outbox::OutboxEvent;
    use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
    use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;

    use super::StasisRuntimeBuilder;

    #[derive(Clone)]
    struct SuccessHandler;

    #[async_trait]
    impl JobHandler for SuccessHandler {
        fn job_type(&self) -> &'static str {
            "test.success"
        }

        async fn execute(
            &self,
            _job: &crate::domain::runtime::job::Job,
        ) -> Result<JobExecutionOutcome> {
            Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: "sttp:out:test".to_string(),
                execution_id: Some("exec:test".to_string()),
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

    #[tokio::test]
    async fn builder_wires_endpoint_routing_delivery_for_in_memory_runtime() {
        let endpoint_store = InMemoryDeliveryEndpointStore::default();
        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.webhook.builder".to_string(),
                name: "Builder Webhook".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/hook".to_string(),
                metadata: None,
                created_at: Utc::now(),
            })
            .await
            .expect("endpoint should insert");

        let calls = Arc::new(RwLock::new(Vec::new()));
        let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
            .with_delivery_endpoint_store(Arc::new(endpoint_store))
            .with_endpoint_transport_publisher(RecordingTransport {
                calls: Arc::clone(&calls),
            })
            .with_endpoint_routing_delivery()
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

        let RuntimeComposition::InMemory(rt) = runtime else {
            panic!("expected in-memory runtime composition");
        };

        let now = Utc::now();
        rt.enqueue(NewJob {
            id: "job-builder-routing".to_string(),
            queue: "default".to_string(),
            job_type: "test.success".to_string(),
            payload_ref: "sttp:in:test".to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-builder-routing".to_string(),
            correlation_id: "corr-builder-routing".to_string(),
            causation_id: "cause-builder-routing".to_string(),
            trace_id: "trace-builder-routing".to_string(),
            sttp_input_node_id: "sttp:in:test".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy::default(),
        })
        .await
        .expect("job should enqueue");

        rt.process_once("default", "worker-builder", now)
            .await
            .expect("process should succeed");

        let published = rt
            .publish_pending_events(10, now)
            .await
            .expect("publish should succeed");
        assert_eq!(published, 1);

        let calls = calls.read().expect("calls read lock should succeed");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], "endpoint.webhook.builder");
    }
}
