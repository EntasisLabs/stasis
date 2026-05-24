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
use crate::domain::errors::Result;
use crate::infrastructure::llm::genai_chat_client::GenaiChatClient;
use crate::infrastructure::memory::locus_context_reader::LocusContextReader;
use crate::infrastructure::memory::locus_context_writer::LocusContextWriter;
use crate::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
use crate::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
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
        }
    }

    pub fn with_chat_client(mut self, chat_client: Arc<dyn AiChatClient>) -> Self {
        self.chat_client = Some(chat_client);
        self
    }

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

    pub fn with_telemetry_chat_middleware(self, metrics: Arc<dyn RuntimeMetrics>) -> Self {
        self.with_chat_middleware(TelemetryChatMiddleware::new(metrics))
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

    pub fn with_memory_context_reader(
        mut self,
        memory_context_reader: Arc<dyn MemoryContextReader>,
    ) -> Self {
        self.memory_context_reader = Some(memory_context_reader);
        self
    }

    pub fn with_memory_context_writer(
        mut self,
        memory_context_writer: Arc<dyn MemoryContextWriter>,
    ) -> Self {
        self.memory_context_writer = Some(memory_context_writer);
        self
    }

    pub fn with_locus_memory(mut self) -> Self {
        self.enable_locus_memory = true;
        self
    }

    pub fn with_identity_memory_store(
        mut self,
        identity_memory_store: Arc<dyn IdentityMemoryStore>,
    ) -> Self {
        self.identity_memory_store = Some(identity_memory_store);
        self
    }

    pub fn with_memory_operations(mut self, memory_operations: Arc<dyn MemoryOperations>) -> Self {
        self.memory_operations = Some(memory_operations);
        self
    }

    pub fn with_thread_store(mut self, thread_store: Arc<dyn ThreadStore>) -> Self {
        self.thread_store = Some(thread_store);
        self
    }

    pub fn with_cluster_node_store(
        mut self,
        cluster_node_store: Arc<dyn ClusterNodeStore>,
    ) -> Self {
        self.cluster_node_store = Some(cluster_node_store);
        self
    }

    pub fn with_delivery_endpoint_store(
        mut self,
        delivery_endpoint_store: Arc<dyn DeliveryEndpointStore>,
    ) -> Self {
        self.delivery_endpoint_store = Some(delivery_endpoint_store);
        self
    }

    pub fn with_endpoint_delivery_status_store(
        mut self,
        status_store: Arc<dyn EndpointDeliveryStatusStore>,
    ) -> Self {
        self.endpoint_delivery_status_store = Some(status_store);
        self
    }

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

    pub fn with_endpoint_routing_delivery(mut self) -> Self {
        self.enable_endpoint_routing_delivery = true;
        self
    }

    pub fn with_endpoint_routing_policy<P: EndpointRoutingPolicy + 'static>(
        mut self,
        policy: P,
    ) -> Self {
        self.endpoint_routing_policy = Some(Arc::new(policy));
        self
    }

    pub fn with_endpoint_routing_policy_arc(
        mut self,
        policy: Arc<dyn EndpointRoutingPolicy>,
    ) -> Self {
        self.endpoint_routing_policy = Some(policy);
        self
    }

    pub fn with_tool<T: StasisTool + 'static>(self, tool: T) -> Result<Self> {
        self.tool_registry.register_tool(tool)?;
        Ok(self)
    }

    pub fn with_extra_handler<H: JobHandler + 'static>(mut self, handler: H) -> Self {
        self.extra_handlers.push(Arc::new(handler));
        self
    }

    pub fn without_grapheme_handlers(mut self) -> Self {
        self.include_grapheme_handlers = false;
        self
    }

    pub fn without_prompt_handler(mut self) -> Self {
        self.include_prompt_handler = false;
        self
    }

    pub fn without_tool_loop_handler(mut self) -> Self {
        self.include_tool_loop_handler = false;
        self
    }

    pub fn without_agent_handlers(mut self) -> Self {
        self.include_agent_handlers = false;
        self
    }

    pub fn without_memory_operation_handlers(mut self) -> Self {
        self.include_memory_operation_handlers = false;
        self
    }

    pub fn without_orchestration_pattern_handlers(mut self) -> Self {
        self.include_orchestration_pattern_handlers = false;
        self
    }

    pub fn without_cluster_control_handlers(mut self) -> Self {
        self.include_cluster_control_handlers = false;
        self
    }

    pub async fn build(self) -> Result<RuntimeComposition> {
        let runtime = RuntimeFactory::build(self.backend).await?;
        let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());
        let chat_client = self
            .chat_client
            .unwrap_or_else(|| Arc::new(GenaiChatClient::from_env()));
        let chat_client = Self::compose_chat_client(chat_client, &self.chat_middlewares);
        let mut memory_context_reader = self.memory_context_reader;
        let mut memory_context_writer = self.memory_context_writer;
        let identity_memory_store = self.identity_memory_store;
        let mut memory_operations = self.memory_operations;
        let default_thread_store = self.thread_store.clone();
        let configured_cluster_store = self.cluster_node_store.clone();
        let configured_endpoint_store = self.delivery_endpoint_store.clone();
        let configured_endpoint_status_store = self.endpoint_delivery_status_store.clone();
        let configured_endpoint_transports = self.endpoint_transport_publishers.clone();
        let configured_endpoint_routing_policy = self.endpoint_routing_policy.clone();

        if self.enable_locus_memory
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

        let tool_registry = Arc::new(self.tool_registry);

        match &runtime {
            RuntimeComposition::InMemory(rt) => {
                let thread_store = default_thread_store
                    .clone()
                    .unwrap_or_else(|| Arc::new(InMemoryThreadStore::default()));
                let cluster_store = configured_cluster_store
                    .clone()
                    .unwrap_or_else(|| Arc::new(InMemoryClusterNodeStore::default()));

                if self.enable_endpoint_routing_delivery {
                    let endpoint_store = configured_endpoint_store
                        .clone()
                        .unwrap_or_else(|| Arc::new(InMemoryDeliveryEndpointStore::default()));
                    let status_store =
                        configured_endpoint_status_store.clone().unwrap_or_else(|| {
                            Arc::new(InMemoryEndpointDeliveryStatusStore::default())
                        });

                    let mut routing_publisher = EndpointRoutingEventPublisher::new(endpoint_store)
                        .fail_on_unsupported_protocol(false);

                    if configured_endpoint_transports.is_empty() {
                        routing_publisher = routing_publisher
                            .with_http_webhook_transport()
                            .with_tcp_socket_transport();
                    } else {
                        for transport in &configured_endpoint_transports {
                            routing_publisher =
                                routing_publisher.with_transport_arc(transport.clone());
                        }
                    }

                    if let Some(policy) = configured_endpoint_routing_policy.clone() {
                        routing_publisher = routing_publisher.with_routing_policy_arc(policy);
                    }

                    routing_publisher = routing_publisher.with_status_store_arc(status_store);

                    rt.register_event_publisher(routing_publisher)?;
                }

                if self.include_grapheme_handlers {
                    rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
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
                        rt.register_handler(MemoryRecallJobHandler::new(reader))?;
                    }
                    if let Some(operations) = memory_operations.clone() {
                        rt.register_handler(MemoryAggregateJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryTransformJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryRollupJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemorySchemaJobHandler::new(operations))?;
                    }
                }

                if self.include_orchestration_pattern_handlers {
                    rt.register_handler(ConcurrentPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store.clone()),
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
                let thread_store = default_thread_store
                    .clone()
                    .unwrap_or_else(|| Arc::new(SurrealThreadStore::new(rt.job_store.db())));
                let cluster_store = configured_cluster_store
                    .clone()
                    .unwrap_or_else(|| Arc::new(SurrealClusterNodeStore::new(rt.job_store.db())));

                if self.enable_endpoint_routing_delivery {
                    let endpoint_store = configured_endpoint_store.clone().unwrap_or_else(|| {
                        Arc::new(SurrealDeliveryEndpointStore::new(rt.job_store.db()))
                    });
                    let status_store =
                        configured_endpoint_status_store.clone().unwrap_or_else(|| {
                            Arc::new(SurrealEndpointDeliveryStatusStore::new(rt.job_store.db()))
                        });

                    let mut routing_publisher = EndpointRoutingEventPublisher::new(endpoint_store)
                        .fail_on_unsupported_protocol(false);

                    if configured_endpoint_transports.is_empty() {
                        routing_publisher = routing_publisher
                            .with_http_webhook_transport()
                            .with_tcp_socket_transport();
                    } else {
                        for transport in &configured_endpoint_transports {
                            routing_publisher =
                                routing_publisher.with_transport_arc(transport.clone());
                        }
                    }

                    if let Some(policy) = configured_endpoint_routing_policy.clone() {
                        routing_publisher = routing_publisher.with_routing_policy_arc(policy);
                    }

                    routing_publisher = routing_publisher.with_status_store_arc(status_store);

                    rt.register_event_publisher(routing_publisher)?;
                }

                if self.include_grapheme_handlers {
                    rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
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
                        rt.register_handler(MemoryRecallJobHandler::new(reader))?;
                    }
                    if let Some(operations) = memory_operations.clone() {
                        rt.register_handler(MemoryAggregateJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryTransformJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemoryRollupJobHandler::new(operations.clone()))?;
                        rt.register_handler(MemorySchemaJobHandler::new(operations))?;
                    }
                }

                if self.include_orchestration_pattern_handlers {
                    rt.register_handler(ConcurrentPatternJobHandler::new_with_thread_store(
                        chat_client.clone(),
                        Some(thread_store.clone()),
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
