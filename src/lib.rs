pub mod application;
pub mod dashboard;
pub mod domain;
pub mod infrastructure;
pub mod ports;
pub mod sdk;

pub mod prelude {
    pub use crate::application::dto::{
        ClusterForwardOutcomeRow, ClusterNodeHealthRow, EndpointDiagnosticsReadModelRow,
        EndpointFailureRateTrendRow, EndpointFailureTrendDirection, ForwardClusterCommandRequest,
        ForwardClusterCommandResponse, HeartbeatClusterNodeRequest,
        InitiateCoordinatorFailoverRequest, InitiateCoordinatorFailoverResponse,
        InitiateCoordinatorHandoffRequest, InitiateCoordinatorHandoffResponse, InvokeAgentRequest,
        ListClusterForwardOutcomesRequest, ListClusterNodeHealthRequest,
        ListEndpointDiagnosticsReadModelRequest, ListEndpointFailureRateTrendsRequest,
        ListQueueOwnershipHealthRequest, ListTopUnhealthyEndpointsRequest,
        PruneEndpointDeliveryStatusesRequest, PruneExpiredClusterNodesRequest,
        QueueOwnershipHealthRow, RebalanceQueueOwnershipRequest, RebalanceQueueOwnershipResponse,
        RegisterAgentRequest, RegisterClusterNodeRequest, RegisterDeliveryEndpointRequest,
        RegisterDeliveryEndpointResponse, RunClusterHeartbeatSweepRequest,
        RunClusterHeartbeatSweepResponse, SetDeliveryEndpointEnabledRequest,
    };
    pub use crate::application::orchestration::agent_session_payload::{
        AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode,
        AgentTurnJobPayload, ConcurrentBranchJobPayload, ConcurrentPatternJobPayload,
        HandoffPatternJobPayload, HandoffTurnJobPayload, OrchestratorPatternJobPayload,
        OrchestratorRouteJobPayload, PromptJobPayload, SequentialPatternJobPayload,
        SequentialStageJobPayload, ToolLoopJobPayload,
    };
    pub use crate::application::orchestration::agent_session_pipeline::{
        AgentIdentity, AgentParticipant, AgentSelectionStrategy, AgentSessionCoordinator,
        AgentSessionPipeline, AgentSessionRunRequest, AgentSessionRunResponse,
        AgentTerminationStrategy, AgentTurnExecutionPolicy, AgentTurnExecutionRequest,
        AgentTurnExecutionResponse, AgentTurnRecord, MaxTurnsTerminationStrategy,
        RoundRobinSelectionStrategy,
    };
    pub use crate::application::orchestration::prompt_pipeline::{
        PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
        PromptExecutionResponse,
    };
    pub use crate::application::orchestration::stasis_workflow_job_builder::StasisWorkflowJobBuilder;
    pub use crate::application::orchestration::tool_registry::{
        InMemoryToolRegistry, StasisTool, ToolRegistry,
    };
    pub use crate::application::runtime::agent_session_job_handler::AgentSessionJobHandler;
    pub use crate::application::runtime::agent_turn_job_handler::AgentTurnJobHandler;
    pub use crate::application::runtime::chat_client_middleware::ChatClientMiddleware;
    pub use crate::application::runtime::concurrent_pattern_job_handler::ConcurrentPatternJobHandler;
    pub use crate::application::runtime::coordinator_failover_job_handler::CoordinatorFailoverJobHandler;
    pub use crate::application::runtime::default_chat_middlewares::{
        CHAT_CACHE_HIT_TOTAL, CHAT_CACHE_MISS_TOTAL, CHAT_DURATION_MS, CHAT_ERRORS_TOTAL,
        CHAT_REQUESTS_TOTAL, CHAT_TOOL_CALLS_TOTAL, CacheChatMiddleware, LoggingChatMiddleware,
        TelemetryChatMiddleware, ToolCallInterceptionChatMiddleware, deterministic_cache_key,
    };
    pub use crate::application::runtime::grapheme_echo_job_handler::GraphemeEchoJobHandler;
    pub use crate::application::runtime::grapheme_healthcheck_job_handler::GraphemeHealthcheckJobHandler;
    pub use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
    pub use crate::application::runtime::grapheme_textops_job_handler::GraphemeTextOpsJobHandler;
    pub use crate::application::runtime::handoff_pattern_job_handler::HandoffPatternJobHandler;
    pub use crate::application::runtime::in_memory_runtime::{
        InMemoryRuntime, JobExecutionOutcome, JobHandler,
    };
    pub use crate::application::runtime::orchestrator_pattern_job_handler::OrchestratorPatternJobHandler;
    pub use crate::application::runtime::prompt_chat_job_handler::PromptChatJobHandler;
    pub use crate::application::runtime::queue_ownership_rebalance_job_handler::QueueOwnershipRebalanceJobHandler;
    pub use crate::application::runtime::replay_report::ReplayReport;
    pub use crate::application::runtime::retention::{RetentionPolicy, RetentionPruneReport};
    pub use crate::application::runtime::runtime_factory::{
        RuntimeBackend, RuntimeComposition, RuntimeFactory,
    };
    pub use crate::application::runtime::sequential_pattern_job_handler::SequentialPatternJobHandler;
    pub use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    pub use crate::application::runtime::surreal_runtime::SurrealRuntime;
    pub use crate::application::runtime::tool_loop_job_handler::ToolLoopJobHandler;
    pub use crate::application::use_cases::investigate_runtime_lineage::{
        InvestigateRuntimeLineage, RuntimeLineageQuery, RuntimeLineageReport,
    };
    pub use crate::application::use_cases::manage_cluster_nodes::{
        ForwardClusterControlCommand, HeartbeatClusterNode, InitiateCoordinatorFailover,
        InitiateCoordinatorHandoff, ListClusterForwardOutcomes, ListClusterNodeHealth,
        ListQueueOwnershipHealth, PruneExpiredClusterNodes, RebalanceQueueOwnership,
        RegisterClusterNode, RunClusterHeartbeatSweep,
    };
    pub use crate::application::use_cases::manage_delivery_endpoints::{
        ListDeliveryEndpoints, RegisterDeliveryEndpoint, SetDeliveryEndpointEnabled,
    };
    pub use crate::application::use_cases::query_endpoint_delivery_statuses::{
        GetEndpointDeliveryStatus, ListEndpointDeliveryStatuses, ListEndpointDiagnosticsReadModel,
        ListEndpointFailureRateTrends, ListTopUnhealthyEndpoints, PruneEndpointDeliveryStatuses,
    };
    pub use crate::dashboard::DashboardState;
    pub use crate::dashboard::InMemoryDashboardQueryService;
    pub use crate::dashboard::router as dashboard_router;
    pub use crate::domain::entities::agent::Agent;
    pub use crate::domain::errors::{Result, StasisError};
    pub use crate::domain::runtime::cluster_node::{
        ClusterControlEvent, ClusterForwardCommand, ClusterForwardOutcome, ClusterNode,
        ClusterNodeHealth, ClusterNodeHealthSnapshot, ClusterNodeRole, NewClusterNode,
        QueueOwnershipMode,
    };
    pub use crate::domain::runtime::delivery_endpoint::{
        DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
    };
    pub use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;
    pub use crate::domain::runtime::job::{BackoffPolicy, JobState, NewJob};
    pub use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
    pub use crate::domain::runtime::outbox::{
        OutboxEvent, OutboxPublishPolicy, OutboxStatus, RuntimeEvent, RuntimeEventType,
    };
    pub use crate::domain::runtime::recurring::RecurringDefinition;
    pub use crate::domain::runtime::thread::{
        NewThread, NewThreadEvent, ThreadEvent, ThreadMergeMetadata, ThreadRecord,
    };
    pub use crate::infrastructure::llm::genai_chat_client::GenaiChatClient;
    pub use crate::infrastructure::llm::genai_gateway::GenaiLlmGateway;
    pub use crate::infrastructure::llm::mock_gateway::MockLlmGateway;
    pub use crate::infrastructure::memory::locus_context_reader::LocusContextReader;
    pub use crate::infrastructure::memory::locus_context_writer::LocusContextWriter;
    pub use crate::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
    pub use crate::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
    pub use crate::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
    pub use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
    pub use crate::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
    pub use crate::infrastructure::runtime::endpoint_routing_event_publisher::EndpointRoutingEventPublisher;
    pub use crate::infrastructure::runtime::endpoint_routing_policy::{
        AllowAllEndpointRoutingPolicy, EndpointRouteRule, RuleBasedEndpointRoutingPolicy,
    };
    pub use crate::infrastructure::runtime::grapheme_sdk_workflow_engine::GraphemeSdkWorkflowEngine;
    pub use crate::infrastructure::runtime::http_cluster_command_forwarder::HttpClusterCommandForwarder;
    pub use crate::infrastructure::runtime::http_cluster_command_forwarder::{
        CLUSTER_FORWARD_ATTEMPTS_TOTAL, CLUSTER_FORWARD_DURATION_MS,
        CLUSTER_FORWARD_FAILURES_TOTAL, CLUSTER_FORWARD_IDEMPOTENT_HITS_TOTAL,
        CLUSTER_FORWARD_NO_ROUTE_TOTAL, CLUSTER_FORWARD_REJECTED_TOTAL,
        CLUSTER_FORWARD_RETRIES_TOTAL, CLUSTER_FORWARD_SUCCESSES_TOTAL,
    };
    pub use crate::infrastructure::runtime::http_webhook_event_publisher::HttpWebhookEventPublisher;
    pub use crate::infrastructure::runtime::http_webhook_event_publisher::HttpWebhookTransportPublisher;
    pub use crate::infrastructure::runtime::in_memory_ai_chat_response_cache::InMemoryAiChatResponseCache;
    pub use crate::infrastructure::runtime::in_memory_cluster_command_forwarder::InMemoryClusterCommandForwarder;
    pub use crate::infrastructure::runtime::in_memory_cluster_control_event_sink::InMemoryClusterControlEventSink;
    pub use crate::infrastructure::runtime::in_memory_cluster_forward_outcome_store::InMemoryClusterForwardOutcomeStore;
    pub use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
    pub use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    pub use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
    pub use crate::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
    pub use crate::infrastructure::runtime::in_memory_thread_store::InMemoryThreadStore;
    #[cfg(feature = "transport-kafka")]
    pub use crate::infrastructure::runtime::kafka_rskafka_transport_publisher::RskafkaTransportPublisher;
    #[cfg(feature = "transport-kafka-wasm")]
    pub use crate::infrastructure::runtime::kafka_wasm_transport_publisher::WasmKafkaTransportPublisher;
    pub use crate::infrastructure::runtime::noop_cluster_command_forwarder::NoopClusterCommandForwarder;
    pub use crate::infrastructure::runtime::noop_cluster_control_event_sink::NoopClusterControlEventSink;
    #[cfg(feature = "transport-rabbitmq")]
    pub use crate::infrastructure::runtime::rabbitmq_lapin_transport_publisher::LapinRabbitMqTransportPublisher;
    pub use crate::infrastructure::runtime::surreal_cluster_forward_outcome_store::SurrealClusterForwardOutcomeStore;
    pub use crate::infrastructure::runtime::surreal_cluster_node_store::SurrealClusterNodeStore;
    pub use crate::infrastructure::runtime::surreal_delivery_endpoint_store::SurrealDeliveryEndpointStore;
    pub use crate::infrastructure::runtime::surreal_endpoint_delivery_status_store::SurrealEndpointDeliveryStatusStore;
    pub use crate::infrastructure::runtime::surreal_thread_store::SurrealThreadStore;
    pub use crate::infrastructure::runtime::system_clock::SystemClock;
    pub use crate::infrastructure::runtime::tcp_socket_transport_publisher::TcpSocketTransportPublisher;
    pub use crate::infrastructure::runtime::tokio_channel_event_publisher::TokioChannelEventPublisher;
    pub use crate::ports::inbound::control_plane_commands::ControlPlaneCommands;
    pub use crate::ports::outbound::ai_chat_client::AiChatClient;
    pub use crate::ports::outbound::ai_chat_response_cache::AiChatResponseCache;
    pub use crate::ports::outbound::ai_chat_tool_interceptor::{
        AiChatToolInterceptor, AiToolCallEnvelope,
    };
    pub use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
    pub use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
    pub use crate::ports::outbound::memory::memory_models::{
        MemoryAggregateRequest, MemoryAggregateResponse, MemoryAvecState, MemoryFallbackPolicy,
        MemoryRecallRequest, MemoryRecallResponse, MemoryRollupRequest, MemoryRollupResponse,
        MemorySchemaResponse, MemoryScope, MemoryStoreRequest, MemoryStoreResponse,
        MemoryStrictnessMode, MemoryTransformOperation, MemoryTransformRequest,
        MemoryTransformResponse,
    };
    pub use crate::ports::outbound::memory::memory_operations::MemoryOperations;
    pub use crate::ports::outbound::runtime::clock::Clock;
    pub use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;
    pub use crate::ports::outbound::runtime::cluster_control_event_sink::ClusterControlEventSink;
    pub use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;
    pub use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
    pub use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
    pub use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
    pub use crate::ports::outbound::runtime::endpoint_routing_policy::EndpointRoutingPolicy;
    pub use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
    pub use crate::ports::outbound::runtime::id_generator::IdGenerator;
    pub use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
    pub use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
    pub use crate::ports::outbound::runtime::thread_store::ThreadStore;
    pub use crate::ports::outbound::runtime::workflow_engine::{
        WorkflowEngine, WorkflowExecutionOutput,
    };
    pub use crate::sdk::control_plane_sdk::ControlPlaneSdk;
    pub use crate::sdk::stasis_sdk::StasisSdk;
    pub use genai::chat::{ChatMessage, ChatOptions, ChatRequest, ChatResponse};
}

#[cfg(test)]
mod tests {
    use crate::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
    use crate::infrastructure::llm::mock_gateway::MockLlmGateway;
    use crate::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
    use crate::sdk::stasis_sdk::StasisSdk;

    #[tokio::test]
    async fn end_to_end_agent_registration_and_invocation_works() {
        let repository = InMemoryAgentRepository::default();
        let llm = MockLlmGateway::new("mock completion");
        let sdk = StasisSdk::new(repository, llm);

        let registration = RegisterAgentRequest {
            id: "planner".to_string(),
            name: "Task Planner".to_string(),
            system_prompt: "Break work down into ordered steps".to_string(),
        };

        sdk.register_agent(registration)
            .await
            .expect("agent should register");

        let response = sdk
            .invoke_agent(InvokeAgentRequest {
                agent_id: "planner".to_string(),
                user_prompt: "Plan a release checklist".to_string(),
            })
            .await
            .expect("agent should invoke");

        assert_eq!(response.completion, "mock completion");
    }
}
