//! Stasis is a durable AI orchestration framework with runtime jobs, control-plane
//! primitives, and memory integration adapters.
//! Use the [prelude] module for a batteries-included public API surface.

pub mod application;
pub mod dashboard;
pub mod domain;
pub mod infrastructure;
pub mod ports;
pub mod sdk;

/// Minimal runtime imports for consumers integrating job handlers and runtime wiring.
pub mod runtime_prelude {
    pub use crate::application::runtime::in_memory_runtime::{
        InMemoryRuntime, JobExecutionOutcome, JobHandler,
    };
    pub use crate::application::runtime::runtime_factory::{
        RuntimeBackend, RuntimeComposition, RuntimeFactory,
    };
    pub use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    pub use crate::domain::errors::{Result, StasisError};
    pub use crate::domain::runtime::job::{BackoffPolicy, JobState, NewJob};
    pub use crate::domain::runtime::recurring::RecurringDefinition;
}

/// Extended runtime imports including orchestration payloads, endpoint routing, and store adapters.
pub mod runtime_prelude_ext {
    pub use crate::application::dto::{
        HeartbeatClusterNodeRequest, RegisterClusterNodeRequest,
        RegisterDeliveryEndpointRequest,
    };
    pub use crate::application::orchestration::runtime_job_payloads::{
        AgentSessionJobPayload, AgentTurnJobPayload, ConcurrentPatternJobPayload,
        HandoffPatternJobPayload, OrchestratorPatternJobPayload, PromptJobPayload,
        SequentialPatternJobPayload, ToolLoopJobPayload,
    };
    pub use crate::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
    pub use crate::application::runtime::in_memory_runtime::{
        InMemoryRuntime, JobExecutionOutcome, JobHandler,
    };
    pub use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    pub use crate::domain::runtime::cluster_node::ClusterNodeRole;
    pub use crate::domain::runtime::delivery_endpoint::{
        DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
    };
    pub use crate::domain::runtime::outbox::OutboxEvent;
    pub use crate::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
    pub use crate::infrastructure::runtime::endpoint_routing_event_publisher::EndpointRoutingEventPublisher;
    pub use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
    pub use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    pub use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
    pub use crate::infrastructure::runtime::surreal_cluster_node_store::SurrealClusterNodeStore;
    pub use crate::infrastructure::runtime::surreal_delivery_endpoint_store::SurrealDeliveryEndpointStore;
    pub use crate::infrastructure::runtime::surreal_endpoint_delivery_status_store::SurrealEndpointDeliveryStatusStore;
    pub use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
    pub use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
    pub use crate::ports::outbound::runtime::endpoint_transport_publisher::EndpointTransportPublisher;
}

/// Minimal memory imports for consumers using context store/recall/transform APIs.
pub mod memory_prelude {
    pub use crate::ports::outbound::memory::memory_models::{
        MemoryAggregateRequest, MemoryRecallRequest, MemoryRollupRequest, MemoryScope,
        MemoryStoreRequest, MemoryTransformRequest,
    };
    pub use crate::ports::outbound::memory::memory_operations::MemoryOperations;
}

/// Extended memory imports including concrete Locus adapters and context reader/writer traits.
pub mod memory_prelude_ext {
    pub use crate::infrastructure::memory::locus_context_reader::LocusContextReader;
    pub use crate::infrastructure::memory::locus_context_writer::LocusContextWriter;
    pub use crate::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
    pub use crate::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
    pub use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
    pub use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
    pub use crate::ports::outbound::memory::memory_models::{
        MemoryAggregateRequest, MemoryRecallRequest, MemoryRollupRequest, MemoryScope,
        MemoryStoreRequest, MemoryTransformRequest,
    };
    pub use crate::ports::outbound::memory::memory_operations::MemoryOperations;
}

/// Minimal SDK imports for common external consumers.
pub mod sdk_prelude {
    pub use crate::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
    pub use crate::domain::errors::{Result, StasisError};
    pub use crate::domain::runtime::job::{BackoffPolicy, NewJob};
    pub use crate::application::runtime::runtime_factory::{RuntimeBackend, RuntimeFactory};
    pub use crate::infrastructure::llm::mock_gateway::MockLlmGateway;
    pub use crate::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
    pub use crate::sdk::runtime_sdk::{RuntimeSdk, StasisRuntime};
    pub use crate::sdk::stasis_sdk::StasisSdk;
}

/// Extended SDK imports including control-plane and provider-specific integration types.
pub mod sdk_prelude_ext {
    pub use crate::application::dto::InvokeAgentResponse;
    pub use crate::application::runtime::runtime_factory::RuntimeComposition;
    pub use crate::domain::runtime::job::JobState;
    pub use crate::domain::runtime::recurring::RecurringDefinition;
    pub use crate::infrastructure::llm::genai_gateway::GenaiLlmGateway;
    pub use crate::sdk::control_plane_sdk::ControlPlaneSdk;
    pub use crate::sdk::runtime_sdk::RuntimeStatsSnapshot;
}

/// Re-exported minimal public API surface for common Stasis consumers.
pub mod prelude {
    pub use crate::memory_prelude::*;
    pub use crate::runtime_prelude::*;
    pub use crate::sdk_prelude::*;
}

/// Re-exported extended API surface for advanced integrations.
pub mod prelude_ext {
    pub use crate::memory_prelude_ext::*;
    pub use crate::runtime_prelude_ext::*;
    pub use crate::sdk_prelude_ext::*;
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
