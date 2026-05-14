pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod ports;
pub mod sdk;

pub mod prelude {
    pub use crate::application::orchestration::agent_session_pipeline::{
        AgentIdentity, AgentParticipant, AgentSelectionStrategy, AgentSessionCoordinator,
        AgentSessionPipeline, AgentSessionRunRequest, AgentSessionRunResponse,
        AgentTerminationStrategy, AgentTurnExecutionPolicy, AgentTurnExecutionRequest,
        AgentTurnExecutionResponse, AgentTurnRecord, MaxTurnsTerminationStrategy,
        RoundRobinSelectionStrategy,
    };
    pub use crate::application::orchestration::agent_session_payload::{
        AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode,
        AgentTurnJobPayload, PromptJobPayload, ToolLoopJobPayload,
    };
    pub use crate::application::orchestration::stasis_workflow_job_builder::StasisWorkflowJobBuilder;
    pub use crate::application::orchestration::prompt_pipeline::{
        PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
        PromptExecutionResponse,
    };
    pub use crate::application::orchestration::tool_registry::{
        InMemoryToolRegistry, StasisTool, ToolRegistry,
    };
    pub use crate::application::runtime::grapheme_echo_job_handler::GraphemeEchoJobHandler;
    pub use crate::application::runtime::grapheme_healthcheck_job_handler::GraphemeHealthcheckJobHandler;
    pub use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
    pub use crate::application::runtime::grapheme_textops_job_handler::GraphemeTextOpsJobHandler;
    pub use crate::application::runtime::agent_session_job_handler::AgentSessionJobHandler;
    pub use crate::application::runtime::agent_turn_job_handler::AgentTurnJobHandler;
    pub use crate::application::runtime::prompt_chat_job_handler::PromptChatJobHandler;
    pub use crate::application::runtime::tool_loop_job_handler::ToolLoopJobHandler;
    pub use crate::application::runtime::in_memory_runtime::{
        InMemoryRuntime, JobExecutionOutcome, JobHandler,
    };
    pub use crate::application::runtime::replay_report::ReplayReport;
    pub use crate::application::runtime::retention::{RetentionPolicy, RetentionPruneReport};
    pub use crate::application::use_cases::investigate_runtime_lineage::{
        InvestigateRuntimeLineage, RuntimeLineageQuery, RuntimeLineageReport,
    };
    pub use crate::application::runtime::runtime_factory::{
        RuntimeBackend, RuntimeComposition, RuntimeFactory,
    };
    pub use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    pub use crate::application::runtime::surreal_runtime::SurrealRuntime;
    pub use crate::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
    pub use crate::domain::entities::agent::Agent;
    pub use crate::domain::errors::{Result, StasisError};
    pub use crate::domain::runtime::job::{BackoffPolicy, JobState, NewJob};
    pub use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
    pub use crate::domain::runtime::outbox::{
        OutboxEvent, OutboxPublishPolicy, OutboxStatus, RuntimeEvent, RuntimeEventType,
    };
    pub use crate::infrastructure::llm::genai_chat_client::GenaiChatClient;
    pub use crate::infrastructure::runtime::tokio_channel_event_publisher::TokioChannelEventPublisher;
    pub use crate::infrastructure::runtime::system_clock::SystemClock;
    pub use crate::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
    pub use crate::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
    pub use crate::infrastructure::runtime::grapheme_sdk_workflow_engine::GraphemeSdkWorkflowEngine;
    pub use crate::ports::outbound::runtime::clock::Clock;
    pub use crate::ports::outbound::runtime::id_generator::IdGenerator;
    pub use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
    pub use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
    pub use crate::ports::outbound::runtime::workflow_engine::{
        WorkflowEngine, WorkflowExecutionOutput,
    };
    pub use crate::ports::outbound::ai_chat_client::AiChatClient;
    pub use genai::chat::{ChatMessage, ChatOptions, ChatRequest, ChatResponse};
    pub use crate::domain::runtime::recurring::RecurringDefinition;
    pub use crate::infrastructure::llm::genai_gateway::GenaiLlmGateway;
    pub use crate::infrastructure::llm::mock_gateway::MockLlmGateway;
    pub use crate::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
    pub use crate::sdk::stasis_sdk::StasisSdk;
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
