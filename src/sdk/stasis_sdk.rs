use async_trait::async_trait;

use crate::application::dto::{InvokeAgentRequest, InvokeAgentResponse, RegisterAgentRequest};
use crate::application::use_cases::invoke_agent::InvokeAgent;
use crate::application::use_cases::register_agent::RegisterAgent;
use crate::domain::errors::Result;
use crate::ports::inbound::agent_commands::AgentCommands;
use crate::ports::outbound::agent_repository::AgentRepository;
use crate::ports::outbound::llm_gateway::LlmGateway;

#[derive(Clone)]
pub struct StasisSdk<R, L>
where
    R: AgentRepository,
    L: LlmGateway,
{
    register_agent: RegisterAgent<R>,
    invoke_agent: InvokeAgent<R, L>,
}

impl<R, L> StasisSdk<R, L>
where
    R: AgentRepository + Clone,
    L: LlmGateway + Clone,
{
    pub fn new(repository: R, llm: L) -> Self {
        Self {
            register_agent: RegisterAgent::new(repository.clone()),
            invoke_agent: InvokeAgent::new(repository, llm),
        }
    }

    pub async fn register_agent(&self, request: RegisterAgentRequest) -> Result<()> {
        self.register_agent.execute(request).await
    }

    pub async fn invoke_agent(&self, request: InvokeAgentRequest) -> Result<InvokeAgentResponse> {
        self.invoke_agent.execute(request).await
    }
}

#[async_trait]
impl<R, L> AgentCommands for StasisSdk<R, L>
where
    R: AgentRepository + Clone + Send + Sync,
    L: LlmGateway + Clone + Send + Sync,
{
    async fn register_agent(&self, request: RegisterAgentRequest) -> Result<()> {
        self.register_agent.execute(request).await
    }

    async fn invoke_agent(&self, request: InvokeAgentRequest) -> Result<InvokeAgentResponse> {
        self.invoke_agent.execute(request).await
    }
}
