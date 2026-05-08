use crate::application::dto::{InvokeAgentRequest, InvokeAgentResponse};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent_repository::AgentRepository;
use crate::ports::outbound::llm_gateway::LlmGateway;

#[derive(Clone)]
pub struct InvokeAgent<R, L>
where
    R: AgentRepository,
    L: LlmGateway,
{
    repository: R,
    llm: L,
}

impl<R, L> InvokeAgent<R, L>
where
    R: AgentRepository,
    L: LlmGateway,
{
    pub fn new(repository: R, llm: L) -> Self {
        Self { repository, llm }
    }

    pub async fn execute(&self, request: InvokeAgentRequest) -> Result<InvokeAgentResponse> {
        let agent = self
            .repository
            .find_by_id(&request.agent_id)
            .await?
            .ok_or_else(|| StasisError::AgentNotFound(request.agent_id.clone()))?;

        let prompt = format!(
            "SYSTEM:\n{}\n\nUSER:\n{}",
            agent.system_prompt, request.user_prompt
        );

        let completion = self.llm.complete(&prompt).await?;

        Ok(InvokeAgentResponse {
            agent_id: agent.id.as_str().to_string(),
            completion,
        })
    }
}
