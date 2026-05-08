use crate::application::dto::RegisterAgentRequest;
use crate::domain::entities::agent::Agent;
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent_repository::AgentRepository;

#[derive(Clone)]
pub struct RegisterAgent<R>
where
    R: AgentRepository,
{
    repository: R,
}

impl<R> RegisterAgent<R>
where
    R: AgentRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn execute(&self, request: RegisterAgentRequest) -> Result<()> {
        if self.repository.find_by_id(&request.id).await?.is_some() {
            return Err(StasisError::AgentAlreadyExists(request.id));
        }

        let agent = Agent::new(request.id, request.name, request.system_prompt)?;
        self.repository.save(agent).await
    }
}
