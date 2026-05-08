use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::entities::agent::Agent;
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent_repository::AgentRepository;

#[derive(Clone, Default)]
pub struct InMemoryAgentRepository {
    agents: Arc<RwLock<HashMap<String, Agent>>>,
}

#[async_trait]
impl AgentRepository for InMemoryAgentRepository {
    async fn save(&self, agent: Agent) -> Result<()> {
        let mut state = self
            .agents
            .write()
            .map_err(|_| StasisError::PortFailure("repository lock poisoned".to_string()))?;

        state.insert(agent.id.as_str().to_string(), agent);
        Ok(())
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Agent>> {
        let state = self
            .agents
            .read()
            .map_err(|_| StasisError::PortFailure("repository lock poisoned".to_string()))?;

        Ok(state.get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<Agent>> {
        let state = self
            .agents
            .read()
            .map_err(|_| StasisError::PortFailure("repository lock poisoned".to_string()))?;

        Ok(state.values().cloned().collect())
    }
}
