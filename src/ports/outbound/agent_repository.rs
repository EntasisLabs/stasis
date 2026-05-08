use async_trait::async_trait;

use crate::domain::entities::agent::Agent;
use crate::domain::errors::Result;

#[async_trait]
pub trait AgentRepository: Send + Sync {
    async fn save(&self, agent: Agent) -> Result<()>;
    async fn find_by_id(&self, id: &str) -> Result<Option<Agent>>;
    async fn list(&self) -> Result<Vec<Agent>>;
}
