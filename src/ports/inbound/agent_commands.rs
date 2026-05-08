use async_trait::async_trait;

use crate::application::dto::{InvokeAgentRequest, InvokeAgentResponse, RegisterAgentRequest};
use crate::domain::errors::Result;

#[async_trait]
pub trait AgentCommands {
    async fn register_agent(&self, request: RegisterAgentRequest) -> Result<()>;
    async fn invoke_agent(&self, request: InvokeAgentRequest) -> Result<InvokeAgentResponse>;
}
