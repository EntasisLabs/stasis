use crate::application::dto::{InvokeAgentRequest, InvokeAgentResponse, RegisterAgentRequest};
use crate::domain::errors::Result;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait AgentCommands {
    async fn register_agent(&self, request: RegisterAgentRequest) -> Result<()>;
    async fn invoke_agent(&self, request: InvokeAgentRequest) -> Result<InvokeAgentResponse>;
}
