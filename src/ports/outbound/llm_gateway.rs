use async_trait::async_trait;

use crate::domain::errors::Result;

#[async_trait]
pub trait LlmGateway: Send + Sync {
    async fn complete(&self, prompt: &str) -> Result<String>;
}
