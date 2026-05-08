use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::ports::outbound::llm_gateway::LlmGateway;

#[derive(Clone, Debug)]
pub struct MockLlmGateway {
    completion: String,
}

impl MockLlmGateway {
    pub fn new(completion: impl Into<String>) -> Self {
        Self {
            completion: completion.into(),
        }
    }
}

#[async_trait]
impl LlmGateway for MockLlmGateway {
    async fn complete(&self, _prompt: &str) -> Result<String> {
        Ok(self.completion.clone())
    }
}
