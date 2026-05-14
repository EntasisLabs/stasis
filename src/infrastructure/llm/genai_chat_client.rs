use async_trait::async_trait;
use genai::Client;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse};

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::ai_chat_client::AiChatClient;

const DEFAULT_MODEL: &str = "gpt-4o-mini";

#[derive(Clone, Debug)]
pub struct GenaiChatClient {
    client: Client,
    model: String,
}

impl GenaiChatClient {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            client: Client::default(),
            model: model.into(),
        }
    }

    pub fn from_env() -> Self {
        let model = std::env::var("STASIS_LLM_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        Self::new(model)
    }

    pub fn model(&self) -> &str {
        &self.model
    }

}

#[async_trait]
impl AiChatClient for GenaiChatClient {
    async fn complete(&self, request: ChatRequest, options: Option<&ChatOptions>) -> Result<ChatResponse> {
        let response = self
            .client
            .exec_chat(&self.model, request, options)
            .await
            .map_err(|err| {
                StasisError::PortFailure(format!(
                    "genai chat completion failed for model '{}': {}",
                    self.model, err
                ))
            })?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::GenaiChatClient;

    #[test]
    fn chat_client_construction_keeps_model_name() {
        let client = GenaiChatClient::new("gpt-4o-mini");
        assert_eq!(client.model(), "gpt-4o-mini");
    }
}
