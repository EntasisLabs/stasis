use async_trait::async_trait;
use genai::chat::ChatRequest;

use crate::domain::errors::Result;
use crate::infrastructure::llm::genai_chat_client::GenaiChatClient;
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::llm_gateway::LlmGateway;

#[derive(Clone, Debug)]
pub struct GenaiLlmGateway {
    chat_client: GenaiChatClient,
}

impl GenaiLlmGateway {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            chat_client: GenaiChatClient::new(model),
        }
    }

    pub fn from_env() -> Self {
        Self {
            chat_client: GenaiChatClient::from_env(),
        }
    }

    pub fn model(&self) -> &str {
        self.chat_client.model()
    }
}

#[async_trait]
impl LlmGateway for GenaiLlmGateway {
    async fn complete(&self, prompt: &str) -> Result<String> {
        let response = self
            .chat_client
            .complete(ChatRequest::from_user(prompt), None)
            .await?;

        let text = response
            .into_first_text()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                crate::domain::errors::StasisError::PortFailure(format!(
                    "genai completion returned empty text for model '{}'",
                    self.model()
                ))
            })?;

        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::GenaiLlmGateway;

    #[test]
    fn gateway_construction_keeps_model_name() {
        let gateway = GenaiLlmGateway::new("gpt-4o-mini");
        assert_eq!(gateway.model(), "gpt-4o-mini");
    }
}
