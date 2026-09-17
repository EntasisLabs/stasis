use async_trait::async_trait;
use genai::ModelIden;
use genai::adapter::AdapterKind;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, MessageContent, Usage};

use crate::domain::errors::Result;
use crate::ports::outbound::ai_chat_client::AiChatClient;

/// Deterministic chat client for native tests (requires `llm-genai` for genai types).
/// WASM / `--no-default-features` hosts use [`super::mock_gateway::MockLlmGateway`].
#[derive(Clone, Debug)]
pub struct MockAiChatClient {
    completion: String,
}

impl MockAiChatClient {
    pub fn new(completion: impl Into<String>) -> Self {
        Self {
            completion: completion.into(),
        }
    }
}

#[async_trait]
impl AiChatClient for MockAiChatClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        Ok(ChatResponse {
            content: MessageContent::from_text(&self.completion),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "mock"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "mock"),
            stop_reason: None,
            usage: Usage::default(),
            captured_raw_body: None,
            response_id: None,
        })
    }
}
