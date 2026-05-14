use async_trait::async_trait;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse};

use crate::domain::errors::Result;

#[async_trait]
pub trait AiChatClient: Send + Sync {
    async fn complete(&self, request: ChatRequest, options: Option<&ChatOptions>) -> Result<ChatResponse>;
}
