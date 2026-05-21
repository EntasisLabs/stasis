use async_trait::async_trait;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse};
use tokio::sync::mpsc;

use crate::domain::errors::Result;

#[derive(Debug, Clone)]
pub enum StreamDelta {
    Content(String),
    Reasoning(String),
    ThoughtSignature(String),
}

#[async_trait]
pub trait AiChatClient: Send + Sync {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse>;

    async fn complete_stream(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
    ) -> Result<ChatResponse> {
        let response = self.complete(request, options).await?;
        if let (Some(tx), Some(text)) = (chunk_tx, response.first_text()) {
            let _ = tx.send(StreamDelta::Content(text.to_string()));
        }
        Ok(response)
    }
}
