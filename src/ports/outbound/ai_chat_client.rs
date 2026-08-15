use async_trait::async_trait;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse};
use tokio::sync::mpsc;

use crate::domain::errors::{Result, StasisError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDelta {
    Content(String),
    Reasoning(String),
    ThoughtSignature(String),
}

/// Await capacity, then deliver one provider delta. A closed receiver is a typed failure.
pub async fn send_stream_delta(tx: &mpsc::Sender<StreamDelta>, delta: StreamDelta) -> Result<()> {
    tx.send(delta).await.map_err(|_| StasisError::StreamClosed)
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
        chunk_tx: Option<&mpsc::Sender<StreamDelta>>,
    ) -> Result<ChatResponse> {
        let response = self.complete(request, options).await?;
        if let (Some(tx), Some(text)) = (chunk_tx, response.first_text()) {
            send_stream_delta(tx, StreamDelta::Content(text.to_string())).await?;
        }
        Ok(response)
    }
}
