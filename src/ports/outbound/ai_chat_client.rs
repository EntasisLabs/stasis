#[cfg(feature = "llm-genai")]
use genai::chat::{ChatOptions, ChatRequest, ChatResponse};
use tokio::sync::mpsc;

use crate::domain::errors::{Result, StasisError};
#[cfg(all(feature = "llm-chat", not(feature = "llm-genai")))]
use crate::ports::outbound::portable_chat::{ChatOptions, ChatRequest, ChatResponse};

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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
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
