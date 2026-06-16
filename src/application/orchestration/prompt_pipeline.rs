use std::sync::Arc;

use genai::chat::{ChatMessage, ChatRequest, ChatResponse};
use tokio::sync::mpsc;

use crate::application::runtime::chat_options_resolver::chat_options_for_context;
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::ai_chat_client::StreamDelta;

#[derive(Clone, Debug, Default)]
pub struct PromptExecutionContext {
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PromptExecutionRequest {
    pub system_prompt: Option<String>,
    pub user_prompt: String,
    pub context: PromptExecutionContext,
}

impl PromptExecutionRequest {
    pub fn from_user_prompt(prompt: impl Into<String>) -> Self {
        Self {
            system_prompt: None,
            user_prompt: prompt.into(),
            context: PromptExecutionContext::default(),
        }
    }

    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    pub fn with_context(mut self, context: PromptExecutionContext) -> Self {
        self.context = context;
        self
    }
}

#[derive(Clone, Debug)]
pub struct PromptExecutionResponse {
    pub text: String,
    pub metadata: PromptExecutionContext,
}

#[derive(Clone, Debug)]
pub struct PromptChatCompletion {
    pub response: ChatResponse,
    pub metadata: PromptExecutionContext,
}

#[derive(Clone)]
pub struct PromptExecutionPipeline {
    chat_client: Arc<dyn AiChatClient>,
}

impl PromptExecutionPipeline {
    pub fn new(chat_client: Arc<dyn AiChatClient>) -> Self {
        Self { chat_client }
    }

    pub async fn complete_chat(
        &self,
        request: ChatRequest,
        context: PromptExecutionContext,
    ) -> Result<PromptChatCompletion> {
        let options = chat_options_for_context(&context).map_err(StasisError::PortFailure)?;
        let options_ref = options.as_ref();
        let response = self.chat_client.complete(request, options_ref).await?;
        Ok(PromptChatCompletion {
            response,
            metadata: context,
        })
    }

    pub async fn complete_chat_stream(
        &self,
        request: ChatRequest,
        context: PromptExecutionContext,
        chunk_tx: Option<&mpsc::UnboundedSender<StreamDelta>>,
    ) -> Result<PromptChatCompletion> {
        let options = chat_options_for_context(&context).map_err(StasisError::PortFailure)?;
        let options_ref = options.as_ref();
        let response = self
            .chat_client
            .complete_stream(request, options_ref, chunk_tx)
            .await?;
        Ok(PromptChatCompletion {
            response,
            metadata: context,
        })
    }

    pub async fn execute(
        &self,
        request: PromptExecutionRequest,
    ) -> Result<PromptExecutionResponse> {
        let context = request.context.clone();
        let mut messages = Vec::with_capacity(2);
        if let Some(system_prompt) = request.system_prompt {
            messages.push(ChatMessage::system(system_prompt));
        }
        messages.push(ChatMessage::user(request.user_prompt));

        let chat_response = self
            .complete_chat(ChatRequest::new(messages), context.clone())
            .await?
            .response;

        let text = chat_response
            .into_first_text()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| StasisError::PortFailure("chat response was empty".to_string()))?;

        Ok(PromptExecutionResponse {
            text,
            metadata: context,
        })
    }
}
