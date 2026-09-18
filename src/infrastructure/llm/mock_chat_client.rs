use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(feature = "llm-genai")]
use genai::ModelIden;
#[cfg(feature = "llm-genai")]
use genai::adapter::AdapterKind;
#[cfg(feature = "llm-genai")]
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, MessageContent, Usage};
#[cfg(all(feature = "llm-chat", not(feature = "llm-genai")))]
use serde_json::json;

use crate::domain::errors::Result;
use crate::ports::outbound::ai_chat_client::AiChatClient;
#[cfg(all(feature = "llm-chat", not(feature = "llm-genai")))]
use crate::ports::outbound::portable_chat::{
    ChatOptions, ChatRequest, ChatResponse, ToolCall,
};

/// Deterministic chat client for tests and the WASM mock create path.
///
/// Native `llm-genai` hosts use genai response types. Slim / WASM hosts
/// (`llm-openai-http` without genai) use portable chat types.
#[derive(Debug)]
pub struct MockAiChatClient {
    completion: String,
    emit_tool_call: bool,
    round: AtomicU32,
}

impl Clone for MockAiChatClient {
    fn clone(&self) -> Self {
        Self {
            completion: self.completion.clone(),
            emit_tool_call: self.emit_tool_call,
            round: AtomicU32::new(self.round.load(Ordering::Relaxed)),
        }
    }
}

impl MockAiChatClient {
    pub fn new(completion: impl Into<String>) -> Self {
        Self {
            completion: completion.into(),
            emit_tool_call: false,
            round: AtomicU32::new(0),
        }
    }

    /// First `complete` with tools returns a function call for the first tool;
    /// later rounds return [`Self::new`]'s completion text.
    pub fn with_first_tool_call(mut self) -> Self {
        self.emit_tool_call = true;
        self
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl AiChatClient for MockAiChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        #[cfg(feature = "llm-genai")]
        {
            let _ = request;
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

        #[cfg(not(feature = "llm-genai"))]
        {
            if self.emit_tool_call
                && !request.tools.is_empty()
                && self.round.fetch_add(1, Ordering::SeqCst) == 0
            {
                let tool = &request.tools[0];
                return Ok(ChatResponse {
                    text: None,
                    tool_calls: vec![ToolCall {
                        call_id: "call_mock_1".to_string(),
                        fn_name: tool.name.clone(),
                        fn_arguments: json!({}),
                    }],
                });
            }

            Ok(ChatResponse {
                text: Some(self.completion.clone()),
                tool_calls: Vec::new(),
            })
        }
    }
}
