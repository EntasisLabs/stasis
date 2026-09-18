//! `AiChatClient` over OpenAI Chat Completions, including function tool calls.
//!
//! Compiles with `llm-openai-http` when `llm-genai` is off (WASM / slim hosts).

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::llm::openai_http_gateway::{
    OpenAiHttpGateway, OpenAiHttpTransport, ReqwestOpenAiTransport, api_error_message,
    message_content_text,
};
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::portable_chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponse, ChatRole, ChatTool, ToolCall,
};

static TOOL_CALL_SEQ: AtomicU64 = AtomicU64::new(1);

/// OpenAI-spec chat client used by prompt / tool-loop handlers on the WASM guest.
#[derive(Clone, Debug)]
pub struct OpenAiHttpChatClient<T = ReqwestOpenAiTransport> {
    gateway: OpenAiHttpGateway<T>,
}

impl OpenAiHttpChatClient<ReqwestOpenAiTransport> {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            gateway: OpenAiHttpGateway::new(api_key, model),
        }
    }
}

impl<T: OpenAiHttpTransport> OpenAiHttpChatClient<T> {
    pub fn with_gateway(gateway: OpenAiHttpGateway<T>) -> Self {
        Self { gateway }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.gateway = self.gateway.with_base_url(base_url);
        self
    }

    pub fn model(&self) -> &str {
        self.gateway.model()
    }

    pub fn base_url(&self) -> &str {
        self.gateway.base_url()
    }

    pub fn gateway(&self) -> &OpenAiHttpGateway<T> {
        &self.gateway
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<T: OpenAiHttpTransport + Send + Sync> AiChatClient for OpenAiHttpChatClient<T> {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let mut body = json!({
            "model": self.gateway.model(),
            "messages": request
                .messages
                .iter()
                .map(openai_message)
                .collect::<Vec<_>>(),
        });
        if !request.tools.is_empty() {
            body["tools"] = Value::Array(request.tools.iter().map(openai_tool).collect());
        }
        if let Some(effort) = options
            .and_then(|opts| opts.reasoning_effort.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            body["reasoning_effort"] = json!(effort);
        }

        let response = self.gateway.post_chat_completions(body).await?;
        chat_response_from_openai(&response, self.gateway.model())
    }
}

fn openai_message(message: &ChatMessage) -> Value {
    let role = match message.role {
        ChatRole::System => "system",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::Tool => "tool",
    };
    let mut obj = json!({ "role": role });
    if let Some(content) = &message.content {
        obj["content"] = json!(content);
    } else if message.tool_calls.is_empty() {
        obj["content"] = json!("");
    }
    if !message.tool_calls.is_empty() {
        obj["tool_calls"] = Value::Array(
            message
                .tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.call_id,
                        "type": "function",
                        "function": {
                            "name": call.fn_name,
                            "arguments": call.fn_arguments.to_string(),
                        }
                    })
                })
                .collect(),
        );
    }
    if let Some(tool_call_id) = &message.tool_call_id {
        obj["tool_call_id"] = json!(tool_call_id);
    }
    obj
}

fn openai_tool(tool: &ChatTool) -> Value {
    let mut function = json!({ "name": tool.name });
    if let Some(description) = &tool.description {
        function["description"] = json!(description);
    }
    if let Some(schema) = &tool.schema {
        function["parameters"] = schema.clone();
    } else {
        function["parameters"] = json!({ "type": "object", "properties": {} });
    }
    json!({
        "type": "function",
        "function": function,
    })
}

fn chat_response_from_openai(response: &Value, model: &str) -> Result<ChatResponse> {
    if let Some(message) = api_error_message(response) {
        return Err(StasisError::PortFailure(message));
    }

    let choice_message = response.pointer("/choices/0/message").ok_or_else(|| {
        StasisError::PortFailure(format!(
            "openai-compatible completion for model '{model}' missing choices[0].message"
        ))
    })?;

    let text = choice_message
        .get("content")
        .and_then(message_content_text);

    let tool_calls = parse_tool_calls(choice_message.get("tool_calls"));

    if text.is_none() && tool_calls.is_empty() {
        return Err(StasisError::PortFailure(format!(
            "openai-compatible completion for model '{model}' returned empty text and no tool calls"
        )));
    }

    Ok(ChatResponse { text, tool_calls })
}

fn parse_tool_calls(raw: Option<&Value>) -> Vec<ToolCall> {
    let Some(Value::Array(items)) = raw else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let function = item.get("function")?;
            let fn_name = function.get("name")?.as_str()?.to_string();
            let call_id = item
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!(
                        "call_auto_{}",
                        TOOL_CALL_SEQ.fetch_add(1, Ordering::Relaxed)
                    )
                });
            let fn_arguments = match function.get("arguments") {
                Some(Value::String(raw)) => serde_json::from_str(raw).unwrap_or(json!({})),
                Some(value) => value.clone(),
                None => json!({}),
            };
            Some(ToolCall {
                call_id,
                fn_name,
                fn_arguments,
            })
        })
        .collect()
}
