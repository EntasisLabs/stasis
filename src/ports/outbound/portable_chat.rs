//! Genai-free chat types for slim / WASM tool-loop hosts (`llm-openai-http` without `llm-genai`).

use serde_json::Value;

/// OpenAI-style function tool advertised to the model.
#[derive(Clone, Debug)]
pub struct ChatTool {
    pub name: String,
    pub description: Option<String>,
    pub schema: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub call_id: String,
    pub fn_name: String,
    pub fn_arguments: Value,
}

#[derive(Clone, Debug)]
pub struct ToolResponse {
    pub call_id: String,
    pub content: String,
}

impl ToolResponse {
    pub fn new(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            call_id: call_id.into(),
            content: content.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub tool_call_id: Option<String>,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::System,
            content: Some(content.into()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::User,
            content: Some(content.into()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Assistant,
            content: Some(content.into()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }
}

impl From<Vec<ToolCall>> for ChatMessage {
    fn from(tool_calls: Vec<ToolCall>) -> Self {
        Self {
            role: ChatRole::Assistant,
            content: None,
            tool_calls,
            tool_call_id: None,
        }
    }
}

impl From<ToolResponse> for ChatMessage {
    fn from(response: ToolResponse) -> Self {
        Self {
            role: ChatRole::Tool,
            content: Some(response.content),
            tool_calls: Vec::new(),
            tool_call_id: Some(response.call_id),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ChatTool>,
}

impl ChatRequest {
    pub fn new(messages: Vec<ChatMessage>) -> Self {
        Self {
            messages,
            tools: Vec::new(),
        }
    }

    pub fn with_tools(mut self, tools: Vec<ChatTool>) -> Self {
        self.tools = tools;
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChatOptions {
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ChatResponse {
    pub text: Option<String>,
    pub tool_calls: Vec<ToolCall>,
}

impl ChatResponse {
    pub fn first_text(&self) -> Option<&str> {
        self.text.as_deref()
    }

    pub fn into_first_text(self) -> Option<String> {
        self.text
    }

    pub fn into_tool_calls(self) -> Vec<ToolCall> {
        self.tool_calls
    }
}
