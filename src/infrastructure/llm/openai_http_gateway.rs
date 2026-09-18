//! OpenAI Chat Completions HTTP adapter (`LlmGateway`).
//!
//! Talks the public OpenAI HTTP spec (`POST /v1/chat/completions`) so WASM and
//! slim native hosts can call OpenAI, Groq, OpenRouter, Ollama `/v1`, llama.cpp,
//! and other compatible servers without `genai` or native TLS on wasm32
//! (`reqwest` uses `fetch` there).

use serde_json::{Value, json};

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::llm_gateway::LlmGateway;

const DEFAULT_MODEL: &str = "gpt-4o-mini";
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// Low-level JSON POST used by [`OpenAiHttpGateway`].
///
/// Tests inject a scripted transport so mapping can be proven without a network.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait OpenAiHttpTransport: Send + Sync {
    async fn post_json(&self, url: &str, bearer_token: &str, body: Value) -> Result<Value>;
}

/// `reqwest` transport (rustls on native, `fetch` on wasm32).
#[derive(Clone, Debug, Default)]
pub struct ReqwestOpenAiTransport {
    client: reqwest::Client,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl OpenAiHttpTransport for ReqwestOpenAiTransport {
    async fn post_json(&self, url: &str, bearer_token: &str, body: Value) -> Result<Value> {
        let response = self
            .client
            .post(url)
            .bearer_auth(bearer_token)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                StasisError::PortFailure(format!("openai http request failed: {err}"))
            })?;

        let status = response.status();
        let bytes = response.bytes().await.map_err(|err| {
            StasisError::PortFailure(format!("openai http response body failed: {err}"))
        })?;
        let parsed: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));

        if !status.is_success() {
            let message = api_error_message(&parsed).unwrap_or_else(|| {
                format!("openai http {status}: {}", String::from_utf8_lossy(&bytes))
            });
            return Err(StasisError::PortFailure(message));
        }

        Ok(parsed)
    }
}

/// OpenAI-compatible chat-completions gateway for [`StasisSdk`](crate::sdk::stasis_sdk::StasisSdk).
#[derive(Clone, Debug)]
pub struct OpenAiHttpGateway<T = ReqwestOpenAiTransport> {
    transport: T,
    base_url: String,
    api_key: String,
    model: String,
}

impl OpenAiHttpGateway<ReqwestOpenAiTransport> {
    /// Builds a gateway against `https://api.openai.com/v1`.
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self::with_transport(ReqwestOpenAiTransport::default(), api_key, model)
    }

    /// Reads `STASIS_OPENAI_API_KEY` / `OPENAI_API_KEY` / `STASIS_LLM_API_KEY`,
    /// `STASIS_LLM_MODEL`, and `STASIS_LLM_BASE_URL` / `STASIS_OPENAI_BASE_URL`.
    ///
    /// Browser WASM hosts usually have no process env — prefer [`Self::new`] and
    /// [`Self::with_base_url`] with keys supplied by the host.
    pub fn from_env() -> Result<Self> {
        let api_key = first_env(&[
            "STASIS_OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "STASIS_LLM_API_KEY",
        ])
        .ok_or_else(|| {
            StasisError::PortFailure(
                "missing OpenAI API key (set STASIS_OPENAI_API_KEY, OPENAI_API_KEY, or STASIS_LLM_API_KEY)"
                    .into(),
            )
        })?;
        let model = first_env(&["STASIS_LLM_MODEL"]).unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let mut gateway = Self::new(api_key, model);
        if let Some(base_url) = first_env(&["STASIS_LLM_BASE_URL", "STASIS_OPENAI_BASE_URL"]) {
            gateway = gateway.with_base_url(base_url);
        }
        Ok(gateway)
    }
}

impl<T: OpenAiHttpTransport> OpenAiHttpGateway<T> {
    pub fn with_transport(
        transport: T,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            transport,
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = normalize_base_url(base_url.into());
        self
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// POST a Chat Completions JSON body (used by [`super::openai_http_chat_client`]).
    pub async fn post_chat_completions(&self, body: Value) -> Result<Value> {
        let url = chat_completions_url(&self.base_url);
        self.transport
            .post_json(&url, &self.api_key, body)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<T: OpenAiHttpTransport + Send + Sync> LlmGateway for OpenAiHttpGateway<T> {
    async fn complete(&self, prompt: &str) -> Result<String> {
        let url = chat_completions_url(&self.base_url);
        let body = chat_completions_body(&self.model, prompt);
        let response = self.transport.post_json(&url, &self.api_key, body).await?;
        completion_text_from_response(&response, &self.model)
    }
}

fn normalize_base_url(raw: String) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

pub(crate) fn chat_completions_url(base_url: &str) -> String {
    let base = normalize_base_url(base_url.to_string());
    if base.ends_with("/chat/completions") {
        base
    } else {
        format!("{base}/chat/completions")
    }
}

fn chat_completions_body(model: &str, prompt: &str) -> Value {
    json!({
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    })
}

fn completion_text_from_response(response: &Value, model: &str) -> Result<String> {
    if let Some(message) = api_error_message(response) {
        return Err(StasisError::PortFailure(message));
    }

    let content = response
        .pointer("/choices/0/message/content")
        .ok_or_else(|| {
            StasisError::PortFailure(format!(
                "openai-compatible completion for model '{model}' missing choices[0].message.content"
            ))
        })?;

    let text = message_content_text(content).ok_or_else(|| {
        StasisError::PortFailure(format!(
            "openai-compatible completion for model '{model}' returned empty text"
        ))
    })?;

    Ok(text)
}

pub(crate) fn message_content_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Array(parts) => {
            let mut out = String::new();
            for part in parts {
                let piece = part.get("text").and_then(Value::as_str).or_else(|| {
                    if part.get("type").and_then(Value::as_str) == Some("text") {
                        part.get("text").and_then(Value::as_str)
                    } else {
                        None
                    }
                });
                if let Some(piece) = piece {
                    out.push_str(piece);
                }
            }
            let trimmed = out.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        _ => None,
    }
}

pub(crate) fn api_error_message(body: &Value) -> Option<String> {
    let error = body.get("error")?;
    if let Some(message) = error.get("message").and_then(Value::as_str) {
        let kind = error.get("type").and_then(Value::as_str).unwrap_or("error");
        return Some(format!("openai-compatible {kind}: {message}"));
    }
    if let Some(message) = error.as_str() {
        return Some(format!("openai-compatible error: {message}"));
    }
    None
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct ScriptedTransport {
        calls: Arc<Mutex<Vec<(String, String, Value)>>>,
        responses: Arc<Mutex<Vec<Result<Value>>>>,
    }

    impl ScriptedTransport {
        fn returning(response: Value) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(vec![Ok(response)])),
            }
        }

        fn failing(message: impl Into<String>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(vec![Err(StasisError::PortFailure(
                    message.into(),
                ))])),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl OpenAiHttpTransport for ScriptedTransport {
        async fn post_json(&self, url: &str, bearer_token: &str, body: Value) -> Result<Value> {
            self.calls.lock().expect("calls lock").push((
                url.to_string(),
                bearer_token.to_string(),
                body,
            ));
            self.responses
                .lock()
                .expect("responses lock")
                .pop()
                .unwrap_or_else(|| Err(StasisError::PortFailure("no scripted response".into())))
        }
    }

    #[test]
    fn completions_url_appends_path_and_strips_slash() {
        assert_eq!(
            chat_completions_url("https://api.openai.com/v1/"),
            "https://api.openai.com/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_url("https://api.openai.com/v1/chat/completions"),
            "https://api.openai.com/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_url("http://127.0.0.1:11434/v1"),
            "http://127.0.0.1:11434/v1/chat/completions"
        );
    }

    #[test]
    fn parses_string_message_content() {
        let body = json!({
            "choices": [{ "message": { "role": "assistant", "content": "  hello from openai  " } }]
        });
        assert_eq!(
            completion_text_from_response(&body, "gpt-4o-mini").unwrap(),
            "hello from openai"
        );
    }

    #[test]
    fn parses_multipart_text_content() {
        let body = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": [
                        { "type": "text", "text": "part-a " },
                        { "type": "text", "text": "part-b" }
                    ]
                }
            }]
        });
        assert_eq!(
            completion_text_from_response(&body, "gpt-4o-mini").unwrap(),
            "part-a part-b"
        );
    }

    #[test]
    fn maps_openai_error_object() {
        let body = json!({
            "error": { "message": "invalid api key", "type": "invalid_request_error" }
        });
        let err = completion_text_from_response(&body, "gpt-4o-mini").unwrap_err();
        assert!(err.to_string().contains("invalid api key"));
    }

    #[tokio::test]
    async fn complete_posts_openai_chat_completions_shape() {
        let transport = ScriptedTransport::returning(json!({
            "choices": [{ "message": { "content": "pong" } }]
        }));
        let gateway =
            OpenAiHttpGateway::with_transport(transport.clone(), "sk-test", "gpt-4o-mini")
                .with_base_url("https://api.openai.com/v1/");

        let text = gateway.complete("ping").await.expect("complete");
        assert_eq!(text, "pong");

        let calls = transport.calls.lock().expect("calls");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "https://api.openai.com/v1/chat/completions");
        assert_eq!(calls[0].1, "sk-test");
        assert_eq!(calls[0].2["model"], "gpt-4o-mini");
        assert_eq!(calls[0].2["messages"][0]["role"], "user");
        assert_eq!(calls[0].2["messages"][0]["content"], "ping");
    }

    #[tokio::test]
    async fn complete_surfaces_transport_errors() {
        let gateway = OpenAiHttpGateway::with_transport(
            ScriptedTransport::failing("openai http 401: unauthorized"),
            "sk-bad",
            "gpt-4o-mini",
        );
        let err = gateway.complete("hi").await.unwrap_err();
        assert!(err.to_string().contains("401"));
    }

    #[tokio::test]
    async fn stasis_sdk_invokes_agent_through_openai_http_gateway() {
        use crate::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
        use crate::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
        use crate::sdk::stasis_sdk::StasisSdk;

        let gateway = OpenAiHttpGateway::with_transport(
            ScriptedTransport::returning(json!({
                "choices": [{ "message": { "content": "real-ish completion" } }]
            })),
            "sk-test",
            "gpt-4o-mini",
        );
        let sdk = StasisSdk::new(InMemoryAgentRepository::default(), gateway);
        sdk.register_agent(RegisterAgentRequest {
            id: "planner".into(),
            name: "Planner".into(),
            system_prompt: "Be brief".into(),
        })
        .await
        .unwrap();
        let out = sdk
            .invoke_agent(InvokeAgentRequest {
                agent_id: "planner".into(),
                user_prompt: "Hi".into(),
            })
            .await
            .unwrap();
        assert_eq!(out.completion, "real-ish completion");
    }

    #[test]
    fn construction_keeps_model_and_base_url() {
        let gateway = OpenAiHttpGateway::new("sk-test", "gpt-4o-mini")
            .with_base_url("https://api.groq.com/openai/v1/");
        assert_eq!(gateway.model(), "gpt-4o-mini");
        assert_eq!(gateway.base_url(), "https://api.groq.com/openai/v1");
    }
}
