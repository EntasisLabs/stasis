use async_trait::async_trait;
use futures_util::StreamExt;
use genai::adapter::AdapterKind;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, ChatStreamEvent, MessageContent, Usage};
use genai::resolver::{AuthData, Endpoint};
use genai::{Client, ServiceTarget};
use tokio::sync::mpsc;

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::ai_chat_client::AiChatClient;

const DEFAULT_MODEL: &str = "gpt-4o-mini";
const DEFAULT_PROVIDER: &str = "openai";

#[derive(Clone, Debug)]
pub struct GenaiChatClient {
    client: Client,
    model: String,
}

impl GenaiChatClient {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            client: Client::default(),
            model: model.into(),
        }
    }

    pub fn new_with_base_url(model: impl Into<String>, base_url: Option<&str>) -> Self {
        let model = model.into();
        let client = Self::build_client(base_url);
        Self { client, model }
    }

    pub fn from_provider_model(provider: Option<&str>, model: &str) -> Self {
        Self::from_provider_model_with_base_url(provider, model, None)
    }

    pub fn from_provider_model_with_base_url(
        provider: Option<&str>,
        model: &str,
        base_url: Option<&str>,
    ) -> Self {
        let target = Self::build_model_target(provider, model);
        Self::new_with_base_url(target, base_url)
    }

    pub fn build_model_target(provider: Option<&str>, model: &str) -> String {
        let model = model.trim();
        if model.contains("::") {
            return model.to_string();
        }

        let provider = provider
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_PROVIDER);
        format!("{provider}::{model}")
    }

    pub fn from_env() -> Self {
        let provider = std::env::var("STASIS_LLM_PROVIDER").ok();
        let model = std::env::var("STASIS_LLM_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        let base_url = std::env::var("STASIS_LLM_BASE_URL").ok();
        Self::from_provider_model_with_base_url(provider.as_deref(), &model, base_url.as_deref())
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    fn build_client(base_url: Option<&str>) -> Client {
        let mut builder = Client::builder().with_auth_resolver_fn(|model_iden: genai::ModelIden| {
            Ok(Self::resolve_auth_data(model_iden.adapter_kind))
        });

        if let Some(base_url) = base_url
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(Self::normalize_base_url)
        {
            builder = builder.with_service_target_resolver_fn(move |service_target: ServiceTarget| {
                let ServiceTarget { auth, model, .. } = service_target;
                Ok(ServiceTarget {
                    endpoint: Endpoint::from_owned(base_url.clone()),
                    auth,
                    model,
                })
            });
        }

        builder.build()
    }

    fn normalize_base_url(base_url: &str) -> String {
        if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{base_url}/")
        }
    }

    fn resolve_auth_data(adapter_kind: AdapterKind) -> Option<AuthData> {
        for env_name in Self::auth_env_candidates(adapter_kind) {
            if let Ok(value) = std::env::var(&env_name) {
                if !value.trim().is_empty() {
                    return Some(AuthData::from_single(value));
                }
            }
        }
        None
    }

    fn auth_env_candidates(adapter_kind: AdapterKind) -> Vec<String> {
        let mut candidates = Vec::with_capacity(3);

        // Provider-scoped Stasis override, e.g. STASIS_ANTHROPIC_API_KEY.
        candidates.push(format!(
            "STASIS_{}_API_KEY",
            adapter_kind.as_lower_str().to_ascii_uppercase()
        ));

        // Default genai provider env mapped under a Stasis prefix, e.g. STASIS_OPENAI_API_KEY.
        if let Some(default_env_name) = adapter_kind.default_key_env_name() {
            let alias = format!("STASIS_{default_env_name}");
            if !candidates.contains(&alias) {
                candidates.push(alias);
            }
        }

        // Optional global fallback for single-key deployments.
        candidates.push("STASIS_LLM_API_KEY".to_string());

        candidates
    }

}

#[async_trait]
impl AiChatClient for GenaiChatClient {
    async fn complete(&self, request: ChatRequest, options: Option<&ChatOptions>) -> Result<ChatResponse> {
        let response = self
            .client
            .exec_chat(&self.model, request, options)
            .await
            .map_err(|err| {
                StasisError::PortFailure(format!(
                    "genai chat completion failed for model '{}': {}",
                    self.model, err
                ))
            })?;
        Ok(response)
    }

    async fn complete_stream(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
        chunk_tx: Option<&mpsc::UnboundedSender<String>>,
    ) -> Result<ChatResponse> {
        let stream_options = options
            .cloned()
            .unwrap_or_default()
            .with_capture_content(true)
            .with_capture_usage(true);

        let mut stream_response = self
            .client
            .exec_chat_stream(&self.model, request, Some(&stream_options))
            .await
            .map_err(|err| {
                StasisError::PortFailure(format!(
                    "genai chat stream failed for model '{}': {}",
                    self.model, err
                ))
            })?;

        let model_iden = stream_response.model_iden.clone();
        let mut streamed_text = String::new();
        let mut captured_content: Option<MessageContent> = None;
        let mut usage: Usage = Usage::default();

        while let Some(event) = stream_response.stream.next().await {
            match event.map_err(|err| {
                StasisError::PortFailure(format!(
                    "genai chat stream event failed for model '{}': {}",
                    self.model, err
                ))
            })? {
                ChatStreamEvent::Chunk(chunk) => {
                    if !chunk.content.is_empty() {
                        streamed_text.push_str(&chunk.content);
                        if let Some(tx) = chunk_tx {
                            let _ = tx.send(chunk.content);
                        }
                    }
                }
                ChatStreamEvent::End(end) => {
                    captured_content = end.captured_content;
                    usage = end.captured_usage.unwrap_or_default();
                }
                _ => {}
            }
        }

        let content = match captured_content {
            Some(content) => content,
            None if !streamed_text.is_empty() => MessageContent::from_text(streamed_text),
            None => MessageContent::default(),
        };

        Ok(ChatResponse {
            content,
            reasoning_content: None,
            model_iden: model_iden.clone(),
            provider_model_iden: model_iden,
            usage,
            captured_raw_body: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use genai::adapter::AdapterKind;

    use super::GenaiChatClient;

    #[test]
    fn chat_client_construction_keeps_model_name() {
        let client = GenaiChatClient::new("openai::gpt-4o-mini");
        assert_eq!(client.model(), "openai::gpt-4o-mini");
    }

    #[test]
    fn build_model_target_uses_provider_namespace() {
        let target = GenaiChatClient::build_model_target(Some("ollama"), "gemma3:4b");
        assert_eq!(target, "ollama::gemma3:4b");
    }

    #[test]
    fn build_model_target_keeps_existing_namespace() {
        let target = GenaiChatClient::build_model_target(Some("openai"), "anthropic::claude-3-5-haiku-latest");
        assert_eq!(target, "anthropic::claude-3-5-haiku-latest");
    }

    #[test]
    fn normalize_base_url_appends_trailing_slash() {
        let normalized = GenaiChatClient::normalize_base_url("http://localhost:11434/v1");
        assert_eq!(normalized, "http://localhost:11434/v1/");
    }

    #[test]
    fn auth_env_candidates_include_provider_scoped_and_global_fallback() {
        let candidates = GenaiChatClient::auth_env_candidates(AdapterKind::Anthropic);
        assert_eq!(candidates[0], "STASIS_ANTHROPIC_API_KEY");
        assert_eq!(candidates[1], "STASIS_LLM_API_KEY");
    }

    #[test]
    fn auth_env_candidates_include_default_env_alias_for_openai_resp() {
        let candidates = GenaiChatClient::auth_env_candidates(AdapterKind::OpenAIResp);
        assert_eq!(candidates[0], "STASIS_OPENAI_RESP_API_KEY");
        assert_eq!(candidates[1], "STASIS_OPENAI_API_KEY");
        assert_eq!(candidates[2], "STASIS_LLM_API_KEY");
    }
}
