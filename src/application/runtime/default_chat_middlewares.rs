use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse};
use sha2::{Digest, Sha256};

use crate::application::runtime::chat_client_middleware::ChatClientMiddleware;
use crate::domain::errors::Result;
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::ai_chat_response_cache::AiChatResponseCache;
use crate::ports::outbound::ai_chat_tool_interceptor::{AiChatToolInterceptor, AiToolCallEnvelope};
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;

pub const CHAT_REQUESTS_TOTAL: &str = "runtime.chat.requests.total";
pub const CHAT_ERRORS_TOTAL: &str = "runtime.chat.errors.total";
pub const CHAT_DURATION_MS: &str = "runtime.chat.duration_ms";
pub const CHAT_CACHE_HIT_TOTAL: &str = "runtime.chat.cache.hit.total";
pub const CHAT_CACHE_MISS_TOTAL: &str = "runtime.chat.cache.miss.total";
pub const CHAT_TOOL_CALLS_TOTAL: &str = "runtime.chat.tool_calls.total";

#[derive(Clone, Default)]
pub struct LoggingChatMiddleware;

impl ChatClientMiddleware for LoggingChatMiddleware {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient> {
        Arc::new(LoggingChatClient { inner })
    }
}

#[derive(Clone)]
struct LoggingChatClient {
    inner: Arc<dyn AiChatClient>,
}

#[async_trait]
impl AiChatClient for LoggingChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let started = Instant::now();
        eprintln!(
            "stasis.chat request messages={} options_present={}",
            request.messages.len(),
            options.is_some()
        );

        match self.inner.complete(request, options).await {
            Ok(response) => {
                eprintln!(
                    "stasis.chat response ok elapsed_ms={}",
                    started.elapsed().as_millis()
                );
                Ok(response)
            }
            Err(err) => {
                eprintln!(
                    "stasis.chat response error elapsed_ms={} error={}",
                    started.elapsed().as_millis(),
                    err
                );
                Err(err)
            }
        }
    }
}

#[derive(Clone)]
pub struct TelemetryChatMiddleware {
    metrics: Arc<dyn RuntimeMetrics>,
}

impl TelemetryChatMiddleware {
    pub fn new(metrics: Arc<dyn RuntimeMetrics>) -> Self {
        Self { metrics }
    }
}

impl ChatClientMiddleware for TelemetryChatMiddleware {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient> {
        Arc::new(TelemetryChatClient {
            inner,
            metrics: self.metrics.clone(),
        })
    }
}

#[derive(Clone)]
struct TelemetryChatClient {
    inner: Arc<dyn AiChatClient>,
    metrics: Arc<dyn RuntimeMetrics>,
}

#[async_trait]
impl AiChatClient for TelemetryChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        self.metrics.incr_counter(CHAT_REQUESTS_TOTAL, 1);
        let started = Instant::now();
        match self.inner.complete(request, options).await {
            Ok(response) => {
                self.metrics
                    .observe_duration_ms(CHAT_DURATION_MS, started.elapsed().as_millis() as u64);
                Ok(response)
            }
            Err(err) => {
                self.metrics.incr_counter(CHAT_ERRORS_TOTAL, 1);
                self.metrics
                    .observe_duration_ms(CHAT_DURATION_MS, started.elapsed().as_millis() as u64);
                Err(err)
            }
        }
    }
}

#[derive(Clone)]
pub struct CacheChatMiddleware {
    cache: Arc<dyn AiChatResponseCache>,
    metrics: Option<Arc<dyn RuntimeMetrics>>,
}

impl CacheChatMiddleware {
    pub fn new(cache: Arc<dyn AiChatResponseCache>) -> Self {
        Self {
            cache,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn RuntimeMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

impl ChatClientMiddleware for CacheChatMiddleware {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient> {
        Arc::new(CacheChatClient {
            inner,
            cache: self.cache.clone(),
            metrics: self.metrics.clone(),
        })
    }
}

#[derive(Clone)]
struct CacheChatClient {
    inner: Arc<dyn AiChatClient>,
    cache: Arc<dyn AiChatResponseCache>,
    metrics: Option<Arc<dyn RuntimeMetrics>>,
}

#[async_trait]
impl AiChatClient for CacheChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let cache_key = deterministic_cache_key(&request, options);
        if let Some(cached) = self.cache.get(&cache_key) {
            if let Some(metrics) = &self.metrics {
                metrics.incr_counter(CHAT_CACHE_HIT_TOTAL, 1);
            }
            return Ok(cached);
        }
        if let Some(metrics) = &self.metrics {
            metrics.incr_counter(CHAT_CACHE_MISS_TOTAL, 1);
        }

        let response = self.inner.complete(request, options).await?;
        self.cache.set(&cache_key, response.clone());
        Ok(response)
    }
}

pub fn deterministic_cache_key(request: &ChatRequest, options: Option<&ChatOptions>) -> String {
    let basis = format!("request={request:?}|options={options:?}");
    let mut hasher = Sha256::new();
    hasher.update(basis.as_bytes());
    format!("chat:{}", hex::encode(hasher.finalize()))
}

#[derive(Clone)]
pub struct ToolCallInterceptionChatMiddleware {
    interceptor: Arc<dyn AiChatToolInterceptor>,
    metrics: Option<Arc<dyn RuntimeMetrics>>,
}

impl ToolCallInterceptionChatMiddleware {
    pub fn new(interceptor: Arc<dyn AiChatToolInterceptor>) -> Self {
        Self {
            interceptor,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn RuntimeMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

impl ChatClientMiddleware for ToolCallInterceptionChatMiddleware {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient> {
        Arc::new(ToolCallInterceptionChatClient {
            inner,
            interceptor: self.interceptor.clone(),
            metrics: self.metrics.clone(),
        })
    }
}

#[derive(Clone)]
struct ToolCallInterceptionChatClient {
    inner: Arc<dyn AiChatClient>,
    interceptor: Arc<dyn AiChatToolInterceptor>,
    metrics: Option<Arc<dyn RuntimeMetrics>>,
}

#[async_trait]
impl AiChatClient for ToolCallInterceptionChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let request_fingerprint = deterministic_cache_key(&request, options);
        let response = self.inner.complete(request, options).await?;

        let tool_calls = response.clone().into_tool_calls();
        if !tool_calls.is_empty() {
            let tool_call_count = tool_calls.len();
            let tool_names = tool_calls.into_iter().map(|call| call.fn_name).collect();
            self.interceptor.on_tool_calls(AiToolCallEnvelope {
                request_fingerprint,
                tool_call_count,
                tool_names,
            });
            if let Some(metrics) = &self.metrics {
                metrics.incr_counter(CHAT_TOOL_CALLS_TOTAL, tool_call_count as u64);
            }
        }

        Ok(response)
    }
}
