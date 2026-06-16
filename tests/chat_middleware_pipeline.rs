use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use genai::ModelIden;
use genai::adapter::AdapterKind;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, MessageContent, ToolCall, Usage};
use tokio::sync::Mutex;

use stasis::application::orchestration::runtime_job_payloads::PromptJobPayload;
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::application::runtime::chat_client_middleware::ChatClientMiddleware;
use stasis::application::runtime::default_chat_middlewares::{
    CHAT_CACHE_HIT_TOTAL, CHAT_CACHE_MISS_TOTAL, CHAT_DURATION_MS, CHAT_ERRORS_TOTAL,
    CHAT_REQUESTS_TOTAL, CHAT_TOOL_CALLS_TOTAL,
};
use stasis::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::domain::errors::{Result, StasisError};
use stasis::domain::runtime::job::JobState;
use stasis::infrastructure::runtime::in_memory_ai_chat_response_cache::InMemoryAiChatResponseCache;
use stasis::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
use stasis::ports::outbound::ai_chat_client::AiChatClient;
use stasis::ports::outbound::ai_chat_tool_interceptor::{
    AiChatToolInterceptor, AiToolCallEnvelope,
};
use stasis::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct RecordingChatClient {
    events: Arc<Mutex<Vec<String>>>,
    fail: bool,
}

#[async_trait]
impl AiChatClient for RecordingChatClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        self.events.lock().await.push("base".to_string());
        if self.fail {
            return Err(StasisError::PortFailure(
                "synthetic middleware failure".to_string(),
            ));
        }

        Ok(ChatResponse {
            content: MessageContent::from_text("middleware pipeline ok"),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            stop_reason: None,
            usage: Usage::default(),
            captured_raw_body: None,
            response_id: None,
        })
    }
}

#[derive(Clone)]
struct RecordingMiddleware {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl ChatClientMiddleware for RecordingMiddleware {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient> {
        Arc::new(RecordingMiddlewareClient {
            name: self.name,
            events: self.events.clone(),
            inner,
        })
    }
}

#[derive(Clone)]
struct RecordingMiddlewareClient {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    inner: Arc<dyn AiChatClient>,
}

#[derive(Clone, Default)]
struct RecordingToolInterceptor {
    envelopes: Arc<Mutex<Vec<AiToolCallEnvelope>>>,
}

impl AiChatToolInterceptor for RecordingToolInterceptor {
    fn on_tool_calls(&self, envelope: AiToolCallEnvelope) {
        if let Ok(mut guard) = self.envelopes.try_lock() {
            guard.push(envelope);
        }
    }
}

#[derive(Clone)]
struct ModelToolCallClient {
    call_count: Arc<AtomicUsize>,
}

impl ModelToolCallClient {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AiChatClient for ModelToolCallClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let call_index = self.call_count.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            return Ok(ChatResponse {
                content: MessageContent::from_tool_calls(vec![ToolCall {
                    call_id: "tool-call-1".to_string(),
                    fn_name: "stasis.web.search.mock".to_string(),
                    fn_arguments: serde_json::json!({"query": "rust"}),
                    thought_signatures: None,
                }]),
                reasoning_content: None,
                model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            stop_reason: None,
                usage: Usage::default(),
                captured_raw_body: None,
            response_id: None,
            });
        }

        Ok(ChatResponse {
            content: MessageContent::from_text("plain text"),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            stop_reason: None,
            usage: Usage::default(),
            captured_raw_body: None,
            response_id: None,
        })
    }
}

#[async_trait]
impl AiChatClient for RecordingMiddlewareClient {
    async fn complete(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        self.events
            .lock()
            .await
            .push(format!("{}:before", self.name));
        match self.inner.complete(request, options).await {
            Ok(response) => {
                self.events
                    .lock()
                    .await
                    .push(format!("{}:after", self.name));
                Ok(response)
            }
            Err(err) => {
                self.events
                    .lock()
                    .await
                    .push(format!("{}:error", self.name));
                Err(err)
            }
        }
    }
}

fn prompt_payload() -> PromptJobPayload {
    PromptJobPayload {
        user_prompt: "middleware parity check".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        reasoning_effort: None,
        memory_policy: None,
    }
}

#[tokio::test]
async fn runtime_builder_chat_middleware_executes_in_registered_order() {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let now = Utc::now();

    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(RecordingChatClient {
            events: events.clone(),
            fail: false,
        }))
        .with_chat_middleware(RecordingMiddleware {
            name: "a",
            events: events.clone(),
        })
        .with_chat_middleware(RecordingMiddleware {
            name: "b",
            events: events.clone(),
        })
        .without_grapheme_handlers()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let job_id = "job-middleware-order-1".to_string();
    let new_job = RuntimeWorkflowJobBuilder::for_prompt(job_id.clone(), &prompt_payload())
        .expect("payload should serialize")
        .with_scheduled_at(now)
        .build();
    runtime.enqueue(new_job).await.expect("job should enqueue");

    runtime
        .process_once("default", "worker-middleware", now)
        .await
        .expect("processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let recorded = events.lock().await.clone();
    assert_eq!(
        recorded,
        vec![
            "a:before".to_string(),
            "b:before".to_string(),
            "base".to_string(),
            "b:after".to_string(),
            "a:after".to_string(),
        ]
    );
}

#[tokio::test]
async fn runtime_builder_chat_middleware_propagates_failures() {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let now = Utc::now();

    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(RecordingChatClient {
            events: events.clone(),
            fail: true,
        }))
        .with_chat_middleware(RecordingMiddleware {
            name: "outer",
            events: events.clone(),
        })
        .with_chat_middleware(RecordingMiddleware {
            name: "inner",
            events: events.clone(),
        })
        .without_grapheme_handlers()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let job_id = "job-middleware-failure-1".to_string();
    let new_job = RuntimeWorkflowJobBuilder::for_prompt(job_id.clone(), &prompt_payload())
        .expect("payload should serialize")
        .with_scheduled_at(now)
        .build();
    runtime.enqueue(new_job).await.expect("job should enqueue");

    runtime
        .process_once("default", "worker-middleware", now)
        .await
        .expect("processing should complete");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempts should load");
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("synthetic middleware failure")
    );

    let recorded = events.lock().await.clone();
    assert_eq!(
        recorded,
        vec![
            "outer:before".to_string(),
            "inner:before".to_string(),
            "base".to_string(),
            "inner:error".to_string(),
            "outer:error".to_string(),
        ]
    );
}

#[tokio::test]
async fn runtime_builder_cache_middleware_reuses_response_for_identical_requests() {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let cache = Arc::new(InMemoryAiChatResponseCache::default());
    let now = Utc::now();

    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(RecordingChatClient {
            events: events.clone(),
            fail: false,
        }))
        .with_cache_chat_middleware(cache)
        .without_grapheme_handlers()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    for idx in 1..=2 {
        let job_id = format!("job-middleware-cache-{idx}");
        let new_job = RuntimeWorkflowJobBuilder::for_prompt(job_id.clone(), &prompt_payload())
            .expect("payload should serialize")
            .with_scheduled_at(now)
            .build();
        runtime.enqueue(new_job).await.expect("job should enqueue");

        runtime
            .process_once("default", "worker-middleware", now)
            .await
            .expect("processing should succeed");

        let job = runtime
            .job_store
            .get(&job_id)
            .await
            .expect("job get should succeed")
            .expect("job should exist");
        assert_eq!(job.state, JobState::Succeeded);
    }

    let recorded = events.lock().await.clone();
    assert_eq!(recorded, vec!["base".to_string()]);
}

#[tokio::test]
async fn runtime_builder_telemetry_and_cache_middlewares_emit_metrics() {
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let cache = Arc::new(InMemoryAiChatResponseCache::default());
    let now = Utc::now();

    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(RecordingChatClient {
            events,
            fail: false,
        }))
        .with_telemetry_chat_middleware(metrics.clone())
        .with_chat_middleware(
            stasis::application::runtime::default_chat_middlewares::CacheChatMiddleware::new(cache)
                .with_metrics(metrics.clone()),
        )
        .without_grapheme_handlers()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    for idx in 1..=2 {
        let job_id = format!("job-middleware-metrics-{idx}");
        let new_job = RuntimeWorkflowJobBuilder::for_prompt(job_id.clone(), &prompt_payload())
            .expect("payload should serialize")
            .with_scheduled_at(now)
            .build();
        runtime.enqueue(new_job).await.expect("job should enqueue");

        runtime
            .process_once("default", "worker-middleware", now)
            .await
            .expect("processing should complete");
    }

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.counters.get(CHAT_REQUESTS_TOTAL).copied(), Some(2));
    assert_eq!(snapshot.counters.get(CHAT_ERRORS_TOTAL).copied(), None);
    assert_eq!(
        snapshot.counters.get(CHAT_CACHE_MISS_TOTAL).copied(),
        Some(1)
    );
    assert_eq!(
        snapshot.counters.get(CHAT_CACHE_HIT_TOTAL).copied(),
        Some(1)
    );

    let durations = snapshot
        .durations_ms
        .get(CHAT_DURATION_MS)
        .cloned()
        .unwrap_or_default();
    assert_eq!(durations.len(), 2);
}

#[tokio::test]
async fn tool_call_interception_middleware_emits_envelope_and_metrics() {
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let interceptor = Arc::new(RecordingToolInterceptor::default());
    let middleware = stasis::application::runtime::default_chat_middlewares::ToolCallInterceptionChatMiddleware::new(interceptor.clone())
        .with_metrics(metrics.clone());
    let client = middleware.wrap(Arc::new(ModelToolCallClient::new()));
    let request = ChatRequest::new(vec![genai::chat::ChatMessage::user("test")]);

    client
        .complete(request.clone(), None)
        .await
        .expect("first call should succeed");
    client
        .complete(request, None)
        .await
        .expect("second call should succeed");

    let envelopes = interceptor.envelopes.lock().await.clone();
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].tool_call_count, 1);
    assert_eq!(
        envelopes[0].tool_names,
        vec!["stasis.web.search.mock".to_string()]
    );
    assert!(!envelopes[0].request_fingerprint.is_empty());

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.counters.get(CHAT_TOOL_CALLS_TOTAL).copied(),
        Some(1)
    );
}
