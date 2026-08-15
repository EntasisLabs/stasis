use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use genai::ModelIden;
use genai::adapter::AdapterKind;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, MessageContent, ToolCall, Usage};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use stasis::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline,
};
use stasis::application::orchestration::tool_loop_pipeline::{
    ToolCallMode, ToolLoopExecutionRequest, ToolLoopPipeline,
};
use stasis::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
use stasis::application::runtime::chat_client_middleware::ChatClientMiddleware;
use stasis::application::runtime::default_chat_middlewares::LoggingChatMiddleware;
use stasis::domain::errors::{Result, StasisError};
use stasis::ports::outbound::ai_chat_client::{AiChatClient, StreamDelta, send_stream_delta};

fn chat_response(text: &str) -> ChatResponse {
    ChatResponse {
        content: MessageContent::from_text(text),
        reasoning_content: None,
        model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
        provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
        stop_reason: None,
        usage: Usage::default(),
        captured_raw_body: None,
        response_id: None,
    }
}

fn empty_request() -> ChatRequest {
    ChatRequest::new(vec![])
}

#[derive(Clone)]
struct CompleteOnlyClient {
    text: Arc<str>,
}

impl CompleteOnlyClient {
    fn new(text: impl Into<String>) -> Self {
        Self {
            text: Arc::<str>::from(text.into()),
        }
    }
}

#[async_trait]
impl AiChatClient for CompleteOnlyClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        Ok(chat_response(&self.text))
    }
}

#[derive(Clone)]
struct ScriptedStreamClient {
    deltas: Arc<Vec<StreamDelta>>,
    sent: Arc<AtomicUsize>,
    seen_max_capacity: Arc<AtomicUsize>,
    final_text: Arc<str>,
}

impl ScriptedStreamClient {
    fn new(deltas: Vec<StreamDelta>, final_text: impl Into<String>) -> Self {
        Self {
            deltas: Arc::new(deltas),
            sent: Arc::new(AtomicUsize::new(0)),
            seen_max_capacity: Arc::new(AtomicUsize::new(0)),
            final_text: Arc::<str>::from(final_text.into()),
        }
    }
}

#[async_trait]
impl AiChatClient for ScriptedStreamClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        Ok(chat_response(&self.final_text))
    }

    async fn complete_stream(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
        chunk_tx: Option<&mpsc::Sender<StreamDelta>>,
    ) -> Result<ChatResponse> {
        let Some(tx) = chunk_tx else {
            return Ok(chat_response(&self.final_text));
        };
        self.seen_max_capacity
            .store(tx.max_capacity(), Ordering::SeqCst);
        for delta in self.deltas.iter().cloned() {
            send_stream_delta(tx, delta).await?;
            self.sent.fetch_add(1, Ordering::SeqCst);
        }
        Ok(chat_response(&self.final_text))
    }
}

/// Mimics GenaiChatClient empty-stream fallback: complete(), then one Content delta.
#[derive(Clone)]
struct EmptyStreamFallbackClient {
    sent: Arc<AtomicUsize>,
    seen_max_capacity: Arc<AtomicUsize>,
    text: Arc<str>,
}

impl EmptyStreamFallbackClient {
    fn new(text: impl Into<String>) -> Self {
        Self {
            sent: Arc::new(AtomicUsize::new(0)),
            seen_max_capacity: Arc::new(AtomicUsize::new(0)),
            text: Arc::<str>::from(text.into()),
        }
    }
}

#[async_trait]
impl AiChatClient for EmptyStreamFallbackClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        Ok(chat_response(&self.text))
    }

    async fn complete_stream(
        &self,
        request: ChatRequest,
        options: Option<&ChatOptions>,
        chunk_tx: Option<&mpsc::Sender<StreamDelta>>,
    ) -> Result<ChatResponse> {
        let response = self.complete(request, options).await?;
        if let Some(tx) = chunk_tx {
            self.seen_max_capacity
                .store(tx.max_capacity(), Ordering::SeqCst);
            if let Some(text) = response.first_text() {
                send_stream_delta(tx, StreamDelta::Content(text.to_string())).await?;
                self.sent.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(response)
    }
}

#[derive(Clone)]
struct ToolLoopStreamClient {
    sent: Arc<AtomicUsize>,
    seen_max_capacity: Arc<AtomicUsize>,
    call_count: Arc<AtomicUsize>,
}

impl ToolLoopStreamClient {
    fn new() -> Self {
        Self {
            sent: Arc::new(AtomicUsize::new(0)),
            seen_max_capacity: Arc::new(AtomicUsize::new(0)),
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AiChatClient for ToolLoopStreamClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        Ok(chat_response("unused"))
    }

    async fn complete_stream(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
        chunk_tx: Option<&mpsc::Sender<StreamDelta>>,
    ) -> Result<ChatResponse> {
        let tx = chunk_tx.expect("tool loop stream path must pass bounded sender");
        self.seen_max_capacity
            .store(tx.max_capacity(), Ordering::SeqCst);
        let call = self.call_count.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            send_stream_delta(tx, StreamDelta::Content("calling ".to_string())).await?;
            self.sent.fetch_add(1, Ordering::SeqCst);
            send_stream_delta(tx, StreamDelta::Content("tool".to_string())).await?;
            self.sent.fetch_add(1, Ordering::SeqCst);
            return Ok(ChatResponse {
                content: MessageContent::from_tool_calls(vec![ToolCall {
                    call_id: "call-1".to_string(),
                    fn_name: "echo_tool".to_string(),
                    fn_arguments: json!({"q": "hi"}),
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

        send_stream_delta(tx, StreamDelta::Content("final ".to_string())).await?;
        self.sent.fetch_add(1, Ordering::SeqCst);
        send_stream_delta(tx, StreamDelta::Content("answer".to_string())).await?;
        self.sent.fetch_add(1, Ordering::SeqCst);
        Ok(chat_response("final answer"))
    }
}

struct EchoTool;

#[async_trait]
impl StasisTool for EchoTool {
    fn name(&self) -> &'static str {
        "echo_tool"
    }

    async fn invoke(&self, input: serde_json::Value) -> Result<serde_json::Value> {
        Ok(input)
    }
}

async fn wait_until_sent(sent: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if sent.load(Ordering::SeqCst) >= expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for sent={expected}, got {}",
                sent.load(Ordering::SeqCst)
            );
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn capacity_one_stalled_consumer_blocks_producer() {
    let client = ScriptedStreamClient::new(
        vec![
            StreamDelta::Content("a".into()),
            StreamDelta::Content("b".into()),
            StreamDelta::Content("c".into()),
        ],
        "abc",
    );
    let sent = client.sent.clone();
    let (tx, _rx) = mpsc::channel(1);
    let client_for_task = client.clone();
    let handle = tokio::spawn(async move {
        client_for_task
            .complete_stream(empty_request(), None, Some(&tx))
            .await
    });

    wait_until_sent(&sent, 1).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        sent.load(Ordering::SeqCst),
        1,
        "must not accumulate past capacity"
    );
    assert!(!handle.is_finished(), "producer must remain blocked");
    drop(handle);
}

#[tokio::test]
async fn releasing_one_slot_resumes_blocked_producer() {
    let client = ScriptedStreamClient::new(
        vec![
            StreamDelta::Content("a".into()),
            StreamDelta::Content("b".into()),
            StreamDelta::Content("c".into()),
        ],
        "abc",
    );
    let sent = client.sent.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let client_for_task = client.clone();
    let handle = tokio::spawn(async move {
        client_for_task
            .complete_stream(empty_request(), None, Some(&tx))
            .await
    });

    wait_until_sent(&sent, 1).await;
    let first = rx.recv().await.expect("first delta");
    assert_eq!(first, StreamDelta::Content("a".into()));
    wait_until_sent(&sent, 2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sent.load(Ordering::SeqCst), 2);
    assert!(!handle.is_finished());
    drop(rx);
    let result = handle.await.expect("join");
    assert!(matches!(result, Err(StasisError::StreamClosed)));
}

#[tokio::test]
async fn dropping_receiver_wakes_producer_with_stream_closed() {
    let client = ScriptedStreamClient::new(
        vec![
            StreamDelta::Content("a".into()),
            StreamDelta::Content("b".into()),
        ],
        "ab",
    );
    let sent = client.sent.clone();
    let (tx, rx) = mpsc::channel(1);
    let client_for_task = client.clone();
    let handle = tokio::spawn(async move {
        client_for_task
            .complete_stream(empty_request(), None, Some(&tx))
            .await
    });

    wait_until_sent(&sent, 1).await;
    drop(rx);
    let result = timeout(Duration::from_secs(2), handle)
        .await
        .expect("producer must wake promptly")
        .expect("join");
    assert!(matches!(result, Err(StasisError::StreamClosed)));
}

#[tokio::test]
async fn cancellation_while_blocked_terminates_promptly() {
    let client = ScriptedStreamClient::new(
        vec![
            StreamDelta::Content("a".into()),
            StreamDelta::Content("b".into()),
        ],
        "ab",
    );
    let sent = client.sent.clone();
    let (tx, _rx) = mpsc::channel(1);
    let client_for_task = client.clone();
    let handle = tokio::spawn(async move {
        client_for_task
            .complete_stream(empty_request(), None, Some(&tx))
            .await
    });

    wait_until_sent(&sent, 1).await;
    handle.abort();
    let join = timeout(Duration::from_millis(500), handle)
        .await
        .expect("aborted producer must finish promptly");
    assert!(join.expect_err("join should be cancelled").is_cancelled());
}

#[tokio::test]
async fn multiple_deltas_retain_exact_order() {
    let deltas = vec![
        StreamDelta::Reasoning("think".into()),
        StreamDelta::Content("hello ".into()),
        StreamDelta::Content("world".into()),
        StreamDelta::ThoughtSignature("sig".into()),
    ];
    let client = ScriptedStreamClient::new(deltas.clone(), "hello world");
    let (tx, mut rx) = mpsc::channel(8);
    client
        .complete_stream(empty_request(), None, Some(&tx))
        .await
        .expect("stream ok");
    drop(tx);

    let mut received = Vec::new();
    while let Some(delta) = rx.recv().await {
        received.push(delta);
    }
    assert_eq!(received, deltas);
}

#[tokio::test]
async fn tool_loop_stream_uses_bounded_sender_and_preserves_order() {
    let client = ToolLoopStreamClient::new();
    let sent = client.sent.clone();
    let seen_cap = client.seen_max_capacity.clone();
    let registry = InMemoryToolRegistry::default();
    registry.register_tool(EchoTool).expect("register tool");
    let pipeline = ToolLoopPipeline::new(
        PromptExecutionPipeline::new(Arc::new(client)),
        Arc::new(registry),
    );
    let request = ToolLoopExecutionRequest {
        user_prompt: "use the tool".into(),
        system_prompt: None,
        context: PromptExecutionContext::default(),
        tool_name: String::new(),
        tool_input: json!({}),
        tool_call_mode: ToolCallMode::Auto,
    };

    let (tx, mut rx) = mpsc::channel(8);
    let response = pipeline
        .execute_with_stream(request, Some(&tx))
        .await
        .expect("tool loop stream");
    drop(tx);

    assert_eq!(response.text, "final answer");
    assert_eq!(seen_cap.load(Ordering::SeqCst), 8);
    assert_eq!(sent.load(Ordering::SeqCst), 4);

    let mut received = Vec::new();
    while let Some(delta) = rx.recv().await {
        received.push(delta);
    }
    assert_eq!(
        received,
        vec![
            StreamDelta::Content("calling ".into()),
            StreamDelta::Content("tool".into()),
            StreamDelta::Content("final ".into()),
            StreamDelta::Content("answer".into()),
        ]
    );
}

#[tokio::test]
async fn tool_loop_legacy_fallback_streams_through_bounded_sender() {
    let client = ScriptedStreamClient::new(
        vec![
            StreamDelta::Content("draft ".into()),
            StreamDelta::Content("then synth".into()),
        ],
        "draft then synth",
    );
    let seen_cap = client.seen_max_capacity.clone();
    let registry = InMemoryToolRegistry::default();
    registry.register_tool(EchoTool).expect("register tool");
    let pipeline = ToolLoopPipeline::new(
        PromptExecutionPipeline::new(Arc::new(client)),
        Arc::new(registry),
    );
    let request = ToolLoopExecutionRequest {
        user_prompt: "please echo".into(),
        system_prompt: None,
        context: PromptExecutionContext::default(),
        tool_name: "echo_tool".into(),
        tool_input: json!({"q": "hi"}),
        tool_call_mode: ToolCallMode::Auto,
    };

    let (tx, mut rx) = mpsc::channel(4);
    let response = pipeline
        .execute_with_stream(request, Some(&tx))
        .await
        .expect("legacy fallback stream");
    drop(tx);

    assert_eq!(
        response.termination_reason,
        "legacy_fallback_no_model_tool_call"
    );
    assert_eq!(seen_cap.load(Ordering::SeqCst), 4);
    let mut received = Vec::new();
    while let Some(delta) = rx.recv().await {
        received.push(delta);
    }
    assert!(
        !received.is_empty(),
        "fallback synthesis must stream deltas"
    );
}

#[tokio::test]
async fn empty_stream_fallback_uses_bounded_send() {
    let client = EmptyStreamFallbackClient::new("recovered");
    let seen_cap = client.seen_max_capacity.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let response = client
        .complete_stream(empty_request(), None, Some(&tx))
        .await
        .expect("fallback ok");
    drop(tx);
    assert_eq!(response.first_text(), Some("recovered"));
    assert_eq!(seen_cap.load(Ordering::SeqCst), 1);
    assert_eq!(
        rx.recv().await,
        Some(StreamDelta::Content("recovered".into()))
    );
}

#[tokio::test]
async fn middleware_forwards_complete_stream_and_backpressures() {
    let inner = ScriptedStreamClient::new(
        vec![
            StreamDelta::Content("a".into()),
            StreamDelta::Content("b".into()),
        ],
        "ab",
    );
    let sent = inner.sent.clone();
    let seen_cap = inner.seen_max_capacity.clone();
    let wrapped = LoggingChatMiddleware.wrap(Arc::new(inner));

    let (tx, _rx) = mpsc::channel(1);
    let handle = tokio::spawn(async move {
        wrapped
            .complete_stream(empty_request(), None, Some(&tx))
            .await
    });

    wait_until_sent(&sent, 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    assert_eq!(seen_cap.load(Ordering::SeqCst), 1);
    assert!(
        !handle.is_finished(),
        "middleware must not absorb backpressure"
    );
    drop(handle);
}

#[tokio::test]
async fn default_complete_stream_fails_when_receiver_closed() {
    let client = CompleteOnlyClient::new("hello");
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let err = client
        .complete_stream(empty_request(), None, Some(&tx))
        .await
        .expect_err("closed receiver");
    assert!(matches!(err, StasisError::StreamClosed));
}
