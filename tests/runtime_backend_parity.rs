use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use genai::ModelIden;
use genai::adapter::AdapterKind;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, MessageContent, ToolCall, Usage};
use serde_json::{Value as JsonValue, json};
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use tokio::sync::Mutex;

use stasis::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
use stasis::application::orchestration::agent_session_pipeline::{
    AgentParticipant, AgentSessionCoordinator, AgentSessionPipeline, AgentSessionRunRequest,
    AgentTurnExecutionPolicy, MaxTurnsTerminationStrategy, RoundRobinSelectionStrategy,
};
use stasis::application::orchestration::agent_session_payload::{
    AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode,
    AgentTurnJobPayload, ToolLoopJobPayload,
};
use stasis::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline,
};
use stasis::application::orchestration::stasis_workflow_job_builder::StasisWorkflowJobBuilder;
use stasis::application::orchestration::tool_loop_pipeline::{ToolCallMode, ToolLoopPipeline};
use stasis::application::runtime::agent_session_job_handler::AgentSessionJobHandler;
use stasis::application::runtime::agent_turn_job_handler::AgentTurnJobHandler;
use stasis::application::runtime::in_memory_runtime::{InMemoryRuntime, JobExecutionOutcome, JobHandler};
use stasis::application::runtime::grapheme_echo_job_handler::GraphemeEchoJobHandler;
use stasis::application::runtime::grapheme_healthcheck_job_handler::GraphemeHealthcheckJobHandler;
use stasis::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use stasis::application::runtime::grapheme_textops_job_handler::GraphemeTextOpsJobHandler;
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::application::runtime::tool_loop_job_handler::ToolLoopJobHandler;
use stasis::application::runtime::retention::RetentionPolicy;
use stasis::application::use_cases::investigate_runtime_lineage::RuntimeLineageQuery;
use stasis::domain::errors::Result;
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::job_attempt::JobAttemptOutcome;
use stasis::domain::runtime::outbox::{OutboxPublishPolicy, OutboxStatus, RuntimeEventType};
use stasis::domain::runtime::recurring::RecurringDefinition;
use stasis::infrastructure::runtime::grapheme_sdk_workflow_engine::{
    GraphemeSdkWorkflowEngine, GraphemeWorkflowGuardrails,
};
use stasis::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
use stasis::infrastructure::runtime::tokio_channel_event_publisher::TokioChannelEventPublisher;
use stasis::ports::outbound::runtime::event_publisher::EventPublisher;
use stasis::ports::outbound::runtime::clock::Clock;
use stasis::ports::outbound::runtime::id_generator::IdGenerator;
use stasis::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;
use stasis::ports::outbound::runtime::workflow_engine::WorkflowEngine;
use stasis::ports::outbound::ai_chat_client::AiChatClient;

struct FixedClock {
    now: DateTime<Utc>,
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

struct PrefixIdGenerator {
    seq: AtomicUsize,
}

impl PrefixIdGenerator {
    fn new() -> Self {
        Self {
            seq: AtomicUsize::new(1),
        }
    }
}

impl IdGenerator for PrefixIdGenerator {
    fn next_id(&self, _prefix: &str) -> String {
        format!("custom-id-{}", self.seq.fetch_add(1, Ordering::SeqCst))
    }
}

struct AlwaysSuccessHandler;

#[async_trait]
impl JobHandler for AlwaysSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.success"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:success".to_string(),
            execution_id: None,
            diagnostics: None,
        })
    }
}

struct ParentSuccessHandler;

#[async_trait]
impl JobHandler for ParentSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.parent"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:parent".to_string(),
            execution_id: None,
            diagnostics: None,
        })
    }
}

struct ChildSuccessHandler;

#[async_trait]
impl JobHandler for ChildSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.child"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:child".to_string(),
            execution_id: None,
            diagnostics: None,
        })
    }
}

#[derive(Clone)]
struct CountingPublisher {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl EventPublisher for CountingPublisher {
    async fn publish(&self, _event: &stasis::domain::runtime::outbox::OutboxEvent) -> Result<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone)]
struct FlakyPublisher {
    failures_before_success: usize,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EventPublisher for FlakyPublisher {
    async fn publish(&self, _event: &stasis::domain::runtime::outbox::OutboxEvent) -> Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.failures_before_success {
            return Err(stasis::domain::errors::StasisError::PortFailure(
                "synthetic publish failure".to_string(),
            ));
        }

        Ok(())
    }
}

struct FatalThenSuccessHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl JobHandler for FatalThenSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.fatal_then_success"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 1 {
            return Ok(JobExecutionOutcome::FatalFailure {
                message: "first run fails".to_string(),
                execution_id: None,
                diagnostics: None,
            });
        }

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:replayed".to_string(),
            execution_id: None,
            diagnostics: None,
        })
    }
}

#[derive(Clone)]
struct ScriptedChatClient {
    responses: Arc<Vec<String>>,
    call_count: Arc<AtomicUsize>,
}

impl ScriptedChatClient {
    fn new(responses: Vec<String>) -> Self {
        Self {
            responses: Arc::new(responses),
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AiChatClient for ScriptedChatClient {
    async fn complete(&self, request: ChatRequest, _options: Option<&ChatOptions>) -> Result<ChatResponse> {
        let index = self.call_count.fetch_add(1, Ordering::SeqCst);
        if index == 1 {
            let includes_tool_output = request
                .messages
                .iter()
                .filter_map(|message| message.content.first_text())
                .any(|text| text.contains("Tool 'stasis.web.search.mock' output JSON"));
            assert!(
                includes_tool_output,
                "second prompt call should include tool output block"
            );
        }

        let text = self
            .responses
            .get(index)
            .cloned()
            .unwrap_or_else(|| "fallback response".to_string());

        Ok(ChatResponse {
            content: MessageContent::from_text(text),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            usage: Usage::default(),
            captured_raw_body: None,
        })
    }
}

#[derive(Clone)]
struct ModelToolCallScriptedClient {
    call_count: Arc<AtomicUsize>,
}

impl ModelToolCallScriptedClient {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AiChatClient for ModelToolCallScriptedClient {
    async fn complete(&self, request: ChatRequest, _options: Option<&ChatOptions>) -> Result<ChatResponse> {
        let index = self.call_count.fetch_add(1, Ordering::SeqCst);

        if index == 0 {
            return Ok(ChatResponse {
                content: MessageContent::from_tool_calls(vec![ToolCall {
                    call_id: "tool-call-1".to_string(),
                    fn_name: "stasis.web.search.mock".to_string(),
                    fn_arguments: json!({ "query": "latest rust trends" }),
                    thought_signatures: None,
                }]),
                reasoning_content: None,
                model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                usage: Usage::default(),
                captured_raw_body: None,
            });
        }

        let includes_tool_response = request
            .messages
            .iter()
            .any(|message| !message.content.tool_responses().is_empty());
        assert!(
            includes_tool_response,
            "second round should include a tool response message"
        );

        Ok(ChatResponse {
            content: MessageContent::from_text("final answer from model tool-call path"),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            usage: Usage::default(),
            captured_raw_body: None,
        })
    }
}

struct MockWebSearchTool;

#[async_trait]
impl StasisTool for MockWebSearchTool {
    fn name(&self) -> &'static str {
        "stasis.web.search.mock"
    }

    fn input_schema(&self) -> Option<JsonValue> {
        Some(json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string"
                }
            },
            "required": ["query"],
            "additionalProperties": false
        }))
    }

    async fn invoke(&self, input: JsonValue) -> Result<JsonValue> {
        let query = input
            .get("query")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");

        Ok(json!({
            "query": query,
            "results": [
                {
                    "title": "Rust in production",
                    "source": "mock://rust-prod"
                }
            ]
        }))
    }
}

fn build_new_job(job_type: &str, now: chrono::DateTime<Utc>) -> NewJob {
    NewJob {
        id: format!("job-{job_type}"),
        queue: "default".to_string(),
        job_type: job_type.to_string(),
        payload_ref: "payload:ref".to_string(),
        priority: 100,
        max_attempts: 3,
        idempotency_key: format!("idem-{job_type}"),
        correlation_id: "corr-1".to_string(),
        causation_id: "cause-1".to_string(),
        trace_id: "trace-1".to_string(),
        sttp_input_node_id: "sttp:in:1".to_string(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 1,
            max_delay_seconds: 8,
        },
    }
}

fn test_backoff_policy() -> BackoffPolicy {
    BackoffPolicy {
        base_delay_seconds: 1,
        max_delay_seconds: 8,
    }
}

fn build_tool_loop_job(
    job_id: &str,
    payload: &ToolLoopJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    StasisWorkflowJobBuilder::for_tool_loop(job_id.to_string(), payload)
        .expect("tool-loop payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

fn build_agent_turn_job(
    job_id: &str,
    payload: &AgentTurnJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    StasisWorkflowJobBuilder::for_agent_turn(job_id.to_string(), payload)
        .expect("agent-turn payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

fn build_agent_session_job(
    job_id: &str,
    payload: &AgentSessionJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    StasisWorkflowJobBuilder::for_agent_session(job_id.to_string(), payload)
        .expect("agent-session payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[tokio::test]
async fn in_memory_runtime_emits_and_publishes_outbox_events() {
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    let published_count = Arc::new(AtomicUsize::new(0));
    runtime
        .register_event_publisher(CountingPublisher {
            count: published_count.clone(),
        })
        .expect("publisher should register");

    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("processing should succeed");

    let job = runtime
        .job_store
        .get("job-test.success")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, OutboxStatus::Pending);
    assert_eq!(pending[0].event.event_type, RuntimeEventType::JobSucceeded);

    let published = runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("publish pending should succeed");

    assert_eq!(published, 1);
    assert_eq!(published_count.load(Ordering::SeqCst), 1);

    let still_pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert!(still_pending.is_empty());
}

#[tokio::test]
async fn in_memory_tool_loop_job_handler_executes_and_persists_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "final answer grounded in tool evidence".to_string(),
    ]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-1",
            "corr-tool-loop-1",
            "cause-tool-loop-1",
            "trace-tool-loop-1",
            "sttp:in:tool-loop:1",
        ))
        .await
        .expect("tool-loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool-loop processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(job.sttp_output_node_id.as_deref(), Some("sttp:tool-loop:job-tool-loop-1"));

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::Succeeded);
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-tool-loop")));
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(diagnostics.get("tool_name"), Some(&json!("stasis.web.search.mock")));
    assert_eq!(diagnostics.get("tool_rounds"), Some(&json!(1)));
    assert_eq!(
        diagnostics.get("termination_reason"),
        Some(&json!("legacy_fallback_no_model_tool_call"))
    );
    assert_eq!(
        diagnostics.pointer("/invoked_tools/0"),
        Some(&json!("stasis.web.search.mock"))
    );
    assert_eq!(
        diagnostics.pointer("/tool_output/query"),
        Some(&json!("latest rust trends"))
    );
    assert!(
        diagnostics
            .get("output_preview")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .contains("final answer")
    );

    let lineage = runtime
        .list_lineage_events(&job_id)
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 1);
    assert_eq!(lineage[0].event.correlation_id, "corr-tool-loop-1");
    assert_eq!(lineage[0].event.trace_id, "trace-tool-loop-1");
}

#[tokio::test]
async fn in_memory_tool_loop_model_emitted_tool_call_roundtrip() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ModelToolCallScriptedClient::new());
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-model-tool-call-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "fallback query" })),
        tool_call_mode: Some(AgentToolCallMode::Strict),
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-model-tool-call-1",
            "corr-tool-loop-model-tool-call-1",
            "cause-tool-loop-model-tool-call-1",
            "trace-tool-loop-model-tool-call-1",
            "sttp:in:tool-loop:model-tool-call:1",
        ))
        .await
        .expect("tool-loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool-loop processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::Succeeded);

    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("tool_rounds"), Some(&json!(2)));
    assert_eq!(
        diagnostics.get("termination_reason"),
        Some(&json!("model_completed_no_tool_calls"))
    );
    assert_eq!(
        diagnostics.pointer("/invoked_tools/0"),
        Some(&json!("stasis.web.search.mock"))
    );
    assert_eq!(
        diagnostics.pointer("/tool_invocations/0/tool_input/query"),
        Some(&json!("latest rust trends"))
    );
    assert_eq!(
        diagnostics.pointer("/tool_output/query"),
        Some(&json!("latest rust trends"))
    );
}

#[tokio::test]
async fn in_memory_agent_turn_job_handler_executes_single_agent_turn() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "agent final answer grounded in tool evidence".to_string(),
    ]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentTurnJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("agent-turn handler should register");

    let now = Utc::now();
    let job_id = "job-agent-turn-1".to_string();
    let payload = AgentTurnJobPayload {
        agent_id: "agent.researcher".to_string(),
        thread_id: Some("thread-42".to_string()),
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_turn_job(
            &job_id,
            &payload,
            now,
            "idem-agent-turn-1",
            "corr-agent-turn-1",
            "cause-agent-turn-1",
            "trace-agent-turn-1",
            "sttp:in:agent-turn:1",
        ))
        .await
        .expect("agent-turn job should enqueue");

    runtime
        .process_once("default", "worker-agent-turn", now)
        .await
        .expect("agent-turn processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(job.sttp_output_node_id.as_deref(), Some("sttp:agent-turn:job-agent-turn-1"));

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::Succeeded);

    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-agent-turn")));
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(diagnostics.get("agent_id"), Some(&json!("agent.researcher")));
    assert_eq!(diagnostics.get("thread_id"), Some(&json!("thread-42")));
    assert_eq!(
        diagnostics.pointer("/invoked_tools/0"),
        Some(&json!("stasis.web.search.mock"))
    );
    assert_eq!(
        diagnostics.get("termination_reason"),
        Some(&json!("legacy_fallback_no_model_tool_call"))
    );

    let lineage = runtime
        .list_lineage_events(&job_id)
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 1);
    assert_eq!(lineage[0].event.correlation_id, "corr-agent-turn-1");
    assert_eq!(lineage[0].event.trace_id, "trace-agent-turn-1");
}

#[tokio::test]
async fn in_memory_agent_session_job_handler_executes_session() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft turn 1".to_string(),
        "final turn 1".to_string(),
        "draft turn 2".to_string(),
        "final turn 2".to_string(),
    ]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentSessionJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("agent-session handler should register");

    let now = Utc::now();
    let job_id = "job-agent-session-1".to_string();
    let payload = AgentSessionJobPayload {
        thread_id: Some("thread-session-1".to_string()),
        initial_user_prompt: "Coordinate a short research answer".to_string(),
        participants: vec![
            AgentSessionParticipantPayload {
                agent_id: "agent.alpha".to_string(),
                system_prompt: Some("You are agent alpha".to_string()),
                tool_name: "stasis.web.search.mock".to_string(),
                tool_input: Some(json!({"query": "rust trends"})),
            },
            AgentSessionParticipantPayload {
                agent_id: "agent.beta".to_string(),
                system_prompt: Some("You are agent beta".to_string()),
                tool_name: "stasis.web.search.mock".to_string(),
                tool_input: Some(json!({"query": "rust trends"})),
            },
        ],
        policy_profile: Some("default".to_string()),
        model_hint: None,
        max_turns: Some(2),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_session_job(
            &job_id,
            &payload,
            now,
            "idem-agent-session-1",
            "corr-agent-session-1",
            "cause-agent-session-1",
            "trace-agent-session-1",
            "sttp:in:agent-session:1",
        ))
        .await
        .expect("agent-session job should enqueue");

    runtime
        .process_once("default", "worker-agent-session", now)
        .await
        .expect("agent-session processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:agent-session:job-agent-session-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::Succeeded);

    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-agent-session")));
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(diagnostics.get("turn_count"), Some(&json!(2)));
    assert_eq!(diagnostics.get("terminated"), Some(&json!(true)));
    assert_eq!(
        diagnostics.pointer("/participant_ids/0"),
        Some(&json!("agent.alpha"))
    );
}

#[tokio::test]
async fn surreal_agent_session_job_handler_executes_session() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_agent_session_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft turn 1".to_string(),
        "final turn 1".to_string(),
        "draft turn 2".to_string(),
        "final turn 2".to_string(),
    ]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentSessionJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("agent-session handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-agent-session-1".to_string();
    let payload = AgentSessionJobPayload {
        thread_id: Some("thread-surreal-session-1".to_string()),
        initial_user_prompt: "Coordinate a short research answer".to_string(),
        participants: vec![
            AgentSessionParticipantPayload {
                agent_id: "agent.alpha".to_string(),
                system_prompt: Some("You are agent alpha".to_string()),
                tool_name: "stasis.web.search.mock".to_string(),
                tool_input: Some(json!({"query": "rust trends"})),
            },
            AgentSessionParticipantPayload {
                agent_id: "agent.beta".to_string(),
                system_prompt: Some("You are agent beta".to_string()),
                tool_name: "stasis.web.search.mock".to_string(),
                tool_input: Some(json!({"query": "rust trends"})),
            },
        ],
        policy_profile: Some("default".to_string()),
        model_hint: None,
        max_turns: Some(2),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_session_job(
            &job_id,
            &payload,
            now,
            "idem-surreal-agent-session-1",
            "corr-surreal-agent-session-1",
            "cause-surreal-agent-session-1",
            "trace-surreal-agent-session-1",
            "sttp:in:surreal-agent-session:1",
        ))
        .await
        .expect("agent-session job should enqueue");

    runtime
        .process_once("default", "worker-agent-session", now)
        .await
        .expect("agent-session processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:agent-session:job-surreal-agent-session-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::Succeeded);

    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-agent-session")));
    assert_eq!(diagnostics.get("turn_count"), Some(&json!(2)));
    assert_eq!(diagnostics.get("terminated"), Some(&json!(true)));
}

#[tokio::test]
async fn agent_session_coordinator_runs_multi_turn_with_round_robin_and_max_turns() {
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft turn 1".to_string(),
        "final turn 1".to_string(),
        "draft turn 2".to_string(),
        "final turn 2".to_string(),
    ]));
    let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");
    let tool_loop_pipeline = ToolLoopPipeline::new(prompt_pipeline, Arc::new(tool_registry));
    let agent_pipeline = AgentSessionPipeline::new(tool_loop_pipeline);
    let coordinator = AgentSessionCoordinator::new(
        agent_pipeline,
        Arc::new(RoundRobinSelectionStrategy::new()),
        Arc::new(MaxTurnsTerminationStrategy::new(2)),
    );

    let response = coordinator
        .run_session(AgentSessionRunRequest {
            thread_id: Some("thread-coord-1".to_string()),
            initial_user_prompt: "Coordinate a short research answer".to_string(),
            participants: vec![
                AgentParticipant {
                    agent_id: "agent.alpha".to_string(),
                    system_prompt: Some("You are agent alpha".to_string()),
                    tool_name: "stasis.web.search.mock".to_string(),
                    tool_input: json!({ "query": "rust trends" }),
                },
                AgentParticipant {
                    agent_id: "agent.beta".to_string(),
                    system_prompt: Some("You are agent beta".to_string()),
                    tool_name: "stasis.web.search.mock".to_string(),
                    tool_input: json!({ "query": "rust trends" }),
                },
            ],
            context: PromptExecutionContext {
                trace_id: Some("trace-coord-1".to_string()),
                correlation_id: Some("corr-coord-1".to_string()),
                policy_profile: Some("default".to_string()),
                model_hint: None,
            },
            max_turns_cap: 4,
            policy: AgentTurnExecutionPolicy {
                tool_call_mode: ToolCallMode::Auto,
            },
        })
        .await
        .expect("coordinator should run successfully");

    assert!(response.terminated);
    assert_eq!(response.turns.len(), 2);
    assert_eq!(response.turns[0].agent_id, "agent.alpha");
    assert_eq!(response.turns[1].agent_id, "agent.beta");
    assert_eq!(response.turns[0].response_text, "final turn 1");
    assert_eq!(response.turns[1].response_text, "final turn 2");
    assert_eq!(
        response.turns[0].termination_reason,
        "legacy_fallback_no_model_tool_call"
    );
    assert_eq!(response.thread_id.as_deref(), Some("thread-coord-1"));
    assert_eq!(response.transcript.len(), 3);
}

#[tokio::test]
async fn in_memory_tool_loop_strict_mode_requires_model_tool_call() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec!["plain model text".to_string()]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-strict-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: Some(AgentToolCallMode::Strict),
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-strict-1",
            "corr-tool-loop-strict-1",
            "cause-tool-loop-strict-1",
            "trace-tool-loop-strict-1",
            "sttp:in:tool-loop:strict:1",
        ))
        .await
        .expect("tool-loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool-loop processing should complete");

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
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(attempts[0].guardrail_code.as_deref(), Some("POLICY_VIOLATION"));
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("strict tool-call mode expected model tool call")
    );
}

#[tokio::test]
async fn in_memory_tool_loop_job_handler_policy_violation_dead_letters_job() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![]));
    let tool_registry = InMemoryToolRegistry::default();

    runtime
        .register_handler(ToolLoopJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-invalid-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "   ".to_string(),
        system_prompt: None,
        policy_profile: None,
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: None,
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-invalid-1",
            "corr-tool-loop-invalid-1",
            "cause-tool-loop-invalid-1",
            "trace-tool-loop-invalid-1",
            "sttp:in:tool-loop:invalid:1",
        ))
        .await
        .expect("tool-loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool-loop processing should complete");

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
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(attempts[0].guardrail_code.as_deref(), Some("POLICY_VIOLATION"));
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("payload.user_prompt must be non-empty")
    );

    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-tool-loop")));
    assert_eq!(diagnostics.get("status"), Some(&json!("failure")));
    assert_eq!(diagnostics.get("guardrail_code"), Some(&json!("POLICY_VIOLATION")));
    assert!(
        diagnostics
            .get("policy_reason")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .contains("payload.user_prompt must be non-empty")
    );

    let lineage = runtime
        .list_lineage_events(&job_id)
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 1);
    assert_eq!(lineage[0].event.event_type, RuntimeEventType::JobDeadLettered);
    assert_eq!(lineage[0].event.correlation_id, "corr-tool-loop-invalid-1");
    assert_eq!(lineage[0].event.trace_id, "trace-tool-loop-invalid-1");
}

#[tokio::test]
async fn in_memory_tool_loop_job_handler_rejects_tool_input_schema_mismatch() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-schema-invalid-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": 123 })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-schema-invalid-1",
            "corr-tool-loop-schema-invalid-1",
            "cause-tool-loop-schema-invalid-1",
            "trace-tool-loop-schema-invalid-1",
            "sttp:in:tool-loop:schema:1",
        ))
        .await
        .expect("tool-loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool-loop processing should complete");

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
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(attempts[0].guardrail_code.as_deref(), Some("POLICY_VIOLATION"));
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("expected type 'string'")
    );

    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("status"), Some(&json!("failure")));
    assert_eq!(diagnostics.get("guardrail_code"), Some(&json!("POLICY_VIOLATION")));
    assert!(
        diagnostics
            .get("policy_reason")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .contains("expected type 'string'")
    );
}

#[tokio::test]
async fn surreal_tool_loop_job_handler_rejects_tool_input_schema_mismatch() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_tool_loop_schema_violation")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let chat_client = Arc::new(ScriptedChatClient::new(vec![]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new(chat_client, Arc::new(tool_registry)))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-tool-loop-schema-invalid-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": 321 })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-surreal-tool-loop-schema-invalid-1",
            "corr-surreal-tool-loop-schema-invalid-1",
            "cause-surreal-tool-loop-schema-invalid-1",
            "trace-surreal-tool-loop-schema-invalid-1",
            "sttp:in:tool-loop:surreal:schema:1",
        ))
        .await
        .expect("tool-loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool-loop processing should complete");

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
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(attempts[0].guardrail_code.as_deref(), Some("POLICY_VIOLATION"));
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("expected type 'string'")
    );
}

#[tokio::test]
async fn in_memory_runtime_uses_injected_clock_and_id_generator() {
    let fixed_now = Utc::now();
    let runtime = InMemoryRuntime::with_dependencies(
        Arc::new(FixedClock { now: fixed_now }),
        Arc::new(PrefixIdGenerator::new()),
    );
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    runtime
        .enqueue(build_new_job("test.success", fixed_now))
        .await
        .expect("job should enqueue");

    runtime
        .process_once_now("default", "worker-clock-id")
        .await
        .expect("processing should succeed");

    let report = runtime
        .get_replay_report("job-test.success")
        .await
        .expect("replay report should load");
    assert_eq!(report.attempts.len(), 1);
    assert_eq!(report.attempts[0].started_at, fixed_now);
    assert!(report.attempts[0].attempt_id.starts_with("custom-id-"));

    let lineage = runtime
        .list_lineage_events("job-test.success")
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 1);
    assert_eq!(lineage[0].event.occurred_at, fixed_now);
    assert!(lineage[0].event_id.starts_with("custom-id-"));
}

#[tokio::test]
async fn in_memory_runtime_emits_runtime_metrics_for_job_and_outbox_flow() {
    let now = Utc::now();
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let runtime = InMemoryRuntime::with_dependencies_and_metrics(
        Arc::new(FixedClock { now }),
        Arc::new(PrefixIdGenerator::new()),
        metrics.clone(),
    );
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");
    runtime
        .register_event_publisher(CountingPublisher {
            count: Arc::new(AtomicUsize::new(0)),
        })
        .expect("publisher should register");

    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    runtime
        .process_once_now("default", "worker-metrics")
        .await
        .expect("processing should succeed");

    runtime
        .publish_pending_events_now(10)
        .await
        .expect("publish should succeed");

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot
            .counters
            .get("runtime.job.succeeded.total")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        snapshot
            .counters
            .get("runtime.outbox.publish.success.total")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert!(
        snapshot
            .durations_ms
            .get("runtime.job.process.duration_ms")
            .map(|values| !values.is_empty())
            .unwrap_or(false)
    );
}

#[tokio::test]
async fn surreal_runtime_matches_core_flow_and_recurring_materialization() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_backend_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    let published_count = Arc::new(AtomicUsize::new(0));
    runtime
        .register_event_publisher(CountingPublisher {
            count: published_count.clone(),
        })
        .expect("publisher should register");

    let now = Utc::now();
    runtime
        .register_recurring(RecurringDefinition {
            id: "recur.scrape".to_string(),
            queue: "default".to_string(),
            job_type: "test.success".to_string(),
            payload_template_ref: "sttp:in:recurring".to_string(),
            cron_expr: "0/1 * * * * * *".to_string(),
            timezone: "UTC".to_string(),
            jitter_seconds: 0,
            enabled: true,
            max_attempts: 4,
            next_run_at: now,
            last_run_at: None,
            lease_owner: None,
            lease_expires_at: None,
        })
        .await
        .expect("recurring should register");

    let created = runtime
        .materialize_recurring(now, "scheduler-1")
        .await
        .expect("materialization should succeed");
    assert_eq!(created, 1);

    let enqueued = runtime
        .job_store
        .list_by_state(JobState::Enqueued)
        .await
        .expect("list by state should succeed");
    assert_eq!(enqueued.len(), 1);

    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("processing should succeed");

    let succeeded = runtime
        .job_store
        .list_by_state(JobState::Succeeded)
        .await
        .expect("list by state should succeed");
    assert_eq!(succeeded.len(), 1);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_type, RuntimeEventType::JobSucceeded);

    let published = runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("publish pending should succeed");
    assert_eq!(published, 1);
    assert_eq!(published_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn surreal_runtime_replays_dead_letter_and_retries_outbox_publish() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_retry_replay")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    runtime
        .configure_outbox_publish_policy(OutboxPublishPolicy {
            max_attempts: 3,
            base_delay_seconds: 1,
            max_delay_seconds: 8,
        })
        .expect("policy should configure");

    let handler_calls = Arc::new(AtomicUsize::new(0));
    runtime
        .register_handler(FatalThenSuccessHandler {
            calls: handler_calls.clone(),
        })
        .expect("handler should register");

    let publisher_calls = Arc::new(AtomicUsize::new(0));
    runtime
        .register_event_publisher(FlakyPublisher {
            failures_before_success: 1,
            calls: publisher_calls.clone(),
        })
        .expect("publisher should register");

    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.fatal_then_success", now))
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("first processing should complete");

    let dead_lettered = runtime
        .job_store
        .get("job-test.fatal_then_success")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(dead_lettered.state, JobState::DeadLetter);

    let replayed = runtime
        .replay_dead_letter("job-test.fatal_then_success", now + Duration::seconds(1))
        .await
        .expect("replay should succeed");
    assert!(replayed);

    runtime
        .process_once("default", "worker-2", now + Duration::seconds(1))
        .await
        .expect("second processing should complete");

    let replay_report = runtime
        .get_replay_report("job-test.fatal_then_success")
        .await
        .expect("replay report should load");
    assert_eq!(replay_report.job_id, "job-test.fatal_then_success");
    assert_eq!(replay_report.attempts.len(), 2);
    assert_eq!(replay_report.attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(replay_report.attempts[1].outcome, JobAttemptOutcome::Succeeded);
    assert!(replay_report.attempts[0].error_message.is_some());
    assert!(replay_report.attempts[1].sttp_output_node_id.is_some());

    let lineage = runtime
        .list_lineage_events("job-test.fatal_then_success")
        .await
        .expect("lineage events should load");
    assert_eq!(lineage.len(), 2);
    assert!(lineage
        .iter()
        .all(|evt| evt.event.correlation_id == "corr-1"));
    assert!(lineage.iter().all(|evt| evt.event.trace_id == "trace-1"));

    let succeeded = runtime
        .job_store
        .get("job-test.fatal_then_success")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(succeeded.state, JobState::Succeeded);

    let first_publish = runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("first publish should complete");
    assert!(first_publish >= 1);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert_eq!(pending.len(), 1);
    assert!(pending.iter().all(|evt| evt.publish_attempts >= 1));

    let second_publish = runtime
        .publish_pending_events(10, now + Duration::seconds(2))
        .await
        .expect("second publish should complete");
    assert!(second_publish >= 1);
    assert!(publisher_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn tokio_channel_publisher_adapter_receives_outbox_events() {
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    let (publisher, rx) = TokioChannelEventPublisher::channel();
    runtime
        .register_event_publisher(publisher)
        .expect("publisher should register");

    let shared_rx = Arc::new(Mutex::new(rx));
    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");
    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("processing should succeed");
    runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("publish pending should succeed");

    let mut guard = shared_rx.lock().await;
    let received = guard
        .recv()
        .await
        .expect("publisher channel should receive event");
    assert_eq!(received.event.event_type, RuntimeEventType::JobSucceeded);
}

#[tokio::test]
async fn surreal_job_leasing_allows_only_one_winner_under_contention() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_lease_contention")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    let (a, b) = tokio::join!(
        runtime.job_store.lease_due("default", "worker-a", now, 30),
        runtime.job_store.lease_due("default", "worker-b", now, 30)
    );

    let leased_a = a.expect("lease call a should succeed");
    let leased_b = b.expect("lease call b should succeed");

    let winners = [leased_a, leased_b].iter().filter(|job| job.is_some()).count();
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn surreal_job_lease_expiry_allows_recovery_by_another_worker() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_lease_recovery")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    let first = runtime
        .job_store
        .lease_due("default", "worker-1", now, 1)
        .await
        .expect("first lease should succeed")
        .expect("first lease should acquire job");
    assert_eq!(first.lease_owner.as_deref(), Some("worker-1"));

    let during_lease = runtime
        .job_store
        .lease_due("default", "worker-2", now, 1)
        .await
        .expect("second lease call should succeed");
    assert!(during_lease.is_none());

    let recovered = runtime
        .job_store
        .lease_due("default", "worker-2", now + Duration::seconds(2), 30)
        .await
        .expect("recovery lease should succeed")
        .expect("recovery lease should acquire job");

    assert_eq!(recovered.lease_owner.as_deref(), Some("worker-2"));
}

#[tokio::test]
async fn in_memory_event_driven_continuation_job_executes_end_to_end() {
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(ParentSuccessHandler)
        .expect("parent handler should register");
    runtime
        .register_handler(ChildSuccessHandler)
        .expect("child handler should register");

    let (publisher, mut rx) = TokioChannelEventPublisher::channel();
    runtime
        .register_event_publisher(publisher)
        .expect("publisher should register");

    let now = Utc::now();
    let parent_job_id = "job-parent-1".to_string();
    runtime
        .enqueue(NewJob {
            id: parent_job_id.clone(),
            queue: "default".to_string(),
            job_type: "test.parent".to_string(),
            payload_ref: "payload:parent".to_string(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-parent-1".to_string(),
            correlation_id: "corr-parent-1".to_string(),
            causation_id: "cause-parent-1".to_string(),
            trace_id: "trace-parent-1".to_string(),
            sttp_input_node_id: "sttp:in:parent".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("parent job should enqueue");

    runtime
        .process_once("default", "worker-parent", now)
        .await
        .expect("parent processing should succeed");

    runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("outbox publish should succeed");

    let evt = rx.recv().await.expect("should receive runtime event");
    assert_eq!(evt.event.event_type, RuntimeEventType::JobSucceeded);
    assert_eq!(evt.event.job_id, parent_job_id);

    let parent_output = evt
        .event
        .sttp_output_node_id
        .clone()
        .expect("parent output node id should exist");

    let child_job_id = "job-child-1".to_string();
    runtime
        .enqueue(NewJob {
            id: child_job_id.clone(),
            queue: "default".to_string(),
            job_type: "test.child".to_string(),
            payload_ref: "payload:child".to_string(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-child-1".to_string(),
            correlation_id: "corr-parent-1".to_string(),
            causation_id: parent_job_id,
            trace_id: "trace-parent-1".to_string(),
            sttp_input_node_id: parent_output,
            scheduled_at: now + Duration::seconds(1),
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("child job should enqueue");

    runtime
        .process_once("default", "worker-child", now + Duration::seconds(1))
        .await
        .expect("child processing should succeed");

    let child = runtime
        .job_store
        .get(&child_job_id)
        .await
        .expect("child get should succeed")
        .expect("child should exist");

    assert_eq!(child.state, JobState::Succeeded);
    assert_eq!(child.sttp_input_node_id, "sttp:out:parent");
    assert_eq!(child.correlation_id, "corr-parent-1");
    assert_eq!(child.trace_id, "trace-parent-1");
}

#[tokio::test]
async fn in_memory_grapheme_sdk_workflow_job_executes_successfully() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeJobHandler::new(workflow_engine))
        .expect("grapheme handler should register");

    let source = r#"import core from "grapheme/core"

query Hello {
    core.echo(message: "hello from stasis grapheme handler") {
        state { current }
    }
}
"#;

    let now = Utc::now();
    let job_id = "job-grapheme-1".to_string();

    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: format!("grapheme:inline:{}", source),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-grapheme-1".to_string(),
            correlation_id: "corr-grapheme-1".to_string(),
            causation_id: "cause-grapheme-1".to_string(),
            trace_id: "trace-grapheme-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("grapheme job should enqueue");

    runtime
        .process_once("default", "worker-grapheme", now)
        .await
        .expect("grapheme processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
    assert!(
        job.sttp_output_node_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("sttp:grapheme:")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_number, 1);
    assert!(
        attempts[0]
            .execution_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("grapheme:")
    );
    assert!(attempts[0].guardrail_code.is_none());
    assert!(attempts[0].policy_reason.is_none());
    assert!(attempts[0].duration_ms.is_some());

    let execution_id = attempts[0]
        .execution_id
        .clone()
        .expect("execution id should be present");
    let attempts_by_execution = runtime
        .list_attempts_by_execution_id(&execution_id)
        .await
        .expect("attempts by execution should succeed");
    assert_eq!(attempts_by_execution.len(), 1);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    let event = pending
        .iter()
        .find(|evt| evt.event.job_id == job_id)
        .expect("outbox event should exist for grapheme job");
    assert!(
        event
            .event
            .execution_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("grapheme:")
    );

    let lineage_by_execution = runtime
        .list_lineage_events_by_execution_id(&execution_id)
        .await
        .expect("lineage by execution should succeed");
    assert_eq!(lineage_by_execution.len(), 1);
    assert_eq!(lineage_by_execution[0].event.job_id, job_id);
}

#[tokio::test]
async fn in_memory_grapheme_healthcheck_workflow_executes_successfully() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine))
        .expect("grapheme healthcheck handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-healthcheck-1".to_string();

    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.healthcheck".to_string(),
            payload_ref: "runtime-ready".to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-healthcheck-1".to_string(),
            correlation_id: "corr-grapheme-healthcheck-1".to_string(),
            causation_id: "cause-grapheme-healthcheck-1".to_string(),
            trace_id: "trace-grapheme-healthcheck-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:healthcheck:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("healthcheck job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-healthcheck", now)
        .await
        .expect("healthcheck processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
    assert!(
        job.sttp_output_node_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("sttp:grapheme:")
    );
}

#[tokio::test]
async fn surreal_grapheme_healthcheck_workflow_executes_successfully() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_grapheme_healthcheck")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());
    runtime
        .register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine))
        .expect("grapheme healthcheck handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-healthcheck-surreal-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.healthcheck".to_string(),
            payload_ref: "surreal-runtime-ready".to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-healthcheck-surreal-1".to_string(),
            correlation_id: "corr-grapheme-healthcheck-surreal-1".to_string(),
            causation_id: "cause-grapheme-healthcheck-surreal-1".to_string(),
            trace_id: "trace-grapheme-healthcheck-surreal-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:healthcheck:surreal:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("healthcheck job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-healthcheck", now)
        .await
        .expect("healthcheck processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
    assert!(
        job.sttp_output_node_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("sttp:grapheme:")
    );
}

#[tokio::test]
async fn in_memory_grapheme_echo_workflow_executes_successfully() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeEchoJobHandler::new(workflow_engine))
        .expect("grapheme echo handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-echo-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.echo".to_string(),
            payload_ref: r#"{"message":"echo-ready"}"#.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-echo-1".to_string(),
            correlation_id: "corr-grapheme-echo-1".to_string(),
            causation_id: "cause-grapheme-echo-1".to_string(),
            trace_id: "trace-grapheme-echo-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:echo:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("echo job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-echo", now)
        .await
        .expect("echo processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
}

#[tokio::test]
async fn surreal_grapheme_echo_workflow_executes_successfully() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_grapheme_echo")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());
    runtime
        .register_handler(GraphemeEchoJobHandler::new(workflow_engine))
        .expect("grapheme echo handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-echo-surreal-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.echo".to_string(),
            payload_ref: r#"{"message":"surreal-echo-ready"}"#.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-echo-surreal-1".to_string(),
            correlation_id: "corr-grapheme-echo-surreal-1".to_string(),
            causation_id: "cause-grapheme-echo-surreal-1".to_string(),
            trace_id: "trace-grapheme-echo-surreal-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:echo:surreal:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("echo job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-echo", now)
        .await
        .expect("echo processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
}

#[tokio::test]
async fn in_memory_grapheme_echo_rejects_invalid_payload_schema() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeEchoJobHandler::new(workflow_engine))
        .expect("grapheme echo handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-echo-invalid-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.echo".to_string(),
            payload_ref: r#"{"wrong":"shape"}"#.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-echo-invalid-1".to_string(),
            correlation_id: "corr-grapheme-echo-invalid-1".to_string(),
            causation_id: "cause-grapheme-echo-invalid-1".to_string(),
            trace_id: "trace-grapheme-echo-invalid-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:echo:invalid:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("echo job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-echo", now)
        .await
        .expect("echo processing should complete");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .list_job_attempts(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].guardrail_code.as_deref(), Some("POLICY_VIOLATION"));
    assert!(attempts[0]
        .policy_reason
        .as_deref()
        .unwrap_or_default()
        .contains("invalid echo payload json"));
}

#[tokio::test]
async fn in_memory_grapheme_textops_workflow_executes_successfully() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeTextOpsJobHandler::new(workflow_engine))
        .expect("grapheme textops handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-textops-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.textops".to_string(),
            payload_ref: r#"{"mode":"summarize","text":"Stasis runtime now supports replay. Grapheme workflows are guarded. Metrics are emitted for operations.","max_items":2}"#.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-textops-1".to_string(),
            correlation_id: "corr-grapheme-textops-1".to_string(),
            causation_id: "cause-grapheme-textops-1".to_string(),
            trace_id: "trace-grapheme-textops-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:textops:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("textops job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-textops", now)
        .await
        .expect("textops processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
}

#[tokio::test]
async fn surreal_grapheme_textops_workflow_executes_successfully() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_grapheme_textops")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());
    runtime
        .register_handler(GraphemeTextOpsJobHandler::new(workflow_engine))
        .expect("grapheme textops handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-textops-surreal-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.textops".to_string(),
            payload_ref: r#"{"mode":"extract_keywords","text":"Runtime orchestration metrics retention lineage diagnostics runtime runtime"}"#.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-textops-surreal-1".to_string(),
            correlation_id: "corr-grapheme-textops-surreal-1".to_string(),
            causation_id: "cause-grapheme-textops-surreal-1".to_string(),
            trace_id: "trace-grapheme-textops-surreal-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:textops:surreal:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("textops job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-textops", now)
        .await
        .expect("textops processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
}

#[tokio::test]
async fn in_memory_grapheme_textops_rejects_invalid_payload_schema() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeTextOpsJobHandler::new(workflow_engine))
        .expect("grapheme textops handler should register");

    let now = Utc::now();
    let job_id = "job-grapheme-textops-invalid-1".to_string();
    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.textops".to_string(),
            payload_ref: r#"{"mode":"summarize","text":"   "}"#.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-textops-invalid-1".to_string(),
            correlation_id: "corr-grapheme-textops-invalid-1".to_string(),
            causation_id: "cause-grapheme-textops-invalid-1".to_string(),
            trace_id: "trace-grapheme-textops-invalid-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:textops:invalid:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("textops job should enqueue");

    runtime
        .process_once("default", "worker-grapheme-textops", now)
        .await
        .expect("textops processing should complete");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .list_job_attempts(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].guardrail_code.as_deref(), Some("POLICY_VIOLATION"));
    assert!(attempts[0]
        .policy_reason
        .as_deref()
        .unwrap_or_default()
        .contains("must be non-empty"));
}

#[tokio::test]
async fn grapheme_sdk_rejects_non_allowlisted_import() {
    let engine = GraphemeSdkWorkflowEngine::new();
    let source = r#"import sql from "grapheme/sql"

query Run {
  sql.query(connection: "local", sql: "select 1") {
    rows
  }
}
"#;

    let err = engine
        .execute_grapheme_source(source)
        .await
        .expect_err("non-allowlisted import should be rejected");

    assert!(
        err.to_string().contains("not allowlisted"),
        "expected allowlist policy violation, got: {err}"
    );
}

#[tokio::test]
async fn grapheme_sdk_rejects_zero_execution_timeout() {
    let guardrails = GraphemeWorkflowGuardrails {
        execution_timeout: StdDuration::from_millis(0),
        ..GraphemeWorkflowGuardrails::default()
    };
    let engine = GraphemeSdkWorkflowEngine::with_guardrails(guardrails);
    let source = r#"import core from "grapheme/core"

query Hello {
  core.echo(message: "hello") {
    state { current }
  }
}
"#;

    let err = engine
        .execute_grapheme_source(source)
        .await
        .expect_err("zero timeout should reject execution");

    assert!(
        err.to_string().contains("timeout must be greater than 0ms"),
        "expected timeout policy violation, got: {err}"
    );
}

#[tokio::test]
async fn in_memory_grapheme_policy_failure_records_guardrail_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeJobHandler::new(workflow_engine))
        .expect("grapheme handler should register");

    let source = r#"import sql from "grapheme/sql"

query Run {
  sql.query(connection: "local", sql: "select 1") {
    rows
  }
}
"#;

    let now = Utc::now();
    let job_id = "job-grapheme-policy-failure".to_string();

    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: format!("grapheme:inline:{}", source),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-policy-failure".to_string(),
            correlation_id: "corr-grapheme-policy-failure".to_string(),
            causation_id: "cause-grapheme-policy-failure".to_string(),
            trace_id: "trace-grapheme-policy-failure".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:policy:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("grapheme job should enqueue");

    runtime
        .process_once("default", "worker-grapheme", now)
        .await
        .expect("grapheme processing should complete with fatal outcome");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .list_job_attempts(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_number, 1);
    assert!(attempts[0].execution_id.is_none());
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("IMPORT_NOT_ALLOWLISTED")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("not allowlisted")
    );
    assert!(attempts[0].duration_ms.is_some());

    let guardrail_attempts = runtime
        .list_attempts_by_guardrail_code("IMPORT_NOT_ALLOWLISTED")
        .await
        .expect("guardrail attempts query should succeed");
    assert!(guardrail_attempts.iter().any(|attempt| attempt.job_id == job_id));

    let diagnostics = attempts[0]
        .diagnostics
        .clone()
        .expect("diagnostics should be present");
    let diagnostics_json: JsonValue =
        serde_json::from_str(&diagnostics).expect("diagnostics should be valid json");

    assert_eq!(diagnostics_json["status"], "failure");
    assert_eq!(diagnostics_json["guardrail_code"], "IMPORT_NOT_ALLOWLISTED");
    assert!(
        diagnostics_json["policy_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("not allowlisted")
    );
    assert!(diagnostics_json["duration_ms"].as_u64().is_some());

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    let event = pending
        .iter()
        .find(|evt| evt.event.job_id == job_id)
        .expect("outbox event should exist for failed grapheme job");

    assert_eq!(event.event.event_type, RuntimeEventType::JobDeadLettered);
    assert!(event.event.execution_id.is_none());
    assert!(
        event
            .event
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("policy violation")
    );
}

#[tokio::test]
async fn in_memory_runtime_retention_prunes_terminal_records() {
    let now = Utc::now();
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");
    runtime
        .register_event_publisher(CountingPublisher {
            count: Arc::new(AtomicUsize::new(0)),
        })
        .expect("publisher should register");

    let old = now - Duration::days(10);
    runtime
        .enqueue(build_new_job("test.success", old))
        .await
        .expect("job should enqueue");
    runtime
        .process_once("default", "worker-retention", old)
        .await
        .expect("processing should succeed");
    runtime
        .publish_pending_events(10, old + Duration::seconds(1))
        .await
        .expect("publish should succeed");

    runtime
        .configure_retention_policy(RetentionPolicy { terminal_ttl_days: 1 })
        .expect("retention policy should configure");

    let report = runtime
        .enforce_retention(now)
        .await
        .expect("retention should enforce");

    assert_eq!(report.jobs_pruned, 1);
    assert_eq!(report.attempts_pruned, 1);
    assert_eq!(report.outbox_events_pruned, 1);

    let job = runtime
        .job_store
        .get("job-test.success")
        .await
        .expect("job get should succeed");
    assert!(job.is_none());
}

#[tokio::test]
async fn surreal_runtime_retention_prunes_terminal_records() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_retention_prune")
        .await
        .expect("namespace and db should be selected");

    let now = Utc::now();
    let old = now - Duration::days(10);
    let runtime = SurrealRuntime::new(db);
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");
    runtime
        .register_event_publisher(CountingPublisher {
            count: Arc::new(AtomicUsize::new(0)),
        })
        .expect("publisher should register");

    runtime
        .enqueue(build_new_job("test.success", old))
        .await
        .expect("job should enqueue");
    runtime
        .process_once("default", "worker-retention", old)
        .await
        .expect("processing should succeed");
    runtime
        .publish_pending_events(10, old + Duration::seconds(1))
        .await
        .expect("publish should succeed");

    runtime
        .configure_retention_policy(RetentionPolicy { terminal_ttl_days: 1 })
        .expect("retention policy should configure");

    let report = runtime
        .enforce_retention(now)
        .await
        .expect("retention should enforce");

    assert_eq!(report.jobs_pruned, 1);
    assert_eq!(report.attempts_pruned, 1);
    assert_eq!(report.outbox_events_pruned, 1);

    let job = runtime
        .job_store
        .get("job-test.success")
        .await
        .expect("job get should succeed");
    assert!(job.is_none());
}

#[tokio::test]
async fn lineage_investigator_queries_success_path_by_execution_id() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeJobHandler::new(workflow_engine))
        .expect("grapheme handler should register");

    let source = r#"import core from "grapheme/core"

query Hello {
  core.echo(message: "lineage investigator") {
    state { current }
  }
}
"#;

    let now = Utc::now();
    let job_id = "job-grapheme-lineage-success".to_string();

    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: format!("grapheme:inline:{}", source),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-lineage-success".to_string(),
            correlation_id: "corr-grapheme-lineage-success".to_string(),
            causation_id: "cause-grapheme-lineage-success".to_string(),
            trace_id: "trace-grapheme-lineage-success".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:lineage:success".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-lineage", now)
        .await
        .expect("processing should succeed");

    let attempts = runtime
        .list_job_attempts(&job_id)
        .await
        .expect("attempts should load");
    let execution_id = attempts[0]
        .execution_id
        .clone()
        .expect("execution id should be present");

    let report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            execution_id: Some(execution_id.clone()),
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("lineage investigation should succeed");

    assert_eq!(report.attempts.len(), 1);
    assert_eq!(report.attempts[0].job_id, job_id);
    assert_eq!(
        report.attempts[0].execution_id.as_deref(),
        Some(execution_id.as_str())
    );
    assert_eq!(report.lineage_events.len(), 1);
    assert_eq!(report.lineage_events[0].event.job_id, job_id);
}

#[tokio::test]
async fn lineage_investigator_queries_guardrail_failures() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeJobHandler::new(workflow_engine))
        .expect("grapheme handler should register");

    let source = r#"import sql from "grapheme/sql"

query Run {
  sql.query(connection: "local", sql: "select 1") {
    rows
  }
}
"#;

    let now = Utc::now();
    let job_id = "job-grapheme-lineage-guardrail".to_string();

    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: format!("grapheme:inline:{}", source),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-grapheme-lineage-guardrail".to_string(),
            correlation_id: "corr-grapheme-lineage-guardrail".to_string(),
            causation_id: "cause-grapheme-lineage-guardrail".to_string(),
            trace_id: "trace-grapheme-lineage-guardrail".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:lineage:guardrail".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-lineage", now)
        .await
        .expect("processing should complete");

    let report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            guardrail_code: Some("IMPORT_NOT_ALLOWLISTED".to_string()),
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("lineage investigation should succeed");

    assert!(report.attempts.iter().any(|attempt| attempt.job_id == job_id));
    assert!(report
        .attempts
        .iter()
        .any(|attempt| attempt.guardrail_code.as_deref() == Some("IMPORT_NOT_ALLOWLISTED")));
    assert!(report
        .lineage_events
        .iter()
        .any(|event| event.event.job_id == job_id));
}

#[tokio::test]
async fn lineage_investigator_requires_selector() {
    let runtime = InMemoryRuntime::new();
    let err = runtime
        .investigate_lineage(RuntimeLineageQuery::default())
        .await
        .expect_err("empty selector should fail");

    assert!(err
        .to_string()
        .contains("requires at least one selector"));
}
