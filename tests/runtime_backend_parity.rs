use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration as StdDuration;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use genai::ModelIden;
use genai::adapter::AdapterKind;
use genai::chat::{ChatOptions, ChatRequest, ChatResponse, MessageContent, ToolCall, Usage};
use serde_json::{Value as JsonValue, json};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tokio::sync::Mutex;

use stasis::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode, AgentTurnJobPayload,
    ConcurrentBranchExecutionMode, ConcurrentBranchJobPayload, ConcurrentPatternJobPayload, HandoffPatternJobPayload,
    HandoffTurnJobPayload, MemoryAggregateJobPayload, MemoryRecallJobPayload,
    MemoryRollupJobPayload, MemorySchemaJobPayload, MemoryTransformJobPayload,
    OrchestratorPatternJobPayload, OrchestratorRouteJobPayload, PromptJobPayload,
    SequentialPatternJobPayload, SequentialStageJobPayload, ToolLoopJobPayload,
};
use stasis::application::orchestration::agent_session_pipeline::{
    AgentParticipant, AgentSessionCoordinator, AgentSessionPipeline, AgentSessionRunRequest,
    AgentTurnExecutionPolicy, MaxTurnsTerminationStrategy, RoundRobinSelectionStrategy,
};
use stasis::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::application::orchestration::tool_loop_pipeline::{ToolCallMode, ToolLoopPipeline};
use stasis::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
use stasis::application::runtime::agent_session_job_handler::AgentSessionJobHandler;
use stasis::application::runtime::agent_turn_job_handler::AgentTurnJobHandler;
use stasis::application::runtime::default_chat_middlewares::{
    CHAT_CACHE_HIT_TOTAL, CHAT_CACHE_MISS_TOTAL, CHAT_REQUESTS_TOTAL, CHAT_TOOL_CALLS_TOTAL,
    CacheChatMiddleware, ToolCallInterceptionChatMiddleware,
};
use stasis::application::runtime::grapheme_echo_job_handler::GraphemeEchoJobHandler;
use stasis::application::runtime::grapheme_healthcheck_job_handler::GraphemeHealthcheckJobHandler;
use stasis::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use stasis::application::runtime::grapheme_textops_job_handler::GraphemeTextOpsJobHandler;
use stasis::application::runtime::in_memory_runtime::{
    InMemoryRuntime, JobExecutionOutcome, JobHandler,
};
use stasis::application::runtime::memory_aggregate_job_handler::MemoryAggregateJobHandler;
use stasis::application::runtime::memory_recall_job_handler::MemoryRecallJobHandler;
use stasis::application::runtime::memory_rollup_job_handler::MemoryRollupJobHandler;
use stasis::application::runtime::memory_schema_job_handler::MemorySchemaJobHandler;
use stasis::application::runtime::memory_transform_job_handler::MemoryTransformJobHandler;
use stasis::application::runtime::prompt_chat_job_handler::PromptChatJobHandler;
use stasis::application::runtime::retention::RetentionPolicy;
use stasis::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::application::runtime::tool_loop_job_handler::ToolLoopJobHandler;
use stasis::application::use_cases::investigate_runtime_lineage::RuntimeLineageQuery;
use stasis::domain::errors::Result;
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::job_attempt::JobAttemptOutcome;
use stasis::domain::runtime::outbox::{OutboxPublishPolicy, OutboxStatus, RuntimeEventType};
use stasis::domain::runtime::recurring::RecurringDefinition;
use stasis::domain::runtime::thread::{NewThread, NewThreadEvent};
use stasis::infrastructure::runtime::grapheme_sdk_workflow_engine::{
    GraphemeSdkWorkflowEngine, GraphemeWorkflowGuardrails,
};
use stasis::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
use stasis::infrastructure::runtime::in_memory_thread_store::InMemoryThreadStore;
use stasis::infrastructure::runtime::surreal_thread_store::SurrealThreadStore;
use stasis::infrastructure::runtime::tokio_channel_event_publisher::TokioChannelEventPublisher;
use stasis::ports::outbound::ai_chat_client::AiChatClient;
use stasis::ports::outbound::ai_chat_tool_interceptor::{
    AiChatToolInterceptor, AiToolCallEnvelope,
};
use stasis::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use stasis::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use stasis::ports::outbound::memory::identity_memory_models::{
    AutonomyScope, EntityRef, GetIdentityContextRequest, GetIdentityContextResponse,
    ListEntityHistoryRequest, ListEntityHistoryResponse, RelationshipEntity, RelationshipKind,
    RelationshipStatus,
};
use stasis::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use stasis::ports::outbound::memory::memory_models::{
    MemoryAggregateRequest, MemoryAggregateResponse, MemoryFindRequest, MemoryFindResponse,
    MemoryNode, MemoryRecallRequest, MemoryRecallResponse,
    MemoryRollupRequest, MemoryRollupResponse, MemorySchemaResponse, MemoryStoreRequest,
    MemoryStoreResponse, MemoryTransformRequest, MemoryTransformResponse,
};
use stasis::ports::outbound::memory::memory_operations::MemoryOperations;
use stasis::ports::outbound::runtime::clock::Clock;
use stasis::ports::outbound::runtime::event_publisher::EventPublisher;
use stasis::ports::outbound::runtime::id_generator::IdGenerator;
use stasis::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;
use stasis::ports::outbound::runtime::thread_store::ThreadStore;
use stasis::ports::outbound::runtime::workflow_engine::WorkflowEngine;

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
    async fn complete(
        &self,
        request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
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
struct PlainScriptedChatClient {
    responses: Arc<Vec<String>>,
    call_count: Arc<AtomicUsize>,
}

impl PlainScriptedChatClient {
    fn new(responses: Vec<String>) -> Self {
        Self {
            responses: Arc::new(responses),
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AiChatClient for PlainScriptedChatClient {
    async fn complete(
        &self,
        _request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let index = self.call_count.fetch_add(1, Ordering::SeqCst);
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
struct CapturingChatClient {
    responses: Arc<Vec<String>>,
    captured_prompts: Arc<StdMutex<Vec<String>>>,
}

impl CapturingChatClient {
    fn new(responses: Vec<String>) -> Self {
        Self {
            responses: Arc::new(responses),
            captured_prompts: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    fn captured_prompts(&self) -> Vec<String> {
        self.captured_prompts
            .lock()
            .map(|state| state.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl AiChatClient for CapturingChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let prompt = request
            .messages
            .iter()
            .filter_map(|message| message.content.first_text())
            .collect::<Vec<_>>()
            .join("\n");
        if let Ok(mut state) = self.captured_prompts.lock() {
            state.push(prompt);
        }

        let index = self.captured_prompts.lock().map(|state| state.len() - 1).unwrap_or(0);
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

fn mock_recall_response(sync_keys: &[&str], raw_nodes: &[&str]) -> MemoryRecallResponse {
    let nodes: Vec<MemoryNode> = sync_keys
        .iter()
        .zip(raw_nodes.iter())
        .map(|(sync_key, raw)| MemoryNode {
            sync_key: (*sync_key).to_string(),
            raw: (*raw).to_string(),
            session_id: "session-mock".to_string(),
            tier: "raw".to_string(),
            ..Default::default()
        })
        .collect();

    MemoryRecallResponse {
        retrieved: nodes.len(),
        retrieval_path: Some("Hybrid".to_string()),
        nodes: nodes.clone(),
        node_sync_keys: nodes.iter().map(|node| node.sync_key.clone()).collect(),
        ..Default::default()
    }
}

#[derive(Clone)]
struct BranchAwareConcurrentTestClient;

#[async_trait]
impl AiChatClient for BranchAwareConcurrentTestClient {
    async fn complete(
        &self,
        request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let user_text = request
            .messages
            .iter()
            .rev()
            .filter_map(|message| message.content.first_text())
            .next()
            .unwrap_or_default();

        if user_text.contains("tool branch") {
            let has_tool_response = request
                .messages
                .iter()
                .any(|message| !message.content.tool_responses().is_empty());

            if !has_tool_response {
                return Ok(ChatResponse {
                    content: MessageContent::from_tool_calls(vec![ToolCall {
                        call_id: "tool-call-concurrent-1".to_string(),
                        fn_name: "stasis.web.search.mock".to_string(),
                        fn_arguments: json!({ "query": "concurrent tool branch" }),
                        thought_signatures: None,
                    }]),
                    reasoning_content: None,
                    model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                    provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                    usage: Usage::default(),
                    captured_raw_body: None,
                });
            }

            return Ok(ChatResponse {
                content: MessageContent::from_text("concurrent tool branch final answer"),
                reasoning_content: None,
                model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
                usage: Usage::default(),
                captured_raw_body: None,
            });
        }

        Ok(ChatResponse {
            content: MessageContent::from_text(format!("echo::{user_text}")),
            reasoning_content: None,
            model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            provider_model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            usage: Usage::default(),
            captured_raw_body: None,
        })
    }
}

#[derive(Clone, Default)]
struct EchoPromptChatClient;

#[async_trait]
impl AiChatClient for EchoPromptChatClient {
    async fn complete(
        &self,
        request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
        let echoed_text = request
            .messages
            .iter()
            .rev()
            .filter_map(|message| message.content.first_text())
            .next()
            .unwrap_or_default();

        Ok(ChatResponse {
            content: MessageContent::from_text(format!("echo::{echoed_text}")),
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
    async fn complete(
        &self,
        request: ChatRequest,
        _options: Option<&ChatOptions>,
    ) -> Result<ChatResponse> {
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

#[derive(Clone, Default)]
struct RecordingToolCallInterceptor {
    envelopes: Arc<StdMutex<Vec<AiToolCallEnvelope>>>,
}

impl RecordingToolCallInterceptor {
    fn snapshot(&self) -> Vec<AiToolCallEnvelope> {
        self.envelopes
            .lock()
            .map(|state| state.clone())
            .unwrap_or_default()
    }
}

impl AiChatToolInterceptor for RecordingToolCallInterceptor {
    fn on_tool_calls(&self, envelope: AiToolCallEnvelope) {
        if let Ok(mut state) = self.envelopes.lock() {
            state.push(envelope);
        }
    }
}

#[derive(Clone)]
struct MockMemoryContextReader {
    response: MemoryRecallResponse,
}

#[async_trait]
impl MemoryContextReader for MockMemoryContextReader {
    async fn recall(&self, _request: &MemoryRecallRequest) -> Result<MemoryRecallResponse> {
        Ok(self.response.clone())
    }

    async fn find(&self, _request: &MemoryFindRequest) -> Result<MemoryFindResponse> {
        Ok(MemoryFindResponse::default())
    }
}

#[derive(Clone)]
struct MockMemoryContextWriter {
    response: MemoryStoreResponse,
}

#[async_trait]
impl MemoryContextWriter for MockMemoryContextWriter {
    async fn store_context(&self, _request: &MemoryStoreRequest) -> Result<MemoryStoreResponse> {
        Ok(self.response.clone())
    }
}

#[derive(Clone, Default)]
struct MockMemoryOperations;

#[async_trait]
impl MemoryOperations for MockMemoryOperations {
    async fn aggregate(
        &self,
        _request: &MemoryAggregateRequest,
    ) -> Result<MemoryAggregateResponse> {
        Ok(MemoryAggregateResponse {
            total_groups: 3,
            scanned_nodes: 42,
        })
    }

    async fn transform(
        &self,
        _request: &MemoryTransformRequest,
    ) -> Result<MemoryTransformResponse> {
        Ok(MemoryTransformResponse {
            scanned: 50,
            selected: 20,
            updated: 18,
            skipped: 2,
            failed: 0,
            duplicate: 0,
            failures: Vec::new(),
        })
    }

    async fn rollup(&self, _request: &MemoryRollupRequest) -> Result<MemoryRollupResponse> {
        Ok(MemoryRollupResponse {
            total_groups: 4,
            scanned_nodes: 40,
        })
    }

    async fn schema(&self) -> Result<MemorySchemaResponse> {
        Ok(MemorySchemaResponse {
            schema_version: "sttp-v1".to_string(),
            sort_fields: vec!["created_at".to_string()],
            filter_fields: vec!["session_id".to_string()],
            group_by_fields: vec!["session_id".to_string()],
            fallback_policies: vec!["on_empty".to_string()],
            strictness_modes: vec!["balanced".to_string()],
            transform_operations: vec!["embed_backfill".to_string()],
        })
    }
}

#[derive(Clone)]
struct MockIdentityMemoryStore {
    response: GetIdentityContextResponse,
}

#[async_trait]
impl IdentityMemoryStore for MockIdentityMemoryStore {
    async fn get_identity_context(
        &self,
        _request: &GetIdentityContextRequest,
    ) -> Result<GetIdentityContextResponse> {
        Ok(self.response.clone())
    }

    async fn propose_entity_update(
        &self,
        _request: &stasis::ports::outbound::memory::identity_memory_models::ProposeEntityUpdateRequest,
    ) -> Result<
        stasis::ports::outbound::memory::identity_memory_models::ProposeEntityUpdateResponse,
    > {
        Ok(Default::default())
    }

    async fn commit_entity_update(
        &self,
        _request: &stasis::ports::outbound::memory::identity_memory_models::CommitEntityUpdateRequest,
    ) -> Result<
        stasis::ports::outbound::memory::identity_memory_models::CommitEntityUpdateResponse,
    > {
        Ok(Default::default())
    }

    async fn list_entity_history(
        &self,
        _request: &ListEntityHistoryRequest,
    ) -> Result<ListEntityHistoryResponse> {
        Ok(Default::default())
    }

    async fn rollback_entity_version(
        &self,
        _request: &stasis::ports::outbound::memory::identity_memory_models::RollbackEntityVersionRequest,
    ) -> Result<
        stasis::ports::outbound::memory::identity_memory_models::RollbackEntityVersionResponse,
    > {
        Ok(Default::default())
    }
}

fn replacement_trace_identity_context() -> GetIdentityContextResponse {
    GetIdentityContextResponse {
        persona: None,
        user: None,
        channel: None,
        contacts: vec![],
        relationships: vec![RelationshipEntity {
            relationship_id: "rel-new".to_string(),
            source_entity_ref: EntityRef {
                entity_type: "PersonaEntity".to_string(),
                entity_id: "p1".to_string(),
            },
            target_entity_ref: EntityRef {
                entity_type: "UserEntity".to_string(),
                entity_id: "u1".to_string(),
            },
            relationship_kind: RelationshipKind::AssistantUser,
            status: RelationshipStatus::Active,
            trust_level: 0.4,
            confidence: 0.9,
            strength_score: 0.8,
            recency_score: 0.7,
            autonomy_scope: AutonomyScope::default(),
            approval_profile_id: None,
            interruption_policy: Default::default(),
            escalation_policy: Default::default(),
            policy_tags: vec![],
            provenance: stasis::ports::outbound::memory::identity_memory_models::UpdateSource::UserDirect,
            parent_relationship_id: None,
            governing_relationship_ids: vec![],
            derived_from_relationship_id: Some("rel-old".to_string()),
            last_transition_reason: Some("replacement".to_string()),
            transition_receipt_id: Some("rcpt-replacement-1".to_string()),
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }],
        policy_profiles: vec![],
        graph_depth_used: 1,
        flattened_claims: vec![],
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

#[allow(clippy::too_many_arguments)]
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
    RuntimeWorkflowJobBuilder::for_tool_loop(job_id.to_string(), payload)
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

#[allow(clippy::too_many_arguments)]
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
    RuntimeWorkflowJobBuilder::for_agent_turn(job_id.to_string(), payload)
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

#[allow(clippy::too_many_arguments)]
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
    RuntimeWorkflowJobBuilder::for_agent_session(job_id.to_string(), payload)
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

#[allow(clippy::too_many_arguments)]
fn build_prompt_job(
    job_id: &str,
    payload: &PromptJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_prompt(job_id.to_string(), payload)
        .expect("prompt payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_orchestration_sequential_job(
    job_id: &str,
    payload: &SequentialPatternJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_orchestration_sequential(job_id.to_string(), payload)
        .expect("orchestration-sequential payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_orchestration_concurrent_job(
    job_id: &str,
    payload: &ConcurrentPatternJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_orchestration_concurrent(job_id.to_string(), payload)
        .expect("orchestration-concurrent payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_orchestration_handoff_job(
    job_id: &str,
    payload: &HandoffPatternJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_orchestration_handoff(job_id.to_string(), payload)
        .expect("orchestration-handoff payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_orchestration_orchestrator_job(
    job_id: &str,
    payload: &OrchestratorPatternJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_orchestration_orchestrator(job_id.to_string(), payload)
        .expect("orchestration-orchestrator payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_memory_recall_job(
    job_id: &str,
    payload: &MemoryRecallJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_memory_recall(job_id.to_string(), payload)
        .expect("memory-recall payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

async fn attempt_diagnostics_for_job(runtime: &InMemoryRuntime, job_id: &str) -> JsonValue {
    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(job_id)
        .await
        .expect("attempt list should succeed");
    serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json")
}

fn assert_orchestration_success_diagnostics(
    diagnostics: &JsonValue,
    expected_provider: &str,
    expected_pattern: &str,
    expected_thread_id: &str,
) {
    assert_eq!(diagnostics.get("provider"), Some(&json!(expected_provider)));
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(diagnostics.get("pattern"), Some(&json!(expected_pattern)));
    assert_eq!(
        diagnostics.get("thread_id"),
        Some(&json!(expected_thread_id))
    );
    assert!(
        diagnostics
            .get("termination_reason")
            .and_then(|value| value.as_str())
            .is_some()
    );
}

fn assert_orchestration_policy_violation_diagnostics(
    diagnostics: &JsonValue,
    expected_provider: &str,
    expected_pattern: &str,
) {
    assert_eq!(diagnostics.get("provider"), Some(&json!(expected_provider)));
    assert_eq!(diagnostics.get("status"), Some(&json!("failure")));
    assert_eq!(diagnostics.get("pattern"), Some(&json!(expected_pattern)));
    assert_eq!(
        diagnostics.get("guardrail_code"),
        Some(&json!("POLICY_VIOLATION"))
    );
    assert!(
        diagnostics
            .get("policy_reason")
            .and_then(|value| value.as_str())
            .is_some()
    );
}

#[allow(clippy::too_many_arguments)]
fn build_memory_aggregate_job(
    job_id: &str,
    payload: &MemoryAggregateJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_memory_aggregate(job_id.to_string(), payload)
        .expect("memory-aggregate payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_memory_transform_job(
    job_id: &str,
    payload: &MemoryTransformJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_memory_transform(job_id.to_string(), payload)
        .expect("memory-transform payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_memory_rollup_job(
    job_id: &str,
    payload: &MemoryRollupJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_memory_rollup(job_id.to_string(), payload)
        .expect("memory-rollup payload should serialize")
        .with_idempotency_key(idempotency_key)
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_trace_id(trace_id)
        .with_sttp_input_node_id(sttp_input_node_id)
        .with_scheduled_at(now)
        .with_backoff_policy(test_backoff_policy())
        .build()
}

#[allow(clippy::too_many_arguments)]
fn build_memory_schema_job(
    job_id: &str,
    payload: &MemorySchemaJobPayload,
    now: DateTime<Utc>,
    idempotency_key: &str,
    correlation_id: &str,
    causation_id: &str,
    trace_id: &str,
    sttp_input_node_id: &str,
) -> NewJob {
    RuntimeWorkflowJobBuilder::for_memory_schema(job_id.to_string(), payload)
        .expect("memory-schema payload should serialize")
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
async fn in_memory_prompt_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(CapturingChatClient::new(vec![
        "prompt completion text".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: mock_recall_response(
            &["sync-a", "sync-b"],
            &[
                "◈⟨ prior rust context A ⟩",
                "◈⟨ prior rust context B ⟩",
            ],
        ),
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:prompt:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });

    runtime
        .register_handler(PromptChatJobHandler::new_with_memory(
            chat_client.clone(),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("prompt handler should register");

    let now = Utc::now();
    let job_id = "job-prompt-memory-1".to_string();
    let payload = PromptJobPayload {
        user_prompt: "summarize rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
    };

    runtime
        .enqueue(build_prompt_job(
            &job_id,
            &payload,
            now,
            "idem-prompt-memory-1",
            "corr-prompt-memory-1",
            "cause-prompt-memory-1",
            "trace-prompt-memory-1",
            "sttp:in:prompt:memory:1",
        ))
        .await
        .expect("prompt job should enqueue");

    runtime
        .process_once("default", "worker-prompt", now)
        .await
        .expect("prompt processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory:prompt:1")
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
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-pipeline")));
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(2))
    );
    let captured_prompts = chat_client.captured_prompts();
    assert_eq!(captured_prompts.len(), 1);
    assert!(captured_prompts[0].contains("Recalled memory context:"));
    assert!(captured_prompts[0].contains("prior rust context A"));
    assert!(captured_prompts[0].contains("summarize rust trends"));
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:prompt:1"))
    );
    let input_query_id = diagnostics
        .get("input_memory_query_id")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_id should be present");
    let input_query_fingerprint = diagnostics
        .get("input_memory_query_fingerprint")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_fingerprint should be present");
    assert!(
        input_query_id.starts_with("mq:"),
        "input_memory_query_id should be generated"
    );
    assert!(
        input_query_fingerprint.contains("alpha="),
        "input_memory_query_fingerprint should include scoring tuple"
    );

    let lineage_events = runtime
        .outbox_store
        .list_by_job_id(&job_id)
        .await
        .expect("lineage events should load");
    assert_eq!(lineage_events.len(), 1);
    assert_eq!(
        lineage_events[0].event.input_memory_query_id.as_deref(),
        Some(input_query_id)
    );
    assert_eq!(
        lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref(),
        Some(input_query_fingerprint)
    );
    assert_eq!(
        lineage_events[0].event.output_memory_node_id.as_deref(),
        Some("sttp:memory:prompt:1")
    );
    assert_eq!(
        lineage_events[0].event.retrieval_path.as_deref(),
        Some("Hybrid")
    );
}

#[tokio::test]
async fn in_memory_prompt_job_handler_identity_trace_includes_replacement_continuity_receipt() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "prompt completion text".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse::default(),
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:prompt:identity-trace:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let identity_store = Arc::new(MockIdentityMemoryStore {
        response: replacement_trace_identity_context(),
    });

    runtime
        .register_handler(PromptChatJobHandler::new_with_memory_and_identity(
            chat_client,
            Some(memory_reader),
            Some(memory_writer),
            Some(identity_store),
        ))
        .expect("prompt handler should register");

    let now = Utc::now();
    let job_id = "job-prompt-identity-trace-1".to_string();
    let payload = PromptJobPayload {
        user_prompt: "summarize rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
    };

    runtime
        .enqueue(build_prompt_job(
            &job_id,
            &payload,
            now,
            "idem-prompt-identity-trace-1",
            "corr-prompt-identity-trace-1",
            "cause-prompt-identity-trace-1",
            "trace-prompt-identity-trace-1",
            "sttp:in:prompt:identity-trace:1",
        ))
        .await
        .expect("prompt job should enqueue");

    runtime
        .process_once("default", "worker-prompt", now)
        .await
        .expect("prompt processing should succeed");

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");

    assert_eq!(
        diagnostics.pointer("/identity_context/attempted"),
        Some(&json!(true))
    );
    let summary = diagnostics
        .pointer("/identity_context/summary")
        .and_then(|value| value.as_str())
        .expect("identity summary should be present");
    assert!(summary.contains("continuity_links=1"));
    assert!(summary.contains("continuity_receipts=1"));
}

#[tokio::test]
async fn surreal_prompt_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_prompt_memory_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "prompt completion text".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 1,
            retrieval_path: Some("ResonanceOnly".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-x".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:prompt:surreal:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });

    runtime
        .register_handler(PromptChatJobHandler::new_with_memory(
            chat_client,
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("prompt handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-prompt-memory-1".to_string();
    let payload = PromptJobPayload {
        user_prompt: "summarize rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
    };

    runtime
        .enqueue(build_prompt_job(
            &job_id,
            &payload,
            now,
            "idem-surreal-prompt-memory-1",
            "corr-surreal-prompt-memory-1",
            "cause-surreal-prompt-memory-1",
            "trace-surreal-prompt-memory-1",
            "sttp:in:prompt:memory:surreal:1",
        ))
        .await
        .expect("prompt job should enqueue");

    runtime
        .process_once("default", "worker-prompt", now)
        .await
        .expect("prompt processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory:prompt:surreal:1")
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
    assert_eq!(diagnostics.get("provider"), Some(&json!("stasis-pipeline")));
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(1))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:prompt:surreal:1"))
    );
    let input_query_id = diagnostics
        .get("input_memory_query_id")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_id should be present");
    let input_query_fingerprint = diagnostics
        .get("input_memory_query_fingerprint")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_fingerprint should be present");
    assert!(
        input_query_id.starts_with("mq:"),
        "input_memory_query_id should be generated"
    );
    assert!(
        input_query_fingerprint.contains("alpha="),
        "input_memory_query_fingerprint should include scoring tuple"
    );

    let lineage_events = runtime
        .outbox_store
        .list_by_job_id(&job_id)
        .await
        .expect("lineage events should load");
    assert_eq!(lineage_events.len(), 1);
    assert_eq!(
        lineage_events[0].event.input_memory_query_id.as_deref(),
        Some(input_query_id)
    );
    assert_eq!(
        lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref(),
        Some(input_query_fingerprint)
    );
    assert_eq!(
        lineage_events[0].event.output_memory_node_id.as_deref(),
        Some("sttp:memory:prompt:surreal:1")
    );
    assert_eq!(
        lineage_events[0].event.retrieval_path.as_deref(),
        Some("ResonanceOnly")
    );
}

#[tokio::test]
async fn in_memory_tool_loop_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "final answer grounded in tool evidence".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 3,
            retrieval_path: Some("Hybrid".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-tool-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:tool-loop:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });

    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new_with_memory(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-memory-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-memory-1",
            "corr-tool-loop-memory-1",
            "cause-tool-loop-memory-1",
            "trace-tool-loop-memory-1",
            "sttp:in:tool-loop:memory:1",
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
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory:tool-loop:1")
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
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(3))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:tool-loop:1"))
    );
    let input_query_id = diagnostics
        .get("input_memory_query_id")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_id should be present");
    let input_query_fingerprint = diagnostics
        .get("input_memory_query_fingerprint")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_fingerprint should be present");
    assert!(input_query_id.starts_with("mq:"));
    assert!(input_query_fingerprint.contains("alpha="));

    let lineage_events = runtime
        .outbox_store
        .list_by_job_id(&job_id)
        .await
        .expect("lineage events should load");
    assert_eq!(lineage_events.len(), 1);
    assert_eq!(
        lineage_events[0].event.input_memory_query_id.as_deref(),
        Some(input_query_id)
    );
    assert_eq!(
        lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref(),
        Some(input_query_fingerprint)
    );
    assert_eq!(
        lineage_events[0].event.output_memory_node_id.as_deref(),
        Some("sttp:memory:tool-loop:1")
    );
    assert_eq!(
        lineage_events[0].event.retrieval_path.as_deref(),
        Some("Hybrid")
    );
}

#[tokio::test]
async fn in_memory_tool_loop_identity_trace_includes_replacement_continuity_receipt() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "final answer grounded in tool evidence".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse::default(),
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:tool-loop:identity-trace:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let identity_store = Arc::new(MockIdentityMemoryStore {
        response: replacement_trace_identity_context(),
    });
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new_with_memory_and_identity(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
            Some(identity_store),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-identity-trace-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-tool-loop-identity-trace-1",
            "corr-tool-loop-identity-trace-1",
            "cause-tool-loop-identity-trace-1",
            "trace-tool-loop-identity-trace-1",
            "sttp:in:tool-loop:identity-trace:1",
        ))
        .await
        .expect("tool loop job should enqueue");

    runtime
        .process_once("default", "worker-tool-loop", now)
        .await
        .expect("tool loop processing should succeed");

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");

    assert_eq!(
        diagnostics.pointer("/identity_context/attempted"),
        Some(&json!(true))
    );
    let summary = diagnostics
        .pointer("/identity_context/summary")
        .and_then(|value| value.as_str())
        .expect("identity summary should be present");
    assert!(summary.contains("continuity_links=1"));
    assert!(summary.contains("continuity_receipts=1"));
}

#[tokio::test]
async fn surreal_tool_loop_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_tool_loop_memory_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "final answer grounded in tool evidence".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 2,
            retrieval_path: Some("ResonanceOnly".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-tool-surreal-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:tool-loop:surreal:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new_with_memory(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-tool-loop-memory-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_tool_loop_job(
            &job_id,
            &payload,
            now,
            "idem-surreal-tool-loop-memory-1",
            "corr-surreal-tool-loop-memory-1",
            "cause-surreal-tool-loop-memory-1",
            "trace-surreal-tool-loop-memory-1",
            "sttp:in:tool-loop:memory:surreal:1",
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
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory:tool-loop:surreal:1")
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
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(2))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:tool-loop:surreal:1"))
    );
    let input_query_id = diagnostics
        .get("input_memory_query_id")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_id should be present");
    let input_query_fingerprint = diagnostics
        .get("input_memory_query_fingerprint")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_fingerprint should be present");
    assert!(input_query_id.starts_with("mq:"));
    assert!(input_query_fingerprint.contains("alpha="));

    let lineage_events = runtime
        .outbox_store
        .list_by_job_id(&job_id)
        .await
        .expect("lineage events should load");
    assert_eq!(lineage_events.len(), 1);
    assert_eq!(
        lineage_events[0].event.input_memory_query_id.as_deref(),
        Some(input_query_id)
    );
    assert_eq!(
        lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref(),
        Some(input_query_fingerprint)
    );
    assert_eq!(
        lineage_events[0].event.output_memory_node_id.as_deref(),
        Some("sttp:memory:tool-loop:surreal:1")
    );
    assert_eq!(
        lineage_events[0].event.retrieval_path.as_deref(),
        Some("ResonanceOnly")
    );
}

#[tokio::test]
async fn in_memory_agent_turn_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "agent final answer grounded in tool evidence".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 4,
            retrieval_path: Some("Hybrid".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-agent-turn-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:agent-turn:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });

    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentTurnJobHandler::new_with_memory(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("agent-turn handler should register");

    let now = Utc::now();
    let job_id = "job-agent-turn-memory-1".to_string();
    let payload = AgentTurnJobPayload {
        agent_id: "agent.researcher".to_string(),
        thread_id: Some("thread-42".to_string()),
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_turn_job(
            &job_id,
            &payload,
            now,
            "idem-agent-turn-memory-1",
            "corr-agent-turn-memory-1",
            "cause-agent-turn-memory-1",
            "trace-agent-turn-memory-1",
            "sttp:in:agent-turn:memory:1",
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
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory:agent-turn:1")
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
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(4))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:agent-turn:1"))
    );
    let input_query_id = diagnostics
        .get("input_memory_query_id")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_id should be present");
    let input_query_fingerprint = diagnostics
        .get("input_memory_query_fingerprint")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_fingerprint should be present");
    assert!(input_query_id.starts_with("mq:"));
    assert!(input_query_fingerprint.contains("alpha="));

    let lineage_events = runtime
        .outbox_store
        .list_by_job_id(&job_id)
        .await
        .expect("lineage events should load");
    assert_eq!(lineage_events.len(), 1);
    assert_eq!(
        lineage_events[0].event.input_memory_query_id.as_deref(),
        Some(input_query_id)
    );
    assert_eq!(
        lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref(),
        Some(input_query_fingerprint)
    );
    assert_eq!(
        lineage_events[0].event.output_memory_node_id.as_deref(),
        Some("sttp:memory:agent-turn:1")
    );
    assert_eq!(
        lineage_events[0].event.retrieval_path.as_deref(),
        Some("Hybrid")
    );
}

#[tokio::test]
async fn in_memory_agent_turn_identity_trace_includes_replacement_continuity_receipt() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "agent final answer grounded in tool evidence".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse::default(),
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:agent-turn:identity-trace:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let identity_store = Arc::new(MockIdentityMemoryStore {
        response: replacement_trace_identity_context(),
    });
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentTurnJobHandler::new_with_memory_and_identity(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
            Some(identity_store),
        ))
        .expect("agent-turn handler should register");

    let now = Utc::now();
    let job_id = "job-agent-turn-identity-trace-1".to_string();
    let payload = AgentTurnJobPayload {
        agent_id: "agent.researcher".to_string(),
        thread_id: Some("thread-42".to_string()),
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_turn_job(
            &job_id,
            &payload,
            now,
            "idem-agent-turn-identity-trace-1",
            "corr-agent-turn-identity-trace-1",
            "cause-agent-turn-identity-trace-1",
            "trace-agent-turn-identity-trace-1",
            "sttp:in:agent-turn:identity-trace:1",
        ))
        .await
        .expect("agent-turn job should enqueue");

    runtime
        .process_once("default", "worker-agent-turn", now)
        .await
        .expect("agent-turn processing should succeed");

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");

    assert_eq!(
        diagnostics.pointer("/identity_context/attempted"),
        Some(&json!(true))
    );
    let summary = diagnostics
        .pointer("/identity_context/summary")
        .and_then(|value| value.as_str())
        .expect("identity summary should be present");
    assert!(summary.contains("continuity_links=1"));
    assert!(summary.contains("continuity_receipts=1"));
}

#[tokio::test]
async fn surreal_agent_turn_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_agent_turn_memory_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft analysis".to_string(),
        "agent final answer grounded in tool evidence".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 2,
            retrieval_path: Some("ResonanceOnly".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-agent-turn-surreal-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:agent-turn:surreal:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });

    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentTurnJobHandler::new_with_memory(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("agent-turn handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-agent-turn-memory-1".to_string();
    let payload = AgentTurnJobPayload {
        agent_id: "agent.researcher".to_string(),
        thread_id: Some("thread-42".to_string()),
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
        tool_name: "stasis.web.search.mock".to_string(),
        tool_input: Some(json!({ "query": "latest rust trends" })),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_turn_job(
            &job_id,
            &payload,
            now,
            "idem-surreal-agent-turn-memory-1",
            "corr-surreal-agent-turn-memory-1",
            "cause-surreal-agent-turn-memory-1",
            "trace-surreal-agent-turn-memory-1",
            "sttp:in:agent-turn:memory:surreal:1",
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
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory:agent-turn:surreal:1")
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
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(2))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:agent-turn:surreal:1"))
    );
    let input_query_id = diagnostics
        .get("input_memory_query_id")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_id should be present");
    let input_query_fingerprint = diagnostics
        .get("input_memory_query_fingerprint")
        .and_then(|value| value.as_str())
        .expect("input_memory_query_fingerprint should be present");
    assert!(input_query_id.starts_with("mq:"));
    assert!(input_query_fingerprint.contains("alpha="));

    let lineage_events = runtime
        .outbox_store
        .list_by_job_id(&job_id)
        .await
        .expect("lineage events should load");
    assert_eq!(lineage_events.len(), 1);
    assert_eq!(
        lineage_events[0].event.input_memory_query_id.as_deref(),
        Some(input_query_id)
    );
    assert_eq!(
        lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref(),
        Some(input_query_fingerprint)
    );
    assert_eq!(
        lineage_events[0].event.output_memory_node_id.as_deref(),
        Some("sttp:memory:agent-turn:surreal:1")
    );
    assert_eq!(
        lineage_events[0].event.retrieval_path.as_deref(),
        Some("ResonanceOnly")
    );
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
        .register_handler(ToolLoopJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
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
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:tool-loop:job-tool-loop-1")
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
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-tool-loop"))
    );
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(
        diagnostics.get("tool_name"),
        Some(&json!("stasis.web.search.mock"))
    );
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
        .register_handler(ToolLoopJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-model-tool-call-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
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
        .register_handler(AgentTurnJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
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
        memory_policy: None,
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
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:agent-turn:job-agent-turn-1")
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
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-agent-turn"))
    );
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
    assert_eq!(
        diagnostics.get("agent_id"),
        Some(&json!("agent.researcher"))
    );
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
        .register_handler(AgentSessionJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
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
        memory_policy: None,
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
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-agent-session"))
    );
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
        .register_handler(AgentSessionJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
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
        memory_policy: None,
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
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-agent-session"))
    );
    assert_eq!(diagnostics.get("turn_count"), Some(&json!(2)));
    assert_eq!(diagnostics.get("terminated"), Some(&json!(true)));
}

#[tokio::test]
async fn in_memory_agent_session_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft turn 1".to_string(),
        "final turn 1".to_string(),
        "draft turn 2".to_string(),
        "final turn 2".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 2,
            retrieval_path: Some("Hybrid".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-agent-session-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:agent-session:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentSessionJobHandler::new_with_memory(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("agent-session handler should register");

    let now = Utc::now();
    let job_id = "job-agent-session-memory-1".to_string();
    let payload = AgentSessionJobPayload {
        thread_id: Some("thread-session-memory-1".to_string()),
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
        memory_policy: None,
        max_turns: Some(2),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_session_job(
            &job_id,
            &payload,
            now,
            "idem-agent-session-memory-1",
            "corr-agent-session-memory-1",
            "cause-agent-session-memory-1",
            "trace-agent-session-memory-1",
            "sttp:in:agent-session:memory:1",
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
        Some("sttp:memory:agent-session:1")
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
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(2))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:agent-session:1"))
    );
}

#[tokio::test]
async fn in_memory_agent_session_identity_trace_includes_replacement_continuity_receipt() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft turn 1".to_string(),
        "final turn 1".to_string(),
        "draft turn 2".to_string(),
        "final turn 2".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse::default(),
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:agent-session:identity-trace:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let identity_store = Arc::new(MockIdentityMemoryStore {
        response: replacement_trace_identity_context(),
    });
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentSessionJobHandler::new_with_memory_and_identity(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
            Some(identity_store),
        ))
        .expect("agent-session handler should register");

    let now = Utc::now();
    let job_id = "job-agent-session-identity-trace-1".to_string();
    let payload = AgentSessionJobPayload {
        thread_id: Some("thread-session-identity-trace-1".to_string()),
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
        memory_policy: None,
        max_turns: Some(2),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_session_job(
            &job_id,
            &payload,
            now,
            "idem-agent-session-identity-trace-1",
            "corr-agent-session-identity-trace-1",
            "cause-agent-session-identity-trace-1",
            "trace-agent-session-identity-trace-1",
            "sttp:in:agent-session:identity-trace:1",
        ))
        .await
        .expect("agent-session job should enqueue");

    runtime
        .process_once("default", "worker-agent-session", now)
        .await
        .expect("agent-session processing should succeed");

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");

    assert_eq!(
        diagnostics.pointer("/identity_context/attempted"),
        Some(&json!(true))
    );
    let summary = diagnostics
        .pointer("/identity_context/summary")
        .and_then(|value| value.as_str())
        .expect("identity summary should be present");
    assert!(summary.contains("continuity_links=1"));
    assert!(summary.contains("continuity_receipts=1"));
}

#[tokio::test]
async fn surreal_agent_session_job_handler_with_memory_persists_memory_node_id_and_diagnostics() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_agent_session_memory_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "draft turn 1".to_string(),
        "final turn 1".to_string(),
        "draft turn 2".to_string(),
        "final turn 2".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 1,
            retrieval_path: Some("ResonanceOnly".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-agent-session-surreal-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:agent-session:surreal:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(AgentSessionJobHandler::new_with_memory(
            chat_client,
            Arc::new(tool_registry),
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("agent-session handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-agent-session-memory-1".to_string();
    let payload = AgentSessionJobPayload {
        thread_id: Some("thread-surreal-session-memory-1".to_string()),
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
        memory_policy: None,
        max_turns: Some(2),
        tool_call_mode: None,
    };

    runtime
        .enqueue(build_agent_session_job(
            &job_id,
            &payload,
            now,
            "idem-surreal-agent-session-memory-1",
            "corr-surreal-agent-session-memory-1",
            "cause-surreal-agent-session-memory-1",
            "trace-surreal-agent-session-memory-1",
            "sttp:in:agent-session:memory:surreal:1",
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
        Some("sttp:memory:agent-session:surreal:1")
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
    assert_eq!(
        diagnostics.pointer("/memory_recall/retrieved"),
        Some(&json!(1))
    );
    assert_eq!(
        diagnostics.pointer("/memory_store/node_id"),
        Some(&json!("sttp:memory:agent-session:surreal:1"))
    );
}

#[tokio::test]
async fn in_memory_memory_workflow_handlers_execute_and_emit_diagnostics() {
    let runtime = InMemoryRuntime::new();
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 5,
            retrieval_path: Some("Hybrid".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-memory-op-1".to_string()],
            ..Default::default()
        },
    });
    let memory_operations = Arc::new(MockMemoryOperations);

    runtime
        .register_handler(MemoryRecallJobHandler::new(memory_reader))
        .expect("memory recall handler should register");
    runtime
        .register_handler(MemoryAggregateJobHandler::new(memory_operations.clone()))
        .expect("memory aggregate handler should register");
    runtime
        .register_handler(MemoryTransformJobHandler::new(memory_operations.clone()))
        .expect("memory transform handler should register");
    runtime
        .register_handler(MemoryRollupJobHandler::new(memory_operations.clone()))
        .expect("memory rollup handler should register");
    runtime
        .register_handler(MemorySchemaJobHandler::new(memory_operations))
        .expect("memory schema handler should register");

    let now = Utc::now();

    let recall_payload = MemoryRecallJobPayload {
        memory_policy: None,
    };
    runtime
        .enqueue(build_memory_recall_job(
            "job-memory-recall-1",
            &recall_payload,
            now,
            "idem-memory-recall-1",
            "corr-memory-recall-1",
            "cause-memory-recall-1",
            "trace-memory-recall-1",
            "sttp:in:memory:recall:1",
        ))
        .await
        .expect("memory recall job should enqueue");

    let aggregate_payload = MemoryAggregateJobPayload {
        session_ids: Some(vec!["session-a".to_string()]),
        tiers: None,
        from_utc: None,
        to_utc: None,
        max_groups: Some(10),
        max_nodes: Some(100),
    };
    runtime
        .enqueue(build_memory_aggregate_job(
            "job-memory-aggregate-1",
            &aggregate_payload,
            now,
            "idem-memory-aggregate-1",
            "corr-memory-aggregate-1",
            "cause-memory-aggregate-1",
            "trace-memory-aggregate-1",
            "sttp:in:memory:aggregate:1",
        ))
        .await
        .expect("memory aggregate job should enqueue");

    let transform_payload = MemoryTransformJobPayload {
        session_ids: Some(vec!["session-a".to_string()]),
        tiers: None,
        from_utc: None,
        to_utc: None,
        operation: None,
        dry_run: Some(true),
        batch_size: Some(50),
        max_nodes: Some(500),
        provider_id: None,
        model: None,
    };
    runtime
        .enqueue(build_memory_transform_job(
            "job-memory-transform-1",
            &transform_payload,
            now,
            "idem-memory-transform-1",
            "corr-memory-transform-1",
            "cause-memory-transform-1",
            "trace-memory-transform-1",
            "sttp:in:memory:transform:1",
        ))
        .await
        .expect("memory transform job should enqueue");

    let rollup_payload = MemoryRollupJobPayload {
        session_ids: Some(vec!["session-a".to_string()]),
        tiers: None,
        from_utc: None,
        to_utc: None,
        max_days: Some(7),
        max_nodes: Some(700),
    };
    runtime
        .enqueue(build_memory_rollup_job(
            "job-memory-rollup-1",
            &rollup_payload,
            now,
            "idem-memory-rollup-1",
            "corr-memory-rollup-1",
            "cause-memory-rollup-1",
            "trace-memory-rollup-1",
            "sttp:in:memory:rollup:1",
        ))
        .await
        .expect("memory rollup job should enqueue");

    let schema_payload = MemorySchemaJobPayload::default();
    runtime
        .enqueue(build_memory_schema_job(
            "job-memory-schema-1",
            &schema_payload,
            now,
            "idem-memory-schema-1",
            "corr-memory-schema-1",
            "cause-memory-schema-1",
            "trace-memory-schema-1",
            "sttp:in:memory:schema:1",
        ))
        .await
        .expect("memory schema job should enqueue");

    for _ in 0..5 {
        runtime
            .process_once("default", "worker-memory-ops", now)
            .await
            .expect("memory op processing should succeed");
    }

    let recall_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-memory-recall-1")
        .await
        .expect("recall attempts should load");
    let recall_diagnostics: JsonValue = serde_json::from_str(
        recall_attempts[0]
            .diagnostics
            .as_deref()
            .expect("recall diagnostics should exist"),
    )
    .expect("recall diagnostics should be json");
    assert_eq!(
        recall_diagnostics.get("provider"),
        Some(&json!("stasis-memory-recall"))
    );
    assert_eq!(recall_diagnostics.get("retrieved"), Some(&json!(5)));

    let aggregate_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-memory-aggregate-1")
        .await
        .expect("aggregate attempts should load");
    let aggregate_diagnostics: JsonValue = serde_json::from_str(
        aggregate_attempts[0]
            .diagnostics
            .as_deref()
            .expect("aggregate diagnostics should exist"),
    )
    .expect("aggregate diagnostics should be json");
    assert_eq!(
        aggregate_diagnostics.get("provider"),
        Some(&json!("stasis-memory-aggregate"))
    );
    assert_eq!(aggregate_diagnostics.get("total_groups"), Some(&json!(3)));

    let transform_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-memory-transform-1")
        .await
        .expect("transform attempts should load");
    let transform_diagnostics: JsonValue = serde_json::from_str(
        transform_attempts[0]
            .diagnostics
            .as_deref()
            .expect("transform diagnostics should exist"),
    )
    .expect("transform diagnostics should be json");
    assert_eq!(
        transform_diagnostics.get("provider"),
        Some(&json!("stasis-memory-transform"))
    );
    assert_eq!(transform_diagnostics.get("updated"), Some(&json!(18)));

    let rollup_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-memory-rollup-1")
        .await
        .expect("rollup attempts should load");
    let rollup_diagnostics: JsonValue = serde_json::from_str(
        rollup_attempts[0]
            .diagnostics
            .as_deref()
            .expect("rollup diagnostics should exist"),
    )
    .expect("rollup diagnostics should be json");
    assert_eq!(
        rollup_diagnostics.get("provider"),
        Some(&json!("stasis-memory-rollup"))
    );
    assert_eq!(rollup_diagnostics.get("total_groups"), Some(&json!(4)));

    let schema_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-memory-schema-1")
        .await
        .expect("schema attempts should load");
    let schema_diagnostics: JsonValue = serde_json::from_str(
        schema_attempts[0]
            .diagnostics
            .as_deref()
            .expect("schema diagnostics should exist"),
    )
    .expect("schema diagnostics should be json");
    assert_eq!(
        schema_diagnostics.get("provider"),
        Some(&json!("stasis-memory-schema"))
    );
    assert_eq!(
        schema_diagnostics.get("schema_version"),
        Some(&json!("sttp-v1"))
    );
}

#[tokio::test]
async fn surreal_memory_recall_job_handler_executes_and_emits_diagnostics() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_memory_recall_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 2,
            retrieval_path: Some("ResonanceOnly".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-memory-surreal-1".to_string()],
            ..Default::default()
        },
    });

    runtime
        .register_handler(MemoryRecallJobHandler::new(memory_reader))
        .expect("memory recall handler should register");

    let now = Utc::now();
    let payload = MemoryRecallJobPayload {
        memory_policy: None,
    };

    runtime
        .enqueue(build_memory_recall_job(
            "job-surreal-memory-recall-1",
            &payload,
            now,
            "idem-surreal-memory-recall-1",
            "corr-surreal-memory-recall-1",
            "cause-surreal-memory-recall-1",
            "trace-surreal-memory-recall-1",
            "sttp:in:surreal:memory:recall:1",
        ))
        .await
        .expect("memory recall job should enqueue");

    runtime
        .process_once("default", "worker-surreal-memory", now)
        .await
        .expect("memory recall processing should succeed");

    let job = runtime
        .job_store
        .get("job-surreal-memory-recall-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:memory-recall:job-surreal-memory-recall-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-surreal-memory-recall-1")
        .await
        .expect("attempts should load");
    assert_eq!(attempts.len(), 1);
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-memory-recall"))
    );
    assert_eq!(diagnostics.get("retrieved"), Some(&json!(2)));
}

#[tokio::test]
async fn in_memory_runtime_builder_with_memory_operations_registers_memory_schema_handler() {
    let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(ScriptedChatClient::new(vec![])))
        .with_memory_context_reader(Arc::new(MockMemoryContextReader {
            response: MemoryRecallResponse::default(),
        }))
        .with_memory_operations(Arc::new(MockMemoryOperations))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers();

    let runtime = builder
        .build()
        .await
        .expect("runtime should build successfully");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime composition");
    };

    let now = Utc::now();
    let schema_payload = MemorySchemaJobPayload::default();
    runtime
        .enqueue(build_memory_schema_job(
            "job-builder-memory-schema-1",
            &schema_payload,
            now,
            "idem-builder-memory-schema-1",
            "corr-builder-memory-schema-1",
            "cause-builder-memory-schema-1",
            "trace-builder-memory-schema-1",
            "sttp:in:builder:memory:schema:1",
        ))
        .await
        .expect("memory schema job should enqueue");

    runtime
        .process_once("default", "worker-builder-memory", now)
        .await
        .expect("memory schema processing should succeed");

    let job = runtime
        .job_store
        .get("job-builder-memory-schema-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
}

#[tokio::test]
async fn in_memory_runtime_builder_without_memory_operation_handlers_dead_letters_memory_job() {
    let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(ScriptedChatClient::new(vec![])))
        .with_memory_context_reader(Arc::new(MockMemoryContextReader {
            response: MemoryRecallResponse::default(),
        }))
        .with_memory_operations(Arc::new(MockMemoryOperations))
        .without_memory_operation_handlers()
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers();

    let runtime = builder
        .build()
        .await
        .expect("runtime should build successfully");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime composition");
    };

    let now = Utc::now();
    let schema_payload = MemorySchemaJobPayload::default();
    runtime
        .enqueue(build_memory_schema_job(
            "job-builder-memory-schema-disabled-1",
            &schema_payload,
            now,
            "idem-builder-memory-schema-disabled-1",
            "corr-builder-memory-schema-disabled-1",
            "cause-builder-memory-schema-disabled-1",
            "trace-builder-memory-schema-disabled-1",
            "sttp:in:builder:memory:schema:disabled:1",
        ))
        .await
        .expect("memory schema job should enqueue");

    runtime
        .process_once("default", "worker-builder-memory", now)
        .await
        .expect("memory schema processing should complete");

    let job = runtime
        .job_store
        .get("job-builder-memory-schema-disabled-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-builder-memory-schema-disabled-1")
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert!(
        attempts[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("no handler registered for job_type=workflow.stasis.memory.schema")
    );
}

#[tokio::test]
async fn in_memory_runtime_builder_with_locus_memory_registers_memory_schema_handler() {
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(ScriptedChatClient::new(vec![])))
        .with_locus_memory()
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .build()
        .await
        .expect("runtime should build successfully");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime composition");
    };

    let now = Utc::now();
    let schema_payload = MemorySchemaJobPayload::default();
    runtime
        .enqueue(build_memory_schema_job(
            "job-builder-locus-memory-schema-1",
            &schema_payload,
            now,
            "idem-builder-locus-memory-schema-1",
            "corr-builder-locus-memory-schema-1",
            "cause-builder-locus-memory-schema-1",
            "trace-builder-locus-memory-schema-1",
            "sttp:in:builder:locus:memory:schema:1",
        ))
        .await
        .expect("memory schema job should enqueue");

    runtime
        .process_once("default", "worker-builder-locus-memory", now)
        .await
        .expect("memory schema processing should succeed");

    let job = runtime
        .job_store
        .get("job-builder-locus-memory-schema-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-builder-locus-memory-schema-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-memory-schema"))
    );
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));
}

#[tokio::test]
async fn surreal_kv_runtime_builder_with_locus_memory_registers_memory_schema_handler() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_nanos();
    let path = env::temp_dir().join(format!("stasis-locus-kv-{nanos}"));
    let path_str = path.to_string_lossy().into_owned();

    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_kv(
        path_str,
        "test",
        format!("runtime_backend_parity_locus_kv_{nanos}"),
    ))
    .with_chat_client(Arc::new(ScriptedChatClient::new(vec![])))
    .with_locus_memory()
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .build()
    .await
    .expect("runtime should build successfully");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime composition");
    };

    let now = Utc::now();
    let schema_payload = MemorySchemaJobPayload::default();
    runtime
        .enqueue(build_memory_schema_job(
            "job-builder-locus-memory-schema-surreal-kv-1",
            &schema_payload,
            now,
            "idem-builder-locus-memory-schema-surreal-kv-1",
            "corr-builder-locus-memory-schema-surreal-kv-1",
            "cause-builder-locus-memory-schema-surreal-kv-1",
            "trace-builder-locus-memory-schema-surreal-kv-1",
            "sttp:in:builder:locus:memory:schema:surreal-kv:1",
        ))
        .await
        .expect("memory schema job should enqueue");

    runtime
        .process_once("default", "worker-builder-locus-memory-surreal-kv", now)
        .await
        .expect("memory schema processing should succeed");

    let job = runtime
        .job_store
        .get("job-builder-locus-memory-schema-surreal-kv-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-builder-locus-memory-schema-surreal-kv-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-memory-schema"))
    );
    assert_eq!(diagnostics.get("status"), Some(&json!("success")));

    drop(runtime);
    let _ = fs::remove_dir_all(path);
}

#[tokio::test]
async fn surreal_ws_runtime_builder_with_locus_memory_registers_memory_schema_handler_if_configured(
) {
    let Some(endpoint) = env::var("STASIS_TEST_SURREAL_WS_ENDPOINT").ok() else {
        return;
    };

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_nanos();

    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_ws(
        endpoint,
        "test",
        format!("runtime_backend_parity_locus_ws_{nanos}"),
    ))
    .with_chat_client(Arc::new(ScriptedChatClient::new(vec![])))
    .with_locus_memory()
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .build()
    .await
    .expect("runtime should build successfully");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime composition");
    };

    let now = Utc::now();
    let schema_payload = MemorySchemaJobPayload::default();
    runtime
        .enqueue(build_memory_schema_job(
            "job-builder-locus-memory-schema-surreal-ws-1",
            &schema_payload,
            now,
            "idem-builder-locus-memory-schema-surreal-ws-1",
            "corr-builder-locus-memory-schema-surreal-ws-1",
            "cause-builder-locus-memory-schema-surreal-ws-1",
            "trace-builder-locus-memory-schema-surreal-ws-1",
            "sttp:in:builder:locus:memory:schema:surreal-ws:1",
        ))
        .await
        .expect("memory schema job should enqueue");

    runtime
        .process_once("default", "worker-builder-locus-memory-surreal-ws", now)
        .await
        .expect("memory schema processing should succeed");

    let job = runtime
        .job_store
        .get("job-builder-locus-memory-schema-surreal-ws-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
}

#[tokio::test]
async fn in_memory_runtime_builder_middleware_chain_enables_cache_telemetry_and_interception() {
    let now = Utc::now();
    let chat_client = Arc::new(ModelToolCallScriptedClient::new());
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let cache = Arc::new(stasis::infrastructure::runtime::in_memory_ai_chat_response_cache::InMemoryAiChatResponseCache::default());
    let interceptor = Arc::new(RecordingToolCallInterceptor::default());

    let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(chat_client.clone())
        .with_chat_middleware(
            ToolCallInterceptionChatMiddleware::new(interceptor.clone())
                .with_metrics(metrics.clone()),
        )
        .with_telemetry_chat_middleware(metrics.clone())
        .with_chat_middleware(CacheChatMiddleware::new(cache).with_metrics(metrics.clone()))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers();
    let builder = builder
        .with_tool(MockWebSearchTool)
        .expect("tool should register");

    let runtime = builder
        .build()
        .await
        .expect("runtime should build successfully");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime composition");
    };

    for idx in 1..=2 {
        let job_id = format!("job-builder-mw-in-memory-{idx}");
        let payload = ToolLoopJobPayload {
            user_prompt: "latest rust trends".to_string(),
            system_prompt: Some("be concise".to_string()),
            policy_profile: Some("default".to_string()),
            model_hint: None,
            memory_policy: None,
            tool_name: "stasis.web.search.mock".to_string(),
            tool_input: Some(json!({ "query": "latest rust trends" })),
            tool_call_mode: Some(AgentToolCallMode::Strict),
        };

        runtime
            .enqueue(build_tool_loop_job(
                &job_id,
                &payload,
                now,
                &format!("idem-builder-mw-in-memory-{idx}"),
                &format!("corr-builder-mw-in-memory-{idx}"),
                &format!("cause-builder-mw-in-memory-{idx}"),
                &format!("trace-builder-mw-in-memory-{idx}"),
                &format!("sttp:in:builder:mw:in-memory:{idx}"),
            ))
            .await
            .expect("tool-loop job should enqueue");

        runtime
            .process_once("default", "worker-builder-mw-in-memory", now)
            .await
            .expect("tool-loop processing should succeed");

        let job = runtime
            .job_store
            .get(&job_id)
            .await
            .expect("job get should succeed")
            .expect("job should exist");
        assert_eq!(job.state, JobState::Succeeded);
    }

    assert_eq!(chat_client.call_count.load(Ordering::SeqCst), 2);

    let envelopes = interceptor.snapshot();
    assert_eq!(envelopes.len(), 2);
    assert!(
        envelopes
            .iter()
            .all(|env| env.tool_names == vec!["stasis.web.search.mock".to_string()])
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.counters.get(CHAT_REQUESTS_TOTAL).copied(), Some(4));
    assert_eq!(
        snapshot.counters.get(CHAT_CACHE_MISS_TOTAL).copied(),
        Some(2)
    );
    assert_eq!(
        snapshot.counters.get(CHAT_CACHE_HIT_TOTAL).copied(),
        Some(2)
    );
    assert_eq!(
        snapshot.counters.get(CHAT_TOOL_CALLS_TOTAL).copied(),
        Some(2)
    );
}

#[tokio::test]
async fn surreal_runtime_builder_middleware_chain_enables_cache_telemetry_and_interception() {
    let now = Utc::now();
    let chat_client = Arc::new(ModelToolCallScriptedClient::new());
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let cache = Arc::new(stasis::infrastructure::runtime::in_memory_ai_chat_response_cache::InMemoryAiChatResponseCache::default());
    let interceptor = Arc::new(RecordingToolCallInterceptor::default());

    let builder = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem(
        "test",
        "runtime_backend_parity_middleware_chain",
    ))
    .with_chat_client(chat_client.clone())
    .with_chat_middleware(
        ToolCallInterceptionChatMiddleware::new(interceptor.clone()).with_metrics(metrics.clone()),
    )
    .with_telemetry_chat_middleware(metrics.clone())
    .with_chat_middleware(CacheChatMiddleware::new(cache).with_metrics(metrics.clone()))
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_agent_handlers()
    .without_memory_operation_handlers();
    let builder = builder
        .with_tool(MockWebSearchTool)
        .expect("tool should register");

    let runtime = builder
        .build()
        .await
        .expect("runtime should build successfully");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime composition");
    };

    for idx in 1..=2 {
        let job_id = format!("job-builder-mw-surreal-{idx}");
        let payload = ToolLoopJobPayload {
            user_prompt: "latest rust trends".to_string(),
            system_prompt: Some("be concise".to_string()),
            policy_profile: Some("default".to_string()),
            model_hint: None,
            memory_policy: None,
            tool_name: "stasis.web.search.mock".to_string(),
            tool_input: Some(json!({ "query": "latest rust trends" })),
            tool_call_mode: Some(AgentToolCallMode::Strict),
        };

        runtime
            .enqueue(build_tool_loop_job(
                &job_id,
                &payload,
                now,
                &format!("idem-builder-mw-surreal-{idx}"),
                &format!("corr-builder-mw-surreal-{idx}"),
                &format!("cause-builder-mw-surreal-{idx}"),
                &format!("trace-builder-mw-surreal-{idx}"),
                &format!("sttp:in:builder:mw:surreal:{idx}"),
            ))
            .await
            .expect("tool-loop job should enqueue");

        runtime
            .process_once("default", "worker-builder-mw-surreal", now)
            .await
            .expect("tool-loop processing should succeed");

        let job = runtime
            .job_store
            .get(&job_id)
            .await
            .expect("job get should succeed")
            .expect("job should exist");
        assert_eq!(job.state, JobState::Succeeded);
    }

    assert_eq!(chat_client.call_count.load(Ordering::SeqCst), 2);

    let envelopes = interceptor.snapshot();
    assert_eq!(envelopes.len(), 2);
    assert!(
        envelopes
            .iter()
            .all(|env| env.tool_names == vec!["stasis.web.search.mock".to_string()])
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.counters.get(CHAT_REQUESTS_TOTAL).copied(), Some(4));
    assert_eq!(
        snapshot.counters.get(CHAT_CACHE_MISS_TOTAL).copied(),
        Some(2)
    );
    assert_eq!(
        snapshot.counters.get(CHAT_CACHE_HIT_TOTAL).copied(),
        Some(2)
    );
    assert_eq!(
        snapshot.counters.get(CHAT_TOOL_CALLS_TOTAL).copied(),
        Some(2)
    );
}

#[tokio::test]
async fn in_memory_orchestration_sequential_pattern_executes_all_stages() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(PlainScriptedChatClient::new(vec![
            "stage-1-output".to_string(),
            "stage-2-output".to_string(),
        ])))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let payload = SequentialPatternJobPayload {
        thread_id: Some("thread.sequential.1".to_string()),
        initial_user_prompt: "start context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![
            SequentialStageJobPayload {
                stage_id: "analyze".to_string(),
                user_prompt_template: "Analyze: {input}".to_string(),
                system_prompt: Some("be concise".to_string()),
                policy_profile: None,
                model_hint: None,
            },
            SequentialStageJobPayload {
                stage_id: "synthesize".to_string(),
                user_prompt_template: "Synthesize: {input}".to_string(),
                system_prompt: Some("be structured".to_string()),
                policy_profile: None,
                model_hint: None,
            },
        ],
    };

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-sequential-in-memory-1",
            &payload,
            now,
            "idem-sequential-in-memory-1",
            "corr-sequential-in-memory-1",
            "cause-sequential-in-memory-1",
            "trace-sequential-in-memory-1",
            "sttp:in:orchestration:sequential:in-memory:1",
        ))
        .await
        .expect("sequential job should enqueue");

    runtime
        .process_once("default", "worker-sequential-in-memory", now)
        .await
        .expect("sequential processing should succeed");

    let job = runtime
        .job_store
        .get("job-sequential-in-memory-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:orchestration:sequential:job-sequential-in-memory-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-sequential-in-memory-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-orchestration-sequential"))
    );
    assert_eq!(diagnostics.get("pattern"), Some(&json!("sequential")));
    assert_eq!(diagnostics.get("stages_executed"), Some(&json!(2)));
    assert_eq!(
        diagnostics.get("termination_reason"),
        Some(&json!("completed_all_stages"))
    );
}

#[tokio::test]
async fn surreal_orchestration_sequential_pattern_policy_violation_dead_letters_job() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem(
        "test",
        "runtime_backend_parity_orchestration_sequential_failure",
    ))
    .with_chat_client(Arc::new(PlainScriptedChatClient::new(vec![
        "unused".to_string(),
    ])))
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .without_memory_operation_handlers()
    .build()
    .await
    .expect("runtime should build");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime");
    };

    let payload = SequentialPatternJobPayload {
        thread_id: Some("thread.sequential.invalid.1".to_string()),
        initial_user_prompt: "   ".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "analyze".to_string(),
            user_prompt_template: "Analyze: {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-sequential-surreal-invalid-1",
            &payload,
            now,
            "idem-sequential-surreal-invalid-1",
            "corr-sequential-surreal-invalid-1",
            "cause-sequential-surreal-invalid-1",
            "trace-sequential-surreal-invalid-1",
            "sttp:in:orchestration:sequential:surreal:invalid:1",
        ))
        .await
        .expect("sequential job should enqueue");

    runtime
        .process_once("default", "worker-sequential-surreal", now)
        .await
        .expect("sequential processing should complete");

    let job = runtime
        .job_store
        .get("job-sequential-surreal-invalid-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-sequential-surreal-invalid-1")
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("initial_user_prompt must be non-empty")
    );
}

#[tokio::test]
async fn surreal_orchestration_concurrent_pattern_executes_all_branches() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem(
        "test",
        "runtime_backend_parity_orchestration_concurrent_success",
    ))
    .with_chat_client(Arc::new(EchoPromptChatClient))
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .without_memory_operation_handlers()
    .build()
    .await
    .expect("runtime should build");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime");
    };

    let payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread.concurrent.1".to_string()),
        initial_user_prompt: "base context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("join_with_headers".to_string()),
        branches: vec![
            ConcurrentBranchJobPayload {
                branch_id: "branch.b".to_string(),
                user_prompt_template: "Branch B uses {input}".to_string(),
                system_prompt: Some("be direct".to_string()),
                policy_profile: None,
                model_hint: None,
                execution_mode: ConcurrentBranchExecutionMode::Prompt,
                tool_name: None,
                tool_input: None,
                tool_call_mode: None,
                memory_policy: None,
            },
            ConcurrentBranchJobPayload {
                branch_id: "branch.a".to_string(),
                user_prompt_template: "Branch A uses {input}".to_string(),
                system_prompt: Some("be concise".to_string()),
                policy_profile: None,
                model_hint: None,
                execution_mode: ConcurrentBranchExecutionMode::Prompt,
                tool_name: None,
                tool_input: None,
                tool_call_mode: None,
                memory_policy: None,
            },
        ],
    };

    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-concurrent-surreal-1",
            &payload,
            now,
            "idem-concurrent-surreal-1",
            "corr-concurrent-surreal-1",
            "cause-concurrent-surreal-1",
            "trace-concurrent-surreal-1",
            "sttp:in:orchestration:concurrent:surreal:1",
        ))
        .await
        .expect("concurrent job should enqueue");

    runtime
        .process_once("default", "worker-concurrent-surreal", now)
        .await
        .expect("concurrent processing should succeed");

    let job = runtime
        .job_store
        .get("job-concurrent-surreal-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:orchestration:concurrent:job-concurrent-surreal-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-concurrent-surreal-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-orchestration-concurrent"))
    );
    assert_eq!(diagnostics.get("pattern"), Some(&json!("concurrent")));
    assert_eq!(diagnostics.get("branches_executed"), Some(&json!(2)));
    assert_eq!(
        diagnostics.get("termination_reason"),
        Some(&json!("completed_all_branches"))
    );

    let final_text = diagnostics
        .get("final_text")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    assert!(final_text.contains("[branch.a]"));
    assert!(final_text.contains("[branch.b]"));
}

#[tokio::test]
async fn in_memory_orchestration_concurrent_pattern_policy_violation_dead_letters_job() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread.concurrent.invalid.1".to_string()),
        initial_user_prompt: "base context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: None,
        branches: vec![],
    };

    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-concurrent-in-memory-invalid-1",
            &payload,
            now,
            "idem-concurrent-in-memory-invalid-1",
            "corr-concurrent-in-memory-invalid-1",
            "cause-concurrent-in-memory-invalid-1",
            "trace-concurrent-in-memory-invalid-1",
            "sttp:in:orchestration:concurrent:in-memory:invalid:1",
        ))
        .await
        .expect("concurrent job should enqueue");

    runtime
        .process_once("default", "worker-concurrent-in-memory", now)
        .await
        .expect("concurrent processing should complete");

    let job = runtime
        .job_store
        .get("job-concurrent-in-memory-invalid-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-concurrent-in-memory-invalid-1")
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("payload.branches must include at least one branch")
    );
}

#[tokio::test]
async fn in_memory_orchestration_concurrent_pattern_persists_branch_thread_lineage() {
    let now = Utc::now();
    let thread_store = Arc::new(InMemoryThreadStore::default());
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_thread_store(thread_store.clone())
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let root_thread_id = "thread.concurrent.lineage.1".to_string();
    let payload = ConcurrentPatternJobPayload {
        thread_id: Some(root_thread_id.clone()),
        initial_user_prompt: "review architecture".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("join_with_headers".to_string()),
        branches: vec![
            ConcurrentBranchJobPayload {
                branch_id: "alpha".to_string(),
                user_prompt_template: "Alpha branch: {input}".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                execution_mode: ConcurrentBranchExecutionMode::Prompt,
                tool_name: None,
                tool_input: None,
                tool_call_mode: None,
                memory_policy: None,
            },
            ConcurrentBranchJobPayload {
                branch_id: "beta".to_string(),
                user_prompt_template: "Beta branch: {input}".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                execution_mode: ConcurrentBranchExecutionMode::Prompt,
                tool_name: None,
                tool_input: None,
                tool_call_mode: None,
                memory_policy: None,
            },
        ],
    };

    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-concurrent-lineage-in-memory-1",
            &payload,
            now,
            "idem-concurrent-lineage-in-memory-1",
            "corr-concurrent-lineage-in-memory-1",
            "cause-concurrent-lineage-in-memory-1",
            "trace-concurrent-lineage-in-memory-1",
            "sttp:in:orchestration:concurrent:lineage:in-memory:1",
        ))
        .await
        .expect("concurrent job should enqueue");

    runtime
        .process_once("default", "worker-concurrent-lineage", now)
        .await
        .expect("concurrent processing should succeed");

    let lineage = thread_store
        .list_lineage("thread.concurrent.lineage.1::branch::alpha")
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[0].thread_id, root_thread_id);
    assert_eq!(
        lineage[1].thread_id,
        "thread.concurrent.lineage.1::branch::alpha"
    );

    let events = thread_store
        .list_events("thread.concurrent.lineage.1::branch::alpha")
        .await
        .expect("branch events should load");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].event_kind,
        "orchestration.concurrent.branch.completed"
    );
}

#[tokio::test]
async fn in_memory_orchestration_concurrent_mixed_tool_and_prompt_branches() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(BranchAwareConcurrentTestClient))
        .with_tool(MockWebSearchTool)
        .expect("tool should register")
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread.concurrent.mixed.1".to_string()),
        initial_user_prompt: "review release".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("join_with_headers".to_string()),
        branches: vec![
            ConcurrentBranchJobPayload::prompt("summary", "Summarize {input}"),
            ConcurrentBranchJobPayload::tool_loop(
                "research",
                "Run tool branch for {input}",
                "stasis.web.search.mock",
                Some(json!({ "query": "review release" })),
            ),
        ],
    };

    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-concurrent-mixed-in-memory-1",
            &payload,
            now,
            "idem-concurrent-mixed-in-memory-1",
            "corr-concurrent-mixed-in-memory-1",
            "cause-concurrent-mixed-in-memory-1",
            "trace-concurrent-mixed-in-memory-1",
            "sttp:in:orchestration:concurrent:mixed:in-memory:1",
        ))
        .await
        .expect("concurrent job should enqueue");

    runtime
        .process_once("default", "worker-concurrent-mixed", now)
        .await
        .expect("concurrent processing should succeed");

    let job = runtime
        .job_store
        .get("job-concurrent-mixed-in-memory-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-concurrent-mixed-in-memory-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(diagnostics.get("tool_loop_branch_count"), Some(&json!(1)));
    assert_eq!(diagnostics.get("prompt_branch_count"), Some(&json!(1)));
    assert_eq!(
        diagnostics.pointer("/branch_summaries/0/execution_mode"),
        Some(&json!("tool_loop"))
    );
}

#[tokio::test]
async fn in_memory_orchestration_concurrent_tool_loop_branch_missing_tool_name_rejects() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient::default()))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let mut tool_branch = ConcurrentBranchJobPayload::tool_loop(
        "research",
        "Research {input}",
        "stasis.web.search.mock",
        None,
    );
    tool_branch.tool_name = None;

    let payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread.concurrent.tool.invalid.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: None,
        branches: vec![tool_branch],
    };

    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-concurrent-tool-invalid-in-memory-1",
            &payload,
            now,
            "idem-concurrent-tool-invalid-in-memory-1",
            "corr-concurrent-tool-invalid-in-memory-1",
            "cause-concurrent-tool-invalid-in-memory-1",
            "trace-concurrent-tool-invalid-in-memory-1",
            "sttp:in:orchestration:concurrent:tool:invalid:in-memory:1",
        ))
        .await
        .expect("concurrent job should enqueue");

    runtime
        .process_once("default", "worker-concurrent-tool-invalid", now)
        .await
        .expect("concurrent processing should complete");

    let job = runtime
        .job_store
        .get("job-concurrent-tool-invalid-in-memory-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);
    assert!(
        runtime
            .job_attempt_store
            .list_by_job_id("job-concurrent-tool-invalid-in-memory-1")
            .await
            .expect("attempt list should succeed")[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("tool_name must be non-empty")
    );
}

#[tokio::test]
async fn in_memory_orchestration_handoff_pattern_executes_turns_and_emits_transitions() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(PlainScriptedChatClient::new(vec![
            "agent-alpha output".to_string(),
            "agent-beta output".to_string(),
        ])))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let payload = HandoffPatternJobPayload {
        thread_id: Some("thread.handoff.1".to_string()),
        initial_user_prompt: "initial context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        turns: vec![
            HandoffTurnJobPayload {
                actor_id: "agent.alpha".to_string(),
                user_prompt_template: "Alpha handles: {input}".to_string(),
                system_prompt: Some("be analytical".to_string()),
                policy_profile: None,
                model_hint: None,
            },
            HandoffTurnJobPayload {
                actor_id: "agent.beta".to_string(),
                user_prompt_template: "Beta finalizes: {input}".to_string(),
                system_prompt: Some("be concise".to_string()),
                policy_profile: None,
                model_hint: None,
            },
        ],
    };

    runtime
        .enqueue(build_orchestration_handoff_job(
            "job-handoff-in-memory-1",
            &payload,
            now,
            "idem-handoff-in-memory-1",
            "corr-handoff-in-memory-1",
            "cause-handoff-in-memory-1",
            "trace-handoff-in-memory-1",
            "sttp:in:orchestration:handoff:in-memory:1",
        ))
        .await
        .expect("handoff job should enqueue");

    runtime
        .process_once("default", "worker-handoff-in-memory", now)
        .await
        .expect("handoff processing should succeed");

    let job = runtime
        .job_store
        .get("job-handoff-in-memory-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:orchestration:handoff:job-handoff-in-memory-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-handoff-in-memory-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-orchestration-handoff"))
    );
    assert_eq!(diagnostics.get("pattern"), Some(&json!("handoff")));
    assert_eq!(diagnostics.get("turns_executed"), Some(&json!(2)));
    assert_eq!(
        diagnostics.get("termination_reason"),
        Some(&json!("completed_all_turns"))
    );
    let handoffs = diagnostics
        .get("handoffs")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(handoffs.len(), 1);
    assert_eq!(
        handoffs[0].get("from_actor_id"),
        Some(&json!("agent.alpha"))
    );
    assert_eq!(handoffs[0].get("to_actor_id"), Some(&json!("agent.beta")));
}

#[tokio::test]
async fn surreal_orchestration_handoff_pattern_policy_violation_dead_letters_job() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem(
        "test",
        "runtime_backend_parity_orchestration_handoff_failure",
    ))
    .with_chat_client(Arc::new(EchoPromptChatClient))
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .without_memory_operation_handlers()
    .build()
    .await
    .expect("runtime should build");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime");
    };

    let payload = HandoffPatternJobPayload {
        thread_id: Some("thread.handoff.invalid.1".to_string()),
        initial_user_prompt: "initial context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        turns: vec![HandoffTurnJobPayload {
            actor_id: " ".to_string(),
            user_prompt_template: "broken actor {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_handoff_job(
            "job-handoff-surreal-invalid-1",
            &payload,
            now,
            "idem-handoff-surreal-invalid-1",
            "corr-handoff-surreal-invalid-1",
            "cause-handoff-surreal-invalid-1",
            "trace-handoff-surreal-invalid-1",
            "sttp:in:orchestration:handoff:surreal:invalid:1",
        ))
        .await
        .expect("handoff job should enqueue");

    runtime
        .process_once("default", "worker-handoff-surreal", now)
        .await
        .expect("handoff processing should complete");

    let job = runtime
        .job_store
        .get("job-handoff-surreal-invalid-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-handoff-surreal-invalid-1")
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("turns[].actor_id must be non-empty")
    );
}

#[tokio::test]
async fn in_memory_orchestration_orchestrator_pattern_selects_matching_route() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let payload = OrchestratorPatternJobPayload {
        thread_id: Some("thread.orchestrator.1".to_string()),
        initial_user_prompt: "Need SQL query tuning help".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        routes: vec![
            OrchestratorRouteJobPayload {
                route_id: "route.general".to_string(),
                selector_keywords: vec!["summary".to_string(), "overview".to_string()],
                user_prompt_template: "General route: {input}".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
            },
            OrchestratorRouteJobPayload {
                route_id: "route.sql".to_string(),
                selector_keywords: vec!["sql".to_string(), "query".to_string()],
                user_prompt_template: "SQL route: {input}".to_string(),
                system_prompt: Some("be technical".to_string()),
                policy_profile: None,
                model_hint: None,
            },
        ],
    };

    runtime
        .enqueue(build_orchestration_orchestrator_job(
            "job-orchestrator-in-memory-1",
            &payload,
            now,
            "idem-orchestrator-in-memory-1",
            "corr-orchestrator-in-memory-1",
            "cause-orchestrator-in-memory-1",
            "trace-orchestrator-in-memory-1",
            "sttp:in:orchestration:orchestrator:in-memory:1",
        ))
        .await
        .expect("orchestrator job should enqueue");

    runtime
        .process_once("default", "worker-orchestrator-in-memory", now)
        .await
        .expect("orchestrator processing should succeed");

    let job = runtime
        .job_store
        .get("job-orchestrator-in-memory-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(
        job.sttp_output_node_id.as_deref(),
        Some("sttp:orchestration:orchestrator:job-orchestrator-in-memory-1")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-orchestrator-in-memory-1")
        .await
        .expect("attempt list should succeed");
    let diagnostics: JsonValue = serde_json::from_str(
        attempts[0]
            .diagnostics
            .as_deref()
            .expect("diagnostics should be present"),
    )
    .expect("diagnostics should be valid json");
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-orchestration-orchestrator"))
    );
    assert_eq!(diagnostics.get("pattern"), Some(&json!("orchestrator")));
    assert_eq!(
        diagnostics.get("selected_route_id"),
        Some(&json!("route.sql"))
    );
    assert!(
        diagnostics
            .get("selection_reason")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .contains("keyword_match")
    );
}

#[tokio::test]
async fn surreal_orchestration_orchestrator_pattern_policy_violation_dead_letters_job() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem(
        "test",
        "runtime_backend_parity_orchestration_orchestrator_failure",
    ))
    .with_chat_client(Arc::new(EchoPromptChatClient))
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .without_memory_operation_handlers()
    .build()
    .await
    .expect("runtime should build");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime");
    };

    let payload = OrchestratorPatternJobPayload {
        thread_id: Some("thread.orchestrator.invalid.1".to_string()),
        initial_user_prompt: "routing".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        routes: vec![OrchestratorRouteJobPayload {
            route_id: " ".to_string(),
            selector_keywords: vec!["routing".to_string()],
            user_prompt_template: "route: {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_orchestrator_job(
            "job-orchestrator-surreal-invalid-1",
            &payload,
            now,
            "idem-orchestrator-surreal-invalid-1",
            "corr-orchestrator-surreal-invalid-1",
            "cause-orchestrator-surreal-invalid-1",
            "trace-orchestrator-surreal-invalid-1",
            "sttp:in:orchestration:orchestrator:surreal:invalid:1",
        ))
        .await
        .expect("orchestrator job should enqueue");

    runtime
        .process_once("default", "worker-orchestrator-surreal", now)
        .await
        .expect("orchestrator processing should complete");

    let job = runtime
        .job_store
        .get("job-orchestrator-surreal-invalid-1")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::DeadLetter);

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-orchestrator-surreal-invalid-1")
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::FatalFailure);
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("routes[].route_id must be non-empty")
    );
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
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "plain model text".to_string(),
    ]));
    let tool_registry = InMemoryToolRegistry::default();
    tool_registry
        .register_tool(MockWebSearchTool)
        .expect("tool should register");

    runtime
        .register_handler(ToolLoopJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-strict-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
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
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
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
        .register_handler(ToolLoopJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-invalid-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "   ".to_string(),
        system_prompt: None,
        policy_profile: None,
        model_hint: None,
        memory_policy: None,
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
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
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
    assert_eq!(
        diagnostics.get("provider"),
        Some(&json!("stasis-tool-loop"))
    );
    assert_eq!(diagnostics.get("status"), Some(&json!("failure")));
    assert_eq!(
        diagnostics.get("guardrail_code"),
        Some(&json!("POLICY_VIOLATION"))
    );
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
    assert_eq!(
        lineage[0].event.event_type,
        RuntimeEventType::JobDeadLettered
    );
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
        .register_handler(ToolLoopJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-tool-loop-schema-invalid-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
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
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
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
    assert_eq!(
        diagnostics.get("guardrail_code"),
        Some(&json!("POLICY_VIOLATION"))
    );
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
        .register_handler(ToolLoopJobHandler::new(
            chat_client,
            Arc::new(tool_registry),
        ))
        .expect("tool loop handler should register");

    let now = Utc::now();
    let job_id = "job-surreal-tool-loop-schema-invalid-1".to_string();
    let payload = ToolLoopJobPayload {
        user_prompt: "latest rust trends".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
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
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
async fn in_memory_thread_store_supports_create_append_fork_and_lineage() {
    let store = InMemoryThreadStore::default();
    let now = Utc::now();

    store
        .create_thread(NewThread {
            thread_id: "thread.root.1".to_string(),
            parent_thread_id: None,
            branch_label: Some("root".to_string()),
            created_at: now,
        })
        .await
        .expect("root thread should create");

    store
        .append_event(NewThreadEvent {
            event_id: "event.root.1".to_string(),
            thread_id: "thread.root.1".to_string(),
            event_kind: "message".to_string(),
            payload_ref: "sttp:event:root:1".to_string(),
            occurred_at: now + Duration::seconds(1),
        })
        .await
        .expect("root event should append");

    store
        .fork_thread(
            "thread.root.1",
            "thread.child.1",
            Some("exploration".to_string()),
            now + Duration::seconds(2),
        )
        .await
        .expect("child thread should fork");

    store
        .append_event(NewThreadEvent {
            event_id: "event.child.1".to_string(),
            thread_id: "thread.child.1".to_string(),
            event_kind: "message".to_string(),
            payload_ref: "sttp:event:child:1".to_string(),
            occurred_at: now + Duration::seconds(3),
        })
        .await
        .expect("child event should append");

    let root_events = store
        .list_events("thread.root.1")
        .await
        .expect("root events should load");
    assert_eq!(root_events.len(), 1);
    assert_eq!(root_events[0].event_id, "event.root.1");

    let lineage = store
        .list_lineage("thread.child.1")
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[0].thread_id, "thread.root.1");
    assert_eq!(lineage[1].thread_id, "thread.child.1");
}

#[tokio::test]
async fn surreal_thread_store_supports_create_append_fork_and_lineage() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_thread_store_parity")
        .await
        .expect("namespace and db should be selected");

    let store = SurrealThreadStore::new(db);
    let now = Utc::now();

    store
        .create_thread(NewThread {
            thread_id: "thread.root.2".to_string(),
            parent_thread_id: None,
            branch_label: Some("root".to_string()),
            created_at: now,
        })
        .await
        .expect("root thread should create");

    store
        .append_event(NewThreadEvent {
            event_id: "event.root.2".to_string(),
            thread_id: "thread.root.2".to_string(),
            event_kind: "message".to_string(),
            payload_ref: "sttp:event:root:2".to_string(),
            occurred_at: now + Duration::seconds(1),
        })
        .await
        .expect("root event should append");

    store
        .fork_thread(
            "thread.root.2",
            "thread.child.2",
            Some("analysis".to_string()),
            now + Duration::seconds(2),
        )
        .await
        .expect("child thread should fork");

    store
        .append_event(NewThreadEvent {
            event_id: "event.child.2".to_string(),
            thread_id: "thread.child.2".to_string(),
            event_kind: "message".to_string(),
            payload_ref: "sttp:event:child:2".to_string(),
            occurred_at: now + Duration::seconds(3),
        })
        .await
        .expect("child event should append");

    let child_events = store
        .list_events("thread.child.2")
        .await
        .expect("child events should load");
    assert_eq!(child_events.len(), 1);
    assert_eq!(child_events[0].event_id, "event.child.2");

    let lineage = store
        .list_lineage("thread.child.2")
        .await
        .expect("lineage should load");
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[0].thread_id, "thread.root.2");
    assert_eq!(lineage[1].thread_id, "thread.child.2");
}

#[tokio::test]
async fn in_memory_thread_store_rejects_event_append_for_unknown_thread() {
    let store = InMemoryThreadStore::default();
    let err = store
        .append_event(NewThreadEvent {
            event_id: "event.missing.1".to_string(),
            thread_id: "thread.missing.1".to_string(),
            event_kind: "message".to_string(),
            payload_ref: "sttp:event:missing:1".to_string(),
            occurred_at: Utc::now(),
        })
        .await
        .expect_err("append should fail for missing thread");

    assert!(err.to_string().contains("thread not found"));
}

#[tokio::test]
async fn surreal_runtime_replays_dead_letter_and_retries_outbox_publish() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
    assert_eq!(
        replay_report.attempts[0].outcome,
        JobAttemptOutcome::FatalFailure
    );
    assert_eq!(
        replay_report.attempts[1].outcome,
        JobAttemptOutcome::Succeeded
    );
    assert!(replay_report.attempts[0].error_message.is_some());
    assert!(replay_report.attempts[1].sttp_output_node_id.is_some());

    let lineage = runtime
        .list_lineage_events("job-test.fatal_then_success")
        .await
        .expect("lineage events should load");
    assert_eq!(lineage.len(), 2);
    assert!(
        lineage
            .iter()
            .all(|evt| evt.event.correlation_id == "corr-1")
    );
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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

    let winners = [leased_a, leased_b]
        .iter()
        .filter(|job| job.is_some())
        .count();
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn surreal_job_lease_expiry_allows_recovery_by_another_worker() {
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("invalid echo payload json")
    );
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
    assert_eq!(
        attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert!(
        attempts[0]
            .policy_reason
            .as_deref()
            .unwrap_or_default()
            .contains("must be non-empty")
    );
}

#[tokio::test]
async fn grapheme_sdk_rejects_non_allowlisted_import() {
    let engine = GraphemeSdkWorkflowEngine::new();
    let source = r#"import sql from "acme/sql"

query Run {
  sql.query(connection: "local", sql: "select 1") {
    rows
  }
}
"#;

    let err = engine
        .execute_grapheme_source(source, None)
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
        .execute_grapheme_source(source, None)
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

    let source = r#"import sql from "acme/sql"

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
    assert!(
        guardrail_attempts
            .iter()
            .any(|attempt| attempt.job_id == job_id)
    );

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
        .configure_retention_policy(RetentionPolicy {
            terminal_ttl_days: 1,
        })
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
    let db = Surreal::<Any>::init();
        db.connect("mem://")
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
        .configure_retention_policy(RetentionPolicy {
            terminal_ttl_days: 1,
        })
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
async fn lineage_investigator_includes_memory_lineage_metadata() {
    let runtime = InMemoryRuntime::new();
    let chat_client = Arc::new(ScriptedChatClient::new(vec![
        "lineage investigator completion".to_string(),
    ]));
    let memory_reader = Arc::new(MockMemoryContextReader {
        response: MemoryRecallResponse {
            retrieved: 2,
            retrieval_path: Some("Hybrid".to_string()),
            fallback_triggered: false,
            fallback_reason: None,
            node_sync_keys: vec!["sync-lineage-1".to_string()],
            ..Default::default()
        },
    });
    let memory_writer = Arc::new(MockMemoryContextWriter {
        response: MemoryStoreResponse {
            node_id: "sttp:memory:lineage-investigator:1".to_string(),
            psi: 2.6,
            valid: true,
            validation_error: None,
        },
    });

    runtime
        .register_handler(PromptChatJobHandler::new_with_memory(
            chat_client,
            Some(memory_reader),
            Some(memory_writer),
        ))
        .expect("prompt handler should register");

    let now = Utc::now();
    let job_id = "job-lineage-memory-metadata-1".to_string();
    let payload = PromptJobPayload {
        user_prompt: "memory lineage check".to_string(),
        system_prompt: Some("be concise".to_string()),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        memory_policy: None,
    };

    runtime
        .enqueue(build_prompt_job(
            &job_id,
            &payload,
            now,
            "idem-lineage-memory-metadata-1",
            "corr-lineage-memory-metadata-1",
            "cause-lineage-memory-metadata-1",
            "trace-lineage-memory-metadata-1",
            "sttp:in:lineage:memory:1",
        ))
        .await
        .expect("prompt job should enqueue");

    runtime
        .process_once("default", "worker-lineage", now)
        .await
        .expect("processing should succeed");

    let report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            job_id: Some(job_id.clone()),
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("lineage investigation should succeed");

    assert_eq!(report.attempts.len(), 1);
    assert_eq!(report.lineage_events.len(), 1);
    assert_eq!(report.lineage_events[0].event.job_id, job_id);
    assert!(
        report.lineage_events[0]
            .event
            .input_memory_query_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("mq:")
    );
    assert!(
        report.lineage_events[0]
            .event
            .input_memory_query_fingerprint
            .as_deref()
            .unwrap_or_default()
            .contains("alpha=")
    );
    assert_eq!(
        report.lineage_events[0]
            .event
            .output_memory_node_id
            .as_deref(),
        Some("sttp:memory:lineage-investigator:1")
    );
    assert_eq!(
        report.lineage_events[0].event.retrieval_path.as_deref(),
        Some("Hybrid")
    );
}

#[tokio::test]
async fn lineage_investigator_queries_guardrail_failures() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeJobHandler::new(workflow_engine))
        .expect("grapheme handler should register");

        let source = r#"import sql from "acme/sql"

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

    assert!(
        report
            .attempts
            .iter()
            .any(|attempt| attempt.job_id == job_id)
    );
    assert!(
        report
            .attempts
            .iter()
            .any(|attempt| attempt.guardrail_code.as_deref() == Some("IMPORT_NOT_ALLOWLISTED"))
    );
    assert!(
        report
            .lineage_events
            .iter()
            .any(|event| event.event.job_id == job_id)
    );
}

#[tokio::test]
async fn lineage_investigator_requires_selector() {
    let runtime = InMemoryRuntime::new();
    let err = runtime
        .investigate_lineage(RuntimeLineageQuery::default())
        .await
        .expect_err("empty selector should fail");

    assert!(err.to_string().contains("requires at least one selector"));
}

#[tokio::test]
async fn lineage_investigator_filters_by_branch_thread_and_includes_ancestry() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let root_thread_id = "thread.lineage.selector.1".to_string();
    let payload = ConcurrentPatternJobPayload {
        thread_id: Some(root_thread_id.clone()),
        initial_user_prompt: "lineage selector context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("join_with_headers".to_string()),
        branches: vec![
            ConcurrentBranchJobPayload {
                branch_id: "alpha".to_string(),
                user_prompt_template: "Alpha branch: {input}".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                execution_mode: ConcurrentBranchExecutionMode::Prompt,
                tool_name: None,
                tool_input: None,
                tool_call_mode: None,
                memory_policy: None,
            },
            ConcurrentBranchJobPayload {
                branch_id: "beta".to_string(),
                user_prompt_template: "Beta branch: {input}".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                execution_mode: ConcurrentBranchExecutionMode::Prompt,
                tool_name: None,
                tool_input: None,
                tool_call_mode: None,
                memory_policy: None,
            },
        ],
    };

    let job_id = "job-lineage-thread-selector-1";
    runtime
        .enqueue(build_orchestration_concurrent_job(
            job_id,
            &payload,
            now,
            "idem-lineage-thread-selector-1",
            "corr-lineage-thread-selector-1",
            "cause-lineage-thread-selector-1",
            "trace-lineage-thread-selector-1",
            "sttp:in:lineage:thread:selector:1",
        ))
        .await
        .expect("concurrent job should enqueue");

    runtime
        .process_once("default", "worker-lineage-thread-selector", now)
        .await
        .expect("processing should succeed");

    let report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            thread_id: Some("thread.lineage.selector.1::branch::alpha".to_string()),
            include_thread_ancestry: true,
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("lineage investigation should succeed");

    assert_eq!(report.attempts.len(), 1);
    assert_eq!(report.lineage_events.len(), 1);
    assert_eq!(report.lineage_events[0].event.job_id, job_id);
    assert_eq!(
        report.thread_ancestry,
        vec![
            "thread.lineage.selector.1".to_string(),
            "thread.lineage.selector.1::branch::alpha".to_string(),
        ]
    );

    let root_report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            thread_id: Some(root_thread_id),
            include_thread_ancestry: true,
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("root lineage investigation should succeed");

    assert!(
        root_report
            .thread_ancestry
            .contains(&"thread.lineage.selector.1::branch::alpha".to_string())
    );
    assert!(
        root_report
            .thread_ancestry
            .contains(&"thread.lineage.selector.1::branch::beta".to_string())
    );
}

#[tokio::test]
async fn lineage_investigator_root_thread_selector_expands_descendants_in_memory() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let root_thread_id = "thread.lineage.root.expand.1";
    let branch_thread_id = "thread.lineage.root.expand.1::branch::alpha";

    let root_payload = SequentialPatternJobPayload {
        thread_id: Some(root_thread_id.to_string()),
        initial_user_prompt: "root context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "root-stage".to_string(),
            user_prompt_template: "Root stage: {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    let branch_payload = SequentialPatternJobPayload {
        thread_id: Some(branch_thread_id.to_string()),
        initial_user_prompt: "branch context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "branch-stage".to_string(),
            user_prompt_template: "Branch stage: {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-lineage-root-expand-in-memory-root",
            &root_payload,
            now,
            "idem-lineage-root-expand-in-memory-root",
            "corr-lineage-root-expand-in-memory-root",
            "cause-lineage-root-expand-in-memory-root",
            "trace-lineage-root-expand-in-memory-root",
            "sttp:in:lineage:root:expand:in-memory:root",
        ))
        .await
        .expect("root job should enqueue");

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-lineage-root-expand-in-memory-branch",
            &branch_payload,
            now,
            "idem-lineage-root-expand-in-memory-branch",
            "corr-lineage-root-expand-in-memory-branch",
            "cause-lineage-root-expand-in-memory-branch",
            "trace-lineage-root-expand-in-memory-branch",
            "sttp:in:lineage:root:expand:in-memory:branch",
        ))
        .await
        .expect("branch job should enqueue");

    runtime
        .process_once("default", "worker-lineage-root-expand-in-memory", now)
        .await
        .expect("first processing should succeed");
    runtime
        .process_once("default", "worker-lineage-root-expand-in-memory", now)
        .await
        .expect("second processing should succeed");

    let report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            thread_id: Some(root_thread_id.to_string()),
            include_thread_ancestry: true,
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("root lineage investigation should succeed");

    assert_eq!(report.attempts.len(), 2);
    assert_eq!(report.lineage_events.len(), 2);
    assert!(
        report
            .lineage_events
            .iter()
            .any(|event| event.event.thread_id.as_deref() == Some(root_thread_id))
    );
    assert!(
        report
            .lineage_events
            .iter()
            .any(|event| event.event.thread_id.as_deref() == Some(branch_thread_id))
    );
    assert!(report.thread_ancestry.contains(&root_thread_id.to_string()));
    assert!(
        report
            .thread_ancestry
            .contains(&branch_thread_id.to_string())
    );
}

#[tokio::test]
async fn lineage_investigator_root_thread_selector_expands_descendants_surreal() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::surreal_mem(
        "test",
        "runtime_backend_parity_lineage_root_expand_surreal",
    ))
    .with_chat_client(Arc::new(EchoPromptChatClient))
    .without_grapheme_handlers()
    .without_prompt_handler()
    .without_tool_loop_handler()
    .without_agent_handlers()
    .without_memory_operation_handlers()
    .build()
    .await
    .expect("runtime should build");

    let RuntimeComposition::Surreal(runtime) = runtime else {
        panic!("expected surreal runtime");
    };

    let root_thread_id = "thread.lineage.root.expand.2";
    let branch_thread_id = "thread.lineage.root.expand.2::branch::alpha";

    let root_payload = SequentialPatternJobPayload {
        thread_id: Some(root_thread_id.to_string()),
        initial_user_prompt: "root context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "root-stage".to_string(),
            user_prompt_template: "Root stage: {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    let branch_payload = SequentialPatternJobPayload {
        thread_id: Some(branch_thread_id.to_string()),
        initial_user_prompt: "branch context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "branch-stage".to_string(),
            user_prompt_template: "Branch stage: {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-lineage-root-expand-surreal-root",
            &root_payload,
            now,
            "idem-lineage-root-expand-surreal-root",
            "corr-lineage-root-expand-surreal-root",
            "cause-lineage-root-expand-surreal-root",
            "trace-lineage-root-expand-surreal-root",
            "sttp:in:lineage:root:expand:surreal:root",
        ))
        .await
        .expect("root job should enqueue");

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-lineage-root-expand-surreal-branch",
            &branch_payload,
            now,
            "idem-lineage-root-expand-surreal-branch",
            "corr-lineage-root-expand-surreal-branch",
            "cause-lineage-root-expand-surreal-branch",
            "trace-lineage-root-expand-surreal-branch",
            "sttp:in:lineage:root:expand:surreal:branch",
        ))
        .await
        .expect("branch job should enqueue");

    runtime
        .process_once("default", "worker-lineage-root-expand-surreal", now)
        .await
        .expect("first processing should succeed");
    runtime
        .process_once("default", "worker-lineage-root-expand-surreal", now)
        .await
        .expect("second processing should succeed");

    let report = runtime
        .investigate_lineage(RuntimeLineageQuery {
            thread_id: Some(root_thread_id.to_string()),
            include_thread_ancestry: true,
            ..RuntimeLineageQuery::default()
        })
        .await
        .expect("root lineage investigation should succeed");

    assert_eq!(report.attempts.len(), 2);
    assert_eq!(report.lineage_events.len(), 2);
    assert!(
        report
            .lineage_events
            .iter()
            .any(|event| event.event.thread_id.as_deref() == Some(root_thread_id))
    );
    assert!(
        report
            .lineage_events
            .iter()
            .any(|event| event.event.thread_id.as_deref() == Some(branch_thread_id))
    );
    assert!(report.thread_ancestry.contains(&root_thread_id.to_string()));
    assert!(
        report
            .thread_ancestry
            .contains(&branch_thread_id.to_string())
    );
}

#[tokio::test]
async fn in_memory_orchestration_handlers_emit_standard_success_diagnostics() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let sequential_payload = SequentialPatternJobPayload {
        thread_id: Some("thread.std.sequential.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "s1".to_string(),
            user_prompt_template: "S1 {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };
    let concurrent_payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread.std.concurrent.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("join_with_headers".to_string()),
        branches: vec![ConcurrentBranchJobPayload {
            branch_id: "b1".to_string(),
            user_prompt_template: "B1 {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
            execution_mode: ConcurrentBranchExecutionMode::Prompt,
            tool_name: None,
            tool_input: None,
            tool_call_mode: None,
            memory_policy: None,
        }],
    };
    let handoff_payload = HandoffPatternJobPayload {
        thread_id: Some("thread.std.handoff.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        turns: vec![HandoffTurnJobPayload {
            actor_id: "actor.a".to_string(),
            user_prompt_template: "Turn {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };
    let orchestrator_payload = OrchestratorPatternJobPayload {
        thread_id: Some("thread.std.orchestrator.1".to_string()),
        initial_user_prompt: "needs sql".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        routes: vec![OrchestratorRouteJobPayload {
            route_id: "route.sql".to_string(),
            selector_keywords: vec!["sql".to_string()],
            user_prompt_template: "Route {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-std-sequential-success",
            &sequential_payload,
            now,
            "idem-std-sequential-success",
            "corr-std-sequential-success",
            "cause-std-sequential-success",
            "trace-std-sequential-success",
            "sttp:in:std:sequential:success",
        ))
        .await
        .expect("sequential should enqueue");
    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-std-concurrent-success",
            &concurrent_payload,
            now,
            "idem-std-concurrent-success",
            "corr-std-concurrent-success",
            "cause-std-concurrent-success",
            "trace-std-concurrent-success",
            "sttp:in:std:concurrent:success",
        ))
        .await
        .expect("concurrent should enqueue");
    runtime
        .enqueue(build_orchestration_handoff_job(
            "job-std-handoff-success",
            &handoff_payload,
            now,
            "idem-std-handoff-success",
            "corr-std-handoff-success",
            "cause-std-handoff-success",
            "trace-std-handoff-success",
            "sttp:in:std:handoff:success",
        ))
        .await
        .expect("handoff should enqueue");
    runtime
        .enqueue(build_orchestration_orchestrator_job(
            "job-std-orchestrator-success",
            &orchestrator_payload,
            now,
            "idem-std-orchestrator-success",
            "corr-std-orchestrator-success",
            "cause-std-orchestrator-success",
            "trace-std-orchestrator-success",
            "sttp:in:std:orchestrator:success",
        ))
        .await
        .expect("orchestrator should enqueue");

    for _ in 0..4 {
        runtime
            .process_once("default", "worker-std-success", now)
            .await
            .expect("processing should succeed");
    }

    assert_orchestration_success_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-sequential-success").await,
        "stasis-orchestration-sequential",
        "sequential",
        "thread.std.sequential.1",
    );
    assert_orchestration_success_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-concurrent-success").await,
        "stasis-orchestration-concurrent",
        "concurrent",
        "thread.std.concurrent.1",
    );
    assert_orchestration_success_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-handoff-success").await,
        "stasis-orchestration-handoff",
        "handoff",
        "thread.std.handoff.1",
    );
    assert_orchestration_success_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-orchestrator-success").await,
        "stasis-orchestration-orchestrator",
        "orchestrator",
        "thread.std.orchestrator.1",
    );
}

#[tokio::test]
async fn in_memory_orchestration_handlers_emit_standard_policy_violation_diagnostics() {
    let now = Utc::now();
    let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_chat_client(Arc::new(EchoPromptChatClient))
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_memory_operation_handlers()
        .build()
        .await
        .expect("runtime should build");

    let RuntimeComposition::InMemory(runtime) = runtime else {
        panic!("expected in-memory runtime");
    };

    let sequential_payload = SequentialPatternJobPayload {
        thread_id: Some("thread.std.sequential.invalid.1".to_string()),
        initial_user_prompt: " ".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        stages: vec![SequentialStageJobPayload {
            stage_id: "s1".to_string(),
            user_prompt_template: "S1 {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };
    let concurrent_payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread.std.concurrent.invalid.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: None,
        branches: vec![],
    };
    let handoff_payload = HandoffPatternJobPayload {
        thread_id: Some("thread.std.handoff.invalid.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        turns: vec![HandoffTurnJobPayload {
            actor_id: " ".to_string(),
            user_prompt_template: "Turn {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };
    let orchestrator_payload = OrchestratorPatternJobPayload {
        thread_id: Some("thread.std.orchestrator.invalid.1".to_string()),
        initial_user_prompt: "context".to_string(),
        policy_profile: Some("default".to_string()),
        model_hint: None,
        routes: vec![OrchestratorRouteJobPayload {
            route_id: " ".to_string(),
            selector_keywords: vec!["sql".to_string()],
            user_prompt_template: "Route {input}".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        }],
    };

    runtime
        .enqueue(build_orchestration_sequential_job(
            "job-std-sequential-failure",
            &sequential_payload,
            now,
            "idem-std-sequential-failure",
            "corr-std-sequential-failure",
            "cause-std-sequential-failure",
            "trace-std-sequential-failure",
            "sttp:in:std:sequential:failure",
        ))
        .await
        .expect("sequential should enqueue");
    runtime
        .enqueue(build_orchestration_concurrent_job(
            "job-std-concurrent-failure",
            &concurrent_payload,
            now,
            "idem-std-concurrent-failure",
            "corr-std-concurrent-failure",
            "cause-std-concurrent-failure",
            "trace-std-concurrent-failure",
            "sttp:in:std:concurrent:failure",
        ))
        .await
        .expect("concurrent should enqueue");
    runtime
        .enqueue(build_orchestration_handoff_job(
            "job-std-handoff-failure",
            &handoff_payload,
            now,
            "idem-std-handoff-failure",
            "corr-std-handoff-failure",
            "cause-std-handoff-failure",
            "trace-std-handoff-failure",
            "sttp:in:std:handoff:failure",
        ))
        .await
        .expect("handoff should enqueue");
    runtime
        .enqueue(build_orchestration_orchestrator_job(
            "job-std-orchestrator-failure",
            &orchestrator_payload,
            now,
            "idem-std-orchestrator-failure",
            "corr-std-orchestrator-failure",
            "cause-std-orchestrator-failure",
            "trace-std-orchestrator-failure",
            "sttp:in:std:orchestrator:failure",
        ))
        .await
        .expect("orchestrator should enqueue");

    for _ in 0..4 {
        runtime
            .process_once("default", "worker-std-failure", now)
            .await
            .expect("processing should complete");
    }

    let sequential_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-std-sequential-failure")
        .await
        .expect("attempt list should succeed");
    let concurrent_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-std-concurrent-failure")
        .await
        .expect("attempt list should succeed");
    let handoff_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-std-handoff-failure")
        .await
        .expect("attempt list should succeed");
    let orchestrator_attempts = runtime
        .job_attempt_store
        .list_by_job_id("job-std-orchestrator-failure")
        .await
        .expect("attempt list should succeed");

    assert_eq!(
        sequential_attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert_eq!(
        concurrent_attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert_eq!(
        handoff_attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );
    assert_eq!(
        orchestrator_attempts[0].guardrail_code.as_deref(),
        Some("POLICY_VIOLATION")
    );

    assert_orchestration_policy_violation_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-sequential-failure").await,
        "stasis-orchestration-sequential",
        "sequential",
    );
    assert_orchestration_policy_violation_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-concurrent-failure").await,
        "stasis-orchestration-concurrent",
        "concurrent",
    );
    assert_orchestration_policy_violation_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-handoff-failure").await,
        "stasis-orchestration-handoff",
        "handoff",
    );
    assert_orchestration_policy_violation_diagnostics(
        &attempt_diagnostics_for_job(&runtime, "job-std-orchestrator-failure").await,
        "stasis-orchestration-orchestrator",
        "orchestrator",
    );
}
