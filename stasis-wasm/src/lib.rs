//! Browser bindings for the slim Stasis kernel.
//!
//! Enables an in-memory agent loop: OpenAI HTTP (`llm-openai-http`), tool-loop +
//! Grapheme (`grapheme-wasm` 0.7.1), Locus memory + identity, and job-store replay.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::*;

use stasis::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
use stasis::application::orchestration::runtime_job_payloads::{
    MemoryPolicyPayload, MemoryRecallJobPayload, ToolLoopJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::application::orchestration::tool_registry::StasisTool;
use stasis::application::runtime::job_context::{JobContext, JobResult};
use stasis::application::runtime::memory_persistence_helpers::{
    SttpPromptNodeFormat, render_prompt_response_sttp_node,
};
use stasis::application::runtime::runtime_factory::RuntimeBackend;
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::errors::Result as StasisResult;
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use stasis::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEventType};
use stasis::domain::runtime::placement::PlacementConstraints;
use stasis::domain::runtime::provenance::ProvenanceRef;
use stasis::domain::runtime::typed_contract::StasisJob;
use stasis::infrastructure::llm::mock_chat_client::MockAiChatClient;
use stasis::infrastructure::llm::mock_gateway::MockLlmGateway;
use stasis::infrastructure::llm::openai_http_gateway::OpenAiHttpGateway;
use stasis::ports::outbound::ai_chat_client::AiChatClient;
use stasis::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;
use stasis::infrastructure::memory::locus_context_reader::LocusContextReader;
use stasis::infrastructure::memory::locus_context_writer::LocusContextWriter;
use stasis::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
use stasis::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
use stasis::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
use stasis::ports::outbound::llm_gateway::LlmGateway;
use stasis::ports::outbound::memory::identity_memory_models::{
    GetIdentityContextRequest, IdentityContextMode, PersonaEntity, UserEntity,
};
use stasis::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use stasis::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use stasis::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use stasis::ports::outbound::memory::memory_models::{
    MemoryRecallRequest, MemoryScope, MemoryStoreRequest,
};
use stasis::sdk::runtime_sdk::RuntimeSdk;
use stasis::sdk::stasis_sdk::StasisSdk;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND: &'static str = r#"
export interface StasisCreateOptions {
  apiKey?: string;
  model?: string;
  baseUrl?: string;
}
"#;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PingJob {
    n: u32,
}

impl StasisJob for PingJob {
    const NAME: &'static str = "wasm.ping";
    const VERSION: u32 = 1;
    type Output = PingJob;
}

struct PingConsumer;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl JobConsumer<PingJob> for PingConsumer {
    async fn consume(&self, job: PingJob, _ctx: JobContext) -> JobResult<PingJob> {
        Ok(job)
    }
}

struct EchoTool;

#[async_trait]
impl StasisTool for EchoTool {
    fn name(&self) -> &'static str {
        "echo"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Echo JSON input back to the caller")
    }

    async fn invoke(&self, input: Value) -> StasisResult<Value> {
        Ok(json!({ "echo": input }))
    }
}

#[derive(Clone)]
enum WasmLlm {
    Mock(MockLlmGateway),
    OpenAi(OpenAiHttpGateway),
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl LlmGateway for WasmLlm {
    async fn complete(&self, prompt: &str) -> StasisResult<String> {
        match self {
            Self::Mock(gateway) => gateway.complete(prompt).await,
            Self::OpenAi(gateway) => gateway.complete(prompt).await,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct CreateOptions {
    #[serde(default, alias = "api_key", rename = "apiKey")]
    api_key: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, alias = "base_url", rename = "baseUrl")]
    base_url: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LlmKind {
    Mock,
    OpenAi,
}

/// In-memory Stasis kernel for browser / wasm-bindgen hosts.
#[wasm_bindgen]
pub struct StasisWasmClient {
    sdk: StasisSdk<InMemoryAgentRepository, WasmLlm>,
    runtime: RuntimeSdk,
    memory_writer: Arc<LocusContextWriter>,
    memory_reader: Arc<LocusContextReader>,
    identity: Arc<InMemoryIdentityMemoryStore>,
    llm_kind: LlmKind,
}

#[wasm_bindgen]
impl StasisWasmClient {
    /// Builds an in-memory SDK + runtime.
    ///
    /// - `create()` / `create(undefined)` — mock LLM (CI / no key)
    /// - `create({ apiKey, model?, baseUrl? })` — `OpenAiHttpGateway` + tool-loop chat client
    ///
    /// Browser hosts almost always need `baseUrl` pointing at a same-origin `/v1` CORS proxy.
    #[wasm_bindgen]
    pub async fn create(config: Option<JsValue>) -> Result<StasisWasmClient, JsValue> {
        let options = parse_create_options(config)?;
        let api_key = options
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let (llm, chat, llm_kind) = if let Some(api_key) = api_key {
            let model = options
                .model
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("gpt-4o-mini");
            let mut gateway = OpenAiHttpGateway::new(api_key, model);
            if let Some(base_url) = options
                .base_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                gateway = gateway.with_base_url(base_url);
            }
            let chat = openai_tool_loop_chat_client(&gateway);
            (WasmLlm::OpenAi(gateway), chat, LlmKind::OpenAi)
        } else {
            let chat = Arc::new(
                MockAiChatClient::new("stasis-wasm mock completion").with_first_tool_call(),
            );
            (
                WasmLlm::Mock(MockLlmGateway::new("stasis-wasm mock completion")),
                chat as Arc<_>,
                LlmKind::Mock,
            )
        };

        let identity = Arc::new(InMemoryIdentityMemoryStore::default());
        identity
            .upsert_persona(PersonaEntity {
                persona_id: "persona:default".into(),
                display_name: "Stasis".into(),
                status: "active".into(),
                version: 1,
                updated_at: Utc::now(),
            })
            .map_err(js_err)?;

        let memory = LocusNodeStoreFactory::in_memory()
            .await
            .map_err(js_err)?;
        let writer = Arc::new(LocusContextWriter::new(memory.clone()));
        let reader = Arc::new(LocusContextReader::new(memory.clone()));
        let operations = Arc::new(LocusMemoryOperations::new(memory, None));

        let runtime = RuntimeSdk::from_builder(
            StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
                .with_chat_client(chat)
                .with_locus_memory()
                .with_memory_context_reader(reader.clone())
                .with_memory_context_writer(writer.clone())
                .with_memory_operations(operations)
                .with_identity_memory_store(identity.clone())
                .with_tool(EchoTool)
                .map_err(js_err)?
                .without_orchestration_pattern_handlers()
                .without_cluster_control_handlers(),
        )
        .await
        .map_err(js_err)?;
        runtime.register_consumer(PingConsumer).map_err(js_err)?;

        Ok(Self {
            sdk: StasisSdk::new(InMemoryAgentRepository::default(), llm),
            runtime,
            memory_writer: writer,
            memory_reader: reader,
            identity,
            llm_kind,
        })
    }

    /// Honest guest capabilities for the marketing demo (including Grapheme gaps).
    #[wasm_bindgen]
    pub fn capabilities(&self) -> String {
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "llm": match self.llm_kind {
                LlmKind::Mock => "mock",
                LlmKind::OpenAi => "openai-http",
            },
            "tools": true,
            "grapheme": true,
            "graphemeEngine": "grapheme-wasm-0.7.1",
            "graphemeStdlib": ["core", "json", "csv", "yaml", "html"],
            "graphemeGaps": [
                "grapheme:file: payloads (no filesystem)",
                "host-only ops (http, sql, pdf, image, …) fail in-guest",
                "execution_timeout is not preemptively enforced (no spawn_blocking / worker threads)",
                "max_steps / max_call_depth use grapheme-wasm RuntimeOptions defaults"
            ],
            "memory": "locus-in-memory",
            "identity": "in-memory",
            "replay": "job-store-attempts-and-lineage",
            "corsNote": "Browser calls to api.openai.com need a same-origin /v1 proxy; pass baseUrl"
        })
        .to_string()
    }

    #[wasm_bindgen]
    pub async fn register_agent(
        &self,
        id: String,
        name: String,
        system_prompt: String,
    ) -> Result<(), JsValue> {
        self.sdk
            .register_agent(RegisterAgentRequest {
                id,
                name,
                system_prompt,
            })
            .await
            .map_err(js_err)
    }

    #[wasm_bindgen]
    pub async fn invoke_agent(
        &self,
        agent_id: String,
        user_prompt: String,
    ) -> Result<String, JsValue> {
        let response = self
            .sdk
            .invoke_agent(InvokeAgentRequest {
                agent_id,
                user_prompt,
            })
            .await
            .map_err(js_err)?;
        Ok(response.completion)
    }

    /// Store an STTP memory node for `session_id` (in-session Locus, no LLM).
    #[wasm_bindgen]
    pub async fn store_memory(
        &self,
        session_id: String,
        content: String,
    ) -> Result<String, JsValue> {
        let raw_node = render_prompt_response_sttp_node(
            &session_id,
            "wasm.memory.store",
            &content,
            SttpPromptNodeFormat::TaggedSchema,
        );
        let stored = self
            .memory_writer
            .store_context(&MemoryStoreRequest {
                session_id,
                raw_node,
            })
            .await
            .map_err(js_err)?;
        Ok(json!({
            "node_id": stored.node_id,
            "valid": stored.valid,
            "psi": stored.psi,
        })
        .to_string())
    }

    /// Recall previously stored nodes for `session_id`.
    #[wasm_bindgen]
    pub async fn recall_memory(
        &self,
        session_id: String,
        query: String,
    ) -> Result<String, JsValue> {
        let response = self
            .memory_reader
            .recall(&MemoryRecallRequest {
                scope: MemoryScope {
                    session_ids: Some(vec![session_id]),
                    ..MemoryScope::default()
                },
                query_text: Some(query),
                ..MemoryRecallRequest::default()
            })
            .await
            .map_err(js_err)?;
        let snippets: Vec<String> = response
            .nodes
            .iter()
            .filter_map(|node| node.context_summary.clone().or_else(|| {
                if node.raw.is_empty() {
                    None
                } else {
                    Some(node.raw.clone())
                }
            }))
            .collect();
        Ok(json!({
            "retrieved": response.retrieved,
            "retrieval_path": response.retrieval_path,
            "snippets": snippets,
        })
        .to_string())
    }

    /// Upsert a persona + user so later tool-loop jobs can load identity context.
    #[wasm_bindgen]
    pub async fn upsert_identity(
        &self,
        user_id: String,
        display_name: String,
    ) -> Result<(), JsValue> {
        self.identity
            .upsert_user(UserEntity {
                user_id,
                timezone: "UTC".into(),
                language_variant: Some("en".into()),
                preferences: Default::default(),
                status: "active".into(),
                version: 1,
                updated_at: Utc::now(),
            })
            .map_err(js_err)?;
        self.identity
            .upsert_persona(PersonaEntity {
                persona_id: "persona:default".into(),
                display_name,
                status: "active".into(),
                version: 1,
                updated_at: Utc::now(),
            })
            .map_err(js_err)
    }

    #[wasm_bindgen]
    pub async fn identity_context(&self, user_id: String) -> Result<String, JsValue> {
        let context = self
            .identity
            .get_identity_context(&GetIdentityContextRequest {
                user_id,
                persona_id: "persona:default".into(),
                channel_id: "channel:default".into(),
                relationship_limit: 8,
                mode: IdentityContextMode::Cognitive,
            })
            .await
            .map_err(js_err)?;
        Ok(json!({
            "persona_present": context.persona.is_some(),
            "user_present": context.user.is_some(),
            "persona_name": context.persona.as_ref().map(|p| p.display_name.clone()),
            "user_id": context.user.as_ref().map(|u| u.user_id.clone()),
            "contacts": context.contacts.len(),
            "relationships": context.relationships.len(),
        })
        .to_string())
    }

    /// Enqueues a typed no-op ping job on the `default` queue.
    #[wasm_bindgen]
    pub async fn enqueue_ping(&self, n: u32) -> Result<String, JsValue> {
        self.runtime
            .enqueue_job(PingJob { n })
            .queue("default")
            .send()
            .await
            .map_err(js_err)
    }

    /// Enqueue `workflow.stasis.tool_loop`. `tool_input_json` is optional JSON.
    #[wasm_bindgen]
    pub async fn enqueue_tool_loop(
        &self,
        job_id: String,
        user_prompt: String,
        tool_name: String,
        tool_input_json: Option<String>,
        session_id: Option<String>,
    ) -> Result<String, JsValue> {
        let tool_input = match tool_input_json {
            Some(raw) if !raw.trim().is_empty() => {
                serde_json::from_str(&raw).map_err(|err| js_err(err))?
            }
            _ => Value::Null,
        };
        let correlation = session_id.clone().unwrap_or_else(|| job_id.clone());
        let payload = ToolLoopJobPayload {
            user_prompt,
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
            reasoning_effort: None,
            tool_name,
            tool_input: Some(tool_input),
            tool_call_mode: None,
            memory_policy: Some(MemoryPolicyPayload {
                session_ids: session_id.map(|id| vec![id]),
                store_mode: Some(
                    stasis::application::orchestration::runtime_job_payloads::MemoryStoreModePayload::Full,
                ),
                ..MemoryPolicyPayload::default()
            }),
        };
        let job = RuntimeWorkflowJobBuilder::for_tool_loop(job_id.clone(), &payload)
            .map_err(js_err)?
            .with_correlation_id(correlation)
            .with_idempotency_key(job_id.clone())
            .build();
        self.runtime.enqueue(job).await.map_err(js_err)?;
        Ok(job_id)
    }

    /// Enqueue `workflow.grapheme.run` with inline source (no LLM).
    #[wasm_bindgen]
    pub async fn enqueue_grapheme(&self, job_id: String, source: String) -> Result<String, JsValue> {
        let now = Utc::now();
        self.runtime
            .enqueue(NewJob {
                id: job_id.clone(),
                queue: "default".into(),
                job_type: "workflow.grapheme.run".into(),
                payload_ref: format!("grapheme:inline:{source}"),
                priority: 100,
                max_attempts: 1,
                idempotency_key: job_id.clone(),
                correlation_id: job_id.clone(),
                causation_id: "stasis-wasm".into(),
                trace_id: format!("trace-{job_id}"),
                input_provenance: Some(ProvenanceRef::sttp("sttp:in:wasm:grapheme")),
                placement: PlacementConstraints::default(),
                scheduled_at: now,
                backoff_policy: BackoffPolicy::default(),
            })
            .await
            .map_err(js_err)?;
        Ok(job_id)
    }

    /// Enqueue `workflow.grapheme.echo` with `{ "message": "..." }`.
    #[wasm_bindgen]
    pub async fn enqueue_grapheme_echo(
        &self,
        job_id: String,
        message: String,
    ) -> Result<String, JsValue> {
        let now = Utc::now();
        self.runtime
            .enqueue(NewJob {
                id: job_id.clone(),
                queue: "default".into(),
                job_type: "workflow.grapheme.echo".into(),
                payload_ref: json!({ "message": message }).to_string(),
                priority: 100,
                max_attempts: 1,
                idempotency_key: job_id.clone(),
                correlation_id: job_id.clone(),
                causation_id: "stasis-wasm".into(),
                trace_id: format!("trace-{job_id}"),
                input_provenance: Some(ProvenanceRef::sttp("sttp:in:wasm:grapheme-echo")),
                placement: PlacementConstraints::default(),
                scheduled_at: now,
                backoff_policy: BackoffPolicy::default(),
            })
            .await
            .map_err(js_err)?;
        Ok(job_id)
    }

    #[wasm_bindgen]
    pub async fn enqueue_memory_recall(
        &self,
        job_id: String,
        session_id: String,
        query: String,
    ) -> Result<String, JsValue> {
        let payload = MemoryRecallJobPayload {
            memory_policy: Some(MemoryPolicyPayload {
                session_ids: Some(vec![session_id]),
                query_text: Some(query),
                ..MemoryPolicyPayload::default()
            }),
        };
        let job = RuntimeWorkflowJobBuilder::for_memory_recall(job_id.clone(), &payload)
            .map_err(js_err)?
            .with_idempotency_key(job_id.clone())
            .build();
        self.runtime.enqueue(job).await.map_err(js_err)?;
        Ok(job_id)
    }

    #[wasm_bindgen]
    pub async fn process_once(
        &self,
        queue: String,
        worker_id: String,
    ) -> Result<Option<String>, JsValue> {
        self.runtime
            .process_once(&queue, &worker_id)
            .await
            .map_err(js_err)
    }

    /// Drain up to `limit` jobs (default 32).
    #[wasm_bindgen]
    pub async fn process_available(
        &self,
        queue: String,
        worker_id: String,
        limit: Option<u32>,
    ) -> Result<String, JsValue> {
        let max = limit.unwrap_or(32).max(1) as usize;
        let mut processed = Vec::new();
        for _ in 0..max {
            match self
                .runtime
                .process_once(&queue, &worker_id)
                .await
                .map_err(js_err)?
            {
                Some(id) => processed.push(id),
                None => break,
            }
        }
        Ok(json!({ "processed": processed, "count": processed.len() }).to_string())
    }

    #[wasm_bindgen]
    pub async fn job_state(&self, job_id: String) -> Result<String, JsValue> {
        let job = self
            .runtime
            .get_job(&job_id)
            .await
            .map_err(js_err)?
            .ok_or_else(|| JsValue::from_str("job not found"))?;
        Ok(job_state_name(job.state).into())
    }

    /// Full job record as JSON (state, payload, provenance).
    #[wasm_bindgen]
    pub async fn job_record(&self, job_id: String) -> Result<String, JsValue> {
        let job = self
            .runtime
            .get_job(&job_id)
            .await
            .map_err(js_err)?
            .ok_or_else(|| JsValue::from_str("job not found"))?;
        Ok(job_to_json(&job).to_string())
    }

    /// Attempt diagnostics + lineage events (kernel job store, not a JS cache).
    #[wasm_bindgen]
    pub async fn job_history(&self, job_id: String) -> Result<String, JsValue> {
        let report = self
            .runtime
            .get_replay_report(&job_id)
            .await
            .map_err(js_err)?;
        Ok(json!({
            "job_id": report.job_id,
            "attempts": report.attempts.iter().map(attempt_to_json).collect::<Vec<_>>(),
            "lineage_events": report.lineage_events.iter().map(outbox_to_json).collect::<Vec<_>>(),
        })
        .to_string())
    }

    #[wasm_bindgen]
    pub async fn replay_report(&self, job_id: String) -> Result<String, JsValue> {
        self.job_history(job_id).await
    }

    /// Replay a dead-lettered job back to `Enqueued` (does not call the LLM by itself).
    #[wasm_bindgen]
    pub async fn replay_dead_letter(&self, job_id: String) -> Result<bool, JsValue> {
        self.runtime
            .replay_dead_letter(&job_id)
            .await
            .map_err(js_err)
    }

    /// "Run again" from durable history: if the job already succeeded, return stored
    /// diagnostics **without** enqueueing a new LLM call. Dead letters are re-enqueued.
    #[wasm_bindgen]
    pub async fn resume_from_history(&self, job_id: String) -> Result<String, JsValue> {
        let job = self
            .runtime
            .get_job(&job_id)
            .await
            .map_err(js_err)?
            .ok_or_else(|| JsValue::from_str("job not found"))?;
        match job.state {
            JobState::Succeeded => {
                let attempts = self
                    .runtime
                    .list_job_attempts(&job_id)
                    .await
                    .map_err(js_err)?;
                let last = attempts.last();
                Ok(json!({
                    "job_id": job_id,
                    "state": "succeeded",
                    "llm_called": false,
                    "replayed_from_history": true,
                    "diagnostics": last.and_then(|attempt| attempt.diagnostics.clone()),
                    "execution_id": last.and_then(|attempt| attempt.execution_id.clone()),
                    "attempt_count": attempts.len(),
                })
                .to_string())
            }
            JobState::DeadLetter => {
                let replayed = self
                    .runtime
                    .replay_dead_letter(&job_id)
                    .await
                    .map_err(js_err)?;
                Ok(json!({
                    "job_id": job_id,
                    "state": "enqueued",
                    "llm_called": false,
                    "dead_letter_replayed": replayed,
                    "note": "job re-enqueued from dead letter; process_once to execute the handler again"
                })
                .to_string())
            }
            other => Err(JsValue::from_str(&format!(
                "resume_from_history requires succeeded or dead_letter job, got {}",
                job_state_name(other)
            ))),
        }
    }
}

fn openai_tool_loop_chat_client(
    gateway: &OpenAiHttpGateway,
) -> Arc<dyn AiChatClient> {
    // Workspace `cargo check` unifies `stasis-rs` features with `llm-genai`, which
    // hides `OpenAiHttpChatClient`. The npm guest is always wasm32, where genai is off.
    #[cfg(target_arch = "wasm32")]
    {
        use stasis::infrastructure::llm::openai_http_chat_client::OpenAiHttpChatClient;
        Arc::new(OpenAiHttpChatClient::with_gateway(gateway.clone()))
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = gateway;
        Arc::new(
            MockAiChatClient::new("stasis-wasm native-host placeholder").with_first_tool_call(),
        )
    }
}

fn parse_create_options(config: Option<JsValue>) -> Result<CreateOptions, JsValue> {
    let Some(config) = config else {
        return Ok(CreateOptions::default());
    };
    if config.is_null() || config.is_undefined() {
        return Ok(CreateOptions::default());
    }
    let raw = js_sys::JSON::stringify(&config)
        .map_err(|err| {
            err.as_string()
                .map(JsValue::from)
                .unwrap_or_else(|| JsValue::from_str("failed to serialize create config"))
        })?
        .as_string()
        .ok_or_else(|| JsValue::from_str("create config must be a JSON object"))?;
    if raw == "null" || raw == "undefined" {
        return Ok(CreateOptions::default());
    }
    serde_json::from_str(&raw).map_err(js_err)
}

fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Enqueued => "enqueued",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::DeadLetter => "dead_letter",
        JobState::Canceled => "canceled",
    }
}

fn job_to_json(job: &Job) -> Value {
    json!({
        "id": job.id,
        "queue": job.queue,
        "job_type": job.job_type,
        "payload_ref": job.payload_ref,
        "state": job_state_name(job.state),
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "idempotency_key": job.idempotency_key,
        "correlation_id": job.correlation_id,
        "causation_id": job.causation_id,
        "trace_id": job.trace_id,
        "last_error": job.last_error,
        "finished_at": job.finished_at.map(|ts| ts.to_rfc3339()),
        "output_provenance": job.output_provenance.as_ref().map(|p| json!({
            "scheme": format!("{:?}", p.scheme),
            "locator": p.locator,
        })),
    })
}

fn attempt_to_json(attempt: &JobAttempt) -> Value {
    let outcome = match attempt.outcome {
        JobAttemptOutcome::Succeeded => "succeeded",
        JobAttemptOutcome::RetryableFailure => "retryable_failure",
        JobAttemptOutcome::FatalFailure => "fatal_failure",
        JobAttemptOutcome::Deferred => "deferred",
    };
    json!({
        "attempt_id": attempt.attempt_id,
        "job_id": attempt.job_id,
        "attempt_number": attempt.attempt_number,
        "worker_id": attempt.worker_id,
        "outcome": outcome,
        "error_message": attempt.error_message,
        "execution_id": attempt.execution_id,
        "guardrail_code": attempt.guardrail_code,
        "duration_ms": attempt.duration_ms,
        "diagnostics": attempt.diagnostics.as_ref().and_then(|raw| serde_json::from_str::<Value>(raw).ok()).unwrap_or(Value::String(attempt.diagnostics.clone().unwrap_or_default())),
    })
}

fn outbox_to_json(event: &OutboxEvent) -> Value {
    let status = match event.status {
        OutboxStatus::Pending => "pending",
        OutboxStatus::Published => "published",
        OutboxStatus::Failed => "failed",
    };
    let event_type = match event.event.event_type {
        RuntimeEventType::JobSucceeded => "job_succeeded",
        RuntimeEventType::JobRetryScheduled => "job_retry_scheduled",
        RuntimeEventType::JobDeadLettered => "job_dead_lettered",
        RuntimeEventType::JobPublished => "job_published",
        RuntimeEventType::JobCanceled => "job_canceled",
    };
    json!({
        "event_id": event.event_id,
        "status": status,
        "event_type": event_type,
        "job_id": event.event.job_id,
        "correlation_id": event.event.correlation_id,
        "trace_id": event.event.trace_id,
        "message": event.event.message,
        "occurred_at": event.event.occurred_at.to_rfc3339(),
    })
}

fn js_err(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}
