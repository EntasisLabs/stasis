//! Browser bindings for the slim Stasis kernel.
//!
//! Enables an in-memory agent loop: OpenAI HTTP (`llm-openai-http`), tool-loop +
//! Grapheme (`grapheme-wasm` 0.7.1), Locus memory + identity, and job-store replay.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, collections::HashMap};

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use {futures_util::future::Either, gloo_timers::future::TimeoutFuture};

use stasis::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
use stasis::application::orchestration::runtime_job_payloads::{
    MemoryPolicyPayload, MemoryRecallJobPayload, ToolLoopJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::application::orchestration::tool_registry::{
    InMemoryToolRegistry, StasisTool, ToolRegistry,
};
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
use stasis::infrastructure::memory::in_memory_identity_memory_store::InMemoryIdentityMemoryStore;
use stasis::infrastructure::memory::locus_context_reader::LocusContextReader;
use stasis::infrastructure::memory::locus_context_writer::LocusContextWriter;
use stasis::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
use stasis::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
use stasis::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
use stasis::ports::outbound::ai_chat_client::AiChatClient;
use stasis::ports::outbound::llm_gateway::LlmGateway;
use stasis::ports::outbound::memory::identity_memory_models::{
    GetIdentityContextRequest, IdentityContextMode, PersonaEntity, UserEntity,
};
use stasis::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use stasis::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use stasis::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use stasis::ports::outbound::memory::memory_models::{
    MemoryRecallRequest, MemoryReflexRequest, MemoryScope, MemoryStoreRequest,
};
use stasis::ports::outbound::memory::memory_operations::MemoryOperations;
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

/** Context for one host-tool invocation. Abort-aware APIs should consume `signal`. */
export interface StasisToolContext {
  tool: string;
  timeoutMs: number;
  signal: AbortSignal;
}

/** A host tool may return any JSON value, directly or through a Promise. */
export type StasisToolCallback = (
  input: unknown,
  context: StasisToolContext,
) => unknown | Promise<unknown>;

export interface StasisToolOptions {
  /** Invocation deadline in milliseconds. Defaults to 30,000; maximum 600,000. */
  timeoutMs?: number;
}

export interface StasisToolSuccess<T = unknown> {
  ok: true;
  result: T;
}

export interface StasisToolFailure {
  ok: false;
  error: { tool: string; code: string; message: string };
}

export type StasisToolResult<T = unknown> = StasisToolSuccess<T> | StasisToolFailure;
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

static NEXT_CALLBACK_SCOPE: AtomicU64 = AtomicU64::new(1);
const DEFAULT_CALLBACK_TIMEOUT_MS: u32 = 30_000;
const MAX_CALLBACK_TIMEOUT_MS: u32 = 600_000;

#[cfg(target_arch = "wasm32")]
thread_local! {
    static HOST_CALLBACKS: RefCell<HashMap<String, js_sys::Function>> = RefCell::new(HashMap::new());
}

struct HostJsTool {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    callback_key: String,
    tool_name: String,
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    timeout_ms: u32,
}

impl HostJsTool {
    fn failure(&self, code: &str, message: impl Into<String>) -> Value {
        json!({
            "ok": false,
            "error": {
                "tool": self.tool_name,
                "code": code,
                "message": message.into(),
            }
        })
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait]
impl StasisTool for HostJsTool {
    fn name(&self) -> &'static str {
        "wasm_host_tool"
    }

    async fn invoke(&self, input: Value) -> StasisResult<Value> {
        let receiver = match dispatch_host_callback(
            &self.callback_key,
            &self.tool_name,
            self.timeout_ms,
            input,
        ) {
            Ok(receiver) => receiver,
            Err(error) => return Ok(error),
        };
        Ok(receiver.await.unwrap_or_else(|_| {
            self.failure("callback_failed", "host callback result channel closed")
        }))
    }
}

#[cfg(target_arch = "wasm32")]
fn dispatch_host_callback(
    callback_key: &str,
    tool_name: &str,
    timeout_ms: u32,
    input: Value,
) -> Result<futures_channel::oneshot::Receiver<Value>, Value> {
    let callback = HOST_CALLBACKS.with(|callbacks| callbacks.borrow().get(callback_key).cloned());
    let Some(callback) = callback else {
        return Err(host_tool_failure(
            tool_name,
            "callback_unavailable",
            "host callback is no longer registered",
        ));
    };
    let input = value_to_js(&input)
        .map_err(|message| host_tool_failure(tool_name, "invalid_arguments", message))?;
    let abort_controller = web_sys::AbortController::new().map_err(|error| {
        host_tool_failure(tool_name, "callback_failed", js_error_message(&error))
    })?;
    let context = js_sys::Object::new();
    js_sys::Reflect::set(
        &context,
        &JsValue::from_str("tool"),
        &JsValue::from_str(tool_name),
    )
    .map_err(|error| host_tool_failure(tool_name, "callback_failed", js_error_message(&error)))?;
    js_sys::Reflect::set(
        &context,
        &JsValue::from_str("timeoutMs"),
        &JsValue::from_f64(timeout_ms.into()),
    )
    .map_err(|error| host_tool_failure(tool_name, "callback_failed", js_error_message(&error)))?;
    js_sys::Reflect::set(
        &context,
        &JsValue::from_str("signal"),
        abort_controller.signal().as_ref(),
    )
    .map_err(|error| host_tool_failure(tool_name, "callback_failed", js_error_message(&error)))?;

    let returned = callback
        .call2(&JsValue::UNDEFINED, &input, &context)
        .map_err(|error| {
            host_tool_failure(tool_name, "callback_failed", js_error_message(&error))
        })?;
    let promise = js_sys::Promise::resolve(&returned);
    let tool_name = tool_name.to_string();
    let (sender, receiver) = futures_channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let callback = JsFuture::from(promise);
        let timeout = TimeoutFuture::new(timeout_ms);
        futures_util::pin_mut!(callback, timeout);
        let result = match futures_util::future::select(callback, timeout).await {
            Either::Left((callback_result, _)) => match callback_result {
                Ok(value) => match js_to_value(&value) {
                    Ok(result) => json!({ "ok": true, "result": result }),
                    Err(message) => {
                        host_tool_failure(&tool_name, "invalid_callback_result", message)
                    }
                },
                Err(error) => {
                    host_tool_failure(&tool_name, "callback_failed", js_error_message(&error))
                }
            },
            Either::Right(((), _)) => {
                abort_controller.abort();
                host_tool_failure(
                    &tool_name,
                    "callback_timeout",
                    format!("host callback exceeded {timeout_ms}ms deadline"),
                )
            }
        };
        let _ = sender.send(result);
    });
    Ok(receiver)
}

#[cfg(target_arch = "wasm32")]
fn host_tool_failure(tool_name: &str, code: &str, message: impl Into<String>) -> Value {
    json!({
        "ok": false,
        "error": {
            "tool": tool_name,
            "code": code,
            "message": message.into(),
        }
    })
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl StasisTool for HostJsTool {
    fn name(&self) -> &'static str {
        "wasm_host_tool"
    }

    async fn invoke(&self, _input: Value) -> StasisResult<Value> {
        Ok(self.failure(
            "callback_unavailable",
            "JavaScript host callbacks require wasm32",
        ))
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolLoopOptions {
    tool: String,
    #[serde(default)]
    tool_input: Option<Value>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    system_prompt: Option<String>,
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
    memory_operations: Arc<LocusMemoryOperations>,
    identity: Arc<InMemoryIdentityMemoryStore>,
    tool_registry: InMemoryToolRegistry,
    callback_scope: String,
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

        let memory = LocusNodeStoreFactory::in_memory().await.map_err(js_err)?;
        let writer = Arc::new(LocusContextWriter::new(memory.clone()));
        let reader = Arc::new(LocusContextReader::new(memory.clone()));
        let operations = Arc::new(LocusMemoryOperations::new(memory, None));

        let tool_registry = InMemoryToolRegistry::default();
        tool_registry.register_tool(EchoTool).map_err(js_err)?;
        let callback_scope = format!(
            "stasis-wasm-{}",
            NEXT_CALLBACK_SCOPE.fetch_add(1, Ordering::Relaxed)
        );

        let runtime = RuntimeSdk::from_builder(
            StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
                .with_chat_client(chat)
                .with_locus_memory()
                .with_memory_context_reader(reader.clone())
                .with_memory_context_writer(writer.clone())
                .with_memory_operations(operations.clone())
                .with_identity_memory_store(identity.clone())
                .with_local_tool_registry(tool_registry.clone())
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
            memory_operations: operations,
            identity,
            tool_registry,
            callback_scope,
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
            "hostToolBootstrap": true,
            "hostToolResultEnvelope": "stasis.tool-result.v1",
            "hostToolCallbackTimeoutMs": DEFAULT_CALLBACK_TIMEOUT_MS,
            "hostToolCallbackTimeoutMaxMs": MAX_CALLBACK_TIMEOUT_MS,
            "hostToolAbortSignal": true,
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
            "memorySchema": "locus-sdk.memory.v4",
            "memoryReflex": "heuristic",
            "identity": "in-memory",
            "replay": "job-store-attempts-and-lineage",
            "corsNote": "Browser calls to api.openai.com need a same-origin /v1 proxy; pass baseUrl"
        })
        .to_string()
    }

    /// Registers a host-defined tool with the default 30-second deadline.
    #[wasm_bindgen]
    pub fn register_tool(
        &self,
        name: String,
        description: String,
        input_schema: JsValue,
        callback: js_sys::Function,
    ) -> Result<(), JsValue> {
        self.register_tool_inner(name, description, input_schema, callback, None)
    }

    /// Registers a host-defined tool with invocation options.
    ///
    /// The callback receives `(input, context)`, where `context.signal` is aborted at the
    /// deadline. Outcomes are normalized to the `stasis.tool-result.v1` envelope.
    #[wasm_bindgen]
    pub fn register_tool_with_options(
        &self,
        name: String,
        description: String,
        input_schema: JsValue,
        callback: js_sys::Function,
        options: Option<JsValue>,
    ) -> Result<(), JsValue> {
        self.register_tool_inner(name, description, input_schema, callback, options)
    }

    fn register_tool_inner(
        &self,
        name: String,
        description: String,
        input_schema: JsValue,
        callback: js_sys::Function,
        options: Option<JsValue>,
    ) -> Result<(), JsValue> {
        let name = name.trim();
        if name.is_empty() {
            return Err(JsValue::from_str("tool name must be non-empty"));
        }
        if description.trim().is_empty() {
            return Err(JsValue::from_str("tool description must be non-empty"));
        }
        if self.tool_registry.contains(name).map_err(js_err)? {
            return Err(JsValue::from_str(&format!(
                "tool already registered: {name}"
            )));
        }
        let schema = js_to_value(&input_schema).map_err(|message| JsValue::from_str(&message))?;
        if !schema.is_object() {
            return Err(JsValue::from_str("tool input schema must be a JSON object"));
        }
        let timeout_ms = parse_tool_timeout(options)?;

        let callback_key = format!("{}:{name}", self.callback_scope);
        #[cfg(target_arch = "wasm32")]
        HOST_CALLBACKS.with(|callbacks| {
            callbacks
                .borrow_mut()
                .insert(callback_key.clone(), callback);
        });
        #[cfg(not(target_arch = "wasm32"))]
        let _ = callback;

        if let Err(error) = self.tool_registry.register_dynamic_tool(
            name,
            description,
            schema,
            true,
            HostJsTool {
                callback_key: callback_key.clone(),
                tool_name: name.to_string(),
                timeout_ms,
            },
        ) {
            #[cfg(target_arch = "wasm32")]
            HOST_CALLBACKS.with(|callbacks| {
                callbacks.borrow_mut().remove(&callback_key);
            });
            return Err(js_err(error));
        }
        Ok(())
    }

    /// Invokes a registered tool without an LLM round trip. This uses the same registry,
    /// schema validation, callback dispatch, and result envelope as the agent tool loop.
    #[wasm_bindgen]
    pub async fn invoke_tool(&self, name: String, arguments: JsValue) -> Result<JsValue, JsValue> {
        let input = js_to_value(&arguments).map_err(|message| JsValue::from_str(&message))?;
        let output = self
            .tool_registry
            .invoke_tool(name.trim(), input)
            .await
            .map_err(js_err)?;
        value_to_js(&output).map_err(|message| JsValue::from_str(&message))
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
            .filter_map(|node| {
                node.context_summary.clone().or_else(|| {
                    if node.raw.is_empty() {
                        None
                    } else {
                        Some(node.raw.clone())
                    }
                })
            })
            .collect();
        Ok(json!({
            "retrieved": response.retrieved,
            "retrieval_path": response.retrieval_path,
            "snippets": snippets,
        })
        .to_string())
    }

    /// Gate `text` into a memory reflex envelope with the offline heuristic decider.
    ///
    /// Does not read or write the store. `kind` is `dispatch`, `ignore`, or `escalate`.
    #[wasm_bindgen]
    pub async fn decide_memory_reflex(
        &self,
        session_id: String,
        text: String,
    ) -> Result<String, JsValue> {
        let response = self
            .memory_operations
            .reflex(&MemoryReflexRequest {
                text,
                role: Some("user".to_string()),
                scope: MemoryScope {
                    session_ids: Some(vec![session_id]),
                    ..MemoryScope::default()
                },
                ..MemoryReflexRequest::default()
            })
            .await
            .map_err(js_err)?;
        serde_json::to_string(&response).map_err(|err| js_err(err.to_string()))
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
    ///
    /// This positional method remains the low-level compatibility surface. New JavaScript hosts
    /// should prefer `enqueue_tool_loop_with_options`, which accepts an object and avoids manual
    /// JSON serialization.
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
        self.enqueue_tool_loop_inner(
            job_id,
            user_prompt,
            tool_name,
            Some(tool_input),
            session_id,
            None,
        )
        .await
    }

    /// Object-based tool-loop enqueue used by the ergonomic JavaScript SDK facade.
    #[wasm_bindgen]
    pub async fn enqueue_tool_loop_with_options(
        &self,
        job_id: String,
        user_prompt: String,
        options: JsValue,
    ) -> Result<String, JsValue> {
        let options: ToolLoopOptions = parse_json_value(&options, "tool-loop options")?;
        if options.tool.trim().is_empty() {
            return Err(JsValue::from_str(
                "tool-loop options.tool must be non-empty",
            ));
        }
        self.enqueue_tool_loop_inner(
            job_id,
            user_prompt,
            options.tool,
            options.tool_input,
            options.session_id,
            options.system_prompt,
        )
        .await
    }

    async fn enqueue_tool_loop_inner(
        &self,
        job_id: String,
        user_prompt: String,
        tool_name: String,
        tool_input: Option<Value>,
        session_id: Option<String>,
        system_prompt: Option<String>,
    ) -> Result<String, JsValue> {
        let correlation = session_id.clone().unwrap_or_else(|| job_id.clone());
        let payload = ToolLoopJobPayload {
            user_prompt,
            system_prompt,
            policy_profile: None,
            model_hint: None,
            reasoning_effort: None,
            tool_name,
            tool_input,
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
    pub async fn enqueue_grapheme(
        &self,
        job_id: String,
        source: String,
    ) -> Result<String, JsValue> {
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

    /// Cancel a non-terminal job and persist the cancellation lifecycle event.
    #[wasm_bindgen]
    pub async fn cancel_job(&self, job_id: String) -> Result<bool, JsValue> {
        self.runtime.cancel(&job_id).await.map_err(js_err)
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

impl Drop for StasisWasmClient {
    fn drop(&mut self) {
        #[cfg(target_arch = "wasm32")]
        HOST_CALLBACKS.with(|callbacks| {
            let prefix = format!("{}:", self.callback_scope);
            callbacks
                .borrow_mut()
                .retain(|key, _| !key.starts_with(&prefix));
        });
    }
}

fn openai_tool_loop_chat_client(gateway: &OpenAiHttpGateway) -> Arc<dyn AiChatClient> {
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

#[derive(Debug, Deserialize, Default)]
struct ToolOptions {
    #[serde(default, alias = "timeout_ms", rename = "timeoutMs")]
    timeout_ms: Option<u32>,
}

fn parse_tool_timeout(options: Option<JsValue>) -> Result<u32, JsValue> {
    let Some(options) = options else {
        return Ok(DEFAULT_CALLBACK_TIMEOUT_MS);
    };
    if options.is_null() || options.is_undefined() {
        return Ok(DEFAULT_CALLBACK_TIMEOUT_MS);
    }
    let raw = js_sys::JSON::stringify(&options)
        .map_err(|error| JsValue::from_str(&js_error_message(&error)))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("tool options must be a JSON object"))?;
    let parsed: ToolOptions = serde_json::from_str(&raw)
        .map_err(|error| JsValue::from_str(&format!("invalid tool options: {error}")))?;
    let timeout_ms = parsed.timeout_ms.unwrap_or(DEFAULT_CALLBACK_TIMEOUT_MS);
    if timeout_ms == 0 || timeout_ms > MAX_CALLBACK_TIMEOUT_MS {
        return Err(JsValue::from_str(&format!(
            "timeoutMs must be between 1 and {MAX_CALLBACK_TIMEOUT_MS}"
        )));
    }
    Ok(timeout_ms)
}

fn parse_json_value<T: for<'de> Deserialize<'de>>(
    value: &JsValue,
    label: &str,
) -> Result<T, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Err(JsValue::from_str(&format!("{label} must be an object")));
    }
    let raw = js_sys::JSON::stringify(value)
        .map_err(|error| JsValue::from_str(&js_error_message(&error)))?
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{label} must be a JSON object")))?;
    serde_json::from_str(&raw)
        .map_err(|error| JsValue::from_str(&format!("invalid {label}: {error}")))
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

fn js_to_value(value: &JsValue) -> Result<Value, String> {
    if value.is_undefined() {
        return Err("value must be JSON-serializable; received undefined".to_string());
    }
    let raw = js_sys::JSON::stringify(value)
        .map_err(|error| {
            format!(
                "value must be JSON-serializable: {}",
                js_error_message(&error)
            )
        })?
        .as_string()
        .ok_or_else(|| "value must be JSON-serializable".to_string())?;
    serde_json::from_str(&raw).map_err(|error| format!("invalid JSON value: {error}"))
}

fn value_to_js(value: &Value) -> Result<JsValue, String> {
    js_sys::JSON::parse(&value.to_string()).map_err(|error| {
        format!(
            "failed to convert JSON for JavaScript: {}",
            js_error_message(&error)
        )
    })
}

fn js_error_message(value: &JsValue) -> String {
    value
        .as_string()
        .or_else(|| {
            js_sys::Reflect::get(value, &JsValue::from_str("message"))
                .ok()?
                .as_string()
        })
        .unwrap_or_else(|| "JavaScript callback failed".to_string())
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
