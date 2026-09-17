//! Browser bindings for the slim Stasis kernel (`stasis-rs --no-default-features`).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::*;

use stasis::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
use stasis::application::runtime::job_context::{JobContext, JobResult};
use stasis::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::runtime::job::JobState;
use stasis::domain::runtime::typed_contract::StasisJob;
use stasis::infrastructure::llm::mock_gateway::MockLlmGateway;
use stasis::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
use stasis::ports::outbound::runtime::job_store::JobStore;
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

#[async_trait]
impl JobConsumer<PingJob> for PingConsumer {
    async fn consume(&self, job: PingJob, _ctx: JobContext) -> JobResult<PingJob> {
        Ok(job)
    }
}

/// In-memory Stasis kernel for browser / wasm-bindgen hosts.
#[wasm_bindgen]
pub struct StasisWasmClient {
    sdk: StasisSdk<InMemoryAgentRepository, MockLlmGateway>,
    runtime: RuntimeSdk,
}

#[wasm_bindgen]
impl StasisWasmClient {
    /// Builds an in-memory SDK + runtime with a mock LLM and a typed ping consumer.
    #[wasm_bindgen]
    pub async fn create() -> Result<StasisWasmClient, JsValue> {
        let sdk = StasisSdk::new(
            InMemoryAgentRepository::default(),
            MockLlmGateway::new("stasis-wasm mock completion"),
        );
        let runtime = RuntimeSdk::from_builder(
            StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
                .without_prompt_handler()
                .without_tool_loop_handler()
                .without_agent_handlers()
                .without_grapheme_handlers()
                .without_memory_operation_handlers()
                .without_orchestration_pattern_handlers()
                .without_cluster_control_handlers(),
        )
        .await
        .map_err(js_err)?;
        runtime.register_consumer(PingConsumer).map_err(js_err)?;
        Ok(Self { sdk, runtime })
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

    #[wasm_bindgen]
    pub async fn job_state(&self, job_id: String) -> Result<String, JsValue> {
        let job = match self.runtime.runtime() {
            RuntimeComposition::InMemory(rt) => rt
                .job_store
                .get(&job_id)
                .await
                .map_err(js_err)?
                .ok_or_else(|| JsValue::from_str("job not found"))?,
            #[allow(unreachable_patterns)]
            _ => return Err(JsValue::from_str("expected in-memory runtime")),
        };
        Ok(match job.state {
            JobState::Enqueued => "enqueued".into(),
            JobState::Leased => "leased".into(),
            JobState::Running => "running".into(),
            JobState::Succeeded => "succeeded".into(),
            JobState::Failed => "failed".into(),
            JobState::DeadLetter => "dead_letter".into(),
            JobState::Canceled => "canceled".into(),
        })
    }
}

fn js_err(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}
