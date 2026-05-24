use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    MemoryPolicyPayload, MemoryStoreModePayload, PromptJobPayload,
};
use crate::application::runtime::identity_context_compiler::{
    load_identity_context_summary, prepend_identity_snapshot,
};
use crate::application::runtime::memory_recall_request_builder::build_memory_recall_request;
use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use crate::ports::outbound::memory::memory_models::{MemoryRecallRequest, MemoryStoreRequest};

pub struct PromptChatJobHandler {
    pipeline: PromptExecutionPipeline,
    memory_reader: Option<Arc<dyn MemoryContextReader>>,
    memory_writer: Option<Arc<dyn MemoryContextWriter>>,
    identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
}

impl PromptChatJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>) -> Self {
        Self::new_with_memory_and_identity(chat_client, None, None, None)
    }

    pub fn new_with_memory(
        chat_client: Arc<dyn AiChatClient>,
        memory_reader: Option<Arc<dyn MemoryContextReader>>,
        memory_writer: Option<Arc<dyn MemoryContextWriter>>,
    ) -> Self {
        Self::new_with_memory_and_identity(chat_client, memory_reader, memory_writer, None)
    }

    pub fn new_with_memory_and_identity(
        chat_client: Arc<dyn AiChatClient>,
        memory_reader: Option<Arc<dyn MemoryContextReader>>,
        memory_writer: Option<Arc<dyn MemoryContextWriter>>,
        identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
    ) -> Self {
        Self {
            pipeline: PromptExecutionPipeline::new(chat_client),
            memory_reader,
            memory_writer,
            identity_memory_store,
        }
    }

    fn parse_payload(raw: &str) -> std::result::Result<PromptJobPayload, String> {
        let payload: PromptJobPayload = serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid prompt job payload json: {err}"))?;

        if payload.user_prompt.trim().is_empty() {
            return Err(
                "policy violation: prompt payload.user_prompt must be non-empty".to_string(),
            );
        }

        Ok(payload)
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "stasis-pipeline",
            "status": "failure",
            "guardrail_code": "POLICY_VIOLATION",
            "policy_reason": message.clone(),
        })
        .to_string();

        JobExecutionOutcome::FatalFailure {
            message,
            execution_id: None,
            diagnostics: Some(diagnostics),
        }
    }

    fn render_sttp_node(session_id: &str, user_prompt: &str, output_text: &str) -> String {
        let escaped_summary = output_text.replace('"', "\\\"");
        let escaped_prompt = user_prompt.replace('"', "\\\"");

        // STTP persistence uses the typed IR form expected by Locus validators.
        format!(
            "⊕⟨ {{ trigger: manual, response_format: temporal_node, origin_session: \"{session_id}\", compression_depth: 1, parent_node: null, prime: {{ attractor_config: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75 }}, context_summary: \"{escaped_summary}\", relevant_tier: raw, retrieval_budget: 10 }} }} ⟩\n\
⦿⟨ {{ timestamp: \"{}\", tier: raw, session_id: \"{session_id}\", user_avec: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75, psi: 2.60 }}, model_avec: {{ stability: 0.82, friction: 0.18, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩\n\
◈⟨ {{ prompt(.88): \"{escaped_prompt}\", response(.95): \"{escaped_summary}\" }} ⟩\n\
⍉⟨ {{ rho: 0.96, kappa: 0.94, psi: 2.60, compression_avec: {{ stability: 0.81, friction: 0.19, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩",
            chrono::Utc::now().to_rfc3339(),
        )
    }

    fn should_store(memory_policy: Option<&MemoryPolicyPayload>) -> bool {
        !matches!(
            memory_policy.and_then(|policy| policy.store_mode.clone()),
            Some(MemoryStoreModePayload::Disabled)
        )
    }

    fn memory_scope_hash(
        correlation_id: &str,
        memory_policy: Option<&MemoryPolicyPayload>,
    ) -> String {
        let basis = format!(
            "corr={correlation_id}|sessions={:?}|tiers={:?}|from={:?}|to={:?}",
            memory_policy.and_then(|policy| policy.session_ids.clone()),
            memory_policy.and_then(|policy| policy.tiers.clone()),
            memory_policy.and_then(|policy| policy.from_utc),
            memory_policy.and_then(|policy| policy.to_utc),
        );
        let mut hasher = DefaultHasher::new();
        basis.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    fn memory_query_id(correlation_id: &str, request: &MemoryRecallRequest) -> String {
        let basis = format!(
            "corr={correlation_id}|query={:?}|sessions={:?}|tiers={:?}|from={:?}|to={:?}|limit={}|alpha={}|beta={}|fallback={:?}|strictness={:?}|include_explain={}",
            request.query_text,
            request.scope.session_ids,
            request.scope.tiers,
            request.scope.from_utc,
            request.scope.to_utc,
            request.limit,
            request.alpha,
            request.beta,
            request.fallback_policy,
            request.strictness,
            request.include_explain,
        );
        let mut hasher = DefaultHasher::new();
        basis.hash(&mut hasher);
        format!("mq:{:x}", hasher.finish())
    }

    fn memory_query_fingerprint(request: &MemoryRecallRequest) -> String {
        format!(
            "sessions={:?}|tiers={:?}|from={:?}|to={:?}|limit={}|alpha={}|beta={}|fallback={:?}|strictness={:?}|include_explain={}",
            request.scope.session_ids,
            request.scope.tiers,
            request.scope.from_utc,
            request.scope.to_utc,
            request.limit,
            request.alpha,
            request.beta,
            request.fallback_policy,
            request.strictness,
            request.include_explain,
        )
    }
}

#[async_trait]
impl JobHandler for PromptChatJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.prompt"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let mut memory_recall = None;
        let mut memory_recall_error = None;
        let mut input_memory_query_id = None;
        let mut input_memory_query_fingerprint = None;

        let memory_policy = payload.memory_policy.as_ref();
        let (identity_summary, identity_error) = load_identity_context_summary(
            self.identity_memory_store.as_ref(),
            &job.correlation_id,
            payload.policy_profile.as_deref(),
        )
        .await;

        let effective_user_prompt =
            prepend_identity_snapshot(&payload.user_prompt, identity_summary.as_deref());

        if let Some(reader) = &self.memory_reader {
            let recall_request = build_memory_recall_request(
                &job.correlation_id,
                Some(&effective_user_prompt),
                memory_policy,
            );
            input_memory_query_id =
                Some(Self::memory_query_id(&job.correlation_id, &recall_request));
            input_memory_query_fingerprint = Some(Self::memory_query_fingerprint(&recall_request));

            match reader.recall(&recall_request).await {
                Ok(response) => memory_recall = Some(response),
                Err(err) => memory_recall_error = Some(err.to_string()),
            }
        }

        let context = PromptExecutionContext {
            trace_id: Some(job.trace_id.clone()),
            correlation_id: Some(job.correlation_id.clone()),
            policy_profile: payload.policy_profile.clone(),
            model_hint: payload.model_hint.clone(),
        };

        let user_prompt = effective_user_prompt;
        let mut request = PromptExecutionRequest::from_user_prompt(user_prompt.clone())
            .with_context(context.clone());
        if let Some(system_prompt) = payload.system_prompt {
            request = request.with_system_prompt(system_prompt);
        }

        let response = match self.pipeline.execute(request).await {
            Ok(response) => response,
            Err(err) => {
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: err.to_string(),
                    execution_id: None,
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-pipeline",
                            "status": "failure",
                            "error": err.to_string(),
                            "policy_profile": context.policy_profile,
                            "model_hint": context.model_hint,
                            "memory_recall": {
                                "attempted": self.memory_reader.is_some(),
                                "error": memory_recall_error,
                            },
                            "identity_context": {
                                "attempted": self.identity_memory_store.is_some(),
                                "summary": identity_summary,
                                "error": identity_error,
                            },
                        })
                        .to_string(),
                    ),
                });
            }
        };

        let mut memory_store = None;
        let mut memory_store_error = None;
        if Self::should_store(memory_policy) {
            if let Some(writer) = &self.memory_writer {
                let store_request = MemoryStoreRequest {
                    session_id: job.correlation_id.clone(),
                    raw_node: Self::render_sttp_node(
                        &job.correlation_id,
                        &user_prompt,
                        &response.text,
                    ),
                };

                match writer.store_context(&store_request).await {
                    Ok(stored) => memory_store = Some(stored),
                    Err(err) => memory_store_error = Some(err.to_string()),
                }
            }
        }

        let sttp_output_node_id = memory_store
            .as_ref()
            .map(|stored| stored.node_id.clone())
            .filter(|node_id| !node_id.trim().is_empty())
            .unwrap_or_else(|| format!("sttp:prompt:{}", job.id));
        let memory_scope_hash = Self::memory_scope_hash(&job.correlation_id, memory_policy);

        let diagnostics = json!({
            "provider": "stasis-pipeline",
            "status": "success",
            "trace_id": context.trace_id,
            "correlation_id": context.correlation_id,
            "policy_profile": response.metadata.policy_profile,
            "model_hint": response.metadata.model_hint,
            "output_text": response.text,
            "output_preview": response.text.chars().take(160).collect::<String>(),
            "memory_retrieved_count": memory_recall.as_ref().map(|value| value.retrieved).unwrap_or_default(),
            "memory_retrieval_path": memory_recall.as_ref().and_then(|value| value.retrieval_path.clone()),
            "memory_fallback_triggered": memory_recall.as_ref().map(|value| value.fallback_triggered).unwrap_or(false),
            "memory_fallback_reason": memory_recall.as_ref().and_then(|value| value.fallback_reason.clone()),
            "memory_scope_hash": memory_scope_hash,
            "memory_store_valid": memory_store.as_ref().map(|value| value.valid).unwrap_or(false),
            "memory_store_node_id": memory_store.as_ref().map(|value| value.node_id.clone()),
            "input_memory_query_id": input_memory_query_id.clone(),
            "input_memory_query_fingerprint": input_memory_query_fingerprint.clone(),
            "output_memory_node_id": memory_store.as_ref().map(|value| value.node_id.clone()),
            "memory_recall": {
                "attempted": self.memory_reader.is_some(),
                "query_id": input_memory_query_id,
                "query_fingerprint": input_memory_query_fingerprint,
                "retrieved": memory_recall.as_ref().map(|value| value.retrieved).unwrap_or_default(),
                "retrieval_path": memory_recall.as_ref().and_then(|value| value.retrieval_path.clone()),
                "fallback_triggered": memory_recall.as_ref().map(|value| value.fallback_triggered).unwrap_or(false),
                "fallback_reason": memory_recall.as_ref().and_then(|value| value.fallback_reason.clone()),
                "error": memory_recall_error,
            },
            "memory_store": {
                "attempted": self.memory_writer.is_some(),
                "node_id": memory_store.as_ref().map(|value| value.node_id.clone()),
                "valid": memory_store.as_ref().map(|value| value.valid).unwrap_or(false),
                "error": memory_store_error,
            },
            "identity_context": {
                "attempted": self.identity_memory_store.is_some(),
                "summary": identity_summary,
                "error": identity_error,
            },
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id,
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}
