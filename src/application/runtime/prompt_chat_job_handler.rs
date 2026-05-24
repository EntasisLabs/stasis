use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    PromptJobPayload,
};
use crate::application::runtime::identity_context_compiler::{
    load_identity_context_summary, prepend_identity_snapshot,
};
use crate::application::runtime::memory_persistence_helpers::{
    SttpPromptNodeFormat, memory_query_fingerprint, memory_query_id, memory_scope_hash,
    render_prompt_response_sttp_node, resolve_sttp_output_node_id, should_store,
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
use crate::ports::outbound::memory::memory_models::MemoryStoreRequest;

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
            input_memory_query_id = Some(memory_query_id(&job.correlation_id, &recall_request));
            input_memory_query_fingerprint = Some(memory_query_fingerprint(&recall_request));

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
        if should_store(memory_policy)
            && let Some(writer) = &self.memory_writer
        {
            let store_request = MemoryStoreRequest {
                session_id: job.correlation_id.clone(),
                raw_node: render_prompt_response_sttp_node(
                    &job.correlation_id,
                    &user_prompt,
                    &response.text,
                    SttpPromptNodeFormat::UntaggedNoSchema,
                ),
            };

            match writer.store_context(&store_request).await {
                Ok(stored) => memory_store = Some(stored),
                Err(err) => memory_store_error = Some(err.to_string()),
            }
        }

        let sttp_output_node_id =
            resolve_sttp_output_node_id(memory_store.as_ref(), format!("sttp:prompt:{}", job.id));
        let memory_scope_hash = memory_scope_hash(&job.correlation_id, memory_policy);

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
