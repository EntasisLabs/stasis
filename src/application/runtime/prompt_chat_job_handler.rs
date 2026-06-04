use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::{
    PromptJobPayload,
};
use crate::application::runtime::identity_context_compiler::{
    load_identity_context_summary, prepend_identity_snapshot,
};
use crate::application::runtime::memory_recall_context_compiler::prepend_memory_recall_context;
use crate::application::runtime::memory_persistence_helpers::{
    SttpPromptNodeFormat, memory_query_fingerprint, memory_query_id, memory_scope_hash,
    render_prompt_response_sttp_node, resolve_sttp_output_node_id, should_store,
};
use crate::application::runtime::memory_recall_request_builder::build_memory_recall_request;
use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::runtime_diagnostics_helpers::{
    build_runtime_failure_identity_context_section, build_runtime_failure_memory_recall_section,
    build_runtime_memory_diagnostics_bundle, RuntimeIdentityDiagnosticsInput,
    RuntimeMemoryRecallDiagnosticsInput, RuntimeMemoryStoreDiagnosticsInput,
};
use crate::application::runtime::runtime_handler_execution_context::RuntimeHandlerExecutionContext;
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
            "policy_reason": &message,
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

        let execution_context = RuntimeHandlerExecutionContext::new(
            job,
            payload.policy_profile.clone(),
            payload.model_hint.clone(),
            self.memory_reader.is_some(),
            self.memory_writer.is_some(),
            self.identity_memory_store.is_some(),
        );

        let mut memory_recall = None;
        let mut memory_recall_error = None;
        let mut input_memory_query_id = None;
        let mut input_memory_query_fingerprint = None;

        let memory_policy = payload.memory_policy.as_ref();
        let (identity_summary, identity_error) = load_identity_context_summary(
            self.identity_memory_store.as_ref(),
            execution_context.correlation_id(),
            execution_context.policy_profile(),
        )
        .await;

        let mut effective_user_prompt =
            prepend_identity_snapshot(&payload.user_prompt, identity_summary.as_deref());

        if let Some(reader) = &self.memory_reader {
            let recall_request = build_memory_recall_request(
                execution_context.correlation_id(),
                Some(&effective_user_prompt),
                memory_policy,
            );
            input_memory_query_id = Some(memory_query_id(
                execution_context.correlation_id(),
                &recall_request,
            ));
            input_memory_query_fingerprint = Some(memory_query_fingerprint(&recall_request));

            match reader.recall(&recall_request).await {
                Ok(response) => {
                    effective_user_prompt = prepend_memory_recall_context(&effective_user_prompt, &response);
                    memory_recall = Some(response);
                }
                Err(err) => memory_recall_error = Some(err.to_string()),
            }
        }

        let context = execution_context.prompt_context_clone();

        let user_prompt = effective_user_prompt;
        let mut request = PromptExecutionRequest::from_user_prompt(user_prompt.clone())
            .with_context(context.clone());
        if let Some(system_prompt) = payload.system_prompt {
            request = request.with_system_prompt(system_prompt);
        }

        let response = match self.pipeline.execute(request).await {
            Ok(response) => response,
            Err(err) => {
                let error = err.to_string();
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: error.clone(),
                    execution_id: None,
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-pipeline",
                            "status": "failure",
                            "error": error,
                            "policy_profile": execution_context.policy_profile_clone(),
                            "model_hint": execution_context.model_hint_clone(),
                            "memory_recall": build_runtime_failure_memory_recall_section(
                                execution_context.memory_reader_enabled(),
                                memory_recall_error,
                            ),
                            "identity_context": build_runtime_failure_identity_context_section(
                                execution_context.identity_enabled(),
                                identity_summary,
                                identity_error,
                            ),
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
                session_id: execution_context.correlation_id().to_string(),
                raw_node: render_prompt_response_sttp_node(
                    execution_context.correlation_id(),
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
        let memory_scope_hash = memory_scope_hash(execution_context.correlation_id(), memory_policy);
        let input_memory_query_id_for_top_level = input_memory_query_id.clone();
        let input_memory_query_fingerprint_for_top_level =
            input_memory_query_fingerprint.clone();
        let diagnostics_bundle = build_runtime_memory_diagnostics_bundle(
            RuntimeMemoryRecallDiagnosticsInput {
                attempted: execution_context.memory_reader_enabled(),
                response: memory_recall,
                query_id: input_memory_query_id,
                query_fingerprint: input_memory_query_fingerprint,
                error: memory_recall_error,
            },
            RuntimeMemoryStoreDiagnosticsInput {
                attempted: execution_context.memory_writer_enabled(),
                response: memory_store,
                error: memory_store_error,
            },
            RuntimeIdentityDiagnosticsInput {
                attempted: execution_context.identity_enabled(),
                summary: identity_summary,
                error: identity_error,
            },
        );

        let diagnostics = json!({
            "provider": "stasis-pipeline",
            "status": "success",
            "trace_id": context.trace_id,
            "correlation_id": context.correlation_id,
            "policy_profile": response.metadata.policy_profile,
            "model_hint": response.metadata.model_hint,
            "output_text": response.text,
            "output_preview": response.text.chars().take(160).collect::<String>(),
            "memory_retrieved_count": diagnostics_bundle.retrieved_count,
            "memory_retrieval_path": diagnostics_bundle.retrieval_path,
            "memory_fallback_triggered": diagnostics_bundle.fallback_triggered,
            "memory_fallback_reason": diagnostics_bundle.fallback_reason,
            "memory_scope_hash": memory_scope_hash,
            "memory_store_valid": diagnostics_bundle.store_valid,
            "memory_store_node_id": diagnostics_bundle.store_node_id,
            "input_memory_query_id": input_memory_query_id_for_top_level,
            "input_memory_query_fingerprint": input_memory_query_fingerprint_for_top_level,
            "output_memory_node_id": diagnostics_bundle.store_node_id,
            "memory_recall": diagnostics_bundle.memory_recall,
            "memory_store": diagnostics_bundle.memory_store,
            "identity_context": diagnostics_bundle.identity_context,
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id,
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}
