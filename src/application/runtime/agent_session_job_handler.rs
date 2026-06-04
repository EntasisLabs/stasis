use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentToolCallMode,
};
use crate::application::runtime::identity_context_compiler::{
    load_identity_context_summary, prepend_identity_snapshot,
};
use crate::application::runtime::memory_recall_context_compiler::prepend_memory_recall_context;
use crate::application::runtime::memory_persistence_helpers::{
    memory_query_fingerprint, memory_query_id, memory_scope_hash, render_session_summary_sttp_node,
    resolve_sttp_output_node_id, should_store,
};
use crate::application::runtime::memory_recall_request_builder::build_memory_recall_request;
use crate::application::orchestration::agent_session_pipeline::{
    AgentParticipant, AgentSessionCoordinator, AgentSessionPipeline, AgentSessionRunRequest,
    AgentTurnExecutionPolicy, MaxTurnsTerminationStrategy, RoundRobinSelectionStrategy,
};
use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionPipeline,
};
use crate::application::orchestration::tool_loop_pipeline::{ToolCallMode, ToolLoopPipeline};
use crate::application::orchestration::tool_registry::ToolRegistry;
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

const DEFAULT_MAX_SESSION_TURNS: usize = 3;

pub struct AgentSessionJobHandler {
    pipeline: AgentSessionPipeline,
    memory_reader: Option<Arc<dyn MemoryContextReader>>,
    memory_writer: Option<Arc<dyn MemoryContextWriter>>,
    identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
}

impl AgentSessionJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>, tool_registry: Arc<dyn ToolRegistry>) -> Self {
        Self::new_with_memory_and_identity(chat_client, tool_registry, None, None, None)
    }

    pub fn new_with_memory(
        chat_client: Arc<dyn AiChatClient>,
        tool_registry: Arc<dyn ToolRegistry>,
        memory_reader: Option<Arc<dyn MemoryContextReader>>,
        memory_writer: Option<Arc<dyn MemoryContextWriter>>,
    ) -> Self {
        Self::new_with_memory_and_identity(
            chat_client,
            tool_registry,
            memory_reader,
            memory_writer,
            None,
        )
    }

    pub fn new_with_memory_and_identity(
        chat_client: Arc<dyn AiChatClient>,
        tool_registry: Arc<dyn ToolRegistry>,
        memory_reader: Option<Arc<dyn MemoryContextReader>>,
        memory_writer: Option<Arc<dyn MemoryContextWriter>>,
        identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
    ) -> Self {
        let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
        let tool_loop_pipeline = ToolLoopPipeline::new(prompt_pipeline, tool_registry);
        Self {
            pipeline: AgentSessionPipeline::new(tool_loop_pipeline),
            memory_reader,
            memory_writer,
            identity_memory_store,
        }
    }

    fn parse_payload(raw: &str) -> std::result::Result<AgentSessionJobPayload, String> {
        let payload: AgentSessionJobPayload = serde_json::from_str(raw).map_err(|err| {
            format!("policy violation: invalid agent-session payload json: {err}")
        })?;

        if payload.initial_user_prompt.trim().is_empty() {
            return Err(
                "policy violation: agent-session payload.initial_user_prompt must be non-empty"
                    .to_string(),
            );
        }
        if payload.participants.is_empty() {
            return Err(
                "policy violation: agent-session payload.participants must be non-empty"
                    .to_string(),
            );
        }

        for participant in &payload.participants {
            if participant.agent_id.trim().is_empty() {
                return Err(
                    "policy violation: agent-session participant.agent_id must be non-empty"
                        .to_string(),
                );
            }
            if participant.tool_name.trim().is_empty() {
                return Err(
                    "policy violation: agent-session participant.tool_name must be non-empty"
                        .to_string(),
                );
            }
        }

        Ok(payload)
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "stasis-agent-session",
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
impl JobHandler for AgentSessionJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.agent_session"
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

        let memory_policy = payload.memory_policy.as_ref();
        let (identity_summary, identity_error) = load_identity_context_summary(
            self.identity_memory_store.as_ref(),
            execution_context.correlation_id(),
            execution_context.policy_profile(),
        )
        .await;
        let mut effective_initial_user_prompt =
            prepend_identity_snapshot(&payload.initial_user_prompt, identity_summary.as_deref());

        let mut memory_recall = None;
        let mut memory_recall_error = None;
        let mut input_memory_query_id = None;
        let mut input_memory_query_fingerprint = None;
        if let Some(reader) = &self.memory_reader {
            let recall_request = build_memory_recall_request(
                execution_context.correlation_id(),
                Some(&effective_initial_user_prompt),
                memory_policy,
            );
            input_memory_query_id = Some(memory_query_id(
                execution_context.correlation_id(),
                &recall_request,
            ));
            input_memory_query_fingerprint = Some(memory_query_fingerprint(&recall_request));

            match reader.recall(&recall_request).await {
                Ok(response) => {
                    effective_initial_user_prompt =
                        prepend_memory_recall_context(&effective_initial_user_prompt, &response);
                    memory_recall = Some(response);
                }
                Err(err) => memory_recall_error = Some(err.to_string()),
            }
        }

        let context = execution_context.prompt_context_clone();

        let participants: Vec<AgentParticipant> = payload
            .participants
            .into_iter()
            .map(|participant| AgentParticipant {
                agent_id: participant.agent_id,
                system_prompt: participant.system_prompt,
                tool_name: participant.tool_name,
                tool_input: participant.tool_input.unwrap_or(Value::Null),
            })
            .collect();

        let coordinator = AgentSessionCoordinator::new(
            self.pipeline.clone(),
            Arc::new(RoundRobinSelectionStrategy::new()),
            Arc::new(MaxTurnsTerminationStrategy::new(
                payload.max_turns.unwrap_or(DEFAULT_MAX_SESSION_TURNS),
            )),
        );

        let run_request = AgentSessionRunRequest {
            thread_id: payload.thread_id,
            initial_user_prompt: effective_initial_user_prompt,
            participants,
            context,
            max_turns_cap: payload.max_turns.unwrap_or(DEFAULT_MAX_SESSION_TURNS),
            policy: AgentTurnExecutionPolicy {
                tool_call_mode: match payload.tool_call_mode {
                    Some(AgentToolCallMode::Strict) => ToolCallMode::Strict,
                    _ => ToolCallMode::Auto,
                },
            },
        };

        let response = match coordinator.run_session(run_request).await {
            Ok(response) => response,
            Err(err) => {
                let error_text = err.to_string();
                let is_policy_violation = error_text.contains("policy violation");
                let diagnostics = if is_policy_violation {
                    json!({
                        "provider": "stasis-agent-session",
                        "status": "failure",
                        "guardrail_code": "POLICY_VIOLATION",
                        "policy_reason": error_text,
                    })
                    .to_string()
                } else {
                    json!({
                        "provider": "stasis-agent-session",
                        "status": "failure",
                        "error": error_text,
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
                    .to_string()
                };

                return Ok(JobExecutionOutcome::FatalFailure {
                    message: error_text,
                    execution_id: None,
                    diagnostics: Some(diagnostics),
                });
            }
        };

        let participant_ids: Vec<String> = response
            .turns
            .iter()
            .map(|turn| turn.agent_id.clone())
            .collect();

        let summary_text = response
            .turns
            .last()
            .map(|turn| turn.response_text.clone())
            .or_else(|| response.transcript.last().cloned())
            .unwrap_or_default();

        let mut memory_store = None;
        let mut memory_store_error = None;
        if should_store(memory_policy)
            && let Some(writer) = &self.memory_writer
        {
            let store_request = MemoryStoreRequest {
                session_id: execution_context.correlation_id().to_string(),
                raw_node: render_session_summary_sttp_node(
                    execution_context.correlation_id(),
                    &summary_text,
                ),
            };

            match writer.store_context(&store_request).await {
                Ok(stored) => memory_store = Some(stored),
                Err(err) => memory_store_error = Some(err.to_string()),
            }
        }

        let sttp_output_node_id = resolve_sttp_output_node_id(
            memory_store.as_ref(),
            format!("sttp:agent-session:{}", job.id),
        );
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
            "provider": "stasis-agent-session",
            "status": "success",
            "thread_id": response.thread_id,
            "turn_count": response.turns.len(),
            "terminated": response.terminated,
            "participant_ids": participant_ids,
            "turns": response.turns,
            "transcript_preview": response.transcript.last().cloned(),
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
