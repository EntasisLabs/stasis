use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};

use crate::application::orchestration::runtime_job_payloads::{
    AgentToolCallMode, ConcurrentBranchExecutionMode, ConcurrentBranchJobPayload,
    ConcurrentPatternJobPayload, MemoryPolicyPayload,
};
use crate::application::orchestration::concurrent_pattern_pipeline::{
    ConcurrentPatternBranch, ConcurrentPatternExecutionRequest, ConcurrentPatternPipeline,
};
use crate::application::orchestration::prompt_pipeline::PromptExecutionPipeline;
use crate::application::orchestration::tool_loop_pipeline::ToolCallMode;
use crate::application::orchestration::tool_registry::ToolRegistry;
use crate::application::runtime::chat_options_resolver::validate_reasoning_effort;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::domain::runtime::thread::{NewThread, NewThreadEvent, ThreadMergeMetadata};
use crate::ports::outbound::ai_chat_client::AiChatClient;
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use crate::ports::outbound::runtime::thread_store::ThreadStore;

pub struct ConcurrentPatternJobHandler {
    pipeline: ConcurrentPatternPipeline,
    thread_store: Option<Arc<dyn ThreadStore>>,
}

impl ConcurrentPatternJobHandler {
    pub fn new(chat_client: Arc<dyn AiChatClient>, tool_registry: Arc<dyn ToolRegistry>) -> Self {
        Self::new_with_thread_store_and_memory(
            chat_client,
            tool_registry,
            None,
            None,
            None,
            None,
        )
    }

    pub fn new_with_thread_store(
        chat_client: Arc<dyn AiChatClient>,
        tool_registry: Arc<dyn ToolRegistry>,
        thread_store: Option<Arc<dyn ThreadStore>>,
    ) -> Self {
        Self::new_with_thread_store_and_memory(
            chat_client,
            tool_registry,
            thread_store,
            None,
            None,
            None,
        )
    }

    pub fn new_with_thread_store_and_memory(
        chat_client: Arc<dyn AiChatClient>,
        tool_registry: Arc<dyn ToolRegistry>,
        thread_store: Option<Arc<dyn ThreadStore>>,
        memory_reader: Option<Arc<dyn MemoryContextReader>>,
        memory_writer: Option<Arc<dyn MemoryContextWriter>>,
        identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
    ) -> Self {
        let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
        Self {
            pipeline: ConcurrentPatternPipeline::new_with_tool_loop(
                prompt_pipeline,
                tool_registry,
                memory_reader,
                memory_writer,
                identity_memory_store,
            ),
            thread_store,
        }
    }

    async fn ensure_thread(
        &self,
        thread_id: &str,
        parent_thread_id: Option<String>,
        branch_label: Option<String>,
        now: chrono::DateTime<Utc>,
    ) {
        let Some(store) = &self.thread_store else {
            return;
        };

        let exists = store.get_thread(thread_id).await.ok().flatten().is_some();
        if exists {
            return;
        }

        let _ = store
            .create_thread(NewThread {
                thread_id: thread_id.to_string(),
                parent_thread_id,
                branch_label,
                created_at: now,
            })
            .await;
    }

    async fn append_thread_event(
        &self,
        event_id: String,
        thread_id: &str,
        event_kind: &str,
        payload_ref: String,
        occurred_at: chrono::DateTime<Utc>,
    ) {
        let Some(store) = &self.thread_store else {
            return;
        };

        let _ = store
            .append_event(NewThreadEvent {
                event_id,
                thread_id: thread_id.to_string(),
                event_kind: event_kind.to_string(),
                payload_ref,
                occurred_at,
            })
            .await;
    }

    fn parse_payload(raw: &str) -> std::result::Result<ConcurrentPatternJobPayload, String> {
        let payload: ConcurrentPatternJobPayload = serde_json::from_str(raw).map_err(|err| {
            format!("policy violation: invalid concurrent-pattern payload json: {err}")
        })?;

        if payload.initial_user_prompt.trim().is_empty() {
            return Err(
                "policy violation: concurrent-pattern payload.initial_user_prompt must be non-empty"
                    .to_string(),
            );
        }
        if payload.branches.is_empty() {
            return Err(
                "policy violation: concurrent-pattern payload.branches must include at least one branch"
                    .to_string(),
            );
        }

        for branch in &payload.branches {
            Self::validate_branch(branch)?;
        }

        validate_reasoning_effort(payload.reasoning_effort.as_deref())
            .map_err(|err| format!("policy violation: {err}"))?;

        Ok(payload)
    }

    fn validate_branch(branch: &ConcurrentBranchJobPayload) -> std::result::Result<(), String> {
        if branch.branch_id.trim().is_empty() {
            return Err(
                "policy violation: concurrent-pattern payload.branches[].branch_id must be non-empty"
                    .to_string(),
            );
        }
        if branch.user_prompt_template.trim().is_empty() {
            return Err(
                "policy violation: concurrent-pattern payload.branches[].user_prompt_template must be non-empty"
                    .to_string(),
            );
        }
        if branch.execution_mode == ConcurrentBranchExecutionMode::ToolLoop {
            let tool_name = branch.tool_name.as_deref().unwrap_or_default().trim();
            if tool_name.is_empty() {
                return Err(
                    "policy violation: concurrent-pattern payload.branches[].tool_name must be non-empty when execution_mode is tool_loop"
                        .to_string(),
                );
            }
        }

        validate_reasoning_effort(branch.reasoning_effort.as_deref())
            .map_err(|err| format!("policy violation: {err}"))?;

        Ok(())
    }

    fn resolve_tool_call_mode(
        branch_mode: Option<AgentToolCallMode>,
        default_mode: Option<AgentToolCallMode>,
    ) -> ToolCallMode {
        match branch_mode.or(default_mode) {
            Some(AgentToolCallMode::Strict) => ToolCallMode::Strict,
            _ => ToolCallMode::Auto,
        }
    }

    fn resolve_memory_policy(
        branch_policy: Option<MemoryPolicyPayload>,
        default_policy: Option<MemoryPolicyPayload>,
    ) -> Option<MemoryPolicyPayload> {
        branch_policy.or(default_policy)
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "stasis-orchestration-concurrent",
            "status": "failure",
            "pattern": "concurrent",
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
impl JobHandler for ConcurrentPatternJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.orchestration.concurrent"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let ConcurrentPatternJobPayload {
            thread_id,
            initial_user_prompt,
            policy_profile,
            model_hint,
            reasoning_effort,
            merge_strategy,
            tool_call_mode,
            memory_policy,
            branches,
        } = payload;

        let pattern_tool_call_mode = tool_call_mode;
        let pattern_memory_policy = memory_policy;

        let now = Utc::now();
        let thread_id = thread_id.unwrap_or_else(|| job.correlation_id.clone());
        self.ensure_thread(&thread_id, None, Some("concurrent".to_string()), now)
            .await;
        self.append_thread_event(
            format!("{}:concurrent:start", job.id),
            &thread_id,
            "orchestration.concurrent.started",
            initial_user_prompt.clone(),
            now,
        )
        .await;

        let request = ConcurrentPatternExecutionRequest {
            initial_user_prompt,
            trace_id: Some(job.trace_id.clone()),
            correlation_id: Some(job.correlation_id.clone()),
            policy_profile,
            model_hint,
            reasoning_effort,
            default_memory_policy: pattern_memory_policy.clone(),
            merge_strategy,
            branches: branches
                .into_iter()
                .map(|branch| {
                    let ConcurrentBranchJobPayload {
                        branch_id,
                        user_prompt_template,
                        system_prompt,
                        policy_profile,
                        model_hint,
                        reasoning_effort,
                        execution_mode,
                        tool_name,
                        tool_input,
                        tool_call_mode,
                        memory_policy,
                    } = branch;

                    ConcurrentPatternBranch {
                        branch_id,
                        user_prompt_template,
                        system_prompt,
                        policy_profile,
                        model_hint,
                        reasoning_effort,
                        execution_mode,
                        tool_name,
                        tool_input,
                        tool_call_mode: Self::resolve_tool_call_mode(
                            tool_call_mode,
                            pattern_tool_call_mode.clone(),
                        ),
                        memory_policy: Self::resolve_memory_policy(
                            memory_policy,
                            pattern_memory_policy.clone(),
                        ),
                    }
                })
                .collect(),
        };

        let response = match self.pipeline.execute(request).await {
            Ok(response) => response,
            Err(err) => {
                let error = err.to_string();
                return Ok(JobExecutionOutcome::FatalFailure {
                    message: error.clone(),
                    execution_id: None,
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-orchestration-concurrent",
                            "status": "failure",
                            "pattern": "concurrent",
                            "error": error,
                        })
                        .to_string(),
                    ),
                });
            }
        };

        let branch_ids: Vec<String> = response
            .branches
            .iter()
            .map(|branch| branch.branch_id.clone())
            .collect();
        let tool_loop_branch_count = response
            .branches
            .iter()
            .filter(|branch| branch.execution_mode == ConcurrentBranchExecutionMode::ToolLoop)
            .count();
        let prompt_branch_count = response.branches.len() - tool_loop_branch_count;
        let branch_summaries: Vec<Value> = response
            .branches
            .iter()
            .map(|branch| {
                json!({
                    "branch_id": branch.branch_id,
                    "execution_mode": match branch.execution_mode {
                        ConcurrentBranchExecutionMode::Prompt => "prompt",
                        ConcurrentBranchExecutionMode::ToolLoop => "tool_loop",
                    },
                    "rounds_executed": branch.rounds_executed,
                    "tool_invocation_count": branch.tool_invocations.len(),
                    "branch_termination_reason": branch.branch_termination_reason,
                    "memory_retrieved_count": branch.memory_retrieved_count,
                    "memory_store_node_id": branch.memory_store_node_id,
                    "input_memory_query_id": branch.input_memory_query_id,
                    "input_memory_query_fingerprint": branch.input_memory_query_fingerprint,
                    "memory_recall_error": branch.memory_recall_error,
                    "memory_store_error": branch.memory_store_error,
                })
            })
            .collect();

        let mut branch_thread_ids = Vec::new();
        for branch in &response.branches {
            let branch_thread_id = format!("{}::branch::{}", thread_id, branch.branch_id);
            let branch_now = Utc::now();
            self.ensure_thread(
                &branch_thread_id,
                Some(thread_id.clone()),
                Some(branch.branch_id.clone()),
                branch_now,
            )
            .await;
            self.append_thread_event(
                format!("{}:concurrent:branch:{}", job.id, branch.branch_id),
                &branch_thread_id,
                "orchestration.concurrent.branch.completed",
                branch.output_text.clone(),
                branch_now,
            )
            .await;
            branch_thread_ids.push(branch_thread_id);
        }

        let merge_metadata = ThreadMergeMetadata {
            parent_thread_id: thread_id.clone(),
            branch_thread_ids: branch_thread_ids.clone(),
            merge_strategy: response.merge_strategy.clone(),
            merged_at: Utc::now(),
        };
        let merge_payload_ref =
            serde_json::to_string(&merge_metadata).unwrap_or_else(|_| response.final_text.clone());

        self.append_thread_event(
            format!("{}:concurrent:completed", job.id),
            &thread_id,
            "orchestration.concurrent.completed",
            merge_payload_ref,
            Utc::now(),
        )
        .await;

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: format!("sttp:orchestration:concurrent:{}", job.id),
            execution_id: None,
            diagnostics: Some(
                json!({
                    "provider": "stasis-orchestration-concurrent",
                    "status": "success",
                    "pattern": "concurrent",
                    "branches_executed": response.branches.len(),
                    "prompt_branch_count": prompt_branch_count,
                    "tool_loop_branch_count": tool_loop_branch_count,
                    "branch_summaries": branch_summaries,
                    "branch_ids": branch_ids,
                    "thread_id": thread_id,
                    "branch_thread_ids": branch_thread_ids,
                    "thread_merge": merge_metadata,
                    "merge_strategy": response.merge_strategy,
                    "final_text": response.final_text,
                    "termination_reason": response.termination_reason,
                })
                .to_string(),
            ),
        })
    }
}
