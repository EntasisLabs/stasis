use std::sync::Arc;

use serde_json::Value;

use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::application::orchestration::runtime_job_payloads::{
    ConcurrentBranchExecutionMode, MemoryPolicyPayload,
};
use crate::application::orchestration::tool_loop_pipeline::{
    ToolCallMode, ToolInvocation, ToolLoopExecutionRequest, ToolLoopPipeline,
};
use crate::application::orchestration::tool_registry::ToolRegistry;
use crate::application::runtime::concurrent_tool_branch_memory::{
    prepare_concurrent_tool_branch, store_concurrent_tool_branch_memory,
};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use tokio::task::JoinSet;

#[derive(Clone, Debug)]
pub struct ConcurrentPatternBranch {
    pub branch_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub execution_mode: ConcurrentBranchExecutionMode,
    pub tool_name: Option<String>,
    pub tool_input: Option<Value>,
    pub tool_call_mode: ToolCallMode,
    pub memory_policy: Option<MemoryPolicyPayload>,
}

#[derive(Clone, Debug)]
pub struct ConcurrentPatternExecutionRequest {
    pub initial_user_prompt: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub default_memory_policy: Option<MemoryPolicyPayload>,
    pub merge_strategy: Option<String>,
    pub branches: Vec<ConcurrentPatternBranch>,
}

#[derive(Clone, Debug)]
pub struct ConcurrentPatternBranchResult {
    pub branch_id: String,
    pub execution_mode: ConcurrentBranchExecutionMode,
    pub rendered_prompt: String,
    pub output_text: String,
    pub tool_name: Option<String>,
    pub tool_output: Option<Value>,
    pub tool_invocations: Vec<ToolInvocation>,
    pub rounds_executed: Option<usize>,
    pub branch_termination_reason: Option<String>,
    pub memory_retrieved_count: Option<usize>,
    pub memory_store_node_id: Option<String>,
    pub input_memory_query_id: Option<String>,
    pub input_memory_query_fingerprint: Option<String>,
    pub memory_recall_error: Option<String>,
    pub memory_store_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ConcurrentPatternExecutionResponse {
    pub final_text: String,
    pub branches: Vec<ConcurrentPatternBranchResult>,
    pub termination_reason: String,
    pub merge_strategy: String,
}

#[derive(Clone)]
pub struct ConcurrentPatternPipeline {
    prompt_pipeline: PromptExecutionPipeline,
    tool_loop_pipeline: Option<ToolLoopPipeline>,
    memory_reader: Option<Arc<dyn MemoryContextReader>>,
    memory_writer: Option<Arc<dyn MemoryContextWriter>>,
    identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
}

#[derive(Clone)]
struct ConcurrentSharedInputs {
    initial_input: Arc<str>,
    trace_id: Arc<Option<String>>,
    correlation_id: Arc<Option<String>>,
    default_policy_profile: Arc<Option<String>>,
    default_model_hint: Arc<Option<String>>,
    default_memory_policy: Arc<Option<MemoryPolicyPayload>>,
}

impl ConcurrentSharedInputs {
    fn build_context(
        &self,
        policy_profile: Option<String>,
        model_hint: Option<String>,
    ) -> PromptExecutionContext {
        PromptExecutionContext {
            trace_id: (*self.trace_id).clone(),
            correlation_id: (*self.correlation_id).clone(),
            policy_profile: policy_profile.or_else(|| (*self.default_policy_profile).clone()),
            model_hint: model_hint.or_else(|| (*self.default_model_hint).clone()),
        }
    }

    fn render_template(&self, template: &str) -> String {
        template
            .replace("{{input}}", &self.initial_input)
            .replace("{input}", &self.initial_input)
    }
}

impl ConcurrentPatternPipeline {
    pub fn new(prompt_pipeline: PromptExecutionPipeline) -> Self {
        Self {
            prompt_pipeline,
            tool_loop_pipeline: None,
            memory_reader: None,
            memory_writer: None,
            identity_memory_store: None,
        }
    }

    pub fn new_with_tool_loop(
        prompt_pipeline: PromptExecutionPipeline,
        tool_registry: Arc<dyn ToolRegistry>,
        memory_reader: Option<Arc<dyn MemoryContextReader>>,
        memory_writer: Option<Arc<dyn MemoryContextWriter>>,
        identity_memory_store: Option<Arc<dyn IdentityMemoryStore>>,
    ) -> Self {
        Self {
            tool_loop_pipeline: Some(ToolLoopPipeline::new(
                prompt_pipeline.clone(),
                tool_registry,
            )),
            prompt_pipeline,
            memory_reader,
            memory_writer,
            identity_memory_store,
        }
    }

    pub async fn execute(
        &self,
        request: ConcurrentPatternExecutionRequest,
    ) -> Result<ConcurrentPatternExecutionResponse> {
        let ConcurrentPatternExecutionRequest {
            initial_user_prompt,
            trace_id,
            correlation_id,
            policy_profile,
            model_hint,
            default_memory_policy,
            merge_strategy,
            branches,
        } = request;

        let merge_strategy = merge_strategy.unwrap_or_else(|| "join_with_headers".to_string());
        let shared_inputs = ConcurrentSharedInputs {
            initial_input: Arc::<str>::from(initial_user_prompt),
            trace_id: Arc::new(trace_id),
            correlation_id: Arc::new(correlation_id),
            default_policy_profile: Arc::new(policy_profile),
            default_model_hint: Arc::new(model_hint),
            default_memory_policy: Arc::new(default_memory_policy),
        };

        let mut join_set: JoinSet<Result<ConcurrentPatternBranchResult>> = JoinSet::new();

        for branch in branches {
            let prompt_pipeline = self.prompt_pipeline.clone();
            let tool_loop_pipeline = self.tool_loop_pipeline.clone();
            let memory_reader = self.memory_reader.clone();
            let memory_writer = self.memory_writer.clone();
            let identity_memory_store = self.identity_memory_store.clone();
            let shared_inputs = shared_inputs.clone();

            join_set.spawn(async move {
                let ConcurrentPatternBranch {
                    branch_id,
                    user_prompt_template,
                    system_prompt,
                    policy_profile,
                    model_hint,
                    execution_mode,
                    tool_name,
                    tool_input,
                    tool_call_mode,
                    memory_policy,
                } = branch;

                let rendered_prompt = shared_inputs.render_template(&user_prompt_template);
                let context = shared_inputs.build_context(policy_profile.clone(), model_hint);
                let correlation_id = shared_inputs
                    .correlation_id
                    .as_deref()
                    .map(str::to_string)
                    .unwrap_or_else(|| "unknown".to_string());
                let resolved_memory_policy = memory_policy
                    .or_else(|| (*shared_inputs.default_memory_policy).clone());
                let memory_policy_ref = resolved_memory_policy.as_ref();

                match execution_mode {
                    ConcurrentBranchExecutionMode::Prompt => {
                        let mut prompt_request =
                            PromptExecutionRequest::from_user_prompt(rendered_prompt.clone())
                                .with_context(context);
                        if let Some(system_prompt) = system_prompt {
                            prompt_request = prompt_request.with_system_prompt(system_prompt);
                        }

                        let response = prompt_pipeline.execute(prompt_request).await?;
                        Ok(ConcurrentPatternBranchResult {
                            branch_id,
                            execution_mode,
                            rendered_prompt,
                            output_text: response.text,
                            tool_name: None,
                            tool_output: None,
                            tool_invocations: Vec::new(),
                            rounds_executed: None,
                            branch_termination_reason: None,
                            memory_retrieved_count: None,
                            memory_store_node_id: None,
                            input_memory_query_id: None,
                            input_memory_query_fingerprint: None,
                            memory_recall_error: None,
                            memory_store_error: None,
                        })
                    }
                    ConcurrentBranchExecutionMode::ToolLoop => {
                        let Some(tool_loop_pipeline) = tool_loop_pipeline else {
                            return Err(StasisError::PortFailure(
                                "concurrent pattern tool_loop branch requires a tool registry"
                                    .to_string(),
                            ));
                        };

                        let tool_name = tool_name.unwrap_or_default();
                        let tool_input =
                            tool_input.unwrap_or_else(|| Value::Object(Default::default()));

                        let prepared = prepare_concurrent_tool_branch(
                            memory_reader.as_ref(),
                            identity_memory_store.as_ref(),
                            &correlation_id,
                            context.policy_profile.as_deref(),
                            &rendered_prompt,
                            memory_policy_ref,
                        )
                        .await;

                        let tool_loop_request = ToolLoopExecutionRequest {
                            user_prompt: prepared.user_prompt,
                            system_prompt,
                            context,
                            tool_name: tool_name.clone(),
                            tool_input,
                            tool_call_mode,
                        };

                        let response = tool_loop_pipeline.execute(tool_loop_request).await?;

                        let stored = store_concurrent_tool_branch_memory(
                            memory_writer.as_ref(),
                            &correlation_id,
                            &branch_id,
                            &response.tool_name,
                            &response.text,
                            memory_policy_ref,
                        )
                        .await;

                        Ok(ConcurrentPatternBranchResult {
                            branch_id,
                            execution_mode,
                            rendered_prompt,
                            output_text: response.text,
                            tool_name: Some(response.tool_name),
                            tool_output: Some(response.tool_output),
                            tool_invocations: response.tool_invocations,
                            rounds_executed: Some(response.rounds_executed),
                            branch_termination_reason: Some(response.termination_reason),
                            memory_retrieved_count: prepared
                                .memory_recall
                                .as_ref()
                                .map(|recall| recall.retrieved),
                            memory_store_node_id: stored
                                .memory_store
                                .as_ref()
                                .map(|store| store.node_id.clone()),
                            input_memory_query_id: prepared.input_memory_query_id,
                            input_memory_query_fingerprint: prepared.input_memory_query_fingerprint,
                            memory_recall_error: prepared.memory_recall_error,
                            memory_store_error: stored.memory_store_error,
                        })
                    }
                }
            });
        }

        let mut results = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            let result = joined.map_err(|err| {
                StasisError::PortFailure(format!("concurrent pattern join failure: {err}"))
            })??;
            results.push(result);
        }

        results.sort_by(|a, b| a.branch_id.cmp(&b.branch_id));

        let final_text = render_final_text(&results, &merge_strategy);

        Ok(ConcurrentPatternExecutionResponse {
            final_text,
            branches: results,
            termination_reason: "completed_all_branches".to_string(),
            merge_strategy,
        })
    }
}

fn render_final_text(results: &[ConcurrentPatternBranchResult], merge_strategy: &str) -> String {
    if results.is_empty() {
        return String::new();
    }

    match merge_strategy {
        "join_lines" => {
            let total_text_len: usize = results.iter().map(|branch| branch.output_text.len()).sum();
            let mut final_text = String::with_capacity(total_text_len + results.len().saturating_sub(1));
            for (idx, branch) in results.iter().enumerate() {
                if idx > 0 {
                    final_text.push('\n');
                }
                final_text.push_str(&branch.output_text);
            }
            final_text
        }
        _ => {
            let total_text_len: usize = results
                .iter()
                .map(|branch| branch.branch_id.len() + branch.output_text.len() + 4)
                .sum();
            let separator_len = 2 * results.len().saturating_sub(1);
            let mut final_text = String::with_capacity(total_text_len + separator_len);
            for (idx, branch) in results.iter().enumerate() {
                if idx > 0 {
                    final_text.push_str("\n\n");
                }
                final_text.push('[');
                final_text.push_str(&branch.branch_id);
                final_text.push_str("]\n");
                final_text.push_str(&branch.output_text);
            }
            final_text
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use genai::adapter::AdapterKind;
    use genai::ModelIden;
    use genai::chat::{
        ChatOptions, ChatRequest, ChatResponse, MessageContent, ToolCall, Usage,
    };
    use serde_json::json;

    use super::*;
    use crate::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
    use crate::domain::errors::Result as StasisResult;
    use crate::ports::outbound::ai_chat_client::AiChatClient;

    struct EchoPromptChatClient;

    #[async_trait]
    impl AiChatClient for EchoPromptChatClient {
        async fn complete(
            &self,
            request: ChatRequest,
            _options: Option<&ChatOptions>,
        ) -> StasisResult<ChatResponse> {
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

    struct BranchAwareToolCallClient;

    #[async_trait]
    impl AiChatClient for BranchAwareToolCallClient {
        async fn complete(
            &self,
            request: ChatRequest,
            _options: Option<&ChatOptions>,
        ) -> StasisResult<ChatResponse> {
            let user_text = request
                .messages
                .iter()
                .rev()
                .filter_map(|message| message.content.first_text())
                .next()
                .unwrap_or_default();

            if user_text.contains("Tool branch") {
                let has_tool_response = request
                    .messages
                    .iter()
                    .any(|message| !message.content.tool_responses().is_empty());

                if !has_tool_response {
                    return Ok(ChatResponse {
                        content: MessageContent::from_tool_calls(vec![ToolCall {
                            call_id: "tool-call-1".to_string(),
                            fn_name: "stasis.web.search.mock".to_string(),
                            fn_arguments: json!({ "query": "branch query" }),
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
                    content: MessageContent::from_text("tool branch final answer"),
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

    struct MockWebSearchTool;

    #[async_trait]
    impl StasisTool for MockWebSearchTool {
        fn name(&self) -> &'static str {
            "stasis.web.search.mock"
        }

        async fn invoke(&self, input: Value) -> StasisResult<Value> {
            Ok(json!({
                "query": input.get("query").cloned().unwrap_or(json!("unknown")),
                "results": [{"title": "mock result"}]
            }))
        }
    }

    #[tokio::test]
    async fn concurrent_pattern_mixed_branches_execute() {
        let chat_client = Arc::new(BranchAwareToolCallClient);
        let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
        let tool_registry = Arc::new(InMemoryToolRegistry::default());
        tool_registry
            .register_tool(MockWebSearchTool)
            .expect("tool should register");

        let pipeline = ConcurrentPatternPipeline::new_with_tool_loop(
            prompt_pipeline,
            tool_registry,
            None,
            None,
            None,
        );

        let response = pipeline
            .execute(ConcurrentPatternExecutionRequest {
                initial_user_prompt: "shared input".to_string(),
                trace_id: None,
                correlation_id: None,
                policy_profile: None,
                model_hint: None,
                default_memory_policy: None,
                merge_strategy: Some("join_with_headers".to_string()),
                branches: vec![
                    ConcurrentPatternBranch {
                        branch_id: "prompt".to_string(),
                        user_prompt_template: "Prompt branch {{input}}".to_string(),
                        system_prompt: None,
                        policy_profile: None,
                        model_hint: None,
                        execution_mode: ConcurrentBranchExecutionMode::Prompt,
                        tool_name: None,
                        tool_input: None,
                        tool_call_mode: ToolCallMode::Auto,
                        memory_policy: None,
                    },
                    ConcurrentPatternBranch {
                        branch_id: "tool".to_string(),
                        user_prompt_template: "Tool branch {{input}}".to_string(),
                        system_prompt: None,
                        policy_profile: None,
                        model_hint: None,
                        execution_mode: ConcurrentBranchExecutionMode::ToolLoop,
                        tool_name: Some("stasis.web.search.mock".to_string()),
                        tool_input: Some(json!({ "query": "shared input" })),
                        tool_call_mode: ToolCallMode::Auto,
                        memory_policy: None,
                    },
                ],
            })
            .await
            .expect("mixed concurrent pattern should succeed");

        assert_eq!(response.branches.len(), 2);
        assert_eq!(response.branches[0].branch_id, "prompt");
        assert_eq!(
            response.branches[0].output_text,
            "echo::Prompt branch shared input"
        );
        assert_eq!(response.branches[1].branch_id, "tool");
        assert_eq!(
            response.branches[1].output_text,
            "tool branch final answer"
        );
        assert_eq!(
            response.branches[1].execution_mode,
            ConcurrentBranchExecutionMode::ToolLoop
        );
        assert_eq!(response.branches[1].tool_invocations.len(), 1);
        assert!(response.final_text.contains("[prompt]"));
        assert!(response.final_text.contains("[tool]"));
    }

    #[tokio::test]
    async fn concurrent_pattern_prompt_only_without_tool_registry() {
        let chat_client = Arc::new(EchoPromptChatClient);
        let pipeline = ConcurrentPatternPipeline::new(PromptExecutionPipeline::new(chat_client));

        let response = pipeline
            .execute(ConcurrentPatternExecutionRequest {
                initial_user_prompt: "base".to_string(),
                trace_id: None,
                correlation_id: None,
                policy_profile: None,
                model_hint: None,
                default_memory_policy: None,
                merge_strategy: None,
                branches: vec![ConcurrentPatternBranch {
                    branch_id: "alpha".to_string(),
                    user_prompt_template: "Branch {input}".to_string(),
                    system_prompt: None,
                    policy_profile: None,
                    model_hint: None,
                    execution_mode: ConcurrentBranchExecutionMode::Prompt,
                    tool_name: None,
                    tool_input: None,
                    tool_call_mode: ToolCallMode::Auto,
                    memory_policy: None,
                }],
            })
            .await
            .expect("prompt-only concurrent pattern should succeed");

        assert_eq!(response.branches[0].output_text, "echo::Branch base");
    }
}
