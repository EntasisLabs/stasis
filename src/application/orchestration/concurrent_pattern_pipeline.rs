use std::sync::Arc;

use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::domain::errors::{Result, StasisError};
use tokio::task::JoinSet;

#[derive(Clone, Debug)]
pub struct ConcurrentPatternBranch {
    pub branch_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ConcurrentPatternExecutionRequest {
    pub initial_user_prompt: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub merge_strategy: Option<String>,
    pub branches: Vec<ConcurrentPatternBranch>,
}

#[derive(Clone, Debug)]
pub struct ConcurrentPatternBranchResult {
    pub branch_id: String,
    pub rendered_prompt: String,
    pub output_text: String,
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
}

#[derive(Clone)]
struct ConcurrentSharedInputs {
    initial_input: Arc<str>,
    trace_id: Arc<Option<String>>,
    correlation_id: Arc<Option<String>>,
    default_policy_profile: Arc<Option<String>>,
    default_model_hint: Arc<Option<String>>,
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
}

impl ConcurrentPatternPipeline {
    pub fn new(prompt_pipeline: PromptExecutionPipeline) -> Self {
        Self { prompt_pipeline }
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
        };

        let mut join_set: JoinSet<Result<ConcurrentPatternBranchResult>> = JoinSet::new();

        for branch in branches {
            let pipeline = self.prompt_pipeline.clone();
            let shared_inputs = shared_inputs.clone();

            join_set.spawn(async move {
                let ConcurrentPatternBranch {
                    branch_id,
                    user_prompt_template,
                    system_prompt,
                    policy_profile,
                    model_hint,
                } = branch;

                let rendered_prompt = user_prompt_template
                    .replace("{{input}}", &shared_inputs.initial_input)
                    .replace("{input}", &shared_inputs.initial_input);

                let context = shared_inputs.build_context(policy_profile, model_hint);

                let mut prompt_request =
                    PromptExecutionRequest::from_user_prompt(rendered_prompt.clone())
                        .with_context(context);
                if let Some(system_prompt) = system_prompt {
                    prompt_request = prompt_request.with_system_prompt(system_prompt);
                }

                let response = pipeline.execute(prompt_request).await?;
                Ok(ConcurrentPatternBranchResult {
                    branch_id,
                    rendered_prompt,
                    output_text: response.text,
                })
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
