use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::application::runtime::chat_options_resolver::resolve_reasoning_effort;
use crate::domain::errors::Result;

#[derive(Clone, Debug)]
pub struct SequentialPatternStage {
    pub stage_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SequentialPatternExecutionRequest {
    pub initial_user_prompt: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub reasoning_effort: Option<String>,
    pub stages: Vec<SequentialPatternStage>,
}

#[derive(Clone, Debug)]
pub struct SequentialPatternStageResult {
    pub stage_id: String,
    pub rendered_prompt: String,
    pub output_text: String,
}

#[derive(Clone, Debug)]
pub struct SequentialPatternExecutionResponse {
    pub final_text: String,
    pub stages: Vec<SequentialPatternStageResult>,
    pub termination_reason: String,
}

#[derive(Clone)]
pub struct SequentialPatternPipeline {
    prompt_pipeline: PromptExecutionPipeline,
}

impl SequentialPatternPipeline {
    pub fn new(prompt_pipeline: PromptExecutionPipeline) -> Self {
        Self { prompt_pipeline }
    }

    pub async fn execute(
        &self,
        request: SequentialPatternExecutionRequest,
    ) -> Result<SequentialPatternExecutionResponse> {
        let mut current_input = request.initial_user_prompt;
        let mut stage_results = Vec::with_capacity(request.stages.len());

        for stage in request.stages {
            let rendered_prompt = stage
                .user_prompt_template
                .replace("{{input}}", &current_input)
                .replace("{input}", &current_input);

            let context = PromptExecutionContext {
                trace_id: request.trace_id.clone(),
                correlation_id: request.correlation_id.clone(),
                policy_profile: stage
                    .policy_profile
                    .clone()
                    .or_else(|| request.policy_profile.clone()),
                model_hint: stage
                    .model_hint
                    .clone()
                    .or_else(|| request.model_hint.clone()),
                reasoning_effort: resolve_reasoning_effort(
                    stage.reasoning_effort.clone(),
                    request.reasoning_effort.clone(),
                ),
            };

            let mut prompt_request =
                PromptExecutionRequest::from_user_prompt(rendered_prompt.clone())
                    .with_context(context);
            if let Some(system_prompt) = stage.system_prompt {
                prompt_request = prompt_request.with_system_prompt(system_prompt);
            }

            let response = self.prompt_pipeline.execute(prompt_request).await?;
            current_input = response.text.clone();
            stage_results.push(SequentialPatternStageResult {
                stage_id: stage.stage_id,
                rendered_prompt,
                output_text: response.text,
            });
        }

        Ok(SequentialPatternExecutionResponse {
            final_text: current_input,
            stages: stage_results,
            termination_reason: "completed_all_stages".to_string(),
        })
    }
}
