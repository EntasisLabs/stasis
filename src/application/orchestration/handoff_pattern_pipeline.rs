use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::domain::errors::Result;

#[derive(Clone, Debug)]
pub struct HandoffPatternTurn {
    pub actor_id: String,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HandoffPatternExecutionRequest {
    pub initial_user_prompt: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub turns: Vec<HandoffPatternTurn>,
}

#[derive(Clone, Debug)]
pub struct HandoffPatternTurnResult {
    pub actor_id: String,
    pub rendered_prompt: String,
    pub output_text: String,
}

#[derive(Clone, Debug)]
pub struct HandoffTransition {
    pub from_actor_id: String,
    pub to_actor_id: String,
}

#[derive(Clone, Debug)]
pub struct HandoffPatternExecutionResponse {
    pub final_text: String,
    pub turns: Vec<HandoffPatternTurnResult>,
    pub handoffs: Vec<HandoffTransition>,
    pub termination_reason: String,
}

#[derive(Clone)]
pub struct HandoffPatternPipeline {
    prompt_pipeline: PromptExecutionPipeline,
}

impl HandoffPatternPipeline {
    pub fn new(prompt_pipeline: PromptExecutionPipeline) -> Self {
        Self { prompt_pipeline }
    }

    pub async fn execute(&self, request: HandoffPatternExecutionRequest) -> Result<HandoffPatternExecutionResponse> {
        let mut current_input = request.initial_user_prompt;
        let mut turn_results = Vec::with_capacity(request.turns.len());
        let mut handoffs = Vec::new();
        let mut previous_actor: Option<String> = None;

        for turn in request.turns {
            if let Some(from_actor_id) = previous_actor.clone() {
                handoffs.push(HandoffTransition {
                    from_actor_id,
                    to_actor_id: turn.actor_id.clone(),
                });
            }

            let rendered_prompt = turn
                .user_prompt_template
                .replace("{{input}}", &current_input)
                .replace("{input}", &current_input);

            let context = PromptExecutionContext {
                trace_id: request.trace_id.clone(),
                correlation_id: request.correlation_id.clone(),
                policy_profile: turn
                    .policy_profile
                    .clone()
                    .or_else(|| request.policy_profile.clone()),
                model_hint: turn.model_hint.clone().or_else(|| request.model_hint.clone()),
            };

            let mut prompt_request =
                PromptExecutionRequest::from_user_prompt(rendered_prompt.clone()).with_context(context);
            if let Some(system_prompt) = turn.system_prompt {
                prompt_request = prompt_request.with_system_prompt(system_prompt);
            }

            let response = self.prompt_pipeline.execute(prompt_request).await?;
            previous_actor = Some(turn.actor_id.clone());
            current_input = response.text.clone();
            turn_results.push(HandoffPatternTurnResult {
                actor_id: turn.actor_id,
                rendered_prompt,
                output_text: response.text,
            });
        }

        Ok(HandoffPatternExecutionResponse {
            final_text: current_input,
            turns: turn_results,
            handoffs,
            termination_reason: "completed_all_turns".to_string(),
        })
    }
}
