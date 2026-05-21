use crate::application::orchestration::prompt_pipeline::{
    PromptExecutionContext, PromptExecutionPipeline, PromptExecutionRequest,
};
use crate::domain::errors::{Result, StasisError};

#[derive(Clone, Debug)]
pub struct OrchestratorPatternRoute {
    pub route_id: String,
    pub selector_keywords: Vec<String>,
    pub user_prompt_template: String,
    pub system_prompt: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OrchestratorPatternExecutionRequest {
    pub initial_user_prompt: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub policy_profile: Option<String>,
    pub model_hint: Option<String>,
    pub routes: Vec<OrchestratorPatternRoute>,
}

#[derive(Clone, Debug)]
pub struct OrchestratorPatternExecutionResponse {
    pub selected_route_id: String,
    pub selection_reason: String,
    pub rendered_prompt: String,
    pub output_text: String,
    pub termination_reason: String,
}

#[derive(Clone)]
pub struct OrchestratorPatternPipeline {
    prompt_pipeline: PromptExecutionPipeline,
}

impl OrchestratorPatternPipeline {
    pub fn new(prompt_pipeline: PromptExecutionPipeline) -> Self {
        Self { prompt_pipeline }
    }

    pub async fn execute(
        &self,
        request: OrchestratorPatternExecutionRequest,
    ) -> Result<OrchestratorPatternExecutionResponse> {
        let (route, selection_reason) =
            Self::select_route(&request.initial_user_prompt, &request.routes)?;

        let rendered_prompt = route
            .user_prompt_template
            .replace("{{input}}", &request.initial_user_prompt)
            .replace("{input}", &request.initial_user_prompt);

        let context = PromptExecutionContext {
            trace_id: request.trace_id,
            correlation_id: request.correlation_id,
            policy_profile: route.policy_profile.clone().or(request.policy_profile),
            model_hint: route.model_hint.clone().or(request.model_hint),
        };

        let mut prompt_request =
            PromptExecutionRequest::from_user_prompt(rendered_prompt.clone()).with_context(context);
        if let Some(system_prompt) = route.system_prompt.clone() {
            prompt_request = prompt_request.with_system_prompt(system_prompt);
        }

        let response = self.prompt_pipeline.execute(prompt_request).await?;

        Ok(OrchestratorPatternExecutionResponse {
            selected_route_id: route.route_id.clone(),
            selection_reason,
            rendered_prompt,
            output_text: response.text,
            termination_reason: "completed_selected_route".to_string(),
        })
    }

    fn select_route<'a>(
        input: &str,
        routes: &'a [OrchestratorPatternRoute],
    ) -> Result<(&'a OrchestratorPatternRoute, String)> {
        let input_lower = input.to_lowercase();
        let mut best: Option<(&OrchestratorPatternRoute, usize)> = None;

        for route in routes {
            let score = route
                .selector_keywords
                .iter()
                .filter(|keyword| {
                    let token = keyword.trim().to_lowercase();
                    !token.is_empty() && input_lower.contains(&token)
                })
                .count();
            if score == 0 {
                continue;
            }

            if best
                .map(|(_, best_score)| score > best_score)
                .unwrap_or(true)
            {
                best = Some((route, score));
            }
        }

        if let Some((route, score)) = best {
            return Ok((
                route,
                format!("keyword_match score={score} route_id={}", route.route_id),
            ));
        }

        let fallback = routes.first().ok_or_else(|| {
            StasisError::PortFailure("orchestrator pattern requires at least one route".to_string())
        })?;
        Ok((
            fallback,
            format!("fallback_first_route route_id={}", fallback.route_id),
        ))
    }
}
