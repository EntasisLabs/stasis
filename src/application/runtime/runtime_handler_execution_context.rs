use std::sync::Arc;

use crate::application::orchestration::prompt_pipeline::PromptExecutionContext;
use crate::domain::runtime::job::Job;

#[derive(Clone)]
pub struct RuntimeHandlerExecutionContext {
    correlation_id: Arc<str>,
    policy_profile: Arc<Option<String>>,
    model_hint: Arc<Option<String>>,
    prompt_context: Arc<PromptExecutionContext>,
    memory_reader_enabled: bool,
    memory_writer_enabled: bool,
    identity_enabled: bool,
}

impl RuntimeHandlerExecutionContext {
    pub fn new(
        job: &Job,
        policy_profile: Option<String>,
        model_hint: Option<String>,
        reasoning_effort: Option<String>,
        memory_reader_enabled: bool,
        memory_writer_enabled: bool,
        identity_enabled: bool,
    ) -> Self {
        let prompt_context = PromptExecutionContext {
            trace_id: Some(job.trace_id.clone()),
            correlation_id: Some(job.correlation_id.clone()),
            policy_profile: policy_profile.clone(),
            model_hint: model_hint.clone(),
            reasoning_effort,
        };

        Self {
            correlation_id: Arc::<str>::from(job.correlation_id.as_str()),
            policy_profile: Arc::new(policy_profile),
            model_hint: Arc::new(model_hint),
            prompt_context: Arc::new(prompt_context),
            memory_reader_enabled,
            memory_writer_enabled,
            identity_enabled,
        }
    }

    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    pub fn prompt_context_clone(&self) -> PromptExecutionContext {
        (*self.prompt_context).clone()
    }

    pub fn policy_profile(&self) -> Option<&str> {
        self.policy_profile.as_ref().as_deref()
    }

    pub fn policy_profile_clone(&self) -> Option<String> {
        (*self.policy_profile).clone()
    }

    pub fn model_hint_clone(&self) -> Option<String> {
        (*self.model_hint).clone()
    }

    pub fn memory_reader_enabled(&self) -> bool {
        self.memory_reader_enabled
    }

    pub fn memory_writer_enabled(&self) -> bool {
        self.memory_writer_enabled
    }

    pub fn identity_enabled(&self) -> bool {
        self.identity_enabled
    }
}