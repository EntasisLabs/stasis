use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use chrono::Utc;

use crate::application::orchestration::runtime_job_payloads::{
    MemoryPolicyPayload, MemoryStoreModePayload,
};
use crate::ports::outbound::memory::memory_models::{MemoryRecallRequest, MemoryStoreResponse};

pub enum SttpPromptNodeFormat {
    TaggedSchema,
    UntaggedNoSchema,
}

pub fn should_store(memory_policy: Option<&MemoryPolicyPayload>) -> bool {
    !matches!(
        memory_policy.and_then(|policy| policy.store_mode.clone()),
        Some(MemoryStoreModePayload::Disabled)
    )
}

pub fn memory_scope_hash(correlation_id: &str, memory_policy: Option<&MemoryPolicyPayload>) -> String {
    let basis = format!(
        "corr={correlation_id}|sessions={:?}|tiers={:?}|from={:?}|to={:?}",
        memory_policy.and_then(|policy| policy.session_ids.clone()),
        memory_policy.and_then(|policy| policy.tiers.clone()),
        memory_policy.and_then(|policy| policy.from_utc),
        memory_policy.and_then(|policy| policy.to_utc),
    );
    let mut hasher = DefaultHasher::new();
    basis.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

pub fn memory_query_id(correlation_id: &str, request: &MemoryRecallRequest) -> String {
    let basis = format!(
        "corr={correlation_id}|query={:?}|sessions={:?}|tiers={:?}|from={:?}|to={:?}|limit={}|alpha={}|beta={}|fallback={:?}|strictness={:?}|include_explain={}",
        request.query_text,
        request.scope.session_ids,
        request.scope.tiers,
        request.scope.from_utc,
        request.scope.to_utc,
        request.limit,
        request.alpha,
        request.beta,
        request.fallback_policy,
        request.strictness,
        request.include_explain,
    );
    let mut hasher = DefaultHasher::new();
    basis.hash(&mut hasher);
    format!("mq:{:x}", hasher.finish())
}

pub fn memory_query_fingerprint(request: &MemoryRecallRequest) -> String {
    format!(
        "sessions={:?}|tiers={:?}|from={:?}|to={:?}|limit={}|alpha={}|beta={}|fallback={:?}|strictness={:?}|include_explain={}",
        request.scope.session_ids,
        request.scope.tiers,
        request.scope.from_utc,
        request.scope.to_utc,
        request.limit,
        request.alpha,
        request.beta,
        request.fallback_policy,
        request.strictness,
        request.include_explain,
    )
}

pub fn resolve_sttp_output_node_id(memory_store: Option<&MemoryStoreResponse>, fallback: String) -> String {
    memory_store
        .map(|stored| stored.node_id.clone())
        .filter(|node_id| !node_id.trim().is_empty())
        .unwrap_or(fallback)
}

pub fn render_prompt_response_sttp_node(
    session_id: &str,
    user_prompt: &str,
    output_text: &str,
    format: SttpPromptNodeFormat,
) -> String {
    let escaped_summary = output_text.replace('"', "\\\"");
    let escaped_prompt = user_prompt.replace('"', "\\\"");
    let timestamp = Utc::now().to_rfc3339();

    match format {
        SttpPromptNodeFormat::TaggedSchema => format!(
            "⊕⟨ ⏣0{{ trigger: manual, response_format: temporal_node, origin_session: \"{session_id}\", compression_depth: 1, parent_node: null, prime: {{ attractor_config: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75 }}, context_summary: \"{escaped_summary}\", relevant_tier: raw, retrieval_budget: 10 }} }} ⟩\n\
⦿⟨ ⏣0{{ timestamp: \"{timestamp}\", tier: raw, session_id: \"{session_id}\", schema_version: \"sttp-1.0\", user_avec: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75, psi: 2.60 }}, model_avec: {{ stability: 0.82, friction: 0.18, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩\n\
◈⟨ ⏣0{{ prompt(.88): \"{escaped_prompt}\", response(.95): \"{escaped_summary}\" }} ⟩\n\
⍉⟨ ⏣0{{ rho: 0.96, kappa: 0.94, psi: 2.60, compression_avec: {{ stability: 0.81, friction: 0.19, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩"
        ),
        SttpPromptNodeFormat::UntaggedNoSchema => format!(
            "⊕⟨ {{ trigger: manual, response_format: temporal_node, origin_session: \"{session_id}\", compression_depth: 1, parent_node: null, prime: {{ attractor_config: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75 }}, context_summary: \"{escaped_summary}\", relevant_tier: raw, retrieval_budget: 10 }} }} ⟩\n\
⦿⟨ {{ timestamp: \"{timestamp}\", tier: raw, session_id: \"{session_id}\", user_avec: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75, psi: 2.60 }}, model_avec: {{ stability: 0.82, friction: 0.18, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩\n\
◈⟨ {{ prompt(.88): \"{escaped_prompt}\", response(.95): \"{escaped_summary}\" }} ⟩\n\
⍉⟨ {{ rho: 0.96, kappa: 0.94, psi: 2.60, compression_avec: {{ stability: 0.81, friction: 0.19, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩"
        ),
    }
}

pub fn render_session_summary_sttp_node(session_id: &str, summary_text: &str) -> String {
    let escaped_summary = summary_text.replace('"', "\\\"");
    let timestamp = Utc::now().to_rfc3339();

    format!(
        "⊕⟨ ⏣0{{ trigger: manual, response_format: temporal_node, origin_session: \"{session_id}\", compression_depth: 1, parent_node: null, prime: {{ attractor_config: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75 }}, context_summary: \"{escaped_summary}\", relevant_tier: raw, retrieval_budget: 10 }} }} ⟩\n\
⦿⟨ ⏣0{{ timestamp: \"{timestamp}\", tier: raw, session_id: \"{session_id}\", schema_version: \"sttp-1.0\", user_avec: {{ stability: 0.80, friction: 0.20, logic: 0.85, autonomy: 0.75, psi: 2.60 }}, model_avec: {{ stability: 0.82, friction: 0.18, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩\n\
◈⟨ ⏣0{{ session_summary(.95): \"{escaped_summary}\" }} ⟩\n\
⍉⟨ ⏣0{{ rho: 0.96, kappa: 0.94, psi: 2.60, compression_avec: {{ stability: 0.81, friction: 0.19, logic: 0.84, autonomy: 0.74, psi: 2.58 }} }} ⟩"
    )
}