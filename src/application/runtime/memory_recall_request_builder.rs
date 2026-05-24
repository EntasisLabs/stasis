use crate::application::orchestration::runtime_job_payloads::{
    MemoryFallbackPolicyPayload, MemoryPolicyPayload, MemoryStrictnessModePayload,
};
use crate::ports::outbound::memory::memory_models::{
    MemoryFallbackPolicy, MemoryRecallRequest, MemoryScope, MemoryStrictnessMode,
};

pub fn build_memory_recall_request(
    correlation_id: &str,
    default_query_text: Option<&str>,
    memory_policy: Option<&MemoryPolicyPayload>,
) -> MemoryRecallRequest {
    let mut request = MemoryRecallRequest::default();
    request.scope = MemoryScope {
        session_ids: memory_policy
            .and_then(|policy| policy.session_ids.clone())
            .or_else(|| Some(vec![correlation_id.to_string()])),
        tiers: memory_policy.and_then(|policy| policy.tiers.clone()),
        from_utc: memory_policy.and_then(|policy| policy.from_utc),
        to_utc: memory_policy.and_then(|policy| policy.to_utc),
    };

    request.query_text = memory_policy
        .and_then(|policy| policy.query_text.clone())
        .or_else(|| default_query_text.map(|value| value.to_string()));

    request.limit = memory_policy
        .and_then(|policy| policy.limit)
        .unwrap_or(request.limit);
    request.alpha = memory_policy
        .and_then(|policy| policy.alpha)
        .unwrap_or(request.alpha);
    request.beta = memory_policy
        .and_then(|policy| policy.beta)
        .unwrap_or(request.beta);
    request.include_explain = memory_policy
        .and_then(|policy| policy.include_explain)
        .unwrap_or(true);

    request.fallback_policy = match memory_policy.and_then(|policy| policy.fallback_policy.clone()) {
        Some(MemoryFallbackPolicyPayload::Never) => MemoryFallbackPolicy::Never,
        Some(MemoryFallbackPolicyPayload::Always) => MemoryFallbackPolicy::Always,
        _ => MemoryFallbackPolicy::OnEmpty,
    };

    request.strictness = match memory_policy.and_then(|policy| policy.strictness.clone()) {
        Some(MemoryStrictnessModePayload::Precision) => MemoryStrictnessMode::Precision,
        Some(MemoryStrictnessModePayload::Recall) => MemoryStrictnessMode::Recall,
        _ => MemoryStrictnessMode::Balanced,
    };

    request
}
