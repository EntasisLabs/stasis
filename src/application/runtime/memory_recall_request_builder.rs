use crate::application::orchestration::runtime_job_payloads::{
    MemoryFallbackPolicyPayload, MemoryFilterPayload, MemoryMetricRangePayload,
    MemoryPolicyPayload, MemoryStrictnessModePayload,
};
use crate::ports::outbound::memory::memory_models::{
    MemoryFallbackPolicy, MemoryFilter, MemoryMetricRange, MemoryRecallRequest, MemoryScope,
    MemoryStrictnessMode,
};

fn map_metric_range(value: &MemoryMetricRangePayload) -> MemoryMetricRange {
    MemoryMetricRange {
        min: value.min,
        max: value.max,
    }
}

pub fn memory_filter_from_payload(payload: &MemoryFilterPayload) -> MemoryFilter {
    MemoryFilter {
        has_embedding: payload.has_embedding,
        embedding_model: payload.embedding_model.clone(),
        psi: payload.psi.as_ref().map(map_metric_range),
        rho: payload.rho.as_ref().map(map_metric_range),
        kappa: payload.kappa.as_ref().map(map_metric_range),
        text_contains: payload.text_contains.clone(),
        tags_contains: payload.tags_contains.clone(),
        has_tag: payload.has_tag.clone(),
        indexed_tags: payload.indexed_tags.clone(),
        tag_prefix: payload.tag_prefix.clone(),
        has_semantic_links: payload.has_semantic_links,
        link_rel: payload.link_rel.clone(),
        link_target: payload.link_target.clone(),
        links_to_ref: payload.links_to_ref.clone(),
    }
}

pub fn build_memory_recall_request(
    correlation_id: &str,
    default_query_text: Option<&str>,
    memory_policy: Option<&MemoryPolicyPayload>,
) -> MemoryRecallRequest {
    let mut request = MemoryRecallRequest::default();
    request.scope = MemoryScope {
        tenant_id: memory_policy.and_then(|policy| policy.tenant_id.clone()),
        session_ids: memory_policy
            .and_then(|policy| policy.session_ids.clone())
            .or_else(|| Some(vec![correlation_id.to_string()])),
        tiers: memory_policy.and_then(|policy| policy.tiers.clone()),
        from_utc: memory_policy.and_then(|policy| policy.from_utc),
        to_utc: memory_policy.and_then(|policy| policy.to_utc),
    };

    if let Some(policy) = memory_policy {
        request.filter = memory_filter_from_payload(&policy.filter);
    }

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
    request.gamma = memory_policy
        .and_then(|policy| policy.gamma)
        .unwrap_or(request.gamma);
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
