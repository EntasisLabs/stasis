use locus_core_rs::domain::models::{SemanticLink, SttpNode};
use locus_sdk::prelude::{
    FallbackPolicy, MemoryFilter as LocusFilter, MemoryScope as LocusScope, MemoryScoring,
    MemorySortField as LocusSortField, SortDirection as LocusSortDirection, StrictnessMode,
};
use locus_sdk::domain::memory::MetricRange as LocusMetricRange;

use crate::ports::outbound::memory::memory_models::{
    MemoryAvecState, MemoryFallbackPolicy, MemoryFilter, MemoryMetricRange, MemoryNode,
    MemoryScope, MemorySemanticLink, MemorySortDirection, MemorySortField, MemoryStrictnessMode,
};

pub fn map_scope(scope: &MemoryScope) -> LocusScope {
    LocusScope {
        tenant_id: scope.tenant_id.clone(),
        session_ids: scope.session_ids.clone(),
        tiers: scope.tiers.clone(),
        from_utc: scope.from_utc,
        to_utc: scope.to_utc,
    }
}

pub fn map_filter(value: &MemoryFilter) -> LocusFilter {
    LocusFilter {
        has_embedding: value.has_embedding,
        embedding_model: value.embedding_model.clone(),
        psi: value.psi.as_ref().map(map_metric_range),
        rho: value.rho.as_ref().map(map_metric_range),
        kappa: value.kappa.as_ref().map(map_metric_range),
        text_contains: value.text_contains.clone(),
        tags_contains: value.tags_contains.clone(),
        has_tag: value.has_tag.clone(),
        indexed_tags: value.indexed_tags.clone(),
        tag_prefix: value.tag_prefix.clone(),
        has_semantic_links: value.has_semantic_links,
        link_rel: value.link_rel.clone(),
        link_target: value.link_target.clone(),
        links_to_ref: value.links_to_ref.clone(),
    }
}

pub fn map_scoring(
    alpha: f32,
    beta: f32,
    gamma: f32,
    fallback_policy: MemoryFallbackPolicy,
    strictness: MemoryStrictnessMode,
) -> MemoryScoring {
    MemoryScoring {
        alpha,
        beta,
        gamma,
        fallback_policy: map_fallback(fallback_policy),
        strictness: map_strictness(strictness),
        ..Default::default()
    }
}

pub fn map_node(node: &SttpNode) -> MemoryNode {
    MemoryNode {
        raw: node.raw.clone(),
        session_id: node.session_id.clone(),
        tier: node.tier.clone(),
        timestamp: node.timestamp,
        compression_depth: node.compression_depth,
        parent_node_id: node.parent_node_id.clone(),
        sync_key: node.sync_key.clone(),
        context_summary: node.context_summary.clone(),
        semantic_tags: node.semantic_tags.clone(),
        semantic_links: node
            .semantic_links
            .as_ref()
            .map(|links| links.iter().map(map_semantic_link).collect()),
        embedding_model: node.embedding_model.clone(),
        embedding_dimensions: node.embedding_dimensions,
        embedded_at: node.embedded_at,
        rho: node.rho,
        kappa: node.kappa,
        psi: node.psi,
        user_avec: map_avec(&node.user_avec),
        model_avec: map_avec(&node.model_avec),
        compression_avec: node.compression_avec.as_ref().map(map_avec),
        updated_at: node.updated_at,
    }
}

fn map_semantic_link(link: &SemanticLink) -> MemorySemanticLink {
    MemorySemanticLink {
        rel: link.rel.clone(),
        target: link.target.clone(),
        confidence: link.confidence,
    }
}

fn map_avec(avec: &locus_core_rs::domain::models::AvecState) -> MemoryAvecState {
    MemoryAvecState {
        stability: avec.stability,
        friction: avec.friction,
        logic: avec.logic,
        autonomy: avec.autonomy,
    }
}

pub fn map_fallback(value: MemoryFallbackPolicy) -> FallbackPolicy {
    match value {
        MemoryFallbackPolicy::Never => FallbackPolicy::Never,
        MemoryFallbackPolicy::OnEmpty => FallbackPolicy::OnEmpty,
        MemoryFallbackPolicy::Always => FallbackPolicy::Always,
    }
}

pub fn map_strictness(value: MemoryStrictnessMode) -> StrictnessMode {
    match value {
        MemoryStrictnessMode::Precision => StrictnessMode::Precision,
        MemoryStrictnessMode::Balanced => StrictnessMode::Balanced,
        MemoryStrictnessMode::Recall => StrictnessMode::Recall,
    }
}

fn map_metric_range(value: &MemoryMetricRange) -> LocusMetricRange {
    LocusMetricRange {
        min: value.min,
        max: value.max,
    }
}

pub fn map_sort_field(value: MemorySortField) -> LocusSortField {
    match value {
        MemorySortField::Timestamp => LocusSortField::Timestamp,
        MemorySortField::UpdatedAt => LocusSortField::UpdatedAt,
        MemorySortField::Psi => LocusSortField::Psi,
        MemorySortField::Rho => LocusSortField::Rho,
        MemorySortField::Kappa => LocusSortField::Kappa,
    }
}

pub fn map_sort_direction(value: MemorySortDirection) -> LocusSortDirection {
    match value {
        MemorySortDirection::Asc => LocusSortDirection::Asc,
        MemorySortDirection::Desc => LocusSortDirection::Desc,
    }
}
