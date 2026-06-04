use std::sync::Arc;

use async_trait::async_trait;
use locus_core_rs::NodeStore;
use locus_core_rs::domain::models::{AvecState, SttpNode};
use locus_sdk::prelude::{
    FallbackPolicy, MemoryExplainRequest, MemoryExplainService,
    MemoryFindRequest as LocusFindRequest, MemoryFindService, MemoryFilter as LocusFilter,
    MemoryRecallRequest as LocusRecallRequest, MemoryRecallService, MemoryScoring, MemorySort,
    MemorySortField as LocusSortField, StrictnessMode, SortDirection as LocusSortDirection,
};
use locus_sdk::domain::memory::MetricRange as LocusMetricRange;

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::{
    MemoryAvecState, MemoryFallbackPolicy, MemoryFilter, MemoryFindRequest, MemoryFindResponse,
    MemoryMetricRange, MemoryNode, MemoryRecallRequest, MemoryRecallResponse, MemorySortDirection,
    MemorySortField, MemoryStrictnessMode,
};

pub struct LocusContextReader {
    recall: MemoryRecallService,
    find: MemoryFindService,
    explain: MemoryExplainService,
}

impl LocusContextReader {
    pub fn new(store: Arc<dyn NodeStore>) -> Self {
        Self {
            recall: MemoryRecallService::new(store.clone()),
            find: MemoryFindService::new(store.clone()),
            explain: MemoryExplainService::new(store),
        }
    }
}

#[async_trait]
impl MemoryContextReader for LocusContextReader {
    async fn recall(&self, request: &MemoryRecallRequest) -> Result<MemoryRecallResponse> {
        let locus_request = LocusRecallRequest {
            scope: locus_sdk::prelude::MemoryScope {
                session_ids: request.scope.session_ids.clone(),
                tiers: request.scope.tiers.clone(),
                from_utc: request.scope.from_utc,
                to_utc: request.scope.to_utc,
                ..Default::default()
            },
            scoring: MemoryScoring {
                alpha: request.alpha,
                beta: request.beta,
                fallback_policy: map_fallback(request.fallback_policy),
                strictness: map_strictness(request.strictness),
                ..Default::default()
            },
            page: locus_sdk::prelude::MemoryPage {
                limit: request.limit,
                cursor: None,
            },
            current_avec: request.current_avec.map(|avec| AvecState {
                stability: avec.stability,
                friction: avec.friction,
                logic: avec.logic,
                autonomy: avec.autonomy,
            }),
            query_text: request.query_text.clone(),
            ..Default::default()
        };

        let recall_result = self
            .recall
            .execute(&locus_request)
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus recall failed: {e}")))?;

        let nodes: Vec<MemoryNode> = recall_result
            .nodes
            .iter()
            .map(map_node)
            .collect();
        let node_sync_keys: Vec<String> = nodes.iter().map(|node| node.sync_key.clone()).collect();

        let mut response = MemoryRecallResponse {
            retrieved: recall_result.retrieved,
            next_cursor: recall_result.next_cursor,
            has_more: recall_result.has_more,
            retrieval_path: Some(format!("{:?}", recall_result.retrieval_path)),
            nodes,
            node_sync_keys,
            ..Default::default()
        };

        if request.include_explain {
            let explain_result = self
                .explain
                .execute(&MemoryExplainRequest {
                    recall: locus_request,
                })
                .await
                .map_err(|e| StasisError::PortFailure(format!("locus explain failed: {e}")))?;

            response.fallback_triggered = explain_result.fallback_triggered;
            response.fallback_reason = explain_result.fallback_reason;
        }

        Ok(response)
    }

    async fn find(&self, request: &MemoryFindRequest) -> Result<MemoryFindResponse> {
        let locus_request = LocusFindRequest {
            scope: locus_sdk::prelude::MemoryScope {
                session_ids: request.scope.session_ids.clone(),
                tiers: request.scope.tiers.clone(),
                from_utc: request.scope.from_utc,
                to_utc: request.scope.to_utc,
                ..Default::default()
            },
            filter: map_filter(&request.filter),
            page: locus_sdk::prelude::MemoryPage {
                limit: request.limit,
                cursor: request.cursor.clone(),
            },
            sort: MemorySort {
                field: map_sort_field(request.sort_field),
                direction: map_sort_direction(request.sort_direction),
            },
        };

        let find_result = self
            .find
            .execute(&locus_request)
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus find failed: {e}")))?;

        let nodes: Vec<MemoryNode> = find_result.nodes.iter().map(map_node).collect();
        let node_sync_keys: Vec<String> = nodes.iter().map(|node| node.sync_key.clone()).collect();

        Ok(MemoryFindResponse {
            retrieved: find_result.retrieved,
            has_more: find_result.has_more,
            next_cursor: find_result.next_cursor,
            nodes,
            node_sync_keys,
        })
    }
}

fn map_avec(avec: &AvecState) -> MemoryAvecState {
    MemoryAvecState {
        stability: avec.stability,
        friction: avec.friction,
        logic: avec.logic,
        autonomy: avec.autonomy,
    }
}

fn map_node(node: &SttpNode) -> MemoryNode {
    MemoryNode {
        raw: node.raw.clone(),
        session_id: node.session_id.clone(),
        tier: node.tier.clone(),
        timestamp: node.timestamp,
        compression_depth: node.compression_depth,
        parent_node_id: node.parent_node_id.clone(),
        sync_key: node.sync_key.clone(),
        context_summary: node.context_summary.clone(),
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

fn map_fallback(value: MemoryFallbackPolicy) -> FallbackPolicy {
    match value {
        MemoryFallbackPolicy::Never => FallbackPolicy::Never,
        MemoryFallbackPolicy::OnEmpty => FallbackPolicy::OnEmpty,
        MemoryFallbackPolicy::Always => FallbackPolicy::Always,
    }
}

fn map_strictness(value: MemoryStrictnessMode) -> StrictnessMode {
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

fn map_filter(value: &MemoryFilter) -> LocusFilter {
    LocusFilter {
        has_embedding: value.has_embedding,
        embedding_model: value.embedding_model.clone(),
        psi: value.psi.as_ref().map(map_metric_range),
        rho: value.rho.as_ref().map(map_metric_range),
        kappa: value.kappa.as_ref().map(map_metric_range),
        text_contains: value.text_contains.clone(),
    }
}

fn map_sort_field(value: MemorySortField) -> LocusSortField {
    match value {
        MemorySortField::Timestamp => LocusSortField::Timestamp,
        MemorySortField::UpdatedAt => LocusSortField::UpdatedAt,
        MemorySortField::Psi => LocusSortField::Psi,
        MemorySortField::Rho => LocusSortField::Rho,
        MemorySortField::Kappa => LocusSortField::Kappa,
    }
}

fn map_sort_direction(value: MemorySortDirection) -> LocusSortDirection {
    match value {
        MemorySortDirection::Asc => LocusSortDirection::Asc,
        MemorySortDirection::Desc => LocusSortDirection::Desc,
    }
}
