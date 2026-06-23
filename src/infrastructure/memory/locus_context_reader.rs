use std::sync::Arc;

use async_trait::async_trait;
use locus_sdk::application::memory_graph::MemoryGraphService;
use locus_sdk::prelude::{
    MemoryExplainRequest, MemoryExplainService, MemoryFindRequest as LocusFindRequest,
    MemoryFindService, MemoryRecallRequest as LocusRecallRequest, MemoryRecallService,
    MemorySort, MemoryPage,
};
use locus_sdk::domain::graph::MemoryGraphRequest as LocusGraphRequest;

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::locus_memory_mapping::{
    map_filter, map_node, map_scope, map_scoring, map_sort_direction, map_sort_field,
};
use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::{
    MemoryFindRequest, MemoryFindResponse, MemoryGraphRequest, MemoryGraphResponse,
    MemoryRecallRequest, MemoryRecallResponse,
};

pub struct LocusContextReader {
    recall: MemoryRecallService,
    find: MemoryFindService,
    explain: MemoryExplainService,
    graph: MemoryGraphService,
}

impl LocusContextReader {
    pub fn new(memory: Arc<LocusMemoryStore>) -> Self {
        let node_store = memory.node_store.clone();
        let semantic_index = memory.semantic_index.clone();
        Self {
            recall: MemoryRecallService::new(node_store.clone())
                .with_semantic_index(semantic_index.clone()),
            find: MemoryFindService::new(node_store.clone())
                .with_semantic_index(semantic_index.clone()),
            explain: MemoryExplainService::new(node_store.clone()),
            graph: MemoryGraphService::new(node_store).with_semantic_index(semantic_index),
        }
    }
}

#[async_trait]
impl MemoryContextReader for LocusContextReader {
    async fn recall(&self, request: &MemoryRecallRequest) -> Result<MemoryRecallResponse> {
        let locus_request = LocusRecallRequest {
            scope: map_scope(&request.scope),
            filter: map_filter(&request.filter),
            scoring: map_scoring(
                request.alpha,
                request.beta,
                request.gamma,
                request.fallback_policy,
                request.strictness,
            ),
            page: MemoryPage {
                limit: request.limit,
                cursor: None,
            },
            current_avec: request.current_avec.map(|avec| locus_core_rs::domain::models::AvecState {
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

        let nodes: Vec<_> = recall_result.nodes.iter().map(map_node).collect();
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
            scope: map_scope(&request.scope),
            filter: map_filter(&request.filter),
            page: MemoryPage {
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

        let nodes: Vec<_> = find_result.nodes.iter().map(map_node).collect();
        let node_sync_keys: Vec<String> = nodes.iter().map(|node| node.sync_key.clone()).collect();

        Ok(MemoryFindResponse {
            retrieved: find_result.retrieved,
            has_more: find_result.has_more,
            next_cursor: find_result.next_cursor,
            nodes,
            node_sync_keys,
        })
    }

    async fn graph(&self, request: &MemoryGraphRequest) -> Result<MemoryGraphResponse> {
        let locus_request = LocusGraphRequest {
            scope: map_scope(&request.scope),
            filter: map_filter(&request.filter),
            include_lineage: request.include_lineage,
            include_semantic: request.include_semantic,
            include_session_topology: request.include_session_topology,
            rel: request.rel.clone(),
            target_prefix: request.target_prefix.clone(),
            limit: request.limit,
        };

        let result = self
            .graph
            .execute(&locus_request)
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus graph failed: {e}")))?;

        Ok(MemoryGraphResponse {
            sessions: result.sessions,
            nodes: result.nodes,
            edges: result.edges,
            retrieved: result.retrieved,
        })
    }
}
