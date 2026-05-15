use std::sync::Arc;

use async_trait::async_trait;
use locus_core_rs::NodeStore;
use locus_core_rs::domain::models::AvecState;
use locus_sdk::prelude::{
    FallbackPolicy, MemoryExplainRequest, MemoryExplainService, MemoryRecallRequest as LocusRecallRequest,
    MemoryRecallService, MemoryScoring, StrictnessMode,
};

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_models::{
    MemoryFallbackPolicy, MemoryRecallRequest, MemoryRecallResponse, MemoryStrictnessMode,
};

pub struct LocusContextReader {
    recall: MemoryRecallService,
    explain: MemoryExplainService,
}

impl LocusContextReader {
    pub fn new(store: Arc<dyn NodeStore>) -> Self {
        Self {
            recall: MemoryRecallService::new(store.clone()),
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

        let mut response = MemoryRecallResponse {
            retrieved: recall_result.retrieved,
            next_cursor: recall_result.next_cursor,
            has_more: recall_result.has_more,
            retrieval_path: Some(format!("{:?}", recall_result.retrieval_path)),
            node_sync_keys: recall_result
                .nodes
                .iter()
                .map(|node| node.sync_key.clone())
                .collect(),
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
