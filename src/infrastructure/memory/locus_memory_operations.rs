use std::sync::Arc;

use async_trait::async_trait;
use locus_core_rs::NodeStore;
use locus_sdk::prelude::{
    AiProviderRegistry, MemoryAggregateRequest as LocusAggregateRequest, MemoryAggregateService,
    MemoryCompositionService, MemoryDailyRollupRequest, MemoryGroupBy, MemorySchemaService,
    MemoryTransformOperation as LocusTransformOperation,
    MemoryTransformRequest as LocusTransformRequest, MemoryTransformService,
};

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::memory::memory_models::{
    MemoryAggregateRequest, MemoryAggregateResponse, MemoryRollupRequest, MemoryRollupResponse,
    MemorySchemaResponse, MemoryTransformOperation, MemoryTransformRequest,
    MemoryTransformResponse,
};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct LocusMemoryOperations {
    aggregate: MemoryAggregateService,
    composition: MemoryCompositionService,
    transform_store: Arc<dyn NodeStore>,
    schema: MemorySchemaService,
    providers: Option<Arc<dyn AiProviderRegistry>>,
}

impl LocusMemoryOperations {
    pub fn new(store: Arc<dyn NodeStore>, providers: Option<Arc<dyn AiProviderRegistry>>) -> Self {
        Self {
            aggregate: MemoryAggregateService::new(store.clone()),
            composition: MemoryCompositionService::new(store.clone()),
            transform_store: store,
            schema: MemorySchemaService::new(),
            providers,
        }
    }
}

#[async_trait]
impl MemoryOperations for LocusMemoryOperations {
    async fn aggregate(&self, request: &MemoryAggregateRequest) -> Result<MemoryAggregateResponse> {
        let result = self
            .aggregate
            .execute(&LocusAggregateRequest {
                scope: locus_sdk::prelude::MemoryScope {
                    session_ids: request.scope.session_ids.clone(),
                    tiers: request.scope.tiers.clone(),
                    from_utc: request.scope.from_utc,
                    to_utc: request.scope.to_utc,
                    ..Default::default()
                },
                group_by: MemoryGroupBy::DateDay,
                max_groups: request.max_groups,
                max_nodes: request.max_nodes,
                ..Default::default()
            })
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus aggregate failed: {e}")))?;

        Ok(MemoryAggregateResponse {
            total_groups: result.total_groups,
            scanned_nodes: result.scanned_nodes,
        })
    }

    async fn transform(&self, request: &MemoryTransformRequest) -> Result<MemoryTransformResponse> {
        let providers = self.providers.clone().ok_or_else(|| {
            StasisError::PortFailure("locus transform requires ai provider registry".to_string())
        })?;

        let service = MemoryTransformService::new(self.transform_store.clone(), providers);
        let result = service
            .execute(&LocusTransformRequest {
                scope: locus_sdk::prelude::MemoryScope {
                    session_ids: request.scope.session_ids.clone(),
                    tiers: request.scope.tiers.clone(),
                    from_utc: request.scope.from_utc,
                    to_utc: request.scope.to_utc,
                    ..Default::default()
                },
                operation: map_transform_operation(request.operation),
                dry_run: request.dry_run,
                batch_size: request.batch_size,
                max_nodes: request.max_nodes,
                provider_id: request.provider_id.clone(),
                model: request.model.clone(),
                ..Default::default()
            })
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus transform failed: {e}")))?;

        Ok(MemoryTransformResponse {
            scanned: result.scanned,
            selected: result.selected,
            updated: result.updated,
            skipped: result.skipped,
            failed: result.failed,
            duplicate: result.duplicate,
            failures: result.failures,
        })
    }

    async fn rollup(&self, request: &MemoryRollupRequest) -> Result<MemoryRollupResponse> {
        let result = self
            .composition
            .daily_rollup(&MemoryDailyRollupRequest {
                scope: locus_sdk::prelude::MemoryScope {
                    session_ids: request.scope.session_ids.clone(),
                    tiers: request.scope.tiers.clone(),
                    from_utc: request.scope.from_utc,
                    to_utc: request.scope.to_utc,
                    ..Default::default()
                },
                max_days: request.max_days,
                max_nodes: request.max_nodes,
                ..Default::default()
            })
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus daily rollup failed: {e}")))?;

        Ok(MemoryRollupResponse {
            total_groups: result.total_groups,
            scanned_nodes: result.scanned_nodes,
        })
    }

    async fn schema(&self) -> Result<MemorySchemaResponse> {
        let schema = self.schema.execute();
        Ok(MemorySchemaResponse {
            schema_version: schema.schema_version,
            sort_fields: schema.sort_fields,
            filter_fields: schema.filter_fields,
            group_by_fields: schema.group_by_fields,
            fallback_policies: schema.fallback_policies,
            strictness_modes: schema.strictness_modes,
            transform_operations: schema.transform_operations,
        })
    }
}

fn map_transform_operation(value: MemoryTransformOperation) -> LocusTransformOperation {
    match value {
        MemoryTransformOperation::EmbedBackfill => LocusTransformOperation::EmbedBackfill,
        MemoryTransformOperation::ReindexEmbeddings => LocusTransformOperation::ReindexEmbeddings,
    }
}
