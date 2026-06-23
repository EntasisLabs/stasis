use std::sync::Arc;

use async_trait::async_trait;
use locus_sdk::application::memory_evict::MemoryEvictService;
use locus_sdk::domain::evict::{
    InboundReferencesPreview as LocusInboundReferencesPreview,
    MemoryEvictMode as LocusEvictMode, MemoryEvictRecord as LocusEvictRecord,
    MemoryEvictRequest as LocusEvictRequest,
};
use locus_sdk::prelude::{
    AiProviderRegistry, MemoryAggregateRequest as LocusAggregateRequest, MemoryAggregateService,
    MemoryCompositionService, MemoryDailyRollupRequest, MemoryGroupBy, MemorySchemaService,
    MemoryTransformOperation as LocusTransformOperation,
    MemoryTransformRequest as LocusTransformRequest, MemoryTransformService,
};

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::locus_memory_mapping::{map_filter, map_scope};
use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;
use crate::ports::outbound::memory::memory_models::{
    MemoryAggregateRequest, MemoryAggregateResponse, MemoryEvictMode, MemoryEvictRecord,
    MemoryEvictRequest, MemoryEvictResponse, MemoryInboundReferencesPreview, MemoryRollupRequest,
    MemoryRollupResponse, MemorySchemaResponse, MemoryTransformOperation, MemoryTransformRequest,
    MemoryTransformResponse,
};
use crate::ports::outbound::memory::memory_operations::MemoryOperations;

pub struct LocusMemoryOperations {
    memory: Arc<LocusMemoryStore>,
    aggregate: MemoryAggregateService,
    composition: MemoryCompositionService,
    schema: MemorySchemaService,
    providers: Option<Arc<dyn AiProviderRegistry>>,
}

impl LocusMemoryOperations {
    pub fn new(memory: Arc<LocusMemoryStore>, providers: Option<Arc<dyn AiProviderRegistry>>) -> Self {
        Self {
            aggregate: MemoryAggregateService::new(memory.node_store.clone()),
            composition: MemoryCompositionService::new(memory.node_store.clone()),
            schema: MemorySchemaService::new(),
            memory,
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
                scope: map_scope(&request.scope),
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

        let service = MemoryTransformService::new(self.memory.node_store.clone(), providers)
            .with_semantic_index(self.memory.semantic_index.clone());
        let result = service
            .execute(&LocusTransformRequest {
                scope: map_scope(&request.scope),
                filter: map_filter(&request.filter),
                operation: map_transform_operation(request.operation),
                dry_run: request.dry_run,
                batch_size: request.batch_size,
                max_nodes: request.max_nodes,
                provider_id: request.provider_id.clone(),
                model: request.model.clone(),
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
                scope: map_scope(&request.scope),
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
            evict_operations: schema.evict_operations,
        })
    }

    async fn evict(&self, request: &MemoryEvictRequest) -> Result<MemoryEvictResponse> {
        let service = MemoryEvictService::new(self.memory.node_store.clone())
            .with_semantic_index(self.memory.semantic_index.clone());
        let result = service
            .execute(&LocusEvictRequest {
                mode: map_evict_mode(request.mode),
                scope: map_scope(&request.scope),
                filter: map_filter(&request.filter),
                sync_keys: request.sync_keys.clone(),
                node_ids: request.node_ids.clone(),
                dry_run: request.dry_run,
                force: request.force,
                max_nodes: request.max_nodes,
                include_calibration: request.include_calibration,
                include_checkpoints: request.include_checkpoints,
            })
            .await
            .map_err(|e| StasisError::PortFailure(format!("locus evict failed: {e}")))?;

        Ok(MemoryEvictResponse {
            dry_run: result.dry_run,
            deleted: result.deleted,
            blocked: result.blocked,
            not_found: result.not_found,
            skipped: result.skipped,
            would_delete: result.would_delete,
            calibrations_deleted: result.calibrations_deleted,
            checkpoints_deleted: result.checkpoints_deleted,
            records: result.records.iter().map(map_evict_record).collect(),
        })
    }
}

fn map_transform_operation(value: MemoryTransformOperation) -> LocusTransformOperation {
    match value {
        MemoryTransformOperation::EmbedBackfill => LocusTransformOperation::EmbedBackfill,
        MemoryTransformOperation::ReindexEmbeddings => LocusTransformOperation::ReindexEmbeddings,
        MemoryTransformOperation::EmbedTagBackfill => LocusTransformOperation::EmbedTagBackfill,
        MemoryTransformOperation::ReindexTagEmbeddings => LocusTransformOperation::ReindexTagEmbeddings,
    }
}

fn map_evict_mode(value: MemoryEvictMode) -> LocusEvictMode {
    match value {
        MemoryEvictMode::BySyncKeys => LocusEvictMode::BySyncKeys,
        MemoryEvictMode::ByNodeIds => LocusEvictMode::ByNodeIds,
        MemoryEvictMode::ByFilter => LocusEvictMode::ByFilter,
        MemoryEvictMode::PurgeSession => LocusEvictMode::PurgeSession,
    }
}

fn map_evict_record(record: &LocusEvictRecord) -> MemoryEvictRecord {
    MemoryEvictRecord {
        node_id: record.node_id.clone(),
        sync_key: record.sync_key.clone(),
        status: record.status.clone(),
        reason: record.reason.clone(),
        inbound_references: record
            .inbound_references
            .as_ref()
            .map(map_inbound_references),
    }
}

fn map_inbound_references(value: &LocusInboundReferencesPreview) -> MemoryInboundReferencesPreview {
    MemoryInboundReferencesPreview {
        child_parent_links: value.child_parent_links.clone(),
        incoming_semantic_refs: value.incoming_semantic_refs.clone(),
    }
}
