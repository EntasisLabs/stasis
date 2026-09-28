use std::sync::Arc;

use async_trait::async_trait;
use locus_sdk::application::memory_evict::MemoryEvictService;
use locus_sdk::domain::evict::{
    InboundReferencesPreview as LocusInboundReferencesPreview, MemoryEvictMode as LocusEvictMode,
    MemoryEvictRecord as LocusEvictRecord, MemoryEvictRequest as LocusEvictRequest,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use locus_sdk::prelude::HttpSystem1;
use locus_sdk::prelude::{
    AiProviderRegistry, MemoryAggregateRequest as LocusAggregateRequest, MemoryAggregateService,
    MemoryCompositionService, MemoryDailyRollupRequest, MemoryGroupBy, MemoryReflexKind,
    MemoryReflexService, MemorySchemaService, MemoryStimulus,
    MemoryTransformOperation as LocusTransformOperation,
    MemoryTransformRequest as LocusTransformRequest, MemoryTransformService, ReflexGate,
    ReflexPolicy, System1Response, memory_reflex_questions,
};

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::locus_memory_mapping::{map_filter, map_scope};
use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;
use crate::ports::outbound::memory::memory_models::{
    MemoryAggregateRequest, MemoryAggregateResponse, MemoryEvictMode, MemoryEvictRecord,
    MemoryEvictRequest, MemoryEvictResponse, MemoryInboundReferencesPreview,
    MemoryReflexAggregateHint, MemoryReflexFindHint, MemoryReflexPersistHint, MemoryReflexPolicy,
    MemoryReflexPropositions, MemoryReflexRecallHint, MemoryReflexRequest, MemoryReflexResponse,
    MemoryRollupRequest, MemoryRollupResponse, MemorySchemaResponse, MemoryTransformOperation,
    MemoryTransformRequest, MemoryTransformResponse,
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
    pub fn new(
        memory: Arc<LocusMemoryStore>,
        providers: Option<Arc<dyn AiProviderRegistry>>,
    ) -> Self {
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
            reflex_actions: schema.reflex_actions,
            decision_types: schema.decision_types,
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

    async fn reflex(&self, request: &MemoryReflexRequest) -> Result<MemoryReflexResponse> {
        let service = reflex_service(request)?;
        let stimulus = MemoryStimulus {
            text: request.text.clone(),
            role: request.role.clone(),
            scope: map_scope(&request.scope),
            metadata: request.metadata.clone(),
            ..Default::default()
        };
        let reflex = if let Some(wire) = &request.system1_response {
            let questions = memory_reflex_questions();
            let decision = System1Response::parse_wire(&questions, wire).map_err(|err| {
                StasisError::PortFailure(format!("locus reflex system1 response: {err}"))
            })?;
            service.apply(&stimulus, &decision).map_err(|err| {
                StasisError::PortFailure(format!("locus reflex apply failed: {err}"))
            })?
        } else {
            service.decide(&stimulus).await.map_err(|err| {
                StasisError::PortFailure(format!("locus reflex decide failed: {err}"))
            })?
        };
        Ok(map_reflex(&reflex))
    }
}

fn reflex_service(request: &MemoryReflexRequest) -> Result<MemoryReflexService> {
    let endpoint = request
        .system1_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let service = match (request.system1_response.is_some(), endpoint) {
        (true, _) | (false, None) => MemoryReflexService::heuristic(),
        (false, Some(endpoint)) => http_reflex_service(endpoint, request)?,
    };
    Ok(match &request.policy {
        Some(policy) => service.with_policy(map_reflex_policy(policy)),
        None => service,
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn http_reflex_service(
    endpoint: &str,
    request: &MemoryReflexRequest,
) -> Result<MemoryReflexService> {
    let mut http = HttpSystem1::new(endpoint);
    if let Some(model) = request
        .system1_model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        http = http.with_model(model);
    }
    if let Some(api_key) = request
        .system1_api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        http = http.with_api_key(api_key);
    }
    Ok(MemoryReflexService::new(Arc::new(http)))
}

#[cfg(not(all(not(target_arch = "wasm32"), feature = "native")))]
fn http_reflex_service(
    _endpoint: &str,
    _request: &MemoryReflexRequest,
) -> Result<MemoryReflexService> {
    Err(StasisError::PortFailure(
        "locus reflex system1_endpoint requires the native feature. Pass system1_response to apply a finished forward pass, or omit the endpoint to use the offline heuristic decider.".to_string(),
    ))
}

fn map_reflex_policy(policy: &MemoryReflexPolicy) -> ReflexPolicy {
    ReflexPolicy {
        min_choice_confidence: policy.min_choice_confidence,
        min_salience: policy.min_salience,
        read_floor: policy.read_floor,
        write_floor: policy.write_floor,
        escalate_at: policy.escalate_at,
        page_limit: policy.page_limit,
    }
}

fn map_reflex(reflex: &locus_sdk::prelude::MemoryReflex) -> MemoryReflexResponse {
    MemoryReflexResponse {
        schema_version: reflex.schema_version.clone(),
        stimulus_id: reflex.stimulus_id.clone(),
        kind: reflex_kind_name(reflex.kind).to_string(),
        action: reflex.action.as_str().to_string(),
        topic: reflex.topic.clone(),
        salience: reflex.salience,
        salience_label: reflex.salience_label.clone(),
        salience_confidence: reflex.salience_confidence,
        confidence: reflex.confidence,
        propositions: MemoryReflexPropositions {
            references_prior: reflex.propositions.references_prior,
            should_persist: reflex.propositions.should_persist,
            needs_system2: reflex.propositions.needs_system2,
        },
        gate: reflex_gate_name(reflex.gate).to_string(),
        companions: reflex
            .companions
            .iter()
            .map(|action| action.as_str().to_string())
            .collect(),
        recall: reflex.recall.as_ref().map(|recall| MemoryReflexRecallHint {
            query_text: recall.query_text.clone(),
            limit: recall.page.limit,
        }),
        find: reflex.find.as_ref().map(|find| MemoryReflexFindHint {
            text_contains: find.filter.text_contains.clone(),
            limit: find.page.limit,
        }),
        aggregate: reflex
            .aggregate
            .as_ref()
            .map(|aggregate| MemoryReflexAggregateHint {
                max_groups: aggregate.max_groups,
                max_nodes: aggregate.max_nodes,
            }),
        persist: reflex
            .persist
            .as_ref()
            .map(|persist| MemoryReflexPersistHint {
                text: persist.text.clone(),
                role: persist.role.clone(),
            }),
        decider_id: reflex.decider_id.clone(),
        checkpoint: reflex.checkpoint.clone(),
    }
}

fn reflex_kind_name(kind: MemoryReflexKind) -> &'static str {
    match kind {
        MemoryReflexKind::Dispatch => "dispatch",
        MemoryReflexKind::Ignore => "ignore",
        MemoryReflexKind::Escalate => "escalate",
    }
}

fn reflex_gate_name(gate: ReflexGate) -> &'static str {
    match gate {
        ReflexGate::Accepted => "accepted",
        ReflexGate::BlankStimulus => "blank_stimulus",
        ReflexGate::BelowSalience => "below_salience",
        ReflexGate::LowConfidence => "low_confidence",
        ReflexGate::PropositionDisagreement => "proposition_disagreement",
        ReflexGate::System2Required => "system2_required",
    }
}

fn map_transform_operation(value: MemoryTransformOperation) -> LocusTransformOperation {
    match value {
        MemoryTransformOperation::EmbedBackfill => LocusTransformOperation::EmbedBackfill,
        MemoryTransformOperation::ReindexEmbeddings => LocusTransformOperation::ReindexEmbeddings,
        MemoryTransformOperation::EmbedTagBackfill => LocusTransformOperation::EmbedTagBackfill,
        MemoryTransformOperation::ReindexTagEmbeddings => {
            LocusTransformOperation::ReindexTagEmbeddings
        }
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
