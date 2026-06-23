use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct MemoryScope {
    pub tenant_id: Option<String>,
    pub session_ids: Option<Vec<String>>,
    pub tiers: Option<Vec<String>>,
    pub from_utc: Option<DateTime<Utc>>,
    pub to_utc: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryAvecState {
    pub stability: f32,
    pub friction: f32,
    pub logic: f32,
    pub autonomy: f32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryFallbackPolicy {
    Never,
    #[default]
    OnEmpty,
    Always,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryStrictnessMode {
    Precision,
    #[default]
    Balanced,
    Recall,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryMetricRange {
    pub min: Option<f32>,
    pub max: Option<f32>,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryFilter {
    pub has_embedding: Option<bool>,
    pub embedding_model: Option<String>,
    pub psi: Option<MemoryMetricRange>,
    pub rho: Option<MemoryMetricRange>,
    pub kappa: Option<MemoryMetricRange>,
    pub text_contains: Option<String>,
    pub tags_contains: Option<Vec<String>>,
    pub has_tag: Option<String>,
    pub indexed_tags: Option<Vec<String>>,
    pub tag_prefix: Option<String>,
    pub has_semantic_links: Option<bool>,
    pub link_rel: Option<String>,
    pub link_target: Option<String>,
    pub links_to_ref: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemorySemanticLink {
    pub rel: String,
    pub target: String,
    pub confidence: Option<f32>,
}

#[derive(Clone, Debug)]
pub struct MemoryRecallRequest {
    pub scope: MemoryScope,
    pub filter: MemoryFilter,
    pub current_avec: Option<MemoryAvecState>,
    pub query_text: Option<String>,
    pub limit: usize,
    pub alpha: f32,
    pub beta: f32,
    pub gamma: f32,
    pub fallback_policy: MemoryFallbackPolicy,
    pub strictness: MemoryStrictnessMode,
    pub include_explain: bool,
}

impl Default for MemoryRecallRequest {
    fn default() -> Self {
        Self {
            scope: MemoryScope::default(),
            filter: MemoryFilter::default(),
            current_avec: None,
            query_text: None,
            limit: 20,
            alpha: 0.7,
            beta: 0.3,
            gamma: 0.0,
            fallback_policy: MemoryFallbackPolicy::OnEmpty,
            strictness: MemoryStrictnessMode::Balanced,
            include_explain: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemorySortField {
    #[default]
    Timestamp,
    UpdatedAt,
    Psi,
    Rho,
    Kappa,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemorySortDirection {
    Asc,
    #[default]
    Desc,
}

#[derive(Clone, Debug)]
pub struct MemoryFindRequest {
    pub scope: MemoryScope,
    pub filter: MemoryFilter,
    pub limit: usize,
    pub cursor: Option<String>,
    pub sort_field: MemorySortField,
    pub sort_direction: MemorySortDirection,
}

impl Default for MemoryFindRequest {
    fn default() -> Self {
        Self {
            scope: MemoryScope::default(),
            filter: MemoryFilter::default(),
            limit: 50,
            cursor: None,
            sort_field: MemorySortField::Timestamp,
            sort_direction: MemorySortDirection::Desc,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryNode {
    pub raw: String,
    pub session_id: String,
    pub tier: String,
    pub timestamp: DateTime<Utc>,
    pub compression_depth: i32,
    pub parent_node_id: Option<String>,
    pub sync_key: String,
    pub context_summary: Option<String>,
    pub semantic_tags: Option<Vec<String>>,
    pub semantic_links: Option<Vec<MemorySemanticLink>>,
    pub embedding_model: Option<String>,
    pub embedding_dimensions: Option<usize>,
    pub embedded_at: Option<DateTime<Utc>>,
    pub rho: f32,
    pub kappa: f32,
    pub psi: f32,
    pub user_avec: MemoryAvecState,
    pub model_avec: MemoryAvecState,
    pub compression_avec: Option<MemoryAvecState>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryFindResponse {
    pub retrieved: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub nodes: Vec<MemoryNode>,
    pub node_sync_keys: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryRecallResponse {
    pub retrieved: usize,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub retrieval_path: Option<String>,
    pub fallback_triggered: bool,
    pub fallback_reason: Option<String>,
    pub nodes: Vec<MemoryNode>,
    pub node_sync_keys: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MemoryStoreRequest {
    pub session_id: String,
    pub raw_node: String,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStoreResponse {
    pub node_id: String,
    pub psi: f32,
    pub valid: bool,
    pub validation_error: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryAggregateRequest {
    pub scope: MemoryScope,
    pub max_groups: usize,
    pub max_nodes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryAggregateResponse {
    pub total_groups: usize,
    pub scanned_nodes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryTransformOperation {
    #[default]
    EmbedBackfill,
    ReindexEmbeddings,
    EmbedTagBackfill,
    ReindexTagEmbeddings,
}

#[derive(Clone, Debug)]
pub struct MemoryTransformRequest {
    pub scope: MemoryScope,
    pub filter: MemoryFilter,
    pub operation: MemoryTransformOperation,
    pub dry_run: bool,
    pub batch_size: usize,
    pub max_nodes: usize,
    pub provider_id: Option<String>,
    pub model: Option<String>,
}

impl Default for MemoryTransformRequest {
    fn default() -> Self {
        Self {
            scope: MemoryScope::default(),
            filter: MemoryFilter::default(),
            operation: MemoryTransformOperation::EmbedBackfill,
            dry_run: true,
            batch_size: 100,
            max_nodes: 5000,
            provider_id: None,
            model: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryTransformResponse {
    pub scanned: usize,
    pub selected: usize,
    pub updated: usize,
    pub skipped: usize,
    pub failed: usize,
    pub duplicate: usize,
    pub failures: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryRollupRequest {
    pub scope: MemoryScope,
    pub max_days: usize,
    pub max_nodes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryRollupResponse {
    pub total_groups: usize,
    pub scanned_nodes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MemorySchemaResponse {
    pub schema_version: String,
    pub sort_fields: Vec<String>,
    pub filter_fields: Vec<String>,
    pub group_by_fields: Vec<String>,
    pub fallback_policies: Vec<String>,
    pub strictness_modes: Vec<String>,
    pub transform_operations: Vec<String>,
    pub evict_operations: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryEvictMode {
    #[default]
    BySyncKeys,
    ByNodeIds,
    ByFilter,
    PurgeSession,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryInboundReferencesPreview {
    pub child_parent_links: Vec<String>,
    pub incoming_semantic_refs: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryEvictRecord {
    pub node_id: String,
    pub sync_key: String,
    pub status: String,
    pub reason: Option<String>,
    pub inbound_references: Option<MemoryInboundReferencesPreview>,
}

#[derive(Clone, Debug)]
pub struct MemoryEvictRequest {
    pub mode: MemoryEvictMode,
    pub scope: MemoryScope,
    pub filter: MemoryFilter,
    pub sync_keys: Option<Vec<String>>,
    pub node_ids: Option<Vec<String>>,
    pub dry_run: bool,
    pub force: bool,
    pub max_nodes: usize,
    pub include_calibration: bool,
    pub include_checkpoints: bool,
}

impl Default for MemoryEvictRequest {
    fn default() -> Self {
        Self {
            mode: MemoryEvictMode::BySyncKeys,
            scope: MemoryScope::default(),
            filter: MemoryFilter::default(),
            sync_keys: None,
            node_ids: None,
            dry_run: true,
            force: false,
            max_nodes: 5000,
            include_calibration: false,
            include_checkpoints: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryEvictResponse {
    pub dry_run: bool,
    pub deleted: usize,
    pub blocked: usize,
    pub not_found: usize,
    pub skipped: usize,
    pub would_delete: Vec<String>,
    pub calibrations_deleted: usize,
    pub checkpoints_deleted: usize,
    pub records: Vec<MemoryEvictRecord>,
}

#[derive(Clone, Debug)]
pub struct MemoryGraphRequest {
    pub scope: MemoryScope,
    pub filter: MemoryFilter,
    pub include_lineage: bool,
    pub include_semantic: bool,
    pub include_session_topology: bool,
    pub rel: Option<String>,
    pub target_prefix: Option<String>,
    pub limit: usize,
}

impl Default for MemoryGraphRequest {
    fn default() -> Self {
        Self {
            scope: MemoryScope::default(),
            filter: MemoryFilter::default(),
            include_lineage: true,
            include_semantic: true,
            include_session_topology: true,
            rel: None,
            target_prefix: None,
            limit: 200,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryGraphResponse {
    pub sessions: Vec<Value>,
    pub nodes: Vec<Value>,
    pub edges: Vec<Value>,
    pub retrieved: usize,
}
