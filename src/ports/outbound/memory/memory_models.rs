use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Default)]
pub struct MemoryScope {
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

#[derive(Clone, Debug)]
pub struct MemoryRecallRequest {
    pub scope: MemoryScope,
    pub current_avec: Option<MemoryAvecState>,
    pub query_text: Option<String>,
    pub limit: usize,
    pub alpha: f32,
    pub beta: f32,
    pub fallback_policy: MemoryFallbackPolicy,
    pub strictness: MemoryStrictnessMode,
    pub include_explain: bool,
}

impl Default for MemoryRecallRequest {
    fn default() -> Self {
        Self {
            scope: MemoryScope::default(),
            current_avec: None,
            query_text: None,
            limit: 20,
            alpha: 0.7,
            beta: 0.3,
            fallback_policy: MemoryFallbackPolicy::OnEmpty,
            strictness: MemoryStrictnessMode::Balanced,
            include_explain: false,
        }
    }
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
}

#[derive(Clone, Debug)]
pub struct MemoryTransformRequest {
    pub scope: MemoryScope,
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
}
