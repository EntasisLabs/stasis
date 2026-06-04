use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use crate::application::dto::{
    ClusterNodeHealthRow, EndpointDiagnosticsReadModelRow, EndpointFailureRateTrendRow,
    ListClusterNodeHealthRequest, ListEndpointDiagnosticsReadModelRequest,
    ListEndpointFailureRateTrendsRequest,
};
use crate::application::runtime::runtime_factory::{RuntimeComposition, RuntimeFactory};
use crate::application::runtime::in_memory_runtime::InMemoryRuntime;
use crate::application::telemetry::request_context::trace_id_for_enqueue;
use crate::dashboard::dto::{
    AttemptInspectorDto, ClusterMapDto, DashboardDto, EndpointRowDto, EventInspectorDto,
    InspectorView, JobInspectorDto, JobRowDto, OutboxEventRowDto, RecurringDefinitionRowDto,
    SystemKpiDto, UiListPanel,
};
use crate::dashboard::mappers::{
    map_cluster_health_row, map_endpoint_inspector, map_endpoint_row, map_job_to_row,
    map_node_inspector, map_outbox_to_row, map_recurring_definition_row,
};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{BackoffPolicy, JobState, NewJob};
use crate::domain::runtime::outbox::OutboxEvent;
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::infrastructure::runtime::grapheme_sdk_workflow_reflection::GraphemeSdkWorkflowReflection;
use crate::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
use crate::infrastructure::runtime::in_memory_workflow_definition_store::InMemoryWorkflowDefinitionStore;
use crate::infrastructure::runtime::surreal_cluster_node_store::SurrealClusterNodeStore;
use crate::infrastructure::runtime::surreal_delivery_endpoint_store::SurrealDeliveryEndpointStore;
use crate::infrastructure::runtime::surreal_endpoint_delivery_status_store::SurrealEndpointDeliveryStatusStore;
use crate::infrastructure::runtime::surreal_workflow_definition_store::SurrealWorkflowDefinitionStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;
use crate::ports::outbound::runtime::workflow_definition_store::WorkflowDefinitionStore;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;
use crate::ports::outbound::runtime::workflow_reflection::{
    WorkflowModuleInfoReflection, WorkflowModuleSearchReflection,
    WorkflowModuleTypesReflection, WorkflowReflectionPort, WorkflowSourceReflection,
};
use crate::sdk::control_plane_sdk::ControlPlaneSdk;

type DashboardControlStore =
    CompositeControlPlaneStore<InMemoryDeliveryEndpointStore, InMemoryClusterNodeStore>;
type DashboardControlPlane = ControlPlaneSdk<DashboardControlStore>;
type DashboardSurrealControlStore =
    CompositeControlPlaneStore<SurrealDeliveryEndpointStore, SurrealClusterNodeStore>;
type DashboardSurrealControlPlane = ControlPlaneSdk<DashboardSurrealControlStore>;

#[derive(Clone)]
enum DashboardControlPlaneKind {
    InMemory(DashboardControlPlane),
    Surreal(DashboardSurrealControlPlane),
}

#[derive(Clone, Debug)]
pub enum InspectEntity {
    Job(String),
    Attempt(String),
    Node(String),
    Endpoint(String),
    Event(String),
}

#[derive(Clone, Debug)]
pub struct WorkflowSaveRequest {
    pub workflow_id: String,
    pub queue: String,
    pub source: String,
    pub compile_mode_hint: Option<String>,
    pub graph_state_json: Option<String>,
    pub graph_modules_csv: Option<String>,
    pub graph_function_steps_csv: Option<String>,
    pub graph_function_inputs_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct WorkflowSaveResult {
    pub workflow_id: String,
    pub queue: String,
    pub revision_id: String,
    pub executable_count: usize,
}

#[derive(Clone, Debug)]
pub struct WorkflowExecuteResult {
    pub workflow_id: String,
    pub queue: String,
    pub revision_id: String,
    pub executable_count: usize,
    pub graph_function_steps_csv: String,
    pub graph_function_inputs_json: String,
    pub source_bytes: usize,
    pub reflected_at_utc: String,
    pub leased_job_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct WorkflowRunDraftRequest {
    pub workflow_id: String,
    pub queue: String,
    pub source: String,
    pub graph_state_json: Option<String>,
    pub graph_modules_csv: Option<String>,
    pub graph_function_steps_csv: Option<String>,
    pub graph_function_inputs_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct WorkflowRunDraftResult {
    pub workflow_id: String,
    pub queue: String,
    pub executable_count: usize,
    pub graph_modules_csv: String,
    pub graph_function_steps_csv: String,
    pub graph_function_inputs_json: String,
    pub source_bytes: usize,
    pub run_id: String,
    pub execution_json: String,
    pub final_state_json: String,
}

#[derive(Clone, Debug)]
pub struct WorkflowSavedRevisionSummary {
    pub workflow_id: String,
    pub revision_id: String,
    pub executable_count: usize,
    pub reflected_at_utc: String,
    pub compile_mode: String,
    pub source: String,
    pub source_bytes: usize,
    pub graph_state_json: String,
    pub graph_modules_csv: String,
    pub graph_function_steps_csv: String,
    pub graph_function_inputs_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowDiagnosticSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowDiagnostic {
    pub severity: WorkflowDiagnosticSeverity,
    pub message: String,
    pub code: Option<String>,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowDiagnosticsResult {
    pub enabled: bool,
    pub provider: String,
    pub summary: String,
    pub diagnostics: Vec<WorkflowDiagnostic>,
}

fn parse_leading_usize(input: &str) -> Option<(usize, &str)> {
    let digits_len = input.chars().take_while(|ch| ch.is_ascii_digit()).count();
    if digits_len == 0 {
        return None;
    }

    let (digits, tail) = input.split_at(digits_len);
    Some((digits.parse().ok()?, tail))
}

fn extract_line_column(message: &str) -> (Option<usize>, Option<usize>) {
    if let Some(anchor_index) = message.find("-->") {
        let tail = message[(anchor_index + 3)..].trim_start();
        if let Some((line, rest)) = parse_leading_usize(tail) {
            let rest = rest.trim_start();
            if let Some(stripped) = rest.strip_prefix(':') {
                let stripped = stripped.trim_start();
                if let Some((column, _)) = parse_leading_usize(stripped) {
                    return (Some(line), Some(column));
                }
            }
        }
    }

    for marker in ["line ", "Line "] {
        if let Some(anchor_index) = message.find(marker) {
            let tail = &message[(anchor_index + marker.len())..];
            if let Some((line, rest)) = parse_leading_usize(tail) {
                let rest_lower = rest.to_ascii_lowercase();
                if let Some(column_anchor) = rest_lower.find("column ") {
                    let col_tail = &rest[(column_anchor + "column ".len())..];
                    if let Some((column, _)) = parse_leading_usize(col_tail) {
                        return (Some(line), Some(column));
                    }
                }

                return (Some(line), None);
            }
        }
    }

    (None, None)
}

fn reflection_code_for_error(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("capability") {
        "REFLECTION_CAPABILITY"
    } else if lowered.contains("schema")
        || lowered.contains("type")
        || lowered.contains("state machine")
    {
        "REFLECTION_SCHEMA"
    } else {
        "REFLECTION"
    }
}

fn normalize_graph_modules_csv(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };

    let mut seen = HashSet::new();
    let mut normalized = Vec::new();

    for part in raw.split(',') {
        let module = part.trim().to_ascii_lowercase();
        let is_allowed = matches!(
            module.as_str(),
            "core"
                | "html"
                | "json"
                | "csv"
                | "yaml"
                | "docs"
                | "io"
                | "http"
                | "web"
                | "websearch"
                | "tcp"
                | "smtp"
                | "sql"
                | "surreal"
                | "memory"
                | "runtime"
                | "secrets"
                | "textops"
                | "healthcheck"
        );

        if is_allowed && seen.insert(module.clone()) {
            normalized.push(module);
        }
    }

    normalized.join(",")
}

fn normalize_graph_function_steps_csv(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };

    let mut seen = HashSet::new();
    let mut normalized = Vec::new();

    for part in raw.split(',') {
        let token = part.trim().to_ascii_lowercase();
        let Some((module_id, function_id)) = token.split_once('.') else {
            continue;
        };
        let module_id = module_id.trim();
        let function_id = function_id.trim();

        if !matches!(
            module_id,
            "core"
                | "html"
                | "json"
                | "csv"
                | "yaml"
                | "docs"
                | "io"
                | "http"
                | "web"
                | "websearch"
                | "tcp"
                | "smtp"
                | "sql"
                | "surreal"
                | "memory"
                | "runtime"
                | "secrets"
                | "textops"
                | "healthcheck"
        ) {
            continue;
        }
        if function_id.is_empty()
            || !function_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }

        let normalized_token = format!("{module_id}.{function_id}");
        if seen.insert(normalized_token.clone()) {
            normalized.push(normalized_token);
        }
    }

    normalized.join(",")
}

fn normalize_graph_function_inputs_json(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return "{}".to_string();
    };

    let parsed = serde_json::from_str::<serde_json::Value>(raw);
    let Ok(value) = parsed else {
        return "{}".to_string();
    };
    let Some(obj) = value.as_object() else {
        return "{}".to_string();
    };

    let mut normalized = serde_json::Map::new();
    for (key, value) in obj {
        if key.trim().is_empty() {
            continue;
        }
        if let Some(payload) = value.as_str() {
            normalized.insert(key.clone(), serde_json::Value::String(payload.to_string()));
        }
    }

    serde_json::Value::Object(normalized).to_string()
}

fn normalize_graph_state_json(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return "{}".to_string();
    };

    let parsed = serde_json::from_str::<serde_json::Value>(raw);
    let Ok(value) = parsed else {
        return "{}".to_string();
    };

    value.to_string()
}

fn validate_compile_graph_state_contract(
    graph: &serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    let query = graph
        .get("query")
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            StasisError::PortFailure(
                "graph_state compile contract requires query.steps".to_string(),
            )
        })?;
    let steps = query
        .get("steps")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            StasisError::PortFailure(
                "graph_state compile contract requires query.steps".to_string(),
            )
        })?;

    if steps.is_empty() {
        return Err(StasisError::PortFailure(
            "graph_state compile contract requires at least one query step".to_string(),
        ));
    }

    if let Some(iterators) = graph.get("iterators") {
        let iterators = iterators.as_array().ok_or_else(|| {
            StasisError::PortFailure(
                "graph_state compile contract requires iterators to be an array"
                    .to_string(),
            )
        })?;

        for (index, iterator) in iterators.iter().enumerate() {
            let iterator_obj = iterator.as_object().ok_or_else(|| {
                StasisError::PortFailure(format!(
                    "graph_state compile contract iterator[{index}] must be an object"
                ))
            })?;
            let loop_obj = iterator_obj
                .get("loop")
                .and_then(|value| value.as_object())
                .ok_or_else(|| {
                    StasisError::PortFailure(format!(
                        "graph_state compile contract iterator[{index}] requires loop"
                    ))
                })?;

            let max = loop_obj
                .get("max")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| {
                    StasisError::PortFailure(format!(
                        "graph_state compile contract iterator[{index}] requires bounded loop.max"
                    ))
                })?;
            if max == 0 {
                return Err(StasisError::PortFailure(format!(
                    "graph_state compile contract iterator[{index}] requires bounded loop.max"
                )));
            }

            let each = loop_obj
                .get("each")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .unwrap_or("");
            if each.is_empty() || !each.starts_with('$') {
                return Err(StasisError::PortFailure(format!(
                    "graph_state compile contract iterator[{index}] requires loop.each path"
                )));
            }
        }
    }

    Ok(())
}

fn validate_topology_graph_state_contract(
    graph: &serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    let nodes = graph
        .get("nodes")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            StasisError::PortFailure(
                "graph_state topology contract requires nodes array".to_string(),
            )
        })?;
    let _edges = graph
        .get("edges")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            StasisError::PortFailure(
                "graph_state topology contract requires edges array".to_string(),
            )
        })?;

    for (index, node) in nodes.iter().enumerate() {
        let node_id = node
            .as_object()
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_str())
            .map(str::trim)
            .unwrap_or("");
        if node_id.is_empty() {
            return Err(StasisError::PortFailure(format!(
                "graph_state topology contract node[{index}] requires id"
            )));
        }
    }

    Ok(())
}

fn validate_and_normalize_graph_state_json(raw: Option<&str>) -> Result<String> {
    let normalized = normalize_graph_state_json(raw);
    if normalized == "{}" {
        return Ok(normalized);
    }

    let value = serde_json::from_str::<serde_json::Value>(&normalized).map_err(|err| {
        StasisError::PortFailure(format!(
            "graph_state must be valid JSON object: {err}"
        ))
    })?;
    let graph = value.as_object().ok_or_else(|| {
        StasisError::PortFailure("graph_state must be a JSON object".to_string())
    })?;

    let has_compile_shape = graph.contains_key("query") || graph.contains_key("iterators");
    let has_topology_shape =
        graph.contains_key("nodes") || graph.contains_key("edges") || graph.contains_key("version");

    if has_compile_shape {
        validate_compile_graph_state_contract(graph)?;
    }
    if has_topology_shape {
        validate_topology_graph_state_contract(graph)?;
    }

    Ok(normalized)
}

fn graph_state_contains_compile_shape(graph_state_json: &str) -> bool {
    let parsed = serde_json::from_str::<serde_json::Value>(graph_state_json);
    let Ok(value) = parsed else {
        return false;
    };
    let Some(graph) = value.as_object() else {
        return false;
    };

    graph.contains_key("query") || graph.contains_key("iterators")
}

fn normalize_compile_mode_hint(raw: Option<&str>) -> Option<String> {
    let value = raw.map(str::trim).unwrap_or_default();
    match value {
        "graph_compiled" | "legacy_function_steps" | "source_passthrough" => {
            Some(value.to_string())
        }
        _ => None,
    }
}

const WORKFLOW_GRAPHEME_JOB_TYPE: &str = "workflow.grapheme.run";
const GRAPHEME_INLINE_PAYLOAD_PREFIX: &str = "grapheme:inline:";

fn workflow_execution_payload_ref(revision: &WorkflowRevision) -> String {
    format!("{}{}", GRAPHEME_INLINE_PAYLOAD_PREFIX, revision.source)
}

fn build_workflow_execution_job(
    workflow_id: &str,
    revision: &WorkflowRevision,
    queue: &str,
    scheduled_at: chrono::DateTime<Utc>,
) -> NewJob {
    let job_id = format!("job-wf-{}-{}", workflow_id, scheduled_at.timestamp_millis());

    NewJob {
        id: job_id.clone(),
        queue: queue.to_string(),
        job_type: WORKFLOW_GRAPHEME_JOB_TYPE.to_string(),
        payload_ref: workflow_execution_payload_ref(revision),
        priority: 100,
        max_attempts: 3,
        idempotency_key: format!("idem-{}", job_id),
        correlation_id: workflow_id.to_string(),
        causation_id: revision.revision_id.clone(),
        trace_id: trace_id_for_enqueue(|| format!("trace-wf-{}", workflow_id)),
        sttp_input_node_id: format!("sttp:in:workflow:{workflow_id}"),
        scheduled_at,
        backoff_policy: BackoffPolicy::default(),
    }
}

#[async_trait]
pub trait DashboardQueryService: Send + Sync {
    async fn dashboard(&self, inspect: Option<InspectEntity>) -> Result<DashboardDto>;
    async fn jobs_stream(&self) -> Result<UiListPanel<JobRowDto>>;
    async fn outbox_stream(&self) -> Result<UiListPanel<OutboxEventRowDto>>;
    async fn endpoint_stream(&self) -> Result<UiListPanel<EndpointRowDto>>;
    async fn recurring_stream(&self) -> Result<UiListPanel<RecurringDefinitionRowDto>>;
    async fn cluster_stream(&self) -> Result<ClusterMapDto>;
    async fn scheduler_materialize_now(&self, scheduler_id: &str) -> Result<usize>;
    async fn scheduler_process_queue_once(
        &self,
        queue: &str,
        worker_id: &str,
    ) -> Result<Option<String>>;
    async fn scheduler_publish_pending_now(&self, limit: usize) -> Result<usize>;
    async fn scheduler_replay_dead_letter_now(&self, job_id: &str) -> Result<bool>;
    async fn workflow_save(&self, request: WorkflowSaveRequest) -> Result<WorkflowSaveResult>;
    async fn workflow_execute(
        &self,
        workflow_id: &str,
        queue: &str,
        worker_id: &str,
    ) -> Result<WorkflowExecuteResult>;
    async fn endpoint_failure_rate_trends(&self) -> Vec<EndpointFailureRateTrendRow>;
    async fn workflow_run_draft(
        &self,
        request: WorkflowRunDraftRequest,
    ) -> Result<WorkflowRunDraftResult>;
    async fn workflow_reflect_source(&self, source: &str) -> Result<WorkflowSourceReflection>;
    async fn workflow_modules_search(&self, query: &str) -> Result<WorkflowModuleSearchReflection>;
    async fn workflow_module_info(&self, module_id: &str) -> Result<Option<WorkflowModuleInfoReflection>>;
    async fn workflow_module_types(&self, module_id: &str) -> Result<Option<WorkflowModuleTypesReflection>>;
    async fn workflow_saved_revision_summary(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowSavedRevisionSummary>>;
    async fn workflow_lsp_diagnostics(&self, source: &str) -> Result<WorkflowDiagnosticsResult>;
    async fn inspect(&self, entity: InspectEntity) -> Result<InspectorView>;
}

#[derive(Clone)]
pub struct RuntimeDashboardQueryService {
    runtime: RuntimeComposition,
    control_plane: DashboardControlPlaneKind,
    workflow_reflection: Arc<dyn WorkflowReflectionPort>,
    workflow_store: Arc<dyn WorkflowDefinitionStore>,
    workflow_engine: Arc<dyn WorkflowEngine>,
}

macro_rules! with_runtime {
    ($service:expr, |$rt:ident| $body:expr) => {
        match &$service.runtime {
            RuntimeComposition::InMemory($rt) => $body,
            RuntimeComposition::Surreal($rt) => $body,
        }
    };
}

macro_rules! with_control_plane {
    ($service:expr, |$cp:ident| $body:expr) => {
        match &$service.control_plane {
            DashboardControlPlaneKind::InMemory($cp) => $body,
            DashboardControlPlaneKind::Surreal($cp) => $body,
        }
    };
}

impl RuntimeDashboardQueryService {
    pub fn new(runtime: Arc<InMemoryRuntime>, control_plane: DashboardControlPlane) -> Self {
        Self::from_in_memory_composition(runtime.as_ref().clone(), control_plane)
    }

    pub fn from_in_memory_composition(
        runtime: InMemoryRuntime,
        control_plane: DashboardControlPlane,
    ) -> Self {
        Self {
            runtime: RuntimeComposition::InMemory(runtime),
            control_plane: DashboardControlPlaneKind::InMemory(control_plane),
            workflow_reflection: Arc::new(GraphemeSdkWorkflowReflection::new()),
            workflow_store: Arc::new(InMemoryWorkflowDefinitionStore::default()),
            workflow_engine: RuntimeFactory::default_workflow_engine(),
        }
    }

    pub fn from_runtime_composition(runtime: RuntimeComposition) -> Self {
        match runtime {
            RuntimeComposition::InMemory(rt) => {
                let endpoint_store = InMemoryDeliveryEndpointStore::default();
                let cluster_store = InMemoryClusterNodeStore::default();
                let status_store = Arc::new(InMemoryEndpointDeliveryStatusStore::default());
                let control_store = CompositeControlPlaneStore::new(endpoint_store, cluster_store);
                let control_plane =
                    ControlPlaneSdk::new_with_status_store(control_store, status_store);

                Self {
                    runtime: RuntimeComposition::InMemory(rt),
                    control_plane: DashboardControlPlaneKind::InMemory(control_plane),
                    workflow_reflection: Arc::new(GraphemeSdkWorkflowReflection::new()),
                    workflow_store: Arc::new(InMemoryWorkflowDefinitionStore::default()),
                    workflow_engine: RuntimeFactory::default_workflow_engine(),
                }
            }
            RuntimeComposition::Surreal(rt) => {
                let endpoint_store = SurrealDeliveryEndpointStore::new(rt.job_store.db());
                let cluster_store = SurrealClusterNodeStore::new(rt.job_store.db());
                let status_store = Arc::new(SurrealEndpointDeliveryStatusStore::new(rt.job_store.db()));
                let control_store = CompositeControlPlaneStore::new(endpoint_store, cluster_store);
                let control_plane =
                    ControlPlaneSdk::new_with_status_store(control_store, status_store);

                let workflow_store: Arc<dyn WorkflowDefinitionStore> =
                    Arc::new(SurrealWorkflowDefinitionStore::new(rt.job_store.db()));

                Self {
                    runtime: RuntimeComposition::Surreal(rt),
                    control_plane: DashboardControlPlaneKind::Surreal(control_plane),
                    workflow_reflection: Arc::new(GraphemeSdkWorkflowReflection::new()),
                    workflow_store,
                    workflow_engine: RuntimeFactory::default_workflow_engine(),
                }
            }
        }
    }

    async fn load_workflow_definition(&self, workflow_id: &str) -> Result<WorkflowDefinition> {
        self.workflow_store
            .get_definition(workflow_id)
            .await?
            .ok_or_else(|| {
                StasisError::PortFailure(format!("workflow definition not found: {workflow_id}"))
            })
    }

    async fn enqueue_runtime_job(&self, job: NewJob) -> Result<()> {
        with_runtime!(self, |rt| rt.enqueue(job).await)
    }

    async fn runtime_process_once(
        &self,
        queue: &str,
        worker_id: &str,
    ) -> Result<Option<String>> {
        with_runtime!(self, |rt| rt.process_once_now(queue, worker_id).await)
    }

    async fn runtime_materialize_recurring(&self, scheduler_id: &str) -> Result<usize> {
        with_runtime!(self, |rt| rt.materialize_recurring_now(scheduler_id).await)
    }

    async fn runtime_publish_pending(&self, limit: usize) -> Result<usize> {
        with_runtime!(self, |rt| rt.publish_pending_events_now(limit).await)
    }

    async fn runtime_replay_dead_letter(&self, job_id: &str) -> Result<bool> {
        with_runtime!(self, |rt| rt.replay_dead_letter_now(job_id).await)
    }

    async fn list_cluster_node_health_rows(&self) -> Result<Vec<ClusterNodeHealthRow>> {
        let request = ListClusterNodeHealthRequest {
            role: None,
            region: None,
            capability_tag: None,
            queue: None,
            health: None,
            offset: 0,
            limit: Some(200),
        };

        with_control_plane!(self, |control_plane| control_plane.list_cluster_node_health(request).await)
    }

    async fn list_endpoint_diagnostics_rows(
        &self,
        endpoint_ids: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        let request = ListEndpointDiagnosticsReadModelRequest {
            endpoint_ids,
            protocol: None,
            min_failure_count: None,
            stale_after_seconds: None,
            unhealthy_only: false,
            include_disabled: true,
            offset: 0,
            limit,
        };

        with_control_plane!(
            self,
            |control_plane| control_plane.list_endpoint_diagnostics_read_model(request).await
        )
    }

    async fn list_endpoint_failure_rate_trends(&self) -> Vec<EndpointFailureRateTrendRow> {
        let request = ListEndpointFailureRateTrendsRequest {
            protocol: None,
            include_disabled: true,
            min_total_attempts: None,
            limit: 100,
        };

        with_control_plane!(self, |control_plane| control_plane
            .list_endpoint_failure_rate_trends(request)
            .await
            .unwrap_or_default())
    }

    async fn list_job_attempts(&self, job_id: &str) -> Result<Vec<crate::domain::runtime::job_attempt::JobAttempt>> {
        with_runtime!(self, |rt| rt.list_job_attempts(job_id).await)
    }

    async fn list_lineage_events_by_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<OutboxEvent>> {
        with_runtime!(self, |rt| rt.list_lineage_events(job_id).await)
    }

    async fn list_all_jobs(&self) -> Result<Vec<crate::domain::runtime::job::Job>> {
        let states = [
            JobState::Enqueued,
            JobState::Leased,
            JobState::Running,
            JobState::Succeeded,
            JobState::Failed,
            JobState::DeadLetter,
            JobState::Canceled,
        ];

        let mut jobs = Vec::new();
        for state in states {
            let mut by_state = with_runtime!(self, |rt| rt.job_store.list_by_state(state).await?);
            jobs.append(&mut by_state);
        }

        jobs.sort_by(|left, right| {
            right
                .scheduled_at
                .cmp(&left.scheduled_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(jobs)
    }

    async fn list_all_outbox_events(&self) -> Result<Vec<OutboxEvent>> {
        let jobs = self.list_all_jobs().await?;
        let mut seen = HashSet::new();
        let mut out = Vec::new();

        for job in jobs {
            for event in self.list_lineage_events_by_job(&job.id).await? {
                if seen.insert(event.event_id.clone()) {
                    out.push(event);
                }
            }
        }

        out.sort_by(|left, right| {
            right
                .event
                .occurred_at
                .cmp(&left.event.occurred_at)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        Ok(out)
    }
}

#[async_trait]
impl DashboardQueryService for RuntimeDashboardQueryService {
    async fn dashboard(&self, inspect: Option<InspectEntity>) -> Result<DashboardDto> {
        let jobs = self.jobs_stream().await?;
        let outbox = self.outbox_stream().await?;
        let cluster = self.cluster_stream().await?;
        let healthy_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.health == "Healthy")
            .count();
        let degraded_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.health == "Degraded")
            .count();
        let offline_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.health == "Offline")
            .count();

        let running_jobs = jobs
            .items
            .iter()
            .filter(|job| job.status == "running")
            .count();
        let enqueued_jobs = jobs
            .items
            .iter()
            .filter(|job| job.status == "enqueued")
            .count();
        let succeeded_jobs = jobs
            .items
            .iter()
            .filter(|job| job.status == "succeeded")
            .count();
        let failed_jobs = jobs
            .items
            .iter()
            .filter(|job| job.status == "failed" || job.status == "dead_letter")
            .count();

        let pending_outbox = outbox
            .items
            .iter()
            .filter(|event| event.delivery_state == "pending")
            .count();
        let failed_outbox = outbox
            .items
            .iter()
            .filter(|event| event.delivery_state == "failed")
            .count();

        let endpoint_trends = self.list_endpoint_failure_rate_trends().await;

        let avg_failure_rate = if endpoint_trends.is_empty() {
            0.0
        } else {
            endpoint_trends
                .iter()
                .map(|row| row.failure_rate)
                .sum::<f64>()
                / endpoint_trends.len() as f64
        };

        let inspector = match inspect {
            Some(entity) => self.inspect(entity).await?,
            None => InspectorView::None,
        };

        Ok(DashboardDto {
            kpis: SystemKpiDto {
                succeeded_jobs,
                failed_jobs,
                enqueued_jobs,
                running_jobs,
                pending_outbox,
                failed_outbox,
                healthy_nodes,
                degraded_nodes,
                offline_nodes,
                endpoint_failure_rate: format!("{:.1}%", avg_failure_rate * 100.0),
            },
            job_stream: jobs,
            outbox_stream: outbox,
            cluster_map: cluster,
            inspector,
        })
    }

    async fn jobs_stream(&self) -> Result<UiListPanel<JobRowDto>> {
        let jobs = self.list_all_jobs().await?;
        let mapped = jobs.iter().map(map_job_to_row).collect::<Vec<_>>();

        Ok(UiListPanel {
            items: mapped.clone(),
            total: Some(mapped.len() as u64),
            cursor: None,
        })
    }

    async fn outbox_stream(&self) -> Result<UiListPanel<OutboxEventRowDto>> {
        let events = self.list_all_outbox_events().await?;
        let mapped = events
            .iter()
            .take(200)
            .map(map_outbox_to_row)
            .collect::<Vec<_>>();

        Ok(UiListPanel {
            items: mapped.clone(),
            total: Some(mapped.len() as u64),
            cursor: None,
        })
    }

    async fn endpoint_stream(&self) -> Result<UiListPanel<EndpointRowDto>> {
        let rows = self.list_endpoint_diagnostics_rows(None, Some(200)).await?;

        let mapped = rows.iter().map(map_endpoint_row).collect::<Vec<_>>();

        Ok(UiListPanel {
            items: mapped.clone(),
            total: Some(mapped.len() as u64),
            cursor: None,
        })
    }

    async fn recurring_stream(&self) -> Result<UiListPanel<RecurringDefinitionRowDto>> {
        let definitions = with_runtime!(self, |rt| rt.recurring_store.list().await?);
        let mut rows = definitions
            .iter()
            .map(map_recurring_definition_row)
            .collect::<Vec<_>>();

        rows.sort_by(|left, right| {
            left.next_run_at
                .cmp(&right.next_run_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        Ok(UiListPanel {
            items: rows.clone(),
            total: Some(rows.len() as u64),
            cursor: None,
        })
    }

    async fn cluster_stream(&self) -> Result<ClusterMapDto> {
        let rows = self.list_cluster_node_health_rows().await?;

        let nodes = rows.iter().map(map_cluster_health_row).collect();

        Ok(ClusterMapDto { nodes })
    }

    async fn scheduler_materialize_now(&self, scheduler_id: &str) -> Result<usize> {
        self.runtime_materialize_recurring(scheduler_id).await
    }

    async fn scheduler_process_queue_once(
        &self,
        queue: &str,
        worker_id: &str,
    ) -> Result<Option<String>> {
        self.runtime_process_once(queue, worker_id).await
    }

    async fn scheduler_publish_pending_now(&self, limit: usize) -> Result<usize> {
        self.runtime_publish_pending(limit).await
    }

    async fn scheduler_replay_dead_letter_now(&self, job_id: &str) -> Result<bool> {
        self.runtime_replay_dead_letter(job_id).await
    }

    async fn workflow_save(&self, request: WorkflowSaveRequest) -> Result<WorkflowSaveResult> {
        let workflow_id = request.workflow_id.trim();
        let queue = request.queue.trim();
        let source = request.source.trim();
        let graph_modules_csv = normalize_graph_modules_csv(request.graph_modules_csv.as_deref());
        let graph_function_steps_csv =
            normalize_graph_function_steps_csv(request.graph_function_steps_csv.as_deref());
        let graph_function_inputs_json =
            normalize_graph_function_inputs_json(request.graph_function_inputs_json.as_deref());
        let graph_state_json =
            validate_and_normalize_graph_state_json(request.graph_state_json.as_deref())?;
        let compile_mode = normalize_compile_mode_hint(request.compile_mode_hint.as_deref())
            .unwrap_or_else(|| {
                if graph_state_contains_compile_shape(graph_state_json.as_str()) {
                    "graph_compiled".to_string()
                } else if !graph_function_steps_csv.is_empty() {
                    "legacy_function_steps".to_string()
                } else {
                    "source_passthrough".to_string()
                }
            });

        if workflow_id.is_empty() {
            return Err(StasisError::PortFailure("workflow_id is required".to_string()));
        }
        if queue.is_empty() {
            return Err(StasisError::PortFailure("queue is required".to_string()));
        }
        if source.is_empty() {
            return Err(StasisError::PortFailure("source is required".to_string()));
        }

        let reflection = self
            .workflow_reflection
            .reflect_executables_from_source(source)?;
        let reflection_receipt_json = serde_json::to_string(&reflection).map_err(|err| {
            StasisError::PortFailure(format!("encode workflow reflection receipt: {err}"))
        })?;
        let now = Utc::now();
        let revision_id = format!(
            "rev-{}-{}",
            now.timestamp_millis(),
            reflection.count.max(1)
        );

        let created_at = self
            .workflow_store
            .get_definition(workflow_id)
            .await?
            .map(|existing| existing.created_at)
            .unwrap_or(now);

        let definition = WorkflowDefinition {
            workflow_id: workflow_id.to_string(),
            queue: queue.to_string(),
            latest_revision_id: revision_id.clone(),
            created_at,
            updated_at: now,
        };

        let revision = WorkflowRevision {
            workflow_id: workflow_id.to_string(),
            revision_id: revision_id.clone(),
            source: source.to_string(),
            graph_state_json,
            compiler_metadata_json: serde_json::json!({
                "compiler_version": "workflow-graph-v1",
                "compile_mode": compile_mode,
                "compiled_at_utc": now.to_rfc3339(),
            })
            .to_string(),
            graph_modules_csv,
            graph_function_steps_csv,
            graph_function_inputs_json,
            reflected_at_utc: now,
            executable_count: reflection.count,
            reflection_receipt_json,
        };

        self.workflow_store.upsert_definition(definition).await?;
        self.workflow_store.insert_revision(revision).await?;

        Ok(WorkflowSaveResult {
            workflow_id: workflow_id.to_string(),
            queue: queue.to_string(),
            revision_id,
            executable_count: reflection.count,
        })
    }

    async fn workflow_execute(
        &self,
        workflow_id: &str,
        queue: &str,
        worker_id: &str,
    ) -> Result<WorkflowExecuteResult> {
        let workflow = self.load_workflow_definition(workflow_id.trim()).await?;
        let revisions = self
            .workflow_store
            .list_revisions(workflow.workflow_id.as_str())
            .await?;
        let latest = revisions
            .iter()
            .find(|revision| revision.revision_id == workflow.latest_revision_id)
            .ok_or_else(|| {
                StasisError::PortFailure(format!(
                    "workflow revision not found: {}",
                    workflow.latest_revision_id
                ))
            })?;
        let resolved_queue = if queue.trim().is_empty() {
            workflow.queue.clone()
        } else {
            queue.trim().to_string()
        };

        let now = Utc::now();
        let execution_job =
            build_workflow_execution_job(workflow_id.trim(), latest, resolved_queue.as_str(), now);
        let enqueued_job_id = execution_job.id.clone();
        self.enqueue_runtime_job(execution_job).await?;

        let leased_job_id = self
            .runtime_process_once(resolved_queue.as_str(), worker_id)
            .await?;

        Ok(WorkflowExecuteResult {
            workflow_id: workflow.workflow_id,
            queue: resolved_queue,
            revision_id: workflow.latest_revision_id,
            executable_count: latest.executable_count,
            graph_function_steps_csv: latest.graph_function_steps_csv.clone(),
            graph_function_inputs_json: latest.graph_function_inputs_json.clone(),
            source_bytes: latest.source.len(),
            reflected_at_utc: latest.reflected_at_utc.to_rfc3339(),
            leased_job_id: leased_job_id.or(Some(enqueued_job_id)),
        })
    }

    async fn endpoint_failure_rate_trends(&self) -> Vec<EndpointFailureRateTrendRow> {
        self.list_endpoint_failure_rate_trends().await
    }

    async fn workflow_run_draft(
        &self,
        request: WorkflowRunDraftRequest,
    ) -> Result<WorkflowRunDraftResult> {
        let workflow_id = request.workflow_id.trim();
        let queue = request.queue.trim();
        let source = request.source.trim();
        let graph_modules_csv = normalize_graph_modules_csv(request.graph_modules_csv.as_deref());
        let graph_function_steps_csv =
            normalize_graph_function_steps_csv(request.graph_function_steps_csv.as_deref());
        let graph_function_inputs_json =
            normalize_graph_function_inputs_json(request.graph_function_inputs_json.as_deref());
        let _graph_state_json =
            validate_and_normalize_graph_state_json(request.graph_state_json.as_deref())?;

        if workflow_id.is_empty() {
            return Err(StasisError::PortFailure("workflow_id is required".to_string()));
        }
        if queue.is_empty() {
            return Err(StasisError::PortFailure("queue is required".to_string()));
        }
        if source.is_empty() {
            return Err(StasisError::PortFailure("source is required".to_string()));
        }

        let reflection = self
            .workflow_reflection
            .reflect_executables_from_source(source)?;
        let output = self
            .workflow_engine
            .execute_grapheme_source(source, None)
            .await?;

        Ok(WorkflowRunDraftResult {
            workflow_id: workflow_id.to_string(),
            queue: queue.to_string(),
            executable_count: reflection.count,
            graph_modules_csv,
            graph_function_steps_csv,
            graph_function_inputs_json,
            source_bytes: source.len(),
            run_id: output.run_id,
            execution_json: output.execution.to_string(),
            final_state_json: output.final_state.to_string(),
        })
    }

    async fn workflow_reflect_source(&self, source: &str) -> Result<WorkflowSourceReflection> {
        self.workflow_reflection
            .reflect_executables_from_source(source)
    }

    async fn workflow_modules_search(&self, query: &str) -> Result<WorkflowModuleSearchReflection> {
        self.workflow_reflection.modules_search(query)
    }

    async fn workflow_module_info(&self, module_id: &str) -> Result<Option<WorkflowModuleInfoReflection>> {
        self.workflow_reflection.module_info(module_id)
    }

    async fn workflow_module_types(&self, module_id: &str) -> Result<Option<WorkflowModuleTypesReflection>> {
        self.workflow_reflection.module_types(module_id)
    }

    async fn workflow_saved_revision_summary(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowSavedRevisionSummary>> {
        let workflow_id = workflow_id.trim();
        if workflow_id.is_empty() {
            return Ok(None);
        }

        let Some(definition) = self.workflow_store.get_definition(workflow_id).await? else {
            return Ok(None);
        };
        let revisions = self.workflow_store.list_revisions(workflow_id).await?;
        let Some(latest) = revisions
            .into_iter()
            .find(|revision| revision.revision_id == definition.latest_revision_id)
        else {
            return Ok(None);
        };
        let compile_mode = serde_json::from_str::<serde_json::Value>(
            latest.compiler_metadata_json.as_str(),
        )
        .ok()
        .and_then(|value| {
            value
                .get("compile_mode")
                .and_then(serde_json::Value::as_str)
                .map(|text| text.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

        Ok(Some(WorkflowSavedRevisionSummary {
            workflow_id: definition.workflow_id,
            revision_id: latest.revision_id,
            executable_count: latest.executable_count,
            reflected_at_utc: latest.reflected_at_utc.to_rfc3339(),
            compile_mode,
            source_bytes: latest.source.len(),
            source: latest.source,
            graph_state_json: latest.graph_state_json,
            graph_modules_csv: latest.graph_modules_csv,
            graph_function_steps_csv: latest.graph_function_steps_csv,
            graph_function_inputs_json: latest.graph_function_inputs_json,
        }))
    }

    async fn workflow_lsp_diagnostics(&self, source: &str) -> Result<WorkflowDiagnosticsResult> {
        let source = source.trim();
        let mut diagnostics = Vec::new();

        if source.is_empty() {
            diagnostics.push(WorkflowDiagnostic {
                severity: WorkflowDiagnosticSeverity::Warning,
                message: "Source is empty. Add a query/mutation/subscription to reflect."
                    .to_string(),
                code: Some("EMPTY_SOURCE".to_string()),
                line: None,
                column: None,
            });
        } else if let Err(parse_err) = grapheme_compiler::parse(source) {
            let parse_message = parse_err.to_string();
            let (line, column) = extract_line_column(parse_message.as_str());

            diagnostics.push(WorkflowDiagnostic {
                severity: WorkflowDiagnosticSeverity::Error,
                message: parse_message,
                code: Some("PARSE".to_string()),
                line,
                column,
            });
        } else if let Err(reflect_err) = self
            .workflow_reflection
            .reflect_executables_from_source(source)
        {
            let reflect_message = reflect_err.to_string();
            let (line, column) = extract_line_column(reflect_message.as_str());

            diagnostics.push(WorkflowDiagnostic {
                severity: WorkflowDiagnosticSeverity::Error,
                message: reflect_message.clone(),
                code: Some(reflection_code_for_error(reflect_message.as_str()).to_string()),
                line,
                column,
            });
        }

        let enabled = true;
        let provider = if cfg!(feature = "dashboard-lsp") {
            "grapheme-compiler+reflection (grapheme-lsp wiring pending)"
        } else {
            "grapheme-compiler+reflection"
        };
        let summary = if diagnostics.is_empty() {
            format!(
                "No issues from {provider} for the current source snapshot."
            )
        } else {
            format!(
                "{provider} reported {} issue(s) in the current source snapshot.",
                diagnostics.len()
            )
        };

        Ok(WorkflowDiagnosticsResult {
            enabled,
            provider: provider.to_string(),
            summary,
            diagnostics,
        })
    }

    async fn inspect(&self, entity: InspectEntity) -> Result<InspectorView> {
        let inspector = match entity {
            InspectEntity::Job(id) => {
                let jobs = self.list_all_jobs().await?;
                let Some(job) = jobs.iter().find(|job| job.id == id) else {
                    return Ok(InspectorView::None);
                };

                InspectorView::Job(JobInspectorDto {
                    id: job.id.clone(),
                    status: format!("{:?}", job.state),
                    queue: job.queue.clone(),
                    trace_id: job.trace_id.clone(),
                    correlation_id: job.correlation_id.clone(),
                    causation_id: job.causation_id.clone(),
                    last_error: job.last_error.clone(),
                })
            }
            InspectEntity::Attempt(id) => {
                let jobs = self.list_all_jobs().await?;
                let mut found = None;
                for job in jobs {
                    for attempt in self.list_job_attempts(&job.id).await? {
                        if attempt.attempt_id == id {
                            found = Some(attempt);
                            break;
                        }
                    }
                    if found.is_some() {
                        break;
                    }
                }

                let Some(attempt) = found else {
                    return Ok(InspectorView::None);
                };

                InspectorView::Attempt(AttemptInspectorDto {
                    attempt_id: attempt.attempt_id,
                    job_id: attempt.job_id,
                    outcome: format!("{:?}", attempt.outcome),
                    worker_id: attempt.worker_id,
                    duration_ms: attempt.duration_ms,
                    guardrail_code: attempt.guardrail_code,
                    policy_reason: attempt.policy_reason,
                })
            }
            InspectEntity::Node(id) => {
                let rows = self.list_cluster_node_health_rows().await?;

                let Some(node) = rows.iter().find(|row| row.snapshot.node.node_id == id) else {
                    return Ok(InspectorView::None);
                };
                InspectorView::Node(map_node_inspector(node))
            }
            InspectEntity::Endpoint(id) => {
                let rows = self
                    .list_endpoint_diagnostics_rows(Some(vec![id.clone()]), Some(1))
                    .await?;

                let Some(endpoint) = rows.first() else {
                    return Ok(InspectorView::None);
                };
                InspectorView::Endpoint(map_endpoint_inspector(endpoint))
            }
            InspectEntity::Event(id) => {
                let events = self.list_all_outbox_events().await?;
                let Some(event) = events.iter().find(|event| event.event_id == id) else {
                    return Ok(InspectorView::None);
                };

                InspectorView::Event(EventInspectorDto {
                    event_id: event.event_id.clone(),
                    event_type: format!("{:?}", event.event.event_type),
                    job_id: event.event.job_id.clone(),
                    correlation_id: event.event.correlation_id.clone(),
                    trace_id: event.event.trace_id.clone(),
                    status: format!("{:?}", event.status),
                })
            }
        };

        Ok(inspector)
    }
}

/// Backward-compatible alias retained for existing callers while naming transitions to runtime-agnostic service.
pub type InMemoryDashboardQueryService = RuntimeDashboardQueryService;

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use crate::application::runtime::runtime_factory::{RuntimeBackend, RuntimeFactory};

    use super::{
        DashboardQueryService, RuntimeDashboardQueryService, WorkflowDiagnosticSeverity,
        WorkflowRunDraftRequest, WorkflowSaveRequest,
    };
    use crate::application::runtime::in_memory_runtime::InMemoryRuntime;
    use crate::application::runtime::runtime_factory::RuntimeComposition;

    fn valid_workflow_source() -> &'static str {
        r#"
import core from "grapheme/core"

query Echo {
  core.echo(message: "ping") {
    state {
      current
    }
  }
}
"#
    }

    #[tokio::test]
    async fn workflow_save_persists_definition_and_revision() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );

        let saved = service
            .workflow_save(WorkflowSaveRequest {
                workflow_id: "wf.phase2".to_string(),
                queue: "queue.phase2".to_string(),
                source: valid_workflow_source().to_string(),
                compile_mode_hint: None,
                graph_state_json: Some(
                    r#"{"query":{"name":"Q","steps":[{"op":"core.echo","args":{"message":"hello"}}]}}"#
                        .to_string(),
                ),
                graph_modules_csv: Some(" core, textops,core,invalid,healthcheck ".to_string()),
                graph_function_steps_csv: Some(
                    " core.echo,textops.to_markdown,core.echo,invalid.step,healthcheck.runtime_ready "
                        .to_string(),
                ),
                graph_function_inputs_json: Some(
                    r#"{"node-fn-core-echo-1":"{\"message\":\"hello\"}"}"#.to_string(),
                ),
            })
            .await
            .expect("workflow save should succeed");

        let definition = service
            .workflow_store
            .get_definition("wf.phase2")
            .await
            .expect("definition load should succeed")
            .expect("definition should exist");
        assert_eq!(definition.workflow_id, "wf.phase2");
        assert_eq!(definition.queue, "queue.phase2");
        assert_eq!(definition.latest_revision_id, saved.revision_id);

        let revisions = service
            .workflow_store
            .list_revisions("wf.phase2")
            .await
            .expect("revisions load should succeed");
        assert_eq!(revisions.len(), 1);

        let latest = &revisions[0];
        assert_eq!(latest.workflow_id, "wf.phase2");
        assert_eq!(latest.revision_id, saved.revision_id);
        assert_eq!(latest.executable_count, saved.executable_count);
        assert_eq!(latest.graph_modules_csv, "core,textops,healthcheck");
        assert_eq!(
            latest.graph_function_steps_csv,
            "core.echo,textops.to_markdown,healthcheck.runtime_ready"
        );
        assert_eq!(
            latest.graph_function_inputs_json,
            r#"{"node-fn-core-echo-1":"{\"message\":\"hello\"}"}"#
        );
        assert_eq!(
            latest.graph_state_json,
            r#"{"query":{"name":"Q","steps":[{"args":{"message":"hello"},"op":"core.echo"}]}}"#
        );

        let compiler_metadata: serde_json::Value =
            serde_json::from_str(&latest.compiler_metadata_json)
                .expect("compiler metadata should be valid json");
        assert_eq!(
            compiler_metadata["compile_mode"].as_str(),
            Some("graph_compiled")
        );

        let receipt: serde_json::Value = serde_json::from_str(&latest.reflection_receipt_json)
            .expect("reflection receipt should be valid json");
        assert_eq!(receipt["count"].as_u64(), Some(saved.executable_count as u64));
    }

    #[tokio::test]
    async fn workflow_save_rejects_compile_graph_state_without_query_steps() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );

        let error = service
            .workflow_save(WorkflowSaveRequest {
                workflow_id: "wf.invalid.graph.contract".to_string(),
                queue: "queue.invalid.graph.contract".to_string(),
                source: valid_workflow_source().to_string(),
                compile_mode_hint: None,
                graph_state_json: Some(
                    r#"{"query":{"name":"Q","steps":[]}}"#.to_string(),
                ),
                graph_modules_csv: None,
                graph_function_steps_csv: Some("core.echo".to_string()),
                graph_function_inputs_json: None,
            })
            .await
            .expect_err("workflow save should reject invalid compile graph contract");

        let error_text = error.to_string();
        assert!(error_text.contains("requires at least one query step"));
    }

    #[tokio::test]
    async fn workflow_save_accepts_topology_graph_state_contract() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );

        let saved = service
            .workflow_save(WorkflowSaveRequest {
                workflow_id: "wf.topology.graph.contract".to_string(),
                queue: "queue.topology.graph.contract".to_string(),
                source: valid_workflow_source().to_string(),
                compile_mode_hint: None,
                graph_state_json: Some(
                    r#"{"version":1,"nodes":[{"id":"node-fn-core-echo-1"}],"edges":[]}"#
                        .to_string(),
                ),
                graph_modules_csv: Some("core".to_string()),
                graph_function_steps_csv: Some("core.echo".to_string()),
                graph_function_inputs_json: None,
            })
            .await
            .expect("workflow save should accept topology graph contract");

        let summary = service
            .workflow_saved_revision_summary("wf.topology.graph.contract")
            .await
            .expect("summary lookup should succeed")
            .expect("summary should exist");
        assert_eq!(summary.revision_id, saved.revision_id);
        assert_eq!(
            summary.graph_state_json,
            r#"{"edges":[],"nodes":[{"id":"node-fn-core-echo-1"}],"version":1}"#
        );

        let revisions = service
            .workflow_store
            .list_revisions("wf.topology.graph.contract")
            .await
            .expect("revisions load should succeed");
        let latest = revisions
            .first()
            .expect("one revision should exist for topology contract test");
        let compiler_metadata: serde_json::Value =
            serde_json::from_str(&latest.compiler_metadata_json)
                .expect("compiler metadata should be valid json");
        assert_eq!(
            compiler_metadata["compile_mode"].as_str(),
            Some("legacy_function_steps")
        );
    }

    #[tokio::test]
    async fn workflow_execute_uses_latest_persisted_revision_metadata() {
        let runtime = InMemoryRuntime::new();
        runtime
            .register_handler(crate::application::runtime::grapheme_job_handler::GraphemeJobHandler::new(
                RuntimeFactory::default_workflow_engine(),
            ))
            .expect("grapheme handler should register");
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(runtime),
        );
        let source = valid_workflow_source();

        let saved = service
            .workflow_save(WorkflowSaveRequest {
                workflow_id: "wf.exec.meta".to_string(),
                queue: "queue.exec.meta".to_string(),
                source: source.to_string(),
                compile_mode_hint: None,
                graph_state_json: None,
                graph_modules_csv: None,
                graph_function_steps_csv: Some("core.echo,textops.to_markdown".to_string()),
                graph_function_inputs_json: Some(
                    r#"{"node-fn-core-echo-1":"{\"message\":\"ping\"}"}"#.to_string(),
                ),
            })
            .await
            .expect("workflow save should succeed");

        let executed = service
            .workflow_execute("wf.exec.meta", "", "workflow-test")
            .await
            .expect("workflow execute should succeed");

        assert_eq!(executed.workflow_id, "wf.exec.meta");
        assert_eq!(executed.queue, "queue.exec.meta");
        assert_eq!(executed.revision_id, saved.revision_id);
        assert_eq!(executed.executable_count, saved.executable_count);
        assert_eq!(executed.graph_function_steps_csv, "core.echo,textops.to_markdown");
        assert_eq!(
            executed.graph_function_inputs_json,
            r#"{"node-fn-core-echo-1":"{\"message\":\"ping\"}"}"#
        );
        assert_eq!(executed.source_bytes, source.trim().len());
        let job_id = executed
            .leased_job_id
            .expect("workflow execute should enqueue and lease a grapheme job");
        assert!(
            DateTime::parse_from_rfc3339(&executed.reflected_at_utc).is_ok(),
            "reflected_at_utc should be RFC3339"
        );

        let jobs = service.jobs_stream().await.expect("jobs stream should load");
        let job = jobs
            .items
            .iter()
            .find(|row| row.id == job_id)
            .expect("executed job should appear in dashboard stream");
        assert_eq!(job.status, "succeeded");
        assert_eq!(job.queue, "queue.exec.meta");
    }

    #[tokio::test]
    async fn workflow_run_draft_executes_without_persisting_revision() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );
        let source = valid_workflow_source();

        let run = service
            .workflow_run_draft(WorkflowRunDraftRequest {
                workflow_id: "wf.draft.run".to_string(),
                queue: "queue.draft.run".to_string(),
                source: source.to_string(),
                graph_state_json: Some(
                    r#"{"query":{"name":"Draft","steps":[{"op":"core.echo","args":{"message":"ping"}}]}}"#
                        .to_string(),
                ),
                graph_modules_csv: Some("core".to_string()),
                graph_function_steps_csv: Some("core.echo".to_string()),
                graph_function_inputs_json: Some(
                    r#"{"node-fn-core-echo-1":"{\"message\":\"ping\"}"}"#.to_string(),
                ),
            })
            .await
            .expect("draft run should succeed");

        assert_eq!(run.workflow_id, "wf.draft.run");
        assert_eq!(run.queue, "queue.draft.run");
        assert_eq!(run.executable_count, 1);
        assert_eq!(run.graph_modules_csv, "core");
        assert_eq!(run.graph_function_steps_csv, "core.echo");
        assert!(run.source_bytes > 0);
        assert!(!run.run_id.is_empty());
        assert!(
            serde_json::from_str::<serde_json::Value>(&run.execution_json)
                .ok()
                .and_then(|value| value.as_object().cloned())
                .is_some(),
            "execution_json should be a JSON object"
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&run.final_state_json)
                .ok()
                .and_then(|value| value.as_object().cloned())
                .is_some(),
            "final_state_json should be a JSON object"
        );

        let definition = service
            .workflow_store
            .get_definition("wf.draft.run")
            .await
            .expect("definition load should succeed");
        assert!(definition.is_none(), "draft run should not persist definition");
    }

    #[tokio::test]
    async fn workflow_reflection_queries_match_between_inmemory_and_surrealmem() {
        let in_memory = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );
        let surreal_runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_phase3_reflection_parity",
        ))
        .await
        .expect("surreal mem runtime should build");
        let surreal = RuntimeDashboardQueryService::from_runtime_composition(surreal_runtime);

        let in_memory_search = in_memory
            .workflow_modules_search("core")
            .await
            .expect("in-memory module search should succeed");
        let surreal_search = surreal
            .workflow_modules_search("core")
            .await
            .expect("surreal module search should succeed");

        assert!(!in_memory_search.matches.is_empty());
        assert_eq!(
            in_memory_search
                .matches
                .iter()
                .map(|row| row.module_id.clone())
                .collect::<Vec<_>>(),
            surreal_search
                .matches
                .iter()
                .map(|row| row.module_id.clone())
                .collect::<Vec<_>>()
        );

        let module_id = in_memory_search.matches[0].module_id.clone();
        let in_memory_info = in_memory
            .workflow_module_info(module_id.as_str())
            .await
            .expect("in-memory module info should succeed")
            .expect("module info should exist");
        let surreal_info = surreal
            .workflow_module_info(module_id.as_str())
            .await
            .expect("surreal module info should succeed")
            .expect("module info should exist");
        assert_eq!(in_memory_info.module_id, surreal_info.module_id);
        assert_eq!(in_memory_info.total_ops, surreal_info.total_ops);

        let in_memory_types = in_memory
            .workflow_module_types(module_id.as_str())
            .await
            .expect("in-memory module types should succeed")
            .expect("module types should exist");
        let surreal_types = surreal
            .workflow_module_types(module_id.as_str())
            .await
            .expect("surreal module types should succeed")
            .expect("module types should exist");
        assert_eq!(in_memory_types.module_id, surreal_types.module_id);
        assert_eq!(in_memory_types.total_types, surreal_types.total_types);
    }

    #[tokio::test]
    async fn workflow_saved_revision_summary_returns_latest_saved_revision() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );
        let source = valid_workflow_source();

        let saved = service
            .workflow_save(WorkflowSaveRequest {
                workflow_id: "wf.summary".to_string(),
                queue: "queue.summary".to_string(),
                source: source.to_string(),
                compile_mode_hint: None,
                graph_state_json: Some(
                    r#"{"query":{"name":"Summary","steps":[{"op":"core.echo","args":{"message":"summary"}}]}}"#
                        .to_string(),
                ),
                graph_modules_csv: Some("core, branch, healthcheck, core".to_string()),
                graph_function_steps_csv: Some(
                    "core.echo,healthcheck.runtime_ready,unknown.bad,core.echo".to_string(),
                ),
                graph_function_inputs_json: Some(
                    r#"{"node-fn-core-echo-1":"{\"message\":\"summary\"}"}"#.to_string(),
                ),
            })
            .await
            .expect("workflow save should succeed");

        let summary = service
            .workflow_saved_revision_summary("wf.summary")
            .await
            .expect("summary lookup should succeed")
            .expect("summary should exist");

        assert_eq!(summary.workflow_id, "wf.summary");
        assert_eq!(summary.revision_id, saved.revision_id);
        assert_eq!(summary.executable_count, saved.executable_count);
        assert_eq!(summary.compile_mode, "graph_compiled");
        assert_eq!(summary.source_bytes, source.trim().len());
        assert_eq!(summary.graph_modules_csv, "core,healthcheck");
        assert_eq!(summary.graph_function_steps_csv, "core.echo,healthcheck.runtime_ready");
        assert_eq!(
            summary.graph_function_inputs_json,
            r#"{"node-fn-core-echo-1":"{\"message\":\"summary\"}"}"#
        );
        assert_eq!(
            summary.graph_state_json,
            r#"{"query":{"name":"Summary","steps":[{"args":{"message":"summary"},"op":"core.echo"}]}}"#
        );
        assert!(summary.source.contains("query Echo"));
    }

    #[tokio::test]
    async fn workflow_lsp_diagnostics_uses_compiler_and_reflection_provider() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );

        let diagnostics = service
            .workflow_lsp_diagnostics(valid_workflow_source())
            .await
            .expect("diagnostics call should succeed");

        assert!(diagnostics.enabled);
        assert!(diagnostics.provider.contains("grapheme-compiler+reflection"));
        assert!(diagnostics.diagnostics.is_empty());
    }

    #[tokio::test]
    async fn workflow_lsp_diagnostics_marks_parse_errors_with_parse_code() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );

        let diagnostics = service
            .workflow_lsp_diagnostics("query Broken {")
            .await
            .expect("diagnostics call should succeed");

        assert!(!diagnostics.diagnostics.is_empty());
        assert_eq!(diagnostics.diagnostics[0].severity, WorkflowDiagnosticSeverity::Error);
        assert_eq!(diagnostics.diagnostics[0].code.as_deref(), Some("PARSE"));
    }

    #[tokio::test]
    async fn workflow_lsp_diagnostics_marks_reflection_errors_with_reflection_code() {
        let service = RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(InMemoryRuntime::new()),
        );

        let source = r#"
import core from "grapheme/core"

query Broken {
  core.not_real(message: "ping")
}
"#;

        let diagnostics = service
            .workflow_lsp_diagnostics(source)
            .await
            .expect("diagnostics call should succeed");

        assert!(!diagnostics.diagnostics.is_empty());
        assert_eq!(diagnostics.diagnostics[0].severity, WorkflowDiagnosticSeverity::Error);
        assert_ne!(diagnostics.diagnostics[0].code.as_deref(), Some("PARSE"));
    }
}
