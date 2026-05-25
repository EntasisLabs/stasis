use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use askama::Template;
use axum::Router;
use axum::extract::{Json, Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::extract::Request;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::Utc;
use serde::Deserialize;

use crate::dashboard::assets;
use crate::dashboard::dto::{
    ClusterNodeCardDto, DashboardDto, EndpointInspectorDto, EventInspectorDto, InspectorView,
    JobInspectorDto, JobRowDto, NodeInspectorDto, OutboxEventRowDto, RecurringDefinitionRowDto,
};
use crate::dashboard::service::{DashboardQueryService, InspectEntity, WorkflowSaveRequest};

#[derive(Clone)]
pub struct DashboardState {
    service: Arc<dyn DashboardQueryService>,
    action_auth_bearer_token: Option<String>,
    action_required_role: Option<String>,
    action_role_claim_header: String,
}

impl DashboardState {
    pub fn new(service: Arc<dyn DashboardQueryService>) -> Self {
        Self {
            service,
            action_auth_bearer_token: None,
            action_required_role: None,
            action_role_claim_header: "x-stasis-role".to_string(),
        }
    }

    pub fn with_action_auth_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.action_auth_bearer_token = Some(token.into());
        self
    }

    pub fn with_action_required_role(mut self, role: impl Into<String>) -> Self {
        self.action_required_role = Some(role.into());
        self
    }

    pub fn with_action_role_claim_header(mut self, header_name: impl Into<String>) -> Self {
        self.action_role_claim_header = header_name.into();
        self
    }
}

pub fn router(state: DashboardState) -> Router {
    let action_routes = Router::new()
        .route("/scheduler/materialize", post(action_scheduler_materialize))
        .route("/scheduler/process", post(action_scheduler_process_queue))
        .route("/scheduler/publish", post(action_scheduler_publish_pending))
        .route("/scheduler/replay", post(action_scheduler_replay_deadletter))
        .route("/workflows/save", post(action_workflow_save))
        .route("/workflows/execute", post(action_workflow_execute))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_action_authorization,
        ))
        .with_state(state.clone());

    Router::new()
        .route("/", get(root))
        .route("/dashboard", get(dashboard))
        .route("/view/{name}", get(view_section))
        .route("/stream/jobs", get(stream_jobs))
        .route("/stream/outbox", get(stream_outbox))
        .route("/stream/nodes", get(stream_nodes))
        .route("/stream/workflow-reflection", get(stream_workflow_reflection))
        .route("/inspect/job/{id}", get(inspect_job))
        .route("/inspect/attempt/{id}", get(inspect_attempt))
        .route("/inspect/node/{id}", get(inspect_node))
        .route("/inspect/endpoint/{id}", get(inspect_endpoint))
        .route("/inspect/event/{id}", get(inspect_event))
        .nest("/action", action_routes)
        .route("/assets/{name}", get(asset))
        .with_state(state)
}

async fn require_action_authorization(
    State(state): State<DashboardState>,
    request: Request,
    next: Next,
) -> Response {
    let expected_token = state.action_auth_bearer_token.as_deref();
    let required_role = state.action_required_role.as_deref();

    if expected_token.is_none() && required_role.is_none() {
        return next.run(request).await;
    }

    let provided = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    if expected_token.is_some() && provided != expected_token {
        return (
            StatusCode::UNAUTHORIZED,
            "dashboard action authorization required",
        )
            .into_response();
    }

    if let Some(required_role) = required_role
        && !request_has_required_role(
            request.headers(),
            state.action_role_claim_header.as_str(),
            required_role,
        )
    {
        return (
            StatusCode::FORBIDDEN,
            "dashboard action role authorization required",
        )
            .into_response();
    }

    next.run(request).await
}

fn request_has_required_role(
    headers: &axum::http::HeaderMap,
    role_header_name: &str,
    required_role: &str,
) -> bool {
    let normalized_required = required_role.trim();
    if normalized_required.is_empty() {
        return true;
    }

    headers
        .get(role_header_name)
        .and_then(|value| value.to_str().ok())
        .map(|roles| {
            roles
                .split([',', ' '])
                .map(str::trim)
                .filter(|role| !role.is_empty())
                .any(|role| role.eq_ignore_ascii_case(normalized_required))
        })
        .unwrap_or(false)
}

async fn root() -> Redirect {
    Redirect::temporary("/dashboard")
}

async fn dashboard() -> Result<Html<String>, (StatusCode, String)> {
    render_template(DashboardPageTemplate {
        refreshed_at: Utc::now().to_rfc3339(),
    })
}

async fn stream_jobs(
    State(state): State<DashboardState>,
) -> Result<Html<String>, (StatusCode, String)> {
    let panel = state.service.jobs_stream().await.map_err(internal_error)?;
    render_template(JobsStreamTemplate { jobs: panel.items })
}

async fn stream_outbox(
    State(state): State<DashboardState>,
) -> Result<Html<String>, (StatusCode, String)> {
    let panel = state
        .service
        .outbox_stream()
        .await
        .map_err(internal_error)?;
    render_template(OutboxStreamTemplate {
        events: panel.items,
    })
}

async fn stream_nodes(
    State(state): State<DashboardState>,
) -> Result<Html<String>, (StatusCode, String)> {
    let panel = state
        .service
        .cluster_stream()
        .await
        .map_err(internal_error)?;
    render_template(NodesStreamTemplate { nodes: panel.nodes })
}

async fn stream_workflow_reflection(
    State(state): State<DashboardState>,
    Query(query): Query<WorkflowReflectionQuery>,
) -> Result<Html<String>, (StatusCode, String)> {
    let advanced_mode = is_advanced_mode(query.mode.as_deref());
    let queue = query
        .queue
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
        .to_string();
    let workflow_id = query
        .workflow_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| format!("wf-{}", queue.replace('.', "-")));

    let reflection = build_workflow_reflection_preview(
        state.service.as_ref(),
        workflow_id.as_str(),
        queue.as_str(),
        query.source.as_deref(),
        query.module_id.as_deref(),
        query.capability.as_deref(),
        query.effect.as_deref(),
        query.op.as_deref(),
    )
    .await;

    render_template(WorkflowReflectionStreamTemplate {
        reflection,
        advanced_mode,
    })
}

fn is_advanced_mode(mode: Option<&str>) -> bool {
    mode
        .map(str::trim)
        .is_some_and(|value| value.eq_ignore_ascii_case("advanced"))
}

async fn build_workflow_reflection_preview(
    service: &dyn DashboardQueryService,
    workflow_id: &str,
    queue: &str,
    source_override: Option<&str>,
    selected_module_id: Option<&str>,
    capability_filter: Option<&str>,
    effect_filter: Option<&str>,
    op_filter: Option<&str>,
) -> WorkflowReflectionPreviewDto {
    let source = source_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| default_grapheme_source(workflow_id, queue));
    let source_reflection = service.workflow_reflect_source(source.as_str()).await.ok();
    let diagnostics = service
        .workflow_lsp_diagnostics(source.as_str())
        .await
        .unwrap_or_else(|_| crate::dashboard::service::WorkflowDiagnosticsResult {
            enabled: false,
            provider: "error".to_string(),
            summary: "Unable to resolve diagnostics preview for current source.".to_string(),
            diagnostics: Vec::new(),
        });
    let saved_revision = service
        .workflow_saved_revision_summary(workflow_id)
        .await
        .ok()
        .flatten();
    let module_search = service.workflow_modules_search("core").await.ok();

    let module_matches = module_search
        .as_ref()
        .map(|rows| {
            rows.matches
                .iter()
                .map(|row| WorkflowModuleMatchRowDto {
                    module_id: row.module_id.clone(),
                    summary: row.summary.clone(),
                    score: row.score,
                    matching_ops: row.matching_ops.clone(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let selected_module_id = selected_module_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .or_else(|| module_matches.first().map(|row| row.module_id.clone()))
        .unwrap_or_else(|| "core".to_string());
    let capability_filter = capability_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    let effect_filter = effect_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    let op_filter = op_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    let module_info = service
        .workflow_module_info(selected_module_id.as_str())
        .await
        .ok()
        .flatten();
    let module_types = service
        .workflow_module_types(selected_module_id.as_str())
        .await
        .ok()
        .flatten();

    let module_detail = module_info.map(|info| {
        let required_capabilities = info.required_capabilities.clone();
        let capability_options = required_capabilities.clone();
        let mut effect_options = info
            .exported_ops
            .iter()
            .map(|op| op.effect.clone())
            .collect::<Vec<_>>();
        effect_options.sort();
        effect_options.dedup();
        let type_count = module_types
            .as_ref()
            .map(|types| types.total_types)
            .unwrap_or(0);
        let operation_total = info.exported_ops.len();
        let capability_filter_lower = capability_filter
            .as_ref()
            .map(|value| value.to_lowercase());
        let effect_filter_lower = effect_filter.as_ref().map(|value| value.to_lowercase());
        let op_filter_lower = op_filter.as_ref().map(|value| value.to_lowercase());
        let operations = info
            .exported_ops
            .into_iter()
            .filter(|op| {
                let capability_ok = capability_filter_lower.as_ref().is_none_or(|needle| {
                    required_capabilities
                        .iter()
                        .any(|capability| capability.to_lowercase() == *needle)
                });
                let effect_ok = effect_filter_lower
                    .as_ref()
                    .is_none_or(|needle| op.effect.to_lowercase() == *needle);
                let op_ok = op_filter_lower.as_ref().is_none_or(|needle| {
                    op.op.to_lowercase().contains(needle)
                });
                capability_ok && effect_ok && op_ok
            })
            .take(8)
            .map(|op| WorkflowModuleOperationRowDto {
                name: op.op,
                stability: op.stability,
                effect: op.effect,
                has_input_schema: op.input_schema_ref.is_some(),
                has_output_schema: op.output_schema_ref.is_some(),
            })
            .collect::<Vec<_>>();

        WorkflowModuleDetailDto {
            module_id: info.module_id,
            version: info.version,
            entrypoint: info.entrypoint,
            required_capabilities,
            total_ops: info.total_ops,
            total_types: type_count,
            operation_total,
            operations,
            capability_options,
            effect_options,
        }
    });

    let executable_count = source_reflection
        .as_ref()
        .map(|payload| payload.count)
        .unwrap_or(0);
    let live_source_bytes = source.len();
    let executables = source_reflection
        .map(|payload| {
            payload
                .executables
                .into_iter()
                .map(|item| WorkflowExecutableRowDto {
                    name: item.name,
                    kind: format!("{:?}", item.kind).to_lowercase(),
                    input_type: item.input_type,
                    output_type: item.output_type,
                    pipeline_count: item.pipeline_count,
                    step_count: item.step_count,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let comparison = match saved_revision {
        Some(saved) => {
            let source_in_sync = saved.source.trim() == source.trim();
            let executables_in_sync = saved.executable_count == executable_count;
            if source_in_sync && executables_in_sync {
                WorkflowRevisionComparisonDto {
                    status_label: "In Sync".to_string(),
                    status_tone: "ok".to_string(),
                    detail: "Live draft matches latest saved revision.".to_string(),
                    saved_revision_id: Some(saved.revision_id),
                    saved_reflected_at_utc: Some(saved.reflected_at_utc),
                    saved_executable_count: Some(saved.executable_count),
                    saved_source_bytes: Some(saved.source_bytes),
                    live_executable_count: executable_count,
                    live_source_bytes,
                }
            } else {
                WorkflowRevisionComparisonDto {
                    status_label: "Unsaved Changes".to_string(),
                    status_tone: "warn".to_string(),
                    detail: "Live draft differs from latest saved revision.".to_string(),
                    saved_revision_id: Some(saved.revision_id),
                    saved_reflected_at_utc: Some(saved.reflected_at_utc),
                    saved_executable_count: Some(saved.executable_count),
                    saved_source_bytes: Some(saved.source_bytes),
                    live_executable_count: executable_count,
                    live_source_bytes,
                }
            }
        }
        None => WorkflowRevisionComparisonDto {
            status_label: "No Saved Revision".to_string(),
            status_tone: "neutral".to_string(),
            detail: "Save this draft to create the first durable workflow revision.".to_string(),
            saved_revision_id: None,
            saved_reflected_at_utc: None,
            saved_executable_count: None,
            saved_source_bytes: None,
            live_executable_count: executable_count,
            live_source_bytes,
        },
    };

    let diagnostics_dto = WorkflowDiagnosticsPreviewDto {
        enabled: diagnostics.enabled,
        provider: diagnostics.provider,
        summary: diagnostics.summary,
        diagnostics: diagnostics
            .diagnostics
            .into_iter()
            .map(|item| WorkflowDiagnosticRowDto {
                severity: format!("{:?}", item.severity).to_lowercase(),
                message: item.message,
                code: item.code,
                line: item.line,
                column: item.column,
            })
            .collect::<Vec<_>>(),
    };

    WorkflowReflectionPreviewDto {
        workflow_id: workflow_id.to_string(),
        queue: queue.to_string(),
        source,
        selected_module_id,
        filter_capability: capability_filter.unwrap_or_default(),
        filter_effect: effect_filter.unwrap_or_default(),
        filter_op: op_filter.unwrap_or_default(),
        executable_count,
        executables,
        module_matches,
        selected_module: module_detail,
        comparison,
        diagnostics: diagnostics_dto,
    }
}

fn default_grapheme_source(workflow_id: &str, queue: &str) -> String {
    let executable_name = sanitize_executable_name(workflow_id);
    format!(
        "import core from \"grapheme/core\"\n\nquery {executable_name} {{\n  core.echo(message: \"queue:{queue}\") {{\n    state {{\n      current\n    }}\n  }}\n}}\n"
    )
}

fn grapheme_module_catalog() -> Vec<(&'static str, &'static str)> {
    vec![
        ("core", "Grapheme Core"),
        ("web", "Grapheme Web"),
        ("websearch", "Grapheme WebSearch"),
        ("http", "Grapheme HTTP"),
        ("html", "Grapheme HTML"),
        ("json", "Grapheme JSON"),
        ("csv", "Grapheme CSV"),
        ("yaml", "Grapheme YAML"),
        ("io", "Grapheme IO"),
        ("sql", "Grapheme SQL"),
        ("surreal", "Grapheme Surreal"),
        ("memory", "Grapheme Memory"),
        ("runtime", "Grapheme Runtime"),
        ("smtp", "Grapheme SMTP"),
        ("tcp", "Grapheme TCP"),
        ("docs", "Grapheme Docs"),
        ("secrets", "Grapheme Secrets"),
        ("textops", "Grapheme TextOps"),
        ("healthcheck", "Grapheme Healthcheck"),
    ]
}

fn is_supported_grapheme_module(module_id: &str) -> bool {
    grapheme_module_catalog()
        .iter()
        .any(|(id, _)| id == &module_id)
}

fn default_function_for_module(module_id: &str) -> &'static str {
    match module_id {
        "core" => "echo",
        "web" => "duckduckgo",
        "websearch" => "search",
        "http" => "get",
        "html" => "to_md",
        "json" => "parse",
        "csv" => "to_list",
        "yaml" => "to_json",
        "io" => "read_text",
        "sql" => "query",
        "surreal" => "query",
        "memory" => "load_context",
        "runtime" => "emit_event",
        "smtp" => "send_mail",
        "tcp" => "connect",
        "docs" => "native_module_registry",
        "secrets" => "get_secret_handle",
        "textops" => "normalize",
        "healthcheck" => "runtime_ready",
        _ => "echo",
    }
}

fn normalize_function_step(raw: &str) -> Option<(String, String)> {
    let raw = raw.trim().to_ascii_lowercase();
    let (module_id, function_id) = raw.split_once('.')?;
    if !is_supported_grapheme_module(module_id) {
        return None;
    }

    let function_id = function_id.trim();
    if function_id.is_empty()
        || !function_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }

    Some((module_id.to_string(), function_id.to_string()))
}

fn parse_function_steps_csv(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(normalize_function_step)
        .collect::<Vec<_>>()
}

fn join_function_steps_csv(steps: &[(String, String)]) -> String {
    steps
        .iter()
        .map(|(module_id, function_id)| format!("{module_id}.{function_id}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_function_inputs_json(raw: &str) -> BTreeMap<String, String> {
    serde_json::from_str::<BTreeMap<String, String>>(raw).unwrap_or_default()
}

fn serialize_function_inputs_json(inputs: &BTreeMap<String, String>) -> String {
    if inputs.is_empty() {
        return "{}".to_string();
    }

    serde_json::to_string(inputs).unwrap_or_else(|_| "{}".to_string())
}

fn format_function_title(function_id: &str) -> String {
    function_id
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            first.to_ascii_uppercase().to_string() + chars.as_str()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn compile_grapheme_source_from_function_steps(
    workflow_id: &str,
    queue: &str,
    function_steps_csv: &str,
) -> Option<String> {
    let steps = parse_function_steps_csv(function_steps_csv);
    if steps.is_empty() {
        return None;
    }

    let executable_name = sanitize_executable_name(workflow_id);
    let step_list = steps
        .iter()
        .map(|(module_id, function_id)| format!("{module_id}.{function_id}"))
        .collect::<Vec<_>>()
        .join(" -> ");

    Some(format!(
        "import core from \"grapheme/core\"\n\nquery {executable_name} {{\n  core.echo(message: \"workflow:{workflow_id} queue:{queue} steps:{step_list}\") {{\n    state {{\n      current\n    }}\n  }}\n}}\n"
    ))
}

fn sanitize_executable_name(workflow_id: &str) -> String {
    let mut out = workflow_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || ch == '_' { ch } else { '_' })
        .collect::<String>();

    if out.is_empty() {
        return "WorkflowPreview".to_string();
    }

    if let Some(first) = out.chars().next()
        && first.is_ascii_digit()
    {
        out.insert(0, '_');
    }

    out
}

async fn view_section(
    State(state): State<DashboardState>,
    Path(name): Path<String>,
    Query(query): Query<ViewSectionQuery>,
) -> Result<Html<String>, (StatusCode, String)> {
    match name.as_str() {
        "jobs" => {
            let dto = state
                .service
                .dashboard(None)
                .await
                .map_err(internal_error)?;
            render_template(JobsViewTemplate { dto })
        }
        "endpoints" => {
            let panel = state
                .service
                .endpoint_stream()
                .await
                .map_err(internal_error)?;
            let total = panel.items.len();
            let unhealthy_count = panel
                .items
                .iter()
                .filter(|endpoint| endpoint.unhealthy)
                .count();
            let disabled_count = panel
                .items
                .iter()
                .filter(|endpoint| !endpoint.enabled)
                .count();
            let total_deliveries = panel
                .items
                .iter()
                .map(|endpoint| endpoint.success_count + endpoint.failure_count)
                .sum::<u64>();

            let endpoints = panel
                .items
                .into_iter()
                .map(|endpoint| {
                    let trend = if endpoint.failure_count == 0 {
                        "Improving"
                    } else if endpoint.failure_rate_percent >= 10.0 || endpoint.unhealthy {
                        "Worsening"
                    } else {
                        "Stable"
                    };

                    EndpointHealthRowDto {
                        endpoint_id: endpoint.endpoint_id,
                        endpoint_name: endpoint.endpoint_name,
                        protocol: endpoint.protocol,
                        target: endpoint.target,
                        enabled: endpoint.enabled,
                        success_count: endpoint.success_count,
                        failure_count: endpoint.failure_count,
                        failure_rate_label: format!("{:.2}%", endpoint.failure_rate_percent),
                        failure_rate_percent: endpoint.failure_rate_percent,
                        unhealthy: endpoint.unhealthy,
                        trend: trend.to_string(),
                        last_error: endpoint.last_error,
                    }
                })
                .collect::<Vec<_>>();

            let unhealthy = endpoints
                .iter()
                .filter(|endpoint| endpoint.unhealthy)
                .cloned()
                .collect::<Vec<_>>();

            render_template(EndpointsViewTemplate {
                endpoints,
                total,
                unhealthy_count,
                disabled_count,
                total_deliveries,
                unhealthy,
            })
        }
        "cluster" => {
            let cluster = state
                .service
                .cluster_stream()
                .await
                .map_err(internal_error)?;
            let total_nodes = cluster.nodes.len();
            let worker_nodes = cluster
                .nodes
                .iter()
                .filter(|node| node.role == "Worker")
                .count();
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
            let total_queues = cluster
                .nodes
                .iter()
                .map(|node| node.queue_ownership_count)
                .sum::<usize>();

            let nodes = cluster
                .nodes
                .into_iter()
                .map(|node| {
                    let mut load_index = (node.queue_ownership_count as i32 * 18).clamp(0, 100);
                    if node.health == "Degraded" {
                        load_index = (load_index + 20).clamp(0, 100);
                    }
                    if node.health == "Offline" {
                        load_index = 100;
                    }
                    let memory_index = (load_index - 10).max(0);

                    ClusterTelemetryNodeDto {
                        node_id: node.node_id,
                        role: node.role,
                        region: node.region,
                        health: node.health,
                        queue_ownership_count: node.queue_ownership_count,
                        capability_count: node.capability_count,
                        load_index,
                        memory_index,
                    }
                })
                .collect::<Vec<_>>();

            let active_jobs_estimate = nodes
                .iter()
                .filter(|node| node.role == "Worker")
                .map(|node| node.queue_ownership_count * 3)
                .sum::<usize>();

            let avg_load_index = if nodes.is_empty() {
                0
            } else {
                nodes.iter().map(|node| node.load_index).sum::<i32>() / nodes.len() as i32
            };

            render_template(ClusterViewTemplate {
                nodes,
                total_nodes,
                worker_nodes,
                healthy_nodes,
                degraded_nodes,
                total_queues,
                active_jobs_estimate,
                avg_load_index,
            })
        }
        "outbox" => {
            let outbox = state
                .service
                .outbox_stream()
                .await
                .map_err(internal_error)?;
            render_template(OutboxViewTemplate {
                events: outbox.items,
            })
        }
        "deadletter" => {
            let jobs = state.service.jobs_stream().await.map_err(internal_error)?;
            let filtered = jobs
                .items
                .into_iter()
                .filter(|job| job.status == "dead_letter" || job.status == "failed")
                .collect::<Vec<_>>();
            render_template(DeadletterViewTemplate { jobs: filtered })
        }
        "workflows" => {
            let advanced_mode = is_advanced_mode(query.mode.as_deref());
            let jobs = state.service.jobs_stream().await.map_err(internal_error)?;

            let mut lanes: BTreeMap<String, WorkflowLaneDto> = BTreeMap::new();
            for job in jobs.items {
                let lane = lanes
                    .entry(job.queue.clone())
                    .or_insert_with(|| WorkflowLaneDto {
                        workflow_id: format!("wf-{}", job.queue.replace('.', "-")),
                        workflow_name: format!("{} Pipeline", job.queue),
                        lane: job.queue.clone(),
                        total_jobs: 0,
                        running_jobs: 0,
                        succeeded_jobs: 0,
                        failed_jobs: 0,
                        selected: false,
                    });

                lane.total_jobs += 1;
                match job.status.as_str() {
                    "running" | "leased" => lane.running_jobs += 1,
                    "succeeded" => lane.succeeded_jobs += 1,
                    "failed" | "dead_letter" => lane.failed_jobs += 1,
                    _ => {}
                }
            }

            let selected_queue = query.queue.clone().or_else(|| lanes.keys().next().cloned());

            let mut lane_rows = lanes.into_values().collect::<Vec<_>>();
            lane_rows.sort_by(|left, right| left.lane.cmp(&right.lane));

            for lane in &mut lane_rows {
                lane.selected = selected_queue
                    .as_ref()
                    .map(|queue| queue == &lane.lane)
                    .unwrap_or(false);
            }

            let selected = lane_rows.iter().find(|lane| lane.selected).cloned();
            let selected_name = selected
                .as_ref()
                .map(|lane| lane.workflow_name.clone())
                .unwrap_or_else(|| "Workflow Pipeline".to_string());
            let selected_id = selected
                .as_ref()
                .map(|lane| lane.workflow_id.clone())
                .unwrap_or_else(|| "wf-none".to_string());
            let selected_queue_name = selected_queue
                .clone()
                .unwrap_or_else(|| "default".to_string());

            let selected_module_catalog = query
                .module_catalog
                .as_deref()
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| is_supported_grapheme_module(value.as_str()))
                .unwrap_or_else(|| "core".to_string());

            let module_catalog_options = grapheme_module_catalog()
                .into_iter()
                .map(|(id, label)| WorkflowModuleCatalogOptionDto {
                    id: id.to_string(),
                    label: label.to_string(),
                    selected: selected_module_catalog == id,
                })
                .collect::<Vec<_>>();

            let accent = match selected_module_catalog.as_str() {
                "textops" => "purple",
                "healthcheck" => "green",
                _ => "blue",
            }
            .to_string();

            let reflected_function_tiles = state
                .service
                .workflow_module_info(selected_module_catalog.as_str())
                .await
                .ok()
                .flatten()
                .map(|info| {
                    info.exported_ops
                        .into_iter()
                        .take(12)
                        .map(|op| WorkflowFunctionTileDto {
                            module_id: selected_module_catalog.clone(),
                            function_id: op.op.clone(),
                            title: format_function_title(op.op.as_str()),
                            purpose: format!(
                                "{} operation with {} stability.",
                                selected_module_catalog, op.stability
                            ),
                            accent: accent.clone(),
                            input_schema_ref: op.input_schema_ref.clone(),
                            output_schema_ref: op.output_schema_ref.clone(),
                            effect: op.effect.clone(),
                            stability: op.stability.clone(),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let function_tiles = if reflected_function_tiles.is_empty() {
                match selected_module_catalog.as_str() {
                "textops" => vec![
                    WorkflowFunctionTileDto {
                        module_id: "textops".to_string(),
                        function_id: "normalize".to_string(),
                        title: "Normalize Text".to_string(),
                        purpose: "Clean and normalize input text before downstream steps."
                            .to_string(),
                        accent: "purple".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                    WorkflowFunctionTileDto {
                        module_id: "textops".to_string(),
                        function_id: "to_markdown".to_string(),
                        title: "Transform To Markdown".to_string(),
                        purpose: "Convert extracted content into markdown-friendly output."
                            .to_string(),
                        accent: "purple".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                    WorkflowFunctionTileDto {
                        module_id: "textops".to_string(),
                        function_id: "truncate".to_string(),
                        title: "Trim Length".to_string(),
                        purpose: "Limit token-heavy output before handing off to the model."
                            .to_string(),
                        accent: "purple".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                ],
                "healthcheck" => vec![
                    WorkflowFunctionTileDto {
                        module_id: "healthcheck".to_string(),
                        function_id: "runtime_ready".to_string(),
                        title: "Runtime Ready Check".to_string(),
                        purpose: "Verify runtime prerequisites before execution.".to_string(),
                        accent: "green".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                    WorkflowFunctionTileDto {
                        module_id: "healthcheck".to_string(),
                        function_id: "provider_probe".to_string(),
                        title: "Provider Probe".to_string(),
                        purpose: "Validate external provider availability for this run."
                            .to_string(),
                        accent: "green".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                    WorkflowFunctionTileDto {
                        module_id: "healthcheck".to_string(),
                        function_id: "queue_access".to_string(),
                        title: "Queue Access Check".to_string(),
                        purpose: "Confirm queue bindings and permissions are valid."
                            .to_string(),
                        accent: "green".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                ],
                _ => vec![
                    WorkflowFunctionTileDto {
                        module_id: "core".to_string(),
                        function_id: "echo".to_string(),
                        title: "Echo Message".to_string(),
                        purpose: "Pass message context forward as a baseline function step."
                            .to_string(),
                        accent: "blue".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                    WorkflowFunctionTileDto {
                        module_id: "core".to_string(),
                        function_id: "websearch".to_string(),
                        title: "Web Search".to_string(),
                        purpose: "Fetch web results as input for extraction and synthesis steps."
                            .to_string(),
                        accent: "blue".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                    WorkflowFunctionTileDto {
                        module_id: "core".to_string(),
                        function_id: "extract_html".to_string(),
                        title: "Extract HTML Elements".to_string(),
                        purpose: "Collect structured fragments from fetched HTML content."
                            .to_string(),
                        accent: "blue".to_string(),
                        input_schema_ref: None,
                        output_schema_ref: None,
                        effect: "Read".to_string(),
                        stability: "stable".to_string(),
                    },
                ],
                }
            } else {
                reflected_function_tiles
            };

            let saved_revision_summary = state
                .service
                .workflow_saved_revision_summary(selected_id.as_str())
                .await
                .map_err(internal_error)?;
            let saved_revision_modules_csv = saved_revision_summary
                .as_ref()
                .map(|summary| summary.graph_modules_csv.clone())
                .unwrap_or_default();
            let saved_revision_function_steps_csv = saved_revision_summary
                .as_ref()
                .map(|summary| summary.graph_function_steps_csv.clone())
                .unwrap_or_default();
            let saved_revision_function_inputs_json = saved_revision_summary
                .as_ref()
                .map(|summary| summary.graph_function_inputs_json.clone())
                .unwrap_or_else(|| "{}".to_string());

            let normalize_module_kind = |kind: &str| -> Option<String> {
                let normalized = kind.trim().to_ascii_lowercase();
                if is_supported_grapheme_module(normalized.as_str()) {
                    Some(normalized)
                } else {
                    None
                }
            };

            let modules_seed = query.modules.clone().unwrap_or(saved_revision_modules_csv);
            let mut custom_module_kinds = modules_seed
                .split(',')
                .filter_map(normalize_module_kind)
                .collect::<Vec<_>>();

            let requested_add_module = query
                .add_module
                .as_deref()
                .and_then(normalize_module_kind);
            if let Some(kind) = requested_add_module.as_ref() {
                custom_module_kinds.push(kind.clone());
            }

            let function_steps_seed = query
                .function_steps
                .as_deref()
                .unwrap_or(saved_revision_function_steps_csv.as_str());
            let mut custom_function_steps = parse_function_steps_csv(function_steps_seed);
            let function_inputs_seed = query
                .function_inputs
                .as_deref()
                .unwrap_or(saved_revision_function_inputs_json.as_str());
            let custom_function_inputs = parse_function_inputs_json(function_inputs_seed);
            let requested_add_function = query
                .add_function
                .as_deref()
                .and_then(normalize_function_step);
            if let Some((module_id, function_id)) = requested_add_function.as_ref() {
                custom_function_steps.push((module_id.clone(), function_id.clone()));
                if !custom_module_kinds.iter().any(|module| module == module_id) {
                    custom_module_kinds.push(module_id.clone());
                }
            }

            for (module_id, _) in &custom_function_steps {
                if !custom_module_kinds.iter().any(|module| module == module_id) {
                    custom_module_kinds.push(module_id.clone());
                }
            }

            if custom_function_steps.is_empty() {
                for module_id in &custom_module_kinds {
                    custom_function_steps.push((
                        module_id.clone(),
                        default_function_for_module(module_id.as_str()).to_string(),
                    ));
                }
            }

            let reflected_module_ids = custom_function_steps
                .iter()
                .map(|(module_id, _)| module_id.clone())
                .collect::<BTreeSet<_>>();
            let mut reflected_module_operations = BTreeMap::new();
            for module_id in reflected_module_ids {
                if let Some(info) = state
                    .service
                    .workflow_module_info(module_id.as_str())
                    .await
                    .ok()
                    .flatten()
                {
                    reflected_module_operations.insert(module_id, info.exported_ops);
                }
            }

            let selected_lane = lane_rows.iter().find(|lane| lane.selected);
            let workflow_count = selected_lane.map(|lane| lane.total_jobs).unwrap_or(0);
            let running_count = selected_lane.map(|lane| lane.running_jobs).unwrap_or(0);
            let succeeded_count = selected_lane.map(|lane| lane.succeeded_jobs).unwrap_or(0);
            let failed_count = selected_lane.map(|lane| lane.failed_jobs).unwrap_or(0);

            let mut graph_nodes = Vec::new();

            let mut added_node_id = None;
            for (index, (module_id, function_id)) in custom_function_steps.iter().enumerate() {
                let id = format!("node-fn-{}-{}-{}", module_id, function_id, index + 1);
                let label = format_function_title(function_id.as_str());
                let operation = reflected_module_operations
                    .get(module_id)
                    .and_then(|ops| ops.iter().find(|op| op.op == *function_id));
                let input_schema_ref = operation.and_then(|op| op.input_schema_ref.clone());
                let output_schema_ref = operation.and_then(|op| op.output_schema_ref.clone());
                let effect = operation
                    .map(|op| op.effect.clone())
                    .unwrap_or_else(|| "Read".to_string());
                let stability = operation
                    .map(|op| op.stability.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                let function_input = custom_function_inputs
                    .get(id.as_str())
                    .cloned()
                    .unwrap_or_else(|| "{}".to_string());

                if requested_add_function.as_ref().is_some_and(|(add_module, add_function)| {
                    add_module == module_id
                        && add_function == function_id
                        && index == custom_function_steps.len() - 1
                }) {
                    added_node_id = Some(id.clone());
                } else if requested_add_module.as_ref().is_some_and(|added| {
                    added == module_id && index == custom_function_steps.len() - 1
                }) {
                    added_node_id = Some(id.clone());
                }

                graph_nodes.push(WorkflowCanvasNodeDto {
                    id,
                    label,
                    node_type: format!("{}.{}", module_id, function_id),
                    status: if running_count > 0 {
                        "running".to_string()
                    } else if succeeded_count > 0 {
                        "ready".to_string()
                    } else if failed_count > 0 {
                        "needs-attention".to_string()
                    } else if workflow_count > 0 {
                        "ready".to_string()
                    } else {
                        "draft".to_string()
                    },
                    status_tone: if failed_count > 0 {
                        "warn".to_string()
                    } else if running_count > 0 {
                        "info".to_string()
                    } else if succeeded_count > 0 || workflow_count > 0 {
                        "ok".to_string()
                    } else {
                        "neutral".to_string()
                    },
                    count: 0,
                    input_hint: input_schema_ref.clone().unwrap_or_else(|| {
                        "Piped from previous Grapheme function".to_string()
                    }),
                    output_hint: output_schema_ref.clone().unwrap_or_else(|| {
                        "Piped to next Grapheme function".to_string()
                    }),
                    function_input,
                    input_schema_ref,
                    output_schema_ref,
                    effect,
                    stability,
                    selected: false,
                });
            }

            let requested_node_id = query
                .node
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string());

            let selected_node_id = requested_node_id
                .filter(|id| graph_nodes.iter().any(|node| node.id == *id))
                .or(added_node_id);

            for node in &mut graph_nodes {
                node.selected = selected_node_id.as_ref().is_some_and(|id| node.id == *id);
            }

            let selected_node = selected_node_id.as_ref().and_then(|id| {
                graph_nodes
                    .iter()
                    .find(|node| node.id == *id)
                    .map(|node| WorkflowSelectedNodeDto {
                        id: node.id.clone(),
                        label: node.label.clone(),
                        node_type: node.node_type.clone(),
                        status: node.status.clone(),
                        summary: match node.node_type.as_str() {
                            _ if node.node_type.contains('.') => {
                                "Executes a Grapheme function step in the current chain.".to_string()
                            }
                            _ => "Handles workflow processing.".to_string(),
                        },
                        input_hint: node.input_hint.clone(),
                        output_hint: node.output_hint.clone(),
                        function_input: node.function_input.clone(),
                        input_schema_ref: node.input_schema_ref.clone(),
                        output_schema_ref: node.output_schema_ref.clone(),
                        effect: node.effect.clone(),
                        stability: node.stability.clone(),
                        count: node.count,
                    })
            });

            let graph_edges = custom_function_steps
                .windows(2)
                .enumerate()
                .map(|(index, window)| {
                    let (from_module, from_function) = (&window[0].0, &window[0].1);
                    let (to_module, to_function) = (&window[1].0, &window[1].1);
                    WorkflowCanvasEdgeDto {
                        from_id: format!("node-fn-{}-{}-{}", from_module, from_function, index + 1),
                        to_id: format!("node-fn-{}-{}-{}", to_module, to_function, index + 2),
                        label: format!("{} -> {}", from_function, to_function),
                    }
                })
                .collect::<Vec<_>>();

            let module_preview = if custom_module_kinds.is_empty() {
                "none".to_string()
            } else {
                custom_module_kinds.join(", ")
            };
            let function_steps_preview = if custom_function_steps.is_empty() {
                "none".to_string()
            } else {
                join_function_steps_csv(custom_function_steps.as_slice())
            };

            let dsl_preview = format!(
                "workflow \"{}\" {{\n  queue \"{}\"\n  modules [{}]\n  function_steps [{}]\n}}",
                selected_name,
                selected_queue_name,
                module_preview,
                function_steps_preview,
            );

            let custom_function_steps_csv = join_function_steps_csv(custom_function_steps.as_slice());
            let custom_function_inputs_json = serialize_function_inputs_json(&custom_function_inputs);

            let grapheme_source_preview =
                default_grapheme_source(selected_id.as_str(), selected_queue_name.as_str());

            render_template(WorkflowsViewTemplate {
                lanes: lane_rows,
                selected_workflow_name: selected_name,
                selected_workflow_id: selected_id,
                selected_queue: selected_queue
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
                selected_module_catalog,
                module_catalog_options,
                function_tiles,
                graph_nodes,
                graph_edges,
                selected_node,
                has_selected_node: selected_node_id.is_some(),
                custom_modules_csv: custom_module_kinds.join(","),
                custom_function_steps_csv,
                custom_function_inputs_json,
                dsl_preview,
                grapheme_source_preview,
                advanced_mode,
                mode_query: if advanced_mode {
                    "advanced".to_string()
                } else {
                    "guided".to_string()
                },
            })
        }
        "lineage" => {
            let outbox = state
                .service
                .outbox_stream()
                .await
                .map_err(internal_error)?;
            let jobs = state.service.jobs_stream().await.map_err(internal_error)?;

            let mut events = outbox
                .items
                .into_iter()
                .take(120)
                .map(|event| LineageEventDto {
                    event_id: event.event_id,
                    event_type: event.event_type,
                    correlation_id: event.correlation_id,
                    delivery_state: event.delivery_state,
                    retry_attempts: event.retry_attempts,
                    occurred_at: event.occurred_at.to_rfc3339(),
                })
                .collect::<Vec<_>>();

            if let Some(state_filter) = query.state.as_ref()
                && state_filter != "all"
            {
                events.retain(|event| &event.delivery_state == state_filter);
            }

            if let Some(text) = query
                .q
                .as_ref()
                .map(|value| value.trim())
                .filter(|v| !v.is_empty())
            {
                let needle = text.to_lowercase();
                events.retain(|event| {
                    event.event_id.to_lowercase().contains(&needle)
                        || event.event_type.to_lowercase().contains(&needle)
                        || event.correlation_id.to_lowercase().contains(&needle)
                });
            }

            let selected_state = query.state.clone().unwrap_or_else(|| "all".to_string());
            let query_text = query.q.clone().unwrap_or_default();

            let graph_nodes = events
                .iter()
                .take(6)
                .enumerate()
                .map(|(index, event)| {
                    let lane = match index % 3 {
                        0 => "job",
                        1 => "execution",
                        _ => "event",
                    };

                    LineageGraphNodeDto {
                        lane: lane.to_string(),
                        title: event.event_type.clone(),
                        node_id: event.event_id.clone(),
                        state: event.delivery_state.clone(),
                        occurred_at: event.occurred_at.clone(),
                    }
                })
                .collect::<Vec<_>>();

            let graph_node_count = graph_nodes.len();
            let connection_count = graph_node_count.saturating_sub(1);
            let depth_label = if graph_node_count == 0 {
                "0 levels".to_string()
            } else {
                "3 levels".to_string()
            };

            render_template(LineageViewTemplate {
                event_count: events.len(),
                active_job_count: jobs.items.len(),
                events,
                selected_state,
                query_text,
                graph_nodes,
                graph_node_count,
                connection_count,
                depth_label,
            })
        }
        "scheduler" => {
            let dashboard = state
                .service
                .dashboard(None)
                .await
                .map_err(internal_error)?;
            let recurring = state
                .service
                .recurring_stream()
                .await
                .map_err(internal_error)?;
            let mut queue_rows: BTreeMap<String, SchedulerQueueDto> = BTreeMap::new();

            for job in dashboard.job_stream.items {
                let row =
                    queue_rows
                        .entry(job.queue.clone())
                        .or_insert_with(|| SchedulerQueueDto {
                            queue: job.queue.clone(),
                            enqueued_jobs: 0,
                            running_jobs: 0,
                            blocked_jobs: 0,
                            completed_jobs: 0,
                        });

                match job.status.as_str() {
                    "enqueued" => row.enqueued_jobs += 1,
                    "running" | "leased" => row.running_jobs += 1,
                    "failed" | "dead_letter" => row.blocked_jobs += 1,
                    "succeeded" => row.completed_jobs += 1,
                    _ => {}
                }
            }

            render_template(SchedulerViewTemplate {
                kpis: dashboard.kpis,
                queues: queue_rows.into_values().collect(),
                recurring: recurring.items,
            })
        }
        _ => Err((StatusCode::NOT_FOUND, format!("unknown section: {name}"))),
    }
}

async fn inspect_job(
    State(state): State<DashboardState>,
    Path(id): Path<String>,
) -> Result<Html<String>, (StatusCode, String)> {
    render_inspector(state, InspectEntity::Job(id)).await
}

async fn inspect_attempt(
    State(state): State<DashboardState>,
    Path(id): Path<String>,
) -> Result<Html<String>, (StatusCode, String)> {
    render_inspector(state, InspectEntity::Attempt(id)).await
}

async fn inspect_node(
    State(state): State<DashboardState>,
    Path(id): Path<String>,
) -> Result<Html<String>, (StatusCode, String)> {
    render_inspector(state, InspectEntity::Node(id)).await
}

async fn inspect_endpoint(
    State(state): State<DashboardState>,
    Path(id): Path<String>,
) -> Result<Html<String>, (StatusCode, String)> {
    render_inspector(state, InspectEntity::Endpoint(id)).await
}

async fn inspect_event(
    State(state): State<DashboardState>,
    Path(id): Path<String>,
) -> Result<Html<String>, (StatusCode, String)> {
    render_inspector(state, InspectEntity::Event(id)).await
}

#[derive(Debug, Deserialize)]
struct SchedulerProcessRequest {
    queue: String,
}

#[derive(Debug, Deserialize)]
struct SchedulerPublishRequest {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SchedulerReplayRequest {
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct WorkflowActionRequest {
    workflow_id: String,
    queue: String,
    source: Option<String>,
    modules: Option<String>,
    function_steps: Option<String>,
    function_inputs: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowReflectionQuery {
    workflow_id: Option<String>,
    queue: Option<String>,
    mode: Option<String>,
    source: Option<String>,
    module_id: Option<String>,
    capability: Option<String>,
    effect: Option<String>,
    op: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ViewSectionQuery {
    queue: Option<String>,
    node: Option<String>,
    modules: Option<String>,
    function_steps: Option<String>,
    function_inputs: Option<String>,
    add_module: Option<String>,
    add_function: Option<String>,
    module_catalog: Option<String>,
    mode: Option<String>,
    state: Option<String>,
    q: Option<String>,
}

async fn action_scheduler_materialize(
    State(state): State<DashboardState>,
) -> Result<Html<String>, (StatusCode, String)> {
    let produced = state
        .service
        .scheduler_materialize_now("dashboard-ui")
        .await
        .map_err(internal_error)?;

    render_template(ActionStatusTemplate {
        title: "Scheduler Tick Completed".to_string(),
        detail: format!("materialized {} recurring jobs", produced),
        kind: "ok".to_string(),
    })
}

async fn action_scheduler_process_queue(
    State(state): State<DashboardState>,
    Json(payload): Json<SchedulerProcessRequest>,
) -> Result<Html<String>, (StatusCode, String)> {
    if payload.queue.trim().is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Queue Processing Rejected".to_string(),
            detail: "queue is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    let leased = state
        .service
        .scheduler_process_queue_once(payload.queue.trim(), "dashboard-ui")
        .await
        .map_err(internal_error)?;

    let detail = match leased {
        Some(id) => format!("processed one job from '{}' (job_id={})", payload.queue, id),
        None => format!("no due jobs available in '{}'", payload.queue),
    };

    render_template(ActionStatusTemplate {
        title: "Queue Process Attempt".to_string(),
        detail,
        kind: "ok".to_string(),
    })
}

async fn action_scheduler_publish_pending(
    State(state): State<DashboardState>,
    Json(payload): Json<SchedulerPublishRequest>,
) -> Result<Html<String>, (StatusCode, String)> {
    let limit = payload.limit.unwrap_or(50).clamp(1, 500);
    let published = state
        .service
        .scheduler_publish_pending_now(limit)
        .await
        .map_err(internal_error)?;

    render_template(ActionStatusTemplate {
        title: "Outbox Publish Sweep".to_string(),
        detail: format!("published {} pending events (limit={})", published, limit),
        kind: "ok".to_string(),
    })
}

async fn action_scheduler_replay_deadletter(
    State(state): State<DashboardState>,
    Json(payload): Json<SchedulerReplayRequest>,
) -> Result<Html<String>, (StatusCode, String)> {
    if payload.job_id.trim().is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Replay Rejected".to_string(),
            detail: "job_id is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    let replayed = state
        .service
        .scheduler_replay_dead_letter_now(payload.job_id.trim())
        .await
        .map_err(internal_error)?;

    render_template(ActionStatusTemplate {
        title: "Dead Letter Replay".to_string(),
        detail: if replayed {
            format!("job {} returned to queue", payload.job_id)
        } else {
            format!("job {} was not replayable", payload.job_id)
        },
        kind: if replayed {
            "ok".to_string()
        } else {
            "warn".to_string()
        },
    })
}

async fn action_workflow_save(
    State(state): State<DashboardState>,
    Json(payload): Json<WorkflowActionRequest>,
) -> Result<Html<String>, (StatusCode, String)> {
    if payload.workflow_id.trim().is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Workflow Save Rejected".to_string(),
            detail: "workflow_id is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    if payload.queue.trim().is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Workflow Save Rejected".to_string(),
            detail: "queue is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    let compiled_source = payload
        .function_steps
        .as_deref()
        .and_then(|steps| {
            compile_grapheme_source_from_function_steps(
                payload.workflow_id.trim(),
                payload.queue.trim(),
                steps,
            )
        });
    let source = compiled_source
        .as_deref()
        .unwrap_or_else(|| payload.source.as_deref().map(str::trim).unwrap_or_default());
    if source.is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Workflow Save Rejected".to_string(),
            detail: "source is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    let saved = state
        .service
        .workflow_save(WorkflowSaveRequest {
            workflow_id: payload.workflow_id.trim().to_string(),
            queue: payload.queue.trim().to_string(),
            source: source.to_string(),
            graph_modules_csv: payload.modules.clone(),
            graph_function_steps_csv: payload.function_steps.clone(),
            graph_function_inputs_json: payload.function_inputs.clone(),
        })
        .await
        .map_err(internal_error)?;

    render_template(ActionStatusTemplate {
        title: "Workflow Saved".to_string(),
        detail: format!(
            "persisted {} for queue {} (revision={}, executables={})",
            saved.workflow_id, saved.queue, saved.revision_id, saved.executable_count
        ),
        kind: "ok".to_string(),
    })
}

async fn action_workflow_execute(
    State(state): State<DashboardState>,
    Json(payload): Json<WorkflowActionRequest>,
) -> Result<Html<String>, (StatusCode, String)> {
    if payload.queue.trim().is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Workflow Execute Rejected".to_string(),
            detail: "queue is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    let executed = state
        .service
        .workflow_execute(payload.workflow_id.trim(), payload.queue.trim(), "workflow-ui")
        .await
        .map_err(internal_error)?;

    let detail = match executed.leased_job_id {
        Some(job_id) => format!(
            "executed one job from {} using {} (revision={}, executables={}, function_steps={}, source_bytes={}, job_id={})",
            executed.queue,
            executed.workflow_id,
            executed.revision_id,
            executed.executable_count,
            if executed.graph_function_steps_csv.is_empty() {
                "none".to_string()
            } else {
                executed.graph_function_steps_csv.clone()
            },
            executed.source_bytes,
            job_id
        ),
        None => format!(
            "{} (revision={}, executables={}, function_steps={}, reflected_at={}) queued execution request, but no due jobs were available in {}",
            executed.workflow_id,
            executed.revision_id,
            executed.executable_count,
            if executed.graph_function_steps_csv.is_empty() {
                "none".to_string()
            } else {
                executed.graph_function_steps_csv.clone()
            },
            executed.reflected_at_utc,
            executed.queue
        ),
    };

    render_template(ActionStatusTemplate {
        title: "Workflow Execute".to_string(),
        detail,
        kind: "ok".to_string(),
    })
}

async fn render_inspector(
    state: DashboardState,
    entity: InspectEntity,
) -> Result<Html<String>, (StatusCode, String)> {
    let inspector = state
        .service
        .inspect(entity)
        .await
        .map_err(internal_error)?;

    render_template(InspectorTemplate { inspector })
}

async fn asset(Path(name): Path<String>) -> Response {
    let Some((bytes, content_type)) = assets::load(&name) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let mut response = bytes.into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    response
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("dashboard error: {err}"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    use crate::dashboard::dto::{
        ClusterMapDto, DashboardDto, EndpointRowDto, InspectorView, JobRowDto, OutboxEventRowDto,
        RecurringDefinitionRowDto, SystemKpiDto, UiListPanel,
    };
    use crate::dashboard::service::{
        DashboardQueryService, InspectEntity, RuntimeDashboardQueryService,
        WorkflowExecuteResult, WorkflowSaveRequest,
        WorkflowDiagnostic, WorkflowDiagnosticSeverity, WorkflowDiagnosticsResult,
        WorkflowSaveResult, WorkflowSavedRevisionSummary,
    };
    use crate::application::runtime::in_memory_runtime::InMemoryRuntime;
    use crate::application::runtime::runtime_factory::RuntimeComposition;
    use crate::ports::outbound::runtime::workflow_reflection::{
        WorkflowModuleInfoReflection, WorkflowModuleSearchReflection,
        WorkflowModuleOperationReflection, WorkflowModuleSearchMatchReflection,
        WorkflowModuleTypesReflection, WorkflowSourceReflection,
        WorkflowExecutableKind, WorkflowExecutableReflection,
    };
    use crate::domain::errors::{Result, StasisError};

    use super::{DashboardState, router};

    #[derive(Clone)]
    struct MockDashboardService {
        materialize_calls: Arc<AtomicUsize>,
        process_calls: Arc<AtomicUsize>,
    }

    impl MockDashboardService {
        fn unsupported<T>() -> Result<T> {
            Err(StasisError::PortFailure("unsupported in test".to_string()))
        }
    }

    #[async_trait]
    impl DashboardQueryService for MockDashboardService {
        async fn dashboard(&self, _inspect: Option<InspectEntity>) -> Result<DashboardDto> {
            Ok(DashboardDto {
                kpis: SystemKpiDto {
                    succeeded_jobs: 0,
                    failed_jobs: 0,
                    enqueued_jobs: 0,
                    running_jobs: 0,
                    pending_outbox: 0,
                    failed_outbox: 0,
                    healthy_nodes: 0,
                    degraded_nodes: 0,
                    offline_nodes: 0,
                    endpoint_failure_rate: "0.0%".to_string(),
                },
                job_stream: UiListPanel::<JobRowDto> {
                    items: vec![],
                    total: Some(0),
                    cursor: None,
                },
                outbox_stream: UiListPanel::<OutboxEventRowDto> {
                    items: vec![],
                    total: Some(0),
                    cursor: None,
                },
                cluster_map: ClusterMapDto { nodes: vec![] },
                inspector: InspectorView::None,
            })
        }

        async fn jobs_stream(&self) -> Result<UiListPanel<JobRowDto>> {
            Ok(UiListPanel {
                items: vec![],
                total: Some(0),
                cursor: None,
            })
        }

        async fn outbox_stream(&self) -> Result<UiListPanel<OutboxEventRowDto>> {
            Self::unsupported()
        }

        async fn endpoint_stream(&self) -> Result<UiListPanel<EndpointRowDto>> {
            Self::unsupported()
        }

        async fn recurring_stream(&self) -> Result<UiListPanel<RecurringDefinitionRowDto>> {
            Self::unsupported()
        }

        async fn cluster_stream(&self) -> Result<ClusterMapDto> {
            Self::unsupported()
        }

        async fn scheduler_materialize_now(&self, _scheduler_id: &str) -> Result<usize> {
            self.materialize_calls.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }

        async fn scheduler_process_queue_once(
            &self,
            _queue: &str,
            _worker_id: &str,
        ) -> Result<Option<String>> {
            self.process_calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        async fn scheduler_publish_pending_now(&self, _limit: usize) -> Result<usize> {
            Self::unsupported()
        }

        async fn scheduler_replay_dead_letter_now(&self, _job_id: &str) -> Result<bool> {
            Self::unsupported()
        }

        async fn workflow_save(&self, _request: WorkflowSaveRequest) -> Result<WorkflowSaveResult> {
            Self::unsupported()
        }

        async fn workflow_execute(
            &self,
            _workflow_id: &str,
            _queue: &str,
            _worker_id: &str,
        ) -> Result<WorkflowExecuteResult> {
            Self::unsupported()
        }

        async fn workflow_reflect_source(&self, _source: &str) -> Result<WorkflowSourceReflection> {
            Ok(WorkflowSourceReflection {
                count: 1,
                executables: vec![WorkflowExecutableReflection {
                    name: "Echo".to_string(),
                    kind: WorkflowExecutableKind::Query,
                    input_type: Some("String".to_string()),
                    output_type: Some("String".to_string()),
                    loop_directive_count: 0,
                    recursive_directive_count: 0,
                    retry_directive_count: 0,
                    timeout_directive_count: 0,
                    pipeline_count: 1,
                    step_count: 1,
                }],
            })
        }

        async fn workflow_modules_search(&self, _query: &str) -> Result<WorkflowModuleSearchReflection> {
            Ok(WorkflowModuleSearchReflection {
                query: "core".to_string(),
                count: 1,
                matches: vec![WorkflowModuleSearchMatchReflection {
                    module_id: "core".to_string(),
                    score: Some(0.99),
                    summary: "Core module operations".to_string(),
                    matching_ops: vec!["echo".to_string()],
                    related_examples: vec!["examples/core-echo.gr".to_string()],
                }],
            })
        }

        async fn workflow_module_info(&self, module_id: &str) -> Result<Option<WorkflowModuleInfoReflection>> {
            Ok(Some(WorkflowModuleInfoReflection {
            module_id: module_id.to_string(),
                version: "1.0.0".to_string(),
                entrypoint: "core.wasm".to_string(),
                required_capabilities: vec!["io".to_string()],
                total_ops: 1,
                exported_ops: vec![WorkflowModuleOperationReflection {
                    op: "echo".to_string(),
                    stability: "stable".to_string(),
                    effect: "Read".to_string(),
                    input_schema_ref: Some("#/types/EchoInput".to_string()),
                    output_schema_ref: Some("#/types/EchoOutput".to_string()),
                }],
            }))
        }

        async fn workflow_module_types(&self, module_id: &str) -> Result<Option<WorkflowModuleTypesReflection>> {
            Ok(Some(WorkflowModuleTypesReflection {
            module_id: module_id.to_string(),
                total_types: 1,
                types: vec![WorkflowModuleOperationReflection {
                    op: "echo".to_string(),
                    stability: "stable".to_string(),
                    effect: "Read".to_string(),
                    input_schema_ref: Some("#/types/EchoInput".to_string()),
                    output_schema_ref: Some("#/types/EchoOutput".to_string()),
                }],
            }))
        }

        async fn workflow_saved_revision_summary(
            &self,
            _workflow_id: &str,
        ) -> Result<Option<WorkflowSavedRevisionSummary>> {
            Ok(None)
        }

        async fn workflow_lsp_diagnostics(
            &self,
            _source: &str,
        ) -> Result<WorkflowDiagnosticsResult> {
            Ok(WorkflowDiagnosticsResult {
                enabled: false,
                provider: "disabled".to_string(),
                summary: "LSP diagnostics are disabled. Enable the dashboard-lsp feature to activate diagnostics preview.".to_string(),
                diagnostics: vec![WorkflowDiagnostic {
                    severity: WorkflowDiagnosticSeverity::Info,
                    message: "dashboard-lsp feature is not enabled".to_string(),
                    code: Some("LSP_DISABLED".to_string()),
                    line: None,
                    column: None,
                }],
            })
        }

        async fn inspect(&self, _entity: InspectEntity) -> Result<InspectorView> {
            Self::unsupported()
        }
    }

    #[tokio::test]
    async fn action_route_rejects_missing_bearer_and_skips_side_effects() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service)).with_action_auth_bearer_token("test-token"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn action_route_accepts_valid_bearer_and_executes_scheduler_action() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service)).with_action_auth_bearer_token("test-token"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn action_route_rejects_invalid_bearer_and_skips_side_effects() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service)).with_action_auth_bearer_token("test-token"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer wrong-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn action_route_allows_requests_when_auth_token_is_not_configured() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn malformed_queue_payload_is_rejected_without_processing_side_effect() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls,
            process_calls: Arc::clone(&process_calls),
        };

        let app = router(
            DashboardState::new(Arc::new(service)).with_action_auth_bearer_token("test-token"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/process")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"queue":"   "}"#))
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(process_calls.load(Ordering::SeqCst), 0);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should decode");
        let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(body_text.contains("queue is required"));
    }

    #[tokio::test]
    async fn non_action_route_is_not_guarded_by_action_bearer_auth() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls,
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service)).with_action_auth_bearer_token("test-token"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn workflow_save_rejects_missing_source() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls,
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service)).with_action_auth_bearer_token("test-token"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/workflows/save")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"workflow_id":"wf-1","queue":"default","source":"   "}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should decode");
        let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(body_text.contains("source is required"));
    }

    #[tokio::test]
    async fn action_route_rejects_missing_required_role_and_skips_side_effects() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service))
                .with_action_auth_bearer_token("test-token")
                .with_action_required_role("scheduler.admin"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn action_route_rejects_non_matching_required_role_and_skips_side_effects() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service))
                .with_action_auth_bearer_token("test-token")
                .with_action_required_role("scheduler.admin"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .header("x-stasis-role", "scheduler.viewer")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn action_route_accepts_matching_required_role_and_executes_action() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service))
                .with_action_auth_bearer_token("test-token")
                .with_action_required_role("scheduler.admin"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .header("x-stasis-role", "scheduler.viewer, scheduler.admin")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn action_route_accepts_matching_required_role_from_custom_header() {
        let materialize_calls = Arc::new(AtomicUsize::new(0));
        let process_calls = Arc::new(AtomicUsize::new(0));
        let service = MockDashboardService {
            materialize_calls: Arc::clone(&materialize_calls),
            process_calls,
        };

        let app = router(
            DashboardState::new(Arc::new(service))
                .with_action_auth_bearer_token("test-token")
                .with_action_required_role("scheduler.admin")
                .with_action_role_claim_header("x-dashboard-role"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .header("x-dashboard-role", "scheduler.admin")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn workflow_reflection_stream_route_renders_runtime_reflection() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-test&queue=queue.test")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("Flow Insights"));
        assert!(html.contains("Saved vs Live Drift"));
        assert!(html.contains("Readiness Guidance"));
        assert!(html.contains("No Saved Revision"));
        assert!(html.contains("Module Catalog"));
        assert!(html.contains("core"));
    }

    #[tokio::test]
    async fn workflow_reflection_stream_route_supports_module_drill_down_query() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-test&queue=queue.test&module_id=core")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("entrypoint=core.wasm"));
        assert!(html.contains("v1.0.0"));
    }

    #[tokio::test]
    async fn workflow_reflection_stream_route_supports_operation_filters() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-test&queue=queue.test&module_id=core&effect=__none__")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("No exported operations matched selected filters."));
    }

    #[tokio::test]
    async fn workflow_reflection_stream_route_uses_source_override_when_provided() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-test&queue=queue.test&mode=advanced&source=custom_source_preview")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("custom_source_preview"));
    }

    #[tokio::test]
    async fn workflows_view_route_hides_advanced_surfaces_in_guided_mode_by_default() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=queue.test")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("Open Advanced Tools"));
        assert!(html.contains("class=\"flex flex-col gap-6 wb-root\""));
        assert!(html.contains("wb-builder-grid"));
        assert!(html.contains("wb-canvas-surface"));
        assert!(html.contains("wb-node-card"));
        assert!(!html.contains("id=\"workflow-source-editor\""));
        assert!(!html.contains("id=\"workflow-reflection-panel\""));
    }

    #[tokio::test]
    async fn workflows_view_route_renders_advanced_surfaces_when_mode_is_advanced() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=queue.test&mode=advanced")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("id=\"workflow-source-editor\""));
        assert!(html.contains("id=\"workflow-reflection-panel\""));
        assert!(html.contains("wb-advanced-panel"));
        assert!(html.contains("Back To Guided Mode"));
    }

    #[tokio::test]
    async fn workflows_view_route_renders_custom_module_from_add_module_query() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=queue.test&add_module=core")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("Echo"));
        assert!(html.contains("node-fn-core-echo-1"));
    }

    #[tokio::test]
    async fn workflows_view_route_renders_selected_module_catalog_function_tiles() {
        let service = MockDashboardService {
            materialize_calls: Arc::new(AtomicUsize::new(0)),
            process_calls: Arc::new(AtomicUsize::new(0)),
        };

        let app = router(DashboardState::new(Arc::new(service)));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=queue.test&module_catalog=textops")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("Function steps in textops"));
        assert!(html.contains("textops.echo"));
        assert!(html.contains("Echo"));
    }

    #[tokio::test]
    async fn workflow_save_route_persists_modules_and_workflows_view_rehydrates_them() {
        let runtime = InMemoryRuntime::new();
        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(runtime),
        ));
        let app = router(DashboardState::new(service));

        let save_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/action/workflows/save")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"workflow_id":"wf-none","queue":"default","source":"import core from \"grapheme/core\"\n\nquery Echo {\n  core.echo(message: \"ping\") {\n    state {\n      current\n    }\n  }\n}\n","modules":" core,textops,core,unknown,healthcheck "}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("save response should build");

        assert_eq!(save_response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=default")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("id=\"workflow-modules-state\" value=\"core,textops,healthcheck\""));
        assert!(html.contains("node-fn-textops-normalize-2"));
        assert!(html.contains("node-fn-healthcheck-runtime_ready-3"));
    }

    #[tokio::test]
    async fn workflow_save_route_compiles_from_function_steps_and_rehydrates_state() {
        let runtime = InMemoryRuntime::new();
        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(runtime),
        ));
        let app = router(DashboardState::new(service));

        let save_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/action/workflows/save")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"workflow_id":"wf-none","queue":"default","modules":"core,textops","function_steps":"core.echo,textops.to_markdown"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("save response should build");

        assert_eq!(save_response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=default")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("id=\"workflow-function-steps-state\" value=\"core.echo,textops.to_markdown\""));
        assert!(html.contains("node-fn-core-echo-1"));
        assert!(html.contains("node-fn-textops-to_markdown-2"));
    }

    #[tokio::test]
    async fn workflows_view_query_modules_override_saved_revision_modules() {
        let runtime = InMemoryRuntime::new();
        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(
            RuntimeComposition::InMemory(runtime),
        ));
        let app = router(DashboardState::new(service));

        let save_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/action/workflows/save")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"workflow_id":"wf-none","queue":"default","source":"import core from \"grapheme/core\"\n\nquery Echo {\n  core.echo(message: \"ping\") {\n    state {\n      current\n    }\n  }\n}\n","modules":"core,textops,healthcheck"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("save response should build");

        assert_eq!(save_response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/view/workflows?queue=default&modules=textops,core")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");

        assert!(html.contains("id=\"workflow-modules-state\" value=\"textops,core\""));
        assert!(html.contains("node-fn-textops-normalize-1"));
        assert!(html.contains("node-fn-core-echo-2"));
        assert!(!html.contains("node-fn-healthcheck-runtime_ready-3"));
    }
}

fn render_template<T: Template>(template: T) -> Result<Html<String>, (StatusCode, String)> {
    let html = template
        .render()
        .map_err(|err| internal_error(format!("template render failed: {err}")))?;
    Ok(Html(html))
}

#[derive(Template)]
#[template(path = "dashboard/index.html")]
struct DashboardPageTemplate {
    refreshed_at: String,
}

#[derive(Template)]
#[template(path = "dashboard/streams/jobs.html")]
struct JobsStreamTemplate {
    jobs: Vec<JobRowDto>,
}

#[derive(Template)]
#[template(path = "dashboard/streams/outbox.html")]
struct OutboxStreamTemplate {
    events: Vec<OutboxEventRowDto>,
}

#[derive(Template)]
#[template(path = "dashboard/streams/nodes.html")]
struct NodesStreamTemplate {
    nodes: Vec<ClusterNodeCardDto>,
}

#[derive(Template)]
#[template(path = "dashboard/views/jobs.html")]
struct JobsViewTemplate {
    dto: DashboardDto,
}

#[derive(Template)]
#[template(path = "dashboard/views/endpoints.html")]
struct EndpointsViewTemplate {
    endpoints: Vec<EndpointHealthRowDto>,
    total: usize,
    unhealthy_count: usize,
    disabled_count: usize,
    total_deliveries: u64,
    unhealthy: Vec<EndpointHealthRowDto>,
}

#[derive(Template)]
#[template(path = "dashboard/views/cluster.html")]
struct ClusterViewTemplate {
    nodes: Vec<ClusterTelemetryNodeDto>,
    total_nodes: usize,
    worker_nodes: usize,
    healthy_nodes: usize,
    degraded_nodes: usize,
    total_queues: usize,
    active_jobs_estimate: usize,
    avg_load_index: i32,
}

#[derive(Template)]
#[template(path = "dashboard/views/outbox.html")]
struct OutboxViewTemplate {
    events: Vec<OutboxEventRowDto>,
}

#[derive(Template)]
#[template(path = "dashboard/views/deadletter.html")]
struct DeadletterViewTemplate {
    jobs: Vec<JobRowDto>,
}

#[derive(Template)]
#[template(path = "dashboard/views/scheduler.html")]
struct SchedulerViewTemplate {
    kpis: crate::dashboard::dto::SystemKpiDto,
    queues: Vec<SchedulerQueueDto>,
    recurring: Vec<RecurringDefinitionRowDto>,
}

#[derive(Template)]
#[template(path = "dashboard/views/workflows.html")]
struct WorkflowsViewTemplate {
    lanes: Vec<WorkflowLaneDto>,
    selected_workflow_name: String,
    selected_workflow_id: String,
    selected_queue: String,
    selected_module_catalog: String,
    module_catalog_options: Vec<WorkflowModuleCatalogOptionDto>,
    function_tiles: Vec<WorkflowFunctionTileDto>,
    graph_nodes: Vec<WorkflowCanvasNodeDto>,
    graph_edges: Vec<WorkflowCanvasEdgeDto>,
    selected_node: Option<WorkflowSelectedNodeDto>,
    has_selected_node: bool,
    custom_modules_csv: String,
    custom_function_steps_csv: String,
    custom_function_inputs_json: String,
    dsl_preview: String,
    grapheme_source_preview: String,
    advanced_mode: bool,
    mode_query: String,
}

#[derive(Template)]
#[template(path = "dashboard/streams/workflow_reflection.html")]
struct WorkflowReflectionStreamTemplate {
    reflection: WorkflowReflectionPreviewDto,
    advanced_mode: bool,
}

#[derive(Template)]
#[template(path = "dashboard/views/lineage.html")]
struct LineageViewTemplate {
    event_count: usize,
    active_job_count: usize,
    events: Vec<LineageEventDto>,
    selected_state: String,
    query_text: String,
    graph_nodes: Vec<LineageGraphNodeDto>,
    graph_node_count: usize,
    connection_count: usize,
    depth_label: String,
}

#[derive(Clone, Debug)]
struct SchedulerQueueDto {
    queue: String,
    enqueued_jobs: usize,
    running_jobs: usize,
    blocked_jobs: usize,
    completed_jobs: usize,
}

#[derive(Clone, Debug)]
struct WorkflowLaneDto {
    workflow_id: String,
    workflow_name: String,
    lane: String,
    total_jobs: usize,
    running_jobs: usize,
    succeeded_jobs: usize,
    failed_jobs: usize,
    selected: bool,
}

#[derive(Clone, Debug)]
struct WorkflowCanvasNodeDto {
    id: String,
    label: String,
    node_type: String,
    status: String,
    status_tone: String,
    count: usize,
    input_hint: String,
    output_hint: String,
    function_input: String,
    input_schema_ref: Option<String>,
    output_schema_ref: Option<String>,
    effect: String,
    stability: String,
    selected: bool,
}

#[derive(Clone, Debug)]
struct WorkflowModuleCatalogOptionDto {
    id: String,
    label: String,
    selected: bool,
}

#[derive(Clone, Debug)]
struct WorkflowFunctionTileDto {
    module_id: String,
    function_id: String,
    title: String,
    purpose: String,
    accent: String,
    input_schema_ref: Option<String>,
    output_schema_ref: Option<String>,
    effect: String,
    stability: String,
}

#[derive(Clone, Debug)]
struct WorkflowCanvasEdgeDto {
    from_id: String,
    to_id: String,
    label: String,
}

#[derive(Clone, Debug)]
struct WorkflowSelectedNodeDto {
    id: String,
    label: String,
    node_type: String,
    status: String,
    summary: String,
    input_hint: String,
    output_hint: String,
    function_input: String,
    input_schema_ref: Option<String>,
    output_schema_ref: Option<String>,
    effect: String,
    stability: String,
    count: usize,
}

#[derive(Clone, Debug)]
struct WorkflowReflectionPreviewDto {
    workflow_id: String,
    queue: String,
    source: String,
    selected_module_id: String,
    filter_capability: String,
    filter_effect: String,
    filter_op: String,
    executable_count: usize,
    executables: Vec<WorkflowExecutableRowDto>,
    module_matches: Vec<WorkflowModuleMatchRowDto>,
    selected_module: Option<WorkflowModuleDetailDto>,
    comparison: WorkflowRevisionComparisonDto,
    diagnostics: WorkflowDiagnosticsPreviewDto,
}

#[derive(Clone, Debug)]
struct WorkflowDiagnosticsPreviewDto {
    enabled: bool,
    provider: String,
    summary: String,
    diagnostics: Vec<WorkflowDiagnosticRowDto>,
}

#[derive(Clone, Debug)]
struct WorkflowDiagnosticRowDto {
    severity: String,
    message: String,
    code: Option<String>,
    line: Option<usize>,
    column: Option<usize>,
}

#[derive(Clone, Debug)]
struct WorkflowRevisionComparisonDto {
    status_label: String,
    status_tone: String,
    detail: String,
    saved_revision_id: Option<String>,
    saved_reflected_at_utc: Option<String>,
    saved_executable_count: Option<usize>,
    saved_source_bytes: Option<usize>,
    live_executable_count: usize,
    live_source_bytes: usize,
}

#[derive(Clone, Debug)]
struct WorkflowExecutableRowDto {
    name: String,
    kind: String,
    input_type: Option<String>,
    output_type: Option<String>,
    pipeline_count: usize,
    step_count: usize,
}

#[derive(Clone, Debug)]
struct WorkflowModuleMatchRowDto {
    module_id: String,
    summary: String,
    score: Option<f64>,
    matching_ops: Vec<String>,
}

#[derive(Clone, Debug)]
struct WorkflowModuleDetailDto {
    module_id: String,
    version: String,
    entrypoint: String,
    required_capabilities: Vec<String>,
    total_ops: usize,
    total_types: usize,
    operation_total: usize,
    operations: Vec<WorkflowModuleOperationRowDto>,
    capability_options: Vec<String>,
    effect_options: Vec<String>,
}

#[derive(Clone, Debug)]
struct WorkflowModuleOperationRowDto {
    name: String,
    stability: String,
    effect: String,
    has_input_schema: bool,
    has_output_schema: bool,
}

#[derive(Clone, Debug)]
struct EndpointHealthRowDto {
    endpoint_id: String,
    endpoint_name: String,
    protocol: String,
    target: String,
    enabled: bool,
    success_count: u64,
    failure_count: u64,
    failure_rate_label: String,
    failure_rate_percent: f64,
    unhealthy: bool,
    trend: String,
    last_error: Option<String>,
}

#[derive(Clone, Debug)]
struct ClusterTelemetryNodeDto {
    node_id: String,
    role: String,
    region: String,
    health: String,
    queue_ownership_count: usize,
    capability_count: usize,
    load_index: i32,
    memory_index: i32,
}

#[derive(Clone, Debug)]
struct LineageEventDto {
    event_id: String,
    event_type: String,
    correlation_id: String,
    delivery_state: String,
    retry_attempts: u32,
    occurred_at: String,
}

#[derive(Clone, Debug)]
struct LineageGraphNodeDto {
    lane: String,
    title: String,
    node_id: String,
    state: String,
    occurred_at: String,
}

#[derive(Template)]
#[template(path = "dashboard/action_status.html")]
struct ActionStatusTemplate {
    title: String,
    detail: String,
    kind: String,
}

#[derive(Template)]
#[template(path = "dashboard/inspector.html")]
struct InspectorTemplate {
    inspector: InspectorView,
}

impl InspectorTemplate {
    fn as_job(&self) -> Option<&JobInspectorDto> {
        if let InspectorView::Job(job) = &self.inspector {
            return Some(job);
        }
        None
    }

    fn as_attempt(&self) -> Option<&crate::dashboard::dto::AttemptInspectorDto> {
        if let InspectorView::Attempt(attempt) = &self.inspector {
            return Some(attempt);
        }
        None
    }

    fn as_endpoint(&self) -> Option<&EndpointInspectorDto> {
        if let InspectorView::Endpoint(endpoint) = &self.inspector {
            return Some(endpoint);
        }
        None
    }

    fn as_node(&self) -> Option<&NodeInspectorDto> {
        if let InspectorView::Node(node) = &self.inspector {
            return Some(node);
        }
        None
    }

    fn as_event(&self) -> Option<&EventInspectorDto> {
        if let InspectorView::Event(event) = &self.inspector {
            return Some(event);
        }
        None
    }
}
