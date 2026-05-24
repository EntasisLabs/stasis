use std::collections::BTreeMap;
use std::sync::Arc;

use askama::Template;
use axum::Router;
use axum::extract::{Json, Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::Utc;
use serde::Deserialize;

use crate::dashboard::assets;
use crate::dashboard::dto::{
    ClusterNodeCardDto, DashboardDto, EndpointInspectorDto, EventInspectorDto, InspectorView,
    JobInspectorDto, JobRowDto, NodeInspectorDto, OutboxEventRowDto, RecurringDefinitionRowDto,
};
use crate::dashboard::service::{DashboardQueryService, InspectEntity};

#[derive(Clone)]
pub struct DashboardState {
    service: Arc<dyn DashboardQueryService>,
}

impl DashboardState {
    pub fn new(service: Arc<dyn DashboardQueryService>) -> Self {
        Self { service }
    }
}

pub fn router(state: DashboardState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/dashboard", get(dashboard))
        .route("/view/{name}", get(view_section))
        .route("/stream/jobs", get(stream_jobs))
        .route("/stream/outbox", get(stream_outbox))
        .route("/stream/nodes", get(stream_nodes))
        .route("/inspect/job/{id}", get(inspect_job))
        .route("/inspect/attempt/{id}", get(inspect_attempt))
        .route("/inspect/node/{id}", get(inspect_node))
        .route("/inspect/endpoint/{id}", get(inspect_endpoint))
        .route("/inspect/event/{id}", get(inspect_event))
        .route(
            "/action/scheduler/materialize",
            post(action_scheduler_materialize),
        )
        .route(
            "/action/scheduler/process",
            post(action_scheduler_process_queue),
        )
        .route(
            "/action/scheduler/publish",
            post(action_scheduler_publish_pending),
        )
        .route(
            "/action/scheduler/replay",
            post(action_scheduler_replay_deadletter),
        )
        .route("/action/workflows/save", post(action_workflow_save))
        .route("/action/workflows/execute", post(action_workflow_execute))
        .route("/assets/{name}", get(asset))
        .with_state(state)
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

            let stages = if let Some(lane) = selected {
                vec![
                    WorkflowStageDto {
                        name: "Input".to_string(),
                        node_type: "source".to_string(),
                        count: lane.total_jobs,
                    },
                    WorkflowStageDto {
                        name: "Transform".to_string(),
                        node_type: "process".to_string(),
                        count: lane.running_jobs,
                    },
                    WorkflowStageDto {
                        name: "Output".to_string(),
                        node_type: "sink".to_string(),
                        count: lane.succeeded_jobs,
                    },
                ]
            } else {
                Vec::new()
            };

            let dsl_preview = format!(
                "workflow \"{}\" {{\n  input \"src\" {{ queue = \"{}\" }}\n  transform \"exec\" {{ running = {} }}\n  output \"sink\" {{ succeeded = {} failed = {} }}\n  src -> exec\n  exec -> sink\n}}",
                selected_name,
                selected_queue_name,
                stages.get(1).map(|stage| stage.count).unwrap_or(0),
                stages.get(2).map(|stage| stage.count).unwrap_or(0),
                lane_rows
                    .iter()
                    .find(|lane| lane.selected)
                    .map(|lane| lane.failed_jobs)
                    .unwrap_or(0)
            );

            render_template(WorkflowsViewTemplate {
                lanes: lane_rows,
                selected_workflow_name: selected_name,
                selected_workflow_id: selected_id,
                selected_queue: selected_queue
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
                stages,
                dsl_preview,
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
}

#[derive(Debug, Deserialize)]
struct ViewSectionQuery {
    queue: Option<String>,
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
    Json(payload): Json<WorkflowActionRequest>,
) -> Result<Html<String>, (StatusCode, String)> {
    if payload.workflow_id.trim().is_empty() {
        return render_template(ActionStatusTemplate {
            title: "Workflow Save Rejected".to_string(),
            detail: "workflow_id is required".to_string(),
            kind: "bad".to_string(),
        });
    }

    render_template(ActionStatusTemplate {
        title: "Workflow Saved".to_string(),
        detail: format!(
            "persisted {} for queue {}",
            payload.workflow_id, payload.queue
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

    let leased = state
        .service
        .scheduler_process_queue_once(payload.queue.trim(), "workflow-ui")
        .await
        .map_err(internal_error)?;

    let detail = match leased {
        Some(job_id) => format!(
            "executed one job from {} using {} (job_id={})",
            payload.queue, payload.workflow_id, job_id
        ),
        None => format!(
            "{} queued execution request, but no due jobs were available in {}",
            payload.workflow_id, payload.queue
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
    stages: Vec<WorkflowStageDto>,
    dsl_preview: String,
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
struct WorkflowStageDto {
    name: String,
    node_type: String,
    count: usize,
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
