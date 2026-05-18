use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use chrono::Utc;
use serde::Deserialize;

use crate::dashboard::assets;
use crate::dashboard::dto::{
    ClusterNodeCardDto, DashboardDto, EndpointInspectorDto, EventInspectorDto, InspectorView,
    JobInspectorDto, JobRowDto, NodeInspectorDto, OutboxEventRowDto,
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
        .route("/stream/jobs", get(stream_jobs))
        .route("/stream/outbox", get(stream_outbox))
        .route("/stream/nodes", get(stream_nodes))
        .route("/inspect/job/{id}", get(inspect_job))
        .route("/inspect/attempt/{id}", get(inspect_attempt))
        .route("/inspect/node/{id}", get(inspect_node))
        .route("/inspect/endpoint/{id}", get(inspect_endpoint))
        .route("/inspect/event/{id}", get(inspect_event))
        .route("/assets/{name}", get(asset))
        .with_state(state)
}

async fn root() -> Redirect {
    Redirect::temporary("/dashboard")
}

#[derive(Debug, Deserialize)]
struct DashboardQuery {
    inspect: Option<String>,
}

async fn dashboard(
    State(state): State<DashboardState>,
    Query(query): Query<DashboardQuery>,
) -> Result<Html<String>, (StatusCode, String)> {
    let inspect = query.inspect.and_then(|raw| parse_inspect_ref(&raw));
    let dto = state
        .service
        .dashboard(inspect)
        .await
        .map_err(internal_error)?;

    render_template(DashboardPageTemplate {
        refreshed_at: Utc::now().to_rfc3339(),
        dto,
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
    let panel = state.service.outbox_stream().await.map_err(internal_error)?;
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

fn parse_inspect_ref(raw: &str) -> Option<InspectEntity> {
    let (kind, id) = raw.split_once(':')?;
    match kind {
        "job" => Some(InspectEntity::Job(id.to_string())),
        "attempt" => Some(InspectEntity::Attempt(id.to_string())),
        "node" => Some(InspectEntity::Node(id.to_string())),
        "endpoint" => Some(InspectEntity::Endpoint(id.to_string())),
        "event" => Some(InspectEntity::Event(id.to_string())),
        _ => None,
    }
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("dashboard error: {err}"))
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
    dto: DashboardDto,
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
