use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct UiPanel<T> {
    pub title: String,
    pub subtitle: Option<String>,
    pub refreshed_at: DateTime<Utc>,
    pub data: T,
}

#[derive(Clone, Debug)]
pub struct UiListPanel<T> {
    pub items: Vec<T>,
    pub total: Option<u64>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub struct UiTimelinePanel<T> {
    pub entity_id: String,
    pub events: Vec<TimelineEvent<T>>,
}

#[derive(Clone, Debug)]
pub struct TimelineEvent<T> {
    pub timestamp: DateTime<Utc>,
    pub kind: String,
    pub payload: T,
}

#[derive(Clone, Debug)]
pub struct UiMetricPanel {
    pub counters: Vec<CounterCard>,
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, Debug)]
pub struct CounterCard {
    pub key: String,
    pub label: String,
    pub value: u64,
    pub trend_hint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TimeSeries {
    pub key: String,
    pub label: String,
    pub points: Vec<TimePoint>,
}

#[derive(Clone, Debug)]
pub struct TimePoint {
    pub at: DateTime<Utc>,
    pub value: f64,
}

#[derive(Clone, Debug)]
pub struct SystemKpiDto {
    pub job_throughput: String,
    pub queue_pressure: String,
    pub outbox_lag: String,
    pub cluster_health: String,
    pub endpoint_failure_rate: String,
}

#[derive(Clone, Debug)]
pub struct DashboardDto {
    pub kpis: SystemKpiDto,
    pub job_stream: UiListPanel<JobRowDto>,
    pub outbox_stream: UiListPanel<OutboxEventRowDto>,
    pub cluster_map: ClusterMapDto,
    pub inspector: InspectorView,
}

#[derive(Clone, Debug)]
pub struct JobRowDto {
    pub id: String,
    pub queue: String,
    pub status: String,
    pub priority: i32,
    pub attempts: u32,
    pub trace_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct OutboxEventRowDto {
    pub event_id: String,
    pub event_type: String,
    pub correlation_id: String,
    pub delivery_state: String,
    pub retry_attempts: u32,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ClusterMapDto {
    pub nodes: Vec<ClusterNodeCardDto>,
}

#[derive(Clone, Debug)]
pub struct ClusterNodeCardDto {
    pub node_id: String,
    pub region: String,
    pub health: String,
    pub queue_ownership_count: usize,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct JobInspectorDto {
    pub id: String,
    pub status: String,
    pub queue: String,
    pub trace_id: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AttemptInspectorDto {
    pub attempt_id: String,
    pub job_id: String,
    pub outcome: String,
    pub worker_id: String,
    pub duration_ms: Option<u64>,
    pub guardrail_code: Option<String>,
    pub policy_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EndpointInspectorDto {
    pub endpoint_id: String,
    pub protocol: String,
    pub target: String,
    pub enabled: bool,
    pub success_count: u64,
    pub failure_count: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NodeInspectorDto {
    pub node_id: String,
    pub region: String,
    pub role: String,
    pub health: String,
    pub queue_ownership: Vec<String>,
    pub capability_tags: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct EventInspectorDto {
    pub event_id: String,
    pub event_type: String,
    pub job_id: String,
    pub correlation_id: String,
    pub trace_id: String,
    pub status: String,
}

#[derive(Clone, Debug)]
pub enum InspectorView {
    Job(JobInspectorDto),
    Attempt(AttemptInspectorDto),
    Endpoint(EndpointInspectorDto),
    Node(NodeInspectorDto),
    Event(EventInspectorDto),
    None,
}
