#Dasboarh Proto-type ideas

1. Global Layout: “Control Plane Cockpit”

Think 3-tier layout:


┌─────────────────────────────────────────────────────────┐
│ Global Header: Cluster status • Queues • Search • Trace │
├───────────────┬───────────────────────────┬─────────────┤
│ NAV (left)    │ MAIN WORKSPACE            │ INSPECTOR   │
│               │                           │ (right)     │
│ - Jobs        │ dynamic view              │ selected    │
│ - Scheduler   │ (boards/graphs/streams)   │ entity      │
│ - Outbox      │                           │ details     │
│ - Cluster     │                           │             │
│ - Endpoints   │                           │             │
│ - Lineage     │                           │             │
└───────────────┴───────────────────────────┴─────────────┘

The key idea:
everything is inspectable → everything opens in the right panel

2. Core Home Dashboard (the “Ops Brain”)

This is not a list. It’s a state synthesis layer.

Top Row: System Health KPIs

From your metrics section:

Job throughput (succeeded / failed / retry / DLQ)
Queue pressure (enqueued vs running ratio)
Outbox lag (pending vs published)
Cluster health (healthy / degraded / offline nodes)
Endpoint failure rate trend

Each KPI is clickable → filters everything else.

Middle Row: “Live System Flow”

Split into 3 live panels:

A. Job Runtime Stream

A live table / kanban hybrid of Job Runtime Board:

Enqueued → Running → Succeeded / Failed
Color-coded by queue + priority
Hover reveals:
correlation_id
trace_id
lease owner
retry state

Click job → opens full timeline + attempts

B. Outbox Event Stream

Outbox Event Stream

Pending / Published / Failed lanes
Each event shows:
event_type
correlation_id
delivery state
retry attempts

Click event → shows delivery chain + endpoint results

C. Cluster Node Grid

Cluster Topology View

Nodes grouped by region
Health overlay:
Healthy / Degraded / Offline
Queue ownership heatmap
Heartbeat freshness indicator

Click node → shows:

leases held
queues owned
recent failures
3. Left Navigation = “Domains, not Features”

Instead of listing 13 systems flat, group them into operational domains:

EXECUTION
Jobs
Attempts
Scheduler
DELIVERY
Outbox
Endpoints
Routing policies
DISTRIBUTED CONTROL
Cluster
Forwarding
Node health
DEBUGGING
Lineage explorer
Thread graph
Replay system
OBSERVABILITY
Metrics
Trends
KPIs

This avoids the “tool dump sidebar problem”.

4. The Right Panel (most important part)

This is where your system becomes powerful.

Whenever anything is clicked, the right panel becomes a contextual inspector:

Job selected:
full lifecycle timeline (scheduled → lease → execute → retry chain)
attempts table
guardrail failures
STTP input/output links
Endpoint selected:
success/failure timeline
last 100 deliveries
failure clustering by error signature
Node selected:
heartbeat history
queue ownership map
recent command forwards

This replaces 10 different pages.

5. Deep Drilldown Views (your “special weapons”)

These are not tabs. These are full-screen modes.

A. Lineage Explorer (Graph Mode)

Lineage Explorer

nodes = jobs / executions / events
edges = causation_id / trace_id / thread_fork
filters:
guardrail_code
execution_id
retry chains

This becomes your “why did this happen?” screen.

B. Replay Mode

Dead-letter Replay Console

select DLQ job
show full execution reconstruction
“Replay now” button
diff view:
original vs replay attempt
C. Scheduler Timeline

Recurring Job Scheduler Panel

vertical time axis
cron jobs plotted as recurring lanes
next_run_at vs last_run_at drift visualization
D. Endpoint Health Radar

Endpoint Registry Dashboard

grouped by protocol (Kafka / HTTP / etc.)
failure trend lines
unhealthy flag clustering
retry pressure indicators
6. The Key UX Idea: “Causal Clicking”

Every click should preserve a causal breadcrumb chain:

Cluster → Node → Queue → Job → Attempt → Event → Endpoint delivery

This is what turns your system from “dashboard” into debuggable reality model.

Add a breadcrumb bar at top:

Cluster / eu-west-1 / worker-12 / queue:billing / job:8831 / attempt:3
7. A Hidden but Powerful Panel: “Control Surface”

This is where your operator commands live:

retry job
replay DLQ
rebalance queues
forward command
enable/disable endpoint
prune retention

But instead of buttons everywhere:

👉 one unified command palette + modal system

Example:

⌘K → “replay dead letter”
⌘K → “failover cluster node”
⌘K → “rebalance queue billing-api”
8. Mental Model Shift (important)

Right now your system is:

many subsystems

The UI should present it as:

one execution organism with observable internal organs

So the UI primitives become:

State (jobs, nodes, endpoints)
Flow (outbox, scheduler)
Causality (lineage, threads)
Control (commands, replay)
Health (metrics, trends)

Everything maps into one of those.




0. Core Architecture Model (what you’re really building)

You already have:

Rust domain models (jobs, attempts, endpoints, etc.)
A DTO layer
A HTMX command mapper
Askama templates
Axum handlers

So the UI system becomes:

Rust Domain → Query Layer → Dashboard DTO → Askama Template → HTMX swaps → DOM

The key design principle:

Every panel is a pure DTO renderer, never a business logic owner.

1. UI SYSTEM PRIMITIVES (define these FIRST in Rust)

You want a small set of reusable DTO “UI blocks”.

1.1 Base UI Envelope

Everything in the dashboard should conform to:

pub struct UiPanel<T> {
    pub title: String,
    pub subtitle: Option<String>,
    pub refreshed_at: chrono::DateTime<chrono::Utc>,
    pub data: T,
}

This gives every panel:

consistent header
time awareness (critical for ops UI)
swappable content
1.2 Standard “List Panel”

Used for jobs, endpoints, attempts, events:

pub struct UiListPanel<T> {
    pub items: Vec<T>,
    pub total: Option<u64>,
    pub cursor: Option<String>,
}
1.3 “Timeline Panel” (critical for your system)

Used for:

job lifecycle
attempts
outbox delivery
scheduler runs
pub struct UiTimelinePanel<T> {
    pub entity_id: String,
    pub events: Vec<TimelineEvent<T>>,
}
pub struct TimelineEvent<T> {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub kind: String,
    pub payload: T,
}
1.4 “Metrics Panel”
pub struct UiMetricPanel {
    pub counters: Vec<CounterCard>,
    pub timeseries: Vec<TimeSeries>,
}
1.5 “Inspector Panel” (RIGHT SIDE CORE)

This is your killer abstraction.

pub enum InspectorView {
    Job(JobInspectorDto),
    Attempt(AttemptInspectorDto),
    Endpoint(EndpointInspectorDto),
    Node(NodeInspectorDto),
    Event(EventInspectorDto),
    None,
}

This is what drives your entire right-side UI.

2. DASHBOARD LAYOUT DTO (MAIN PAGE CONTRACT)

This is what your /dashboard endpoint returns.

pub struct DashboardDto {
    pub kpis: SystemKpiDto,
    pub job_stream: UiListPanel<JobRowDto>,
    pub outbox_stream: UiListPanel<OutboxEventDto>,
    pub cluster_map: ClusterMapDto,
    pub inspector: InspectorView,
}

This maps directly to the 3-column layout:

KPIs (top)
LEFT: streams
CENTER: main view (optional drilldown)
RIGHT: inspector
3. DOMAIN → DTO MAPPING STRATEGY (IMPORTANT)

You should NOT let Askama touch raw domain objects.

Instead define a strict mapping layer:

/mappers
    job_mapper.rs
    endpoint_mapper.rs
    cluster_mapper.rs
    outbox_mapper.rs

Example:

pub fn map_job_to_row(job: Job) -> JobRowDto {
    JobRowDto {
        id: job.id,
        queue: job.queue,
        status: job.state.into(),
        priority: job.priority,
        attempts: job.attempts.len(),
        trace_id: job.trace_id,
        updated_at: job.updated_at,
    }
}

This ensures HTMX/UI never leaks domain complexity.

4. HTMX CONTRACT DESIGN (VERY IMPORTANT)

You said you already have a mapper—this is how to structure it cleanly.

4.1 Core HTMX actions

Standardize:

1. Replace panel
hx-get="/panel/job/{id}"
hx-target="#inspector"
hx-swap="innerHTML"
2. Refresh stream
hx-get="/stream/jobs"
hx-target="#job-stream"
hx-swap="outerHTML"
hx-trigger="every 2s"
3. Drilldown navigation
hx-get="/dashboard?inspect=job:{id}"
4.2 HTMX command DTO (your mapper output)

If your library generates HTMX commands, standardize output like:

pub struct HtmxSwap {
    pub target: String,
    pub swap: String,
    pub url: String,
    pub trigger: Option<String>,
}

Example mapping:

HtmxSwap {
    target: "#inspector".to_string(),
    swap: "innerHTML".to_string(),
    url: format!("/inspect/job/{}", job_id),
    trigger: None,
}
5. ASKAMA TEMPLATE STRUCTURE

You want templates to reflect panels, not pages.

5.1 Base layout
<!-- base.html -->
<div class="grid grid-cols-12 h-screen">

  <aside class="col-span-2">
    {% include "nav.html" %}
  </aside>

  <main class="col-span-7">
    {% block main %}{% endblock %}
  </main>

  <aside id="inspector" class="col-span-3">
    {% include "inspector/none.html" %}
  </aside>

</div>
5.2 Job Stream Component
<!-- job_stream.html -->
<div id="job-stream">
  {% for job in jobs.items %}
    <div
      hx-get="/inspect/job/{{ job.id }}"
      hx-target="#inspector"
      hx-swap="innerHTML"
      class="job-row {{ job.status }}"
    >
      <span>{{ job.id }}</span>
      <span>{{ job.queue }}</span>
      <span>{{ job.status }}</span>
      <span>{{ job.attempts }}</span>
    </div>
  {% endfor %}
</div>
5.3 Inspector Template (dynamic)
<!-- inspector/job.html -->
<div>
  <h2>Job {{ job.id }}</h2>

  <div>Status: {{ job.status }}</div>
  <div>Trace: {{ job.trace_id }}</div>

  <button hx-post="/job/{{ job.id }}/retry"
          hx-target="#inspector"
          hx-swap="innerHTML">
    Retry
  </button>
</div>
6. ROUTING STRUCTURE (AXUM)

This is where your copilot will actually wire things.

6.1 Core routes
GET  /dashboard
GET  /stream/jobs
GET  /stream/outbox
GET  /stream/nodes

GET  /inspect/job/:id
GET  /inspect/attempt/:id
GET  /inspect/node/:id
GET  /inspect/endpoint/:id
6.2 Action routes
POST /job/:id/retry
POST /job/:id/cancel
POST /job/:id/replay

POST /endpoint/:id/disable
POST /node/:id/heartbeat
7. STATE MODELING (IMPORTANT FOR LIVE SYSTEM FEEL)

If you want HTMX to feel “alive”, you need consistent polling sources:

Streams:
/stream/jobs
/stream/outbox
/stream/cluster

Each returns partial HTML fragments, not JSON.

8. HOW YOUR COPILOT SHOULD THINK

Give it this instruction:

“Never output business logic in templates. Only render DTOs. All transformations happen in Rust mapper layer.”

And:

“Every UI component must correspond to exactly one DTO struct.”

And:

“Every inspector view must be a separate endpoint returning a fragment.”

9. CLEAN MENTAL MODEL (IMPORTANT)

Your system becomes:

State domains
Jobs
Attempts
Outbox
Endpoints
Cluster nodes
Views
Stream view (list DTOs)
Inspector view (single entity DTO)
Timeline view (event DTOs)
Metrics view (aggregated DTOs)
Actions
retry
replay
forward
rebalance
disable