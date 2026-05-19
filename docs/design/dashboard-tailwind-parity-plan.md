# Dashboard Tailwind Parity Plan

## Goal

Rebuild the Stasis command center UI to match the React prototype's layout and component style while keeping the current runtime stack:
- backend: Axum + Askama
- assets: embedded static files
- live data: existing dashboard service + Medousa daemon-backed runtime data

No Figma runtime libraries will be used.

## Source UI Characteristics Captured

The prototype defines a modern operator shell with:
- left navigation rail with icon + label items
- top header with current section title and connection indicator
- section-based views:
  - Job Runtime
  - Cluster Topology
  - Workflow Builder
  - Lineage Explorer
  - Endpoint Health
  - Scheduler / Outbox / Dead Letter placeholders
- card and table-heavy content style using a Tailwind token system
- subtle surface separation and hover states

## Architecture Decision

Use Askama templates and server-rendered fragments with Tailwind-style utility classes.

Two viable implementation paths:

1. Tailwind build pipeline (recommended for parity speed)
- add a minimal frontend toolchain only for CSS compilation
- compile a static dashboard.css from Tailwind utility usage in templates
- keep runtime behavior and routing in Rust

2. Tailwind-parity SCSS (no new JS tooling)
- replicate the utility token system in SCSS
- preserve existing grass build path
- slower to reach exact visual parity

Recommendation: path 1 for fastest visual convergence.

## Template and Route Mapping

Current dashboard routes already support this split:
- main shell: /dashboard
- live fragments: /stream/jobs, /stream/outbox, /stream/nodes
- inspector: /inspect/*

Planned section routes for parity:
- /view/jobs -> existing jobs stream + KPI cards
- /view/cluster -> cluster topology cards
- /view/workflows -> workflow builder panel (read-only first)
- /view/lineage -> lineage explorer panel (read-only first)
- /view/endpoints -> endpoint diagnostics table
- /view/scheduler -> placeholder
- /view/outbox -> outbox-focused table
- /view/deadletter -> dead letter + replay queue view

## Component Parity Matrix

1. Shell and navigation
- prototype: App shell with sidebar + header
- stasis target: dashboard index template shell
- status: partially present, style differs

2. Job Runtime Board
- prototype: KPI cards + active jobs table
- stasis target: jobs stream + KPI section
- status: live data present; needs table layout and card style parity

3. Endpoint Health
- prototype: endpoint registry table + unhealthy alert card
- stasis target: new endpoint view template using existing control plane diagnostics calls
- status: data accessible via SDK; UI not built

4. Cluster Topology
- prototype: node cards with CPU/memory/queues/capabilities
- stasis target: nodes stream + expanded node cards
- status: partial card data present; CPU/memory mock fields need source strategy

5. Workflow Builder
- prototype: visual builder + DSL preview
- stasis target: read-only workflow panel first, then editing controls
- status: placeholder only

6. Lineage Explorer
- prototype: graph-style lineage panel with filters
- stasis target: lineage read model panel (table/timeline first, graph second)
- status: placeholder only

## Data Contract Updates Needed

Current DTOs cover jobs/outbox/nodes/inspector. Add:
- endpoint view DTO with:
  - endpoint id/name/protocol/target/enabled
  - success/failure counts
  - failure rate and trend
  - unhealthy flag and last error
- optional cluster telemetry extension DTO:
  - cpu usage
  - memory usage
  - active job counts per node
- lineage summary DTO:
  - nodes and edges for thread/job/event ancestry
- workflow summary DTO:
  - workflow id/name/nodes/edges
  - optional DSL preview string

## Interaction Model

Keep current server-driven model and expand it:
- nav click changes active section and triggers section fragment fetch
- each section has independent polling interval where needed
- inspector remains universal side panel
- fallback placeholder cards for unimplemented actions

## Migration Phases

### Phase 1: Visual Shell Parity
- replace current shell with prototype-equivalent layout
- preserve existing jobs/outbox/nodes data feeds
- maintain sidebar collapse + pin behavior

### Phase 2: Section Routing and View Fragments
- add section fragment endpoints
- add endpoint health and cluster topology detailed views
- map nav items to real view swaps

### Phase 3: Data Expansion
- add endpoint trend/unhealthy DTOs
- add cluster telemetry fields when available
- add outbox/dead letter dedicated views

### Phase 4: Workflow + Lineage Surfaces
- add read-only workflow and lineage pages
- later: add control actions and editing

### Phase 5: Medousa Daemon Integration
- wire dashboard service to daemon API-backed stats where applicable
- remove seeded assumptions for any remaining mock values

## Testing and Validation

For each phase:
- cargo check for stasis dashboard binary
- route smoke tests for all section and stream endpoints
- inspector route checks for each entity type
- visual regression screenshots for desktop/tablet/mobile breakpoints

## Risks

- visual mismatch if token system is not centralized
- overfetching from aggressive polling across many sections
- coupling dashboard UI assumptions to incomplete read models

Mitigations:
- centralized design tokens and spacing scale
- per-view polling only when visible
- introduce explicit view DTOs and avoid template-side inference

## Immediate Execution Slice

1. Introduce section-view endpoint model and template structure.
2. Port Job Runtime and Endpoint Health views first (highest operator value).
3. Keep existing live streams and inspector behavior intact.
4. Validate with daemon-generated real data before expanding to workflow/lineage editors.
