# Dashboard Operations Guide

## Document Metadata

- Document Type: Operations Guide
- Audience: Operator, SRE, Runtime Engineer
- Stability: Active
- Last Verified: 2026-05-27
- Verified Against:
  - src/bin/stasis_dashboard.rs
  - src/dashboard/handlers.rs
  - src/dashboard/service.rs
  - src/dashboard/integration.rs
  - templates/dashboard/views/workflows.html
  - templates/dashboard/index.html

## Purpose

This page documents the implemented dashboard behavior for production operators.

Use this guide for:

1. Mounting and securing dashboard routes.
2. Running standalone dashboard service in production-like mode.
3. Understanding workflow builder payload contracts and compile behavior.
4. Operating scheduler, replay, and workflow actions safely.

## Deployment Modes

### Standalone binary

Run the dedicated server:

```bash
cargo run --bin stasis_dashboard
```

Defaults:

1. Bind address: 127.0.0.1:3007
2. Backend: in-memory unless overridden

Backend environment variables:

1. STASIS_DASHBOARD_RUNTIME_BACKEND=in-memory|surreal-mem|surreal-ws|surreal-kv
2. STASIS_DASHBOARD_SURREAL_NAMESPACE (default stasis)
3. STASIS_DASHBOARD_SURREAL_DATABASE (default runtime)
4. STASIS_DASHBOARD_SURREAL_ENDPOINT (required for surreal-ws)
5. STASIS_DASHBOARD_SURREAL_KV_PATH (required for surreal-kv)

Optional demo seed (in-memory only):

1. STASIS_DASHBOARD_DEMO_SEED=true

### Embedded routes in existing Axum app

Use DashboardRouterExt:

```rust
use std::sync::Arc;

use axum::Router;
use stasis::dashboard::{DashboardRouterExt, RuntimeDashboardQueryService};

fn app(service: Arc<RuntimeDashboardQueryService>) -> Router {
    Router::new().add_dashboard_with(service, |state| {
        state
            .with_action_auth_bearer_token("replace-me")
            .with_action_required_role("scheduler.admin")
    })
}
```

Reference: src/dashboard/integration.rs

## Route Map

### UI and stream routes

| Route | Method | Purpose |
|---|---|---|
| / | GET | Redirects to /dashboard |
| /dashboard | GET | Main shell |
| /view/{name} | GET | Section content swap |
| /stream/jobs | GET | Live jobs panel |
| /stream/outbox | GET | Live outbox panel |
| /stream/nodes | GET | Live cluster nodes panel |
| /stream/workflow-reflection | GET | Reflection preview stream |
| /inspect/job/{id} | GET | Job inspector |
| /inspect/attempt/{id} | GET | Attempt inspector |
| /inspect/node/{id} | GET | Node inspector |
| /inspect/endpoint/{id} | GET | Endpoint inspector |
| /inspect/event/{id} | GET | Event inspector |
| /assets/{name} | GET | Static dashboard assets |

### Action routes

All action routes are under /action and can be auth-protected.

| Route | Method | Purpose |
|---|---|---|
| /action/scheduler/materialize | POST | Materialize recurring jobs now |
| /action/scheduler/process | POST | Process one queue tick |
| /action/scheduler/publish | POST | Publish pending outbox events |
| /action/scheduler/replay | POST | Replay dead-letter job |
| /action/workflows/run-draft | POST | Compile and run draft workflow without save |
| /action/workflows/save | POST | Compile and persist workflow definition |
| /action/workflows/execute | POST | Execute saved workflow definition |

Reference: src/dashboard/handlers.rs

## Action Authorization Model

If neither token nor role is configured, action routes are open.

If configured:

1. Bearer token is checked from Authorization header.
2. Required role is checked from configurable role-claim header.

Configuration methods on DashboardState:

1. with_action_auth_bearer_token
2. with_action_required_role
3. with_action_role_claim_header

Standalone environment variables:

1. STASIS_DASHBOARD_ACTION_AUTH_BEARER
2. STASIS_DASHBOARD_ACTION_REQUIRED_ROLE
3. STASIS_DASHBOARD_ACTION_ROLE_CLAIM_HEADER

## Workflow Builder Contract

Workflow actions accept this request shape:

| Field | Type | Required | Notes |
|---|---|---|---|
| workflow_id | string | yes | Non-empty |
| queue | string | yes | Non-empty |
| source | string | conditional | Used when no graph/step compile result is produced |
| modules | string | no | CSV module ids |
| function_steps | string | no | CSV module.function tokens |
| function_inputs | string | no | JSON object where values are string payloads |
| graph_state | string | no | JSON string representing compile/topology graph |

Reference: src/dashboard/handlers.rs and src/dashboard/service.rs

### Compile precedence and mode

Save and draft-run compile in this order:

1. graph_state compile path
2. legacy function_steps compile path
3. source passthrough

Compile mode hints used by save:

1. graph_compiled
2. legacy_function_steps
3. source_passthrough

### Graph state validation

Two graph shapes are supported.

Compile-shape contract requires:

1. query.steps array exists and is non-empty.
2. Optional iterators entries must include bounded loop.max and loop.each path starting with $.

Topology-shape contract requires:

1. nodes array.
2. edges array.
3. Each node has non-empty id.

If graph_state contains both shapes, both validations apply.

## Starting Object Behavior

The workflow canvas exposes a Starting Object editor.

Frontend behavior:

1. Input must be valid JSON object.
2. Empty input is allowed (optional state).
3. Value is stored in graph_state as initial_state.

Backend compile behavior:

1. If initial_state exists, query body emits a set block before pipeline steps.
2. If initial_state is missing, no set block is emitted.

This keeps Starting Object optional while supporting deterministic seeded runs.

## Save vs Draft Run

### Save

1. Compiles source using precedence rules.
2. Persists workflow revision and graph state fields.
3. Returns status fragment summarizing revision and executable count.

### Run draft

1. Compiles source using the same precedence rules.
2. Executes immediately without persisting definition.
3. Returns draft result fragment including outcome and final state payload.

## Example Action Call

Action routes return HTML fragments suitable for HTMX swap targets.

```bash
curl -sS -X POST http://127.0.0.1:3007/action/workflows/run-draft \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer replace-me' \
  -d '{
    "workflow_id": "wf-search",
    "queue": "default",
    "graph_state": "{\"version\":1,\"nodes\":[{\"id\":\"node-fn-core-echo-1\"}],\"edges\":[],\"initial_state\":{\"query\":\"rust async runtime\",\"attempt\":1}}"
  }'
```

## Production Hardening Checklist

1. Always enable bearer token and required role for action routes.
2. Prefer surreal-ws or surreal-kv backend for durable operations.
3. Keep workflow IDs and queue naming conventions explicit and stable.
4. Track action failure rates for scheduler replay, save, and draft run endpoints.
5. Validate graph_state payload generation in CI when changing frontend serializer logic.
6. Periodically smoke-test /stream/workflow-reflection for reflection backend health.

## Related Pages

1. [Dashboard Concept](./command-center-dashboard.md)
2. [Grapheme Workflow Handlers](./grapheme-workflow-handlers.md)
3. [Lineage and Observability](./lineage-observability.md)
