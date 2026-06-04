# Runtime and Dashboard Bootstrap

## Outcome

Bring up a production-like Stasis runtime with dashboard observability and secured action routes.

## Recipe

### 1. Pick runtime backend and configure environment

In-memory quick start:

```bash
export STASIS_DASHBOARD_RUNTIME_BACKEND=in-memory
export STASIS_DASHBOARD_ADDR=127.0.0.1:3007
```

Surreal websocket example:

```bash
export STASIS_DASHBOARD_RUNTIME_BACKEND=surreal-ws
export STASIS_DASHBOARD_SURREAL_ENDPOINT=ws://127.0.0.1:8000/rpc
export STASIS_DASHBOARD_SURREAL_NAMESPACE=stasis
export STASIS_DASHBOARD_SURREAL_DATABASE=runtime
export STASIS_DASHBOARD_ADDR=127.0.0.1:3007
```

### 2. Secure action routes

```bash
export STASIS_DASHBOARD_ACTION_AUTH_BEARER=replace-me
export STASIS_DASHBOARD_ACTION_REQUIRED_ROLE=scheduler.admin
export STASIS_DASHBOARD_ACTION_ROLE_CLAIM_HEADER=x-stasis-role
```

### 3. Launch dashboard server

The dashboard binary wires a production-like runtime through `StasisRuntimeBuilder` (grapheme, prompt, agent, memory, orchestration, and cluster handlers). Optional toggles:

```bash
# Enable Locus memory adapters (requires Locus configuration)
export STASIS_DASHBOARD_LOCUS_MEMORY=true

# Demo fixtures (sample jobs + endpoints; in-memory backend only)
export STASIS_DASHBOARD_DEMO_SEED=true

# OpenTelemetry (requires building with --features otel)
export STASIS_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
```

```bash
cargo run --bin stasis_dashboard

# With OpenTelemetry export enabled at compile time:
cargo run --features otel --bin stasis_dashboard
```

Open:

1. http://127.0.0.1:3007/dashboard

### 4. Smoke-test protected action route

```bash
curl -i -X POST http://127.0.0.1:3007/action/scheduler/materialize
```

Expected result without auth: 401 Unauthorized.

Then call with auth:

```bash
curl -i -X POST http://127.0.0.1:3007/action/scheduler/materialize \
  -H 'authorization: Bearer replace-me' \
  -H 'x-stasis-role: scheduler.admin'
```

Expected result: 200 and HTML action status fragment.

## Embedded Variant

Mount dashboard routes into your existing Axum application using the same bootstrap as the standalone binary:

```rust
use std::sync::Arc;

use axum::Router;
use stasis::dashboard::{
    build_dashboard_query_service, DashboardBootstrapOptions, DashboardRouterExt, DashboardState,
};

#[tokio::main]
async fn main() -> stasis::domain::errors::Result<()> {
    stasis::config_prelude::bootstrap()?;

    let service = build_dashboard_query_service(DashboardBootstrapOptions::default()).await?;

    let app = Router::new().add_dashboard_with(service, |state| {
        state
            .with_action_auth_bearer_token("replace-me")
            .with_action_required_role("scheduler.admin")
            .with_action_role_claim_header("x-stasis-role")
    });

    // bind and serve...
    Ok(())
}
```

## Production Notes

1. Prefer surreal-ws or surreal-kv for durable job and workflow history.
2. Keep STASIS_DASHBOARD_ACTION_AUTH_BEARER in secret storage, not source control.
3. Add health checks for `/dashboard` and one live view such as `/view/jobs`.
4. Optional: probe `/stream/jobs` as an HTMX fragment health check for custom integrations.
5. Alert on repeated non-200 responses from /action routes.
6. For OTLP verification, see [OpenTelemetry — Local collector smoke test](../opentelemetry.md#local-collector-smoke-test).
