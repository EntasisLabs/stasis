# Stasis

Stasis is a Rust framework for AI orchestration with durable runtime jobs, cluster-aware control plane primitives, and memory integration hooks.

## Architecture

- `domain`: Runtime models, policies, events, and error contracts.
- `application`: Use-cases, orchestration pipelines, and runtime handlers.
- `ports`: Stable inbound/outbound interfaces.
- `infrastructure`: Adapters for in-memory, SurrealDB, networking, and providers.
- `sdk`: Consumer-facing facades (`StasisSdk`, `RuntimeSdk`, `ControlPlaneSdk`).

## SDK Surface

- `StasisSdk`: agent registration and prompt invocation flows.
- `RuntimeSdk`: enqueue, process, publish, recurring materialization, runtime stats.
- `ControlPlaneSdk`: endpoint and cluster coordination commands.

## Quick Start

```rust
use stasis::sdk_prelude::{
    InvokeAgentRequest, InMemoryAgentRepository, MockLlmGateway, RegisterAgentRequest, StasisSdk,
};

#[tokio::main]
async fn main() {
    let repo = InMemoryAgentRepository::default();
    let llm = MockLlmGateway::new("mock response");
    let sdk = StasisSdk::new(repo, llm);

    sdk.register_agent(RegisterAgentRequest {
        id: "planner".into(),
        name: "Planner".into(),
        system_prompt: "Break tasks into steps".into(),
    })
    .await
    .unwrap();

    let out = sdk
        .invoke_agent(InvokeAgentRequest {
            agent_id: "planner".into(),
            user_prompt: "Plan a sprint kickoff".into(),
        })
        .await
        .unwrap();

    println!("{}", out.completion);
}
```

Prelude tiers:

- `stasis::prelude`: minimal, stable default imports.
- `stasis::prelude_ext`: extended runtime/memory/control-plane imports.
- `stasis::sdk_prelude`: minimal SDK-first imports for app code.

To use a real provider via `genai`, set a provider key (for example `OPENAI_API_KEY`) and optionally configure model/provider routing:

```bash
export STASIS_LLM_PROVIDER=openai
export STASIS_LLM_MODEL=gpt-4o-mini
```

You can also set a Stasis-scoped fallback key:

```bash
export STASIS_LLM_API_KEY=...
```

Provider-specific overrides are supported:

- `STASIS_OPENAI_API_KEY`
- `STASIS_ANTHROPIC_API_KEY`
- `STASIS_OLLAMA_API_KEY`

Runtime examples are available in [examples](examples).

## Embedded Dashboard

You can embed the dashboard into your existing Axum app behind an optional feature flag.

Enable feature:

```bash
cargo add stasis --features dashboard-embedded
```

Mount dashboard routes in your app code:

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

The standalone `stasis_dashboard` binary remains available for separate operations workflows.

Dashboard runtime backend selection (for `stasis_dashboard`):

- `STASIS_DASHBOARD_RUNTIME_BACKEND=in-memory|surreal-mem|surreal-ws|surreal-kv`
- `STASIS_DASHBOARD_SURREAL_NAMESPACE` (default: `stasis`)
- `STASIS_DASHBOARD_SURREAL_DATABASE` (default: `runtime`)
- `STASIS_DASHBOARD_SURREAL_ENDPOINT` (required for `surreal-ws`)
- `STASIS_DASHBOARD_SURREAL_KV_PATH` (required for `surreal-kv`)

Demo seeding remains opt-in and only applies to in-memory mode:

- `STASIS_DASHBOARD_DEMO_SEED=true`

## Documentation

- Docs index: [docs/README.md](docs/README.md)
- V1 draft: [docs/v1-runtime-draft.md](docs/v1-runtime-draft.md)
- Runtime design: [docs/design/job-runtime-design.md](docs/design/job-runtime-design.md)
- Architecture overview: [docs/architecture/overview.md](docs/architecture/overview.md)
- ADR index: [docs/adr/README.md](docs/adr/README.md)

## mdBook

- Book root: [docs-book](docs-book)
- Table of contents: [docs-book/src/SUMMARY.md](docs-book/src/SUMMARY.md)

Build locally:

```bash
mdbook build docs-book
```

Serve locally:

```bash
mdbook serve docs-book --open
```
