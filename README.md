# Stasis

Stasis is a Rust framework for building long-running AI systems that behave like distributed applications.

It provides durable runtime orchestration, background workflows, recurring scheduling, multi-agent coordination, memory integration, and cluster-aware control plane primitives while still scaling down cleanly to simple chat-style agent execution.

Unlike prompt orchestration frameworks focused primarily on request/response composition, Stasis is designed for operational reliability:

- durable execution    
- queue ownership and worker coordination    
- endpoint routing    
- runtime observability    
- scheduling and retries
- typed tool contracts
- distributed runtime control

Start with lightweight in-memory agents using `StasisSdk`, then progressively adopt runtime and control-plane capabilities as your system evolves.

## Why Stasis?

Most AI frameworks optimize for prompt composition. Stasis optimizes for production runtime behavior:

- Durable execution across backend choices.
- Orchestration reliability with explicit queues, workers, and policies.
- Runtime observability and operational diagnostics.
- Cluster coordination and endpoint routing support.
- Typed tool contracts with schema-aware invocation.
- Built-In WASM Compatible Workflow Engine with no code builder. Powered by [Grapheme](https://github.com/EntasisLabs/grapheme) 
- Memory-aware workflows with recall, store, aggregate, and rollup paths. Powered by [Locus](https://github.com/EntasisLabs/locus)

## Architecture

- `domain`: Runtime models, policies, events, and error contracts.
- `application`: Use-cases, orchestration pipelines, and runtime handlers.
- `ports`: Stable inbound/outbound interfaces.
- `infrastructure`: Adapters for in-memory, SurrealDB, networking, and providers.
- `sdk`: Consumer-facing facades (`StasisSdk`, `RuntimeSdk`, `ControlPlaneSdk`).

### Layout
```text
Client App
    |
    v
StasisSdk / RuntimeSdk / ControlPlaneSdk
    |
    v
Application Runtime + Orchestration
    |
    v
Ports
    |
    v
Infrastructure Adapters (LLM, memory, storage, transport, workflow engine)
    |
    v
Providers / Surreal Backends / Cluster Integrations
```

### Process Flow
```text
Request/Trigger
      ↓
Workflow Runtime
      ↓
Durable Job Queue
      ↓
Workers / Agents
      ↓
Memory + Tool + LLM Adapters
```

## SDK Surface

- `StasisSdk`: agent registration and prompt invocation flows.
- `RuntimeSdk`: enqueue, process, publish, recurring materialization, runtime stats.
- `ControlPlaneSdk`: endpoint and cluster coordination commands.

## When To Use Which SDK

Use this as a practical selection guide:

- `StasisSdk`:
    - Best for chat-style assistants, lightweight copilots, and direct request/response flows.
    - Start here when you do not need background workers, scheduling, or queue durability.
- `RuntimeSdk`:
    - Add when work must run asynchronously, survive retries/failures, or execute on schedules.
    - Use for workflow pipelines, outbox delivery, and operational runtime visibility.
- `ControlPlaneSdk`:
    - Add when orchestration is distributed across nodes/endpoints and needs coordination commands.
    - Use for endpoint routing, cluster ownership, and control-plane driven operations.

Typical adoption path:

1. Start with `StasisSdk` for simple chat and agent prompts.
2. Add `RuntimeSdk` when workloads become asynchronous or policy-driven.
3. Add `ControlPlaneSdk` when operating multi-node or cluster-aware deployments.

## Runtime Capabilities

- Durable backend options for queue/thread state (`surreal-ws` / `surreal-kv`), with `in-memory` for local runs.
- Retry and failure-policy aware job execution with bounded attempts.
- Recurring schedule materialization and worker-driven queue processing.
- Outbox publication workflows for delivery and endpoint diagnostics.
- Runtime stats snapshots for enqueued/running/succeeded/failed/dead-letter visibility.
- Cluster/control-plane primitives for node and endpoint coordination.

## Quick Start

```rust
use stasis::sdk_prelude::{InvokeAgentRequest, InMemoryAgentRepository, RegisterAgentRequest, StasisSdk};
use stasis::sdk_prelude_ext::GenaiLlmGateway;

#[tokio::main]
async fn main() -> stasis::domain::errors::Result<()> {
    let repo = InMemoryAgentRepository::default();
    let llm = GenaiLlmGateway::from_env();
    let sdk = StasisSdk::new(repo, llm);

    sdk.register_agent(RegisterAgentRequest {
        id: "planner".into(),
        name: "Planner".into(),
        system_prompt: "Break tasks into steps".into(),
    })
    .await?;

    let out = sdk
        .invoke_agent(InvokeAgentRequest {
            agent_id: "planner".into(),
            user_prompt: "Plan a sprint kickoff".into(),
        })
        .await?;

    println!("{}", out.completion);
    Ok(())
}
```

In this example:

- Agents are stored in-memory.
- Prompt execution uses the configured provider gateway from environment variables.
- Runtime state is local and ephemeral.
- No external infrastructure is required.
- This is `StasisSdk`-only mode (the simplest operating model).

For a deterministic local smoke test (no provider dependency), use [examples/simple_agent.rs](examples/simple_agent.rs).

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

Package note: the crates.io package is `stasis-rs` while Rust imports use `stasis`.

### Tool Macro (Signature-Driven)

`StasisTool` can be generated from a typed async function using `#[stasis_tool(...)]`:

```rust
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stasis::domain::errors::Result;
use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct SearchInput {
    query: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct SearchOutput {
    summary: String,
}

#[stasis_tool(
    name = "search_docs",
    description = "Searches internal docs",
    output_schema = true
)]
async fn search_docs(input: SearchInput) -> Result<SearchOutput> {
    Ok(SearchOutput {
        summary: format!("query={}", input.query),
    })
}

// Generated symbols:
// - struct SearchDocsTool;
// - fn search_docs_tool() -> SearchDocsTool;
```

This avoids repetitive manual trait implementations while preserving strict JSON-schema-based validation.

Macro contract:

- Function must be `async` and take exactly one typed input argument.
- Return type must be `Result<OutputType>`.
- Input type must implement `Deserialize + JsonSchema`.
- Output type must implement `Serialize`.
- When `output_schema = true`, output type must also implement `JsonSchema`.

Production-focused entry points:

- [examples/simple_agent_production.rs](examples/simple_agent_production.rs): minimal real-provider invocation.
- [examples/agentic_workflows_production.rs](examples/agentic_workflows_production.rs): full workflow set with `STASIS_EXAMPLE_TEAM_PROFILE` (`all|sre|product|support`), `STASIS_EXAMPLE_RUNTIME_BACKEND` (`in-memory|surreal-mem|surreal-ws|surreal-kv`), and `STASIS_EXAMPLE_DRY_RUN=1` for provider-safe smoke runs.
- [examples/runtime_backends_profiles.rs](examples/runtime_backends_profiles.rs): backend profile bootstrap for in-memory, Surreal websocket, and Surreal KV modes.
- [examples/team_role_workflows.rs](examples/team_role_workflows.rs): role-specific scenario packs for SRE incident, product planning, and support triage loops.

CI-friendly smoke harness:

- [scripts/smoke-agentic-workflows.sh](scripts/smoke-agentic-workflows.sh)

## Embedded Dashboard

You can embed the dashboard into your existing Axum app behind an optional feature flag.

### Main View
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/b1a083f5-79b9-4a7b-ae4b-70310da81840" />

### Grapheme Workflow Builder
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/e74345fc-cbf3-4c4e-80cb-6f0a5b779c25" />

### Job Scheduler
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/38fd383b-874f-4ecf-a40c-866dda5c6ea6" />


Enable feature:

```bash
cargo add stasis-rs --features dashboard-embedded
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

## Codebase Health Analysis Provided by ACC
<img width="1854" height="838" alt="image" src="https://github.com/user-attachments/assets/d8e74f74-d5f8-4b25-b327-7aa415d0b847" />

