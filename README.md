# Stasis

Stasis is an agentic framework SDK scaffold using DDD + Hexagonal architecture.

## Architecture

- `domain`: Core business rules, entities, value objects, events, and errors.
- `application`: Use-cases and request/response DTOs.
- `ports`: Inbound and outbound interfaces.
- `infrastructure`: Concrete adapters (in-memory persistence, mock LLM).
- `sdk`: Composition root exposing the public SDK facade.

## Current Scaffold

- Domain entity: `Agent`
- Value object: `AgentId`
- Use-cases:
  - `RegisterAgent`
  - `InvokeAgent`
- Outbound ports:
  - `AgentRepository`
  - `LlmGateway`
- Inbound port:
  - `AgentCommands`
- Infrastructure adapters:
  - `InMemoryAgentRepository`
  - `MockLlmGateway`
- Facade:
  - `StasisSdk`

## Quick Start

```rust
use stasis::prelude::{
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

## Next Build Targets

1. Add workflow/domain aggregates for multi-agent plans.
2. Add real adapters for model providers and storage.
3. Add telemetry and event publishing ports.
4. Add policy/guardrails as domain services.

## V1 Runtime Docs

- V1 draft: [docs/v1-runtime-draft.md](docs/v1-runtime-draft.md)
- Implementation design: [docs/design/job-runtime-design.md](docs/design/job-runtime-design.md)

## Architecture Pack

- Docs index: [docs/README.md](docs/README.md)
- Architecture overview: [docs/architecture/overview.md](docs/architecture/overview.md)
- SurrealDB schema spec: [docs/architecture/surrealdb-schema.md](docs/architecture/surrealdb-schema.md)
- ADRs and decision map: [docs/adr/README.md](docs/adr/README.md)

## Architecture Book (mdBook)

- Book root: [docs-book](docs-book)
- Table of contents: [docs-book/src/SUMMARY.md](docs-book/src/SUMMARY.md)
- Build output: [docs-book/book](docs-book/book)

Build locally:

```bash
mdbook build docs-book
```

Serve locally:

```bash
mdbook serve docs-book --open
```
