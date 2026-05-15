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
  - `GenaiChatClient` (provider adapter)
  - `GenaiLlmGateway`
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

To use a real provider via `genai`, set a provider key (for example `OPENAI_API_KEY`) and optionally set:

```bash
export STASIS_LLM_MODEL=gpt-4o-mini
```

For explicit provider/model routing (recommended):

```bash
export STASIS_LLM_PROVIDER=openai
export STASIS_LLM_MODEL=gpt-4o-mini
```

Multi-provider auth works through genai's per-adapter default env keys. You can set multiple keys at once and switch with `--provider` / `STASIS_LLM_PROVIDER`:

- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `GEMINI_API_KEY`
- `GROQ_API_KEY`
- `XAI_API_KEY`
- `DEEPSEEK_API_KEY`
- `COHERE_API_KEY`
- `TOGETHER_API_KEY`
- `FIREWORKS_API_KEY`
- `ZAI_API_KEY`

Stasis also supports auth-resolver overrides with Stasis-scoped key envs:

- `STASIS_<PROVIDER>_API_KEY` (for example `STASIS_OPENAI_API_KEY`, `STASIS_ANTHROPIC_API_KEY`, `STASIS_OLLAMA_API_KEY`)
- `STASIS_LLM_API_KEY` as a global fallback

Resolver behavior:

1. `STASIS_<PROVIDER>_API_KEY`
2. `STASIS_<GENAI_DEFAULT_KEY_ENV>` (for providers where this differs from `<PROVIDER>`)
3. `STASIS_LLM_API_KEY`
4. If none set, genai falls back to provider defaults (for example `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, etc.)

Ollama (local) does not require an API key by default.
The default endpoint used by genai is `http://localhost:11434/v1/`.
To point to a different Ollama host (or any custom provider endpoint), set one of:

```bash
export MEDOUSA_OLLAMA_BASE_URL=http://localhost:11434/v1/
# or generic
export MEDOUSA_LLM_BASE_URL=http://localhost:11434/v1/
# or stasis-wide
export STASIS_LLM_BASE_URL=http://localhost:11434/v1/
```

You can also pass runtime flags:

```bash
cargo run -p medousa --bin medousa_cli -- llm "hello" --provider ollama --model gemma3:4b --base-url http://localhost:11434/v1/
```

Then construct `GenaiLlmGateway` instead of `MockLlmGateway`.

Architecture note:
- `ChatClient` is the provider-agnostic chat abstraction layer.
- `GenaiChatClient` is the concrete provider adapter using `genai`.
- `GenaiLlmGateway` remains as a compatibility adapter for prompt-only use cases.

    ## Medousa (Workspace Crate)

    Medousa is a product-oriented web researcher agent crate in this workspace.

    Run a local ask flow:

    ```bash
    cargo run -p medousa --bin medousa_cli -- ask "can you give me a report on the latest rust trends?"
    ```

    Run a direct LLM completion via genai routing:

    ```bash
    export STASIS_LLM_PROVIDER=openai
    export STASIS_LLM_MODEL=gpt-4o-mini
    export OPENAI_API_KEY=...
    cargo run -p medousa --bin medousa_cli -- llm "Summarize the latest Rust trends in 5 bullets"
    ```

    Run with local Ollama:

    ```bash
    export STASIS_LLM_PROVIDER=ollama
    export STASIS_LLM_MODEL=gemma3:4b
    export MEDOUSA_OLLAMA_BASE_URL=http://localhost:11434/v1/
    cargo run -p medousa --bin medousa_cli -- llm "Summarize the latest Rust trends in 5 bullets"
    ```

    Start daemon loop:

    ```bash
    cargo run -p medousa --bin medousa_daemon -- --backend in-memory
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
