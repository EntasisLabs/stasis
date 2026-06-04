# Getting Started

## Document Metadata

- Document Type: Quickstart Guide
- Audience: Beginner, Engineer
- Stability: Evolving
- Last Verified: 2026-05-24
- Verified Against:
  - src/application/runtime/stasis_runtime_builder.rs
  - src/application/orchestration/runtime_job_payloads.rs
  - src/application/orchestration/runtime_workflow_job_builder.rs
  - src/sdk/runtime_sdk.rs

## Purpose

Get a first Stasis runtime working in minutes: build a runtime, enqueue one job, process one queue tick, and inspect resulting runtime state.

Naming note:

1. This guide uses `StasisRuntime` as the public name.
2. In current code, this is implemented by `RuntimeSdk`.

## What You Will Build

In this page, you will:

1. Create an in-memory runtime.
2. Build a prompt workflow job payload.
3. Enqueue and process one job.
4. Read a simple runtime stats snapshot.

## First Runtime Walkthrough

```rust
use stasis::application::orchestration::runtime_job_payloads::PromptJobPayload;
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::prelude::{RuntimeBackend, RuntimeSdk, StasisRuntimeBuilder};

#[tokio::main]
async fn main() -> stasis::domain::errors::Result<()> {
    // 1) Build a runtime composition.
    let composition = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .build()
        .await?;

    // 2) Wrap it with the backend-agnostic facade.
    let runtime = RuntimeSdk::new(composition); // StasisRuntime naming in docs

    // 3) Create one prompt job payload.
    let payload = PromptJobPayload {
        user_prompt: "Say hello from Stasis".to_string(),
        system_prompt: Some("You are a concise assistant".to_string()),
        policy_profile: None,
        model_hint: None,
        memory_policy: None,
    };

    // 4) Build and enqueue the job.
    let job = RuntimeWorkflowJobBuilder::for_prompt("hello-job", &payload)?.build();
    runtime.enqueue(job).await?;

    // 5) Process one queue tick.
    let processed = runtime.process_once("default", "worker-1").await?;
    println!("processed job id: {:?}", processed);

    // 6) Inspect snapshot metrics.
    let stats = runtime.stats_snapshot(100).await?;
    println!(
        "enqueued={}, running={}, succeeded={}, dead_letter={}",
        stats.enqueued_jobs, stats.running_jobs, stats.succeeded_jobs, stats.dead_letter_jobs
    );

    Ok(())
}
```

## Common Beginner Notes

1. Use `RuntimeBackend::InMemory` first. It removes database setup from your first run.
2. Keep `queue` as `default` until your first workflow is stable.
3. `process_once` is perfect for tests and tutorials. Production workers typically run it in a loop.
4. If no chat client is configured, Stasis uses `GenaiChatClient::from_env()` when handlers need model calls.

## Environment and secrets

For local development, copy `.env.example` to `.env` and call `bootstrap()` once at startup:

```rust
use stasis::config_prelude::{bootstrap, with_default};

#[tokio::main]
async fn main() -> stasis::domain::errors::Result<()> {
    bootstrap().map_err(stasis::domain::errors::StasisError::PortFailure)?;
    let model = with_default("STASIS_LLM_MODEL", "gpt-4o-mini");
    // ...
    Ok(())
}
```

See [Environment Configuration](./environment-configuration.md) for Vault/file secret mounts and the full variable reference.

## What To Read Next

1. [Environment Configuration](./environment-configuration.md) for `.env`, secrets dir, and Vault-friendly mounts.
2. [Runtime Builder and Wiring Guide](./runtime-builder.md) for all builder options.
2. [Job Runtime Design](./runtime-job-design.md) for lifecycle and durability semantics.
3. [Extension Points and Port Contracts](./extension-points.md) for custom adapters.
4. [Production Agentic Workflows](./cookbook/production-agentic-workflows.md) for real-provider loop and orchestration examples.
5. [Dashboard Operations Guide](./dashboard-operations-guide.md) for operator workflows and route contracts.
6. [Cookbook Overview](./cookbook.md) for copy/paste production recipes.