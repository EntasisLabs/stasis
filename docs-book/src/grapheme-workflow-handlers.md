# Grapheme Workflow Handlers

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect, SRE
- Stability: Stable
- Last Verified: 2026-05-15
- Verified Against:
  - src/application/runtime/grapheme_job_handler.rs
  - src/application/runtime/grapheme_echo_job_handler.rs
  - src/application/runtime/grapheme_healthcheck_job_handler.rs
  - src/application/runtime/grapheme_textops_job_handler.rs
  - src/infrastructure/runtime/grapheme_sdk_workflow_engine.rs
  - src/ports/outbound/runtime/workflow_engine.rs
  - src/application/runtime/stasis_runtime_builder.rs

## Purpose

Document the Grapheme Workflow plane in Stasis — a policy-governed scripted workflow execution layer backed by `grapheme-sdk`. Covers the four registered job handlers, source resolution semantics, guardrail policy, diagnostics contracts, and tracing behavior.

## Scope

Grapheme jobs are one of the three capability planes in Stasis (alongside the orchestration core and Locus memory). They execute compiled Grapheme-language scripts inside a sandboxed engine with enforced policy limits. This document does not cover AI prompt handlers or Locus memory handlers.

## Invariants

1. Every Grapheme job inherits durable retry, dead-letter, and lineage semantics from the Stasis runtime.
2. Policy validation runs before engine execution. Any guardrail violation produces a `FatalFailure` — no retry is attempted.
3. All four handlers share a single `WorkflowEngine` port instance wired at builder time via `GraphemeSdkWorkflowEngine`.
4. `execution_id` in success diagnostics is the `grapheme:<artifact_id>` value returned by the engine.
5. Every attempt record receives a `diagnostics` JSON blob regardless of outcome — success and failure paths both emit structured diagnostics.
6. Grapheme handlers are enabled by default in `StasisRuntimeBuilder`. They can be suppressed with `.without_grapheme_handlers()` for tests or specialized runtimes that do not need workflow execution.

---

## System Context

Grapheme handlers sit between the Stasis worker runtime and the `grapheme-sdk` engine. The `WorkflowEngine` port decouples the handler layer from the SDK, allowing the engine implementation to be swapped in tests.

```
Worker Runtime
    │
    ▼
[GraphemeJobHandler / specialised handler]
    │
    ▼
WorkflowEngine port
    │
    ▼
GraphemeSdkWorkflowEngine (infrastructure adapter)
    │  ├── validate_source()  ← guardrail pre-check
    │  ├── tokio::time::timeout()  ← execution timeout enforcement
    │  └── GraphemeEngine::execute_source()  ← grapheme-sdk
    ▼
WorkflowExecutionOutput { run_id: "grapheme:<artifact_id>" }
```

---

## Runtime Builder Wiring

Grapheme handlers are registered by `StasisRuntimeBuilder` when `include_grapheme_handlers` is `true` (the default).

```rust
let runtime = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
    // Grapheme handlers are on by default. Suppress with:
    // .without_grapheme_handlers()
    .build()
    .await?;
```

All four handlers share one `GraphemeSdkWorkflowEngine` instance created at build time:

```rust
let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
rt.register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine.clone()))?;
rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
```

To use custom guardrails:

```rust
use stasis::infrastructure::runtime::grapheme_sdk_workflow_engine::{
    GraphemeSdkWorkflowEngine, GraphemeWorkflowGuardrails,
};
use std::time::Duration;

let guardrails = GraphemeWorkflowGuardrails {
    allowed_imports: vec!["grapheme/core".to_string(), "grapheme/text".to_string()],
    max_source_bytes: 64 * 1024,
    execution_timeout: Duration::from_secs(1),
    max_steps: Some(5_000),
    max_call_depth: Some(8),
};

let engine = Arc::new(GraphemeSdkWorkflowEngine::with_guardrails(guardrails));
```

---

## Guardrails Policy

`GraphemeWorkflowGuardrails` is evaluated by `GraphemeSdkWorkflowEngine` before the script reaches the engine. All violations produce a `POLICY_VIOLATION`-class guardrail code in the attempt diagnostics.

| Guardrail | Default | Description |
|---|---|---|
| `allowed_imports` | `["grapheme/core"]` | Import statements not in this list are rejected |
| `max_source_bytes` | `131,072` (128 KiB) | Source exceeding this limit is rejected before parsing |
| `execution_timeout` | `2 seconds` | Execution aborted if the engine does not return within this duration |
| `max_steps` | `10,000` | Maximum engine evaluation steps (passed to `GraphemeEngine::builder`) |
| `max_call_depth` | `16` | Maximum call stack depth (passed to `GraphemeEngine::builder`) |

### Import validation

The engine extracts all `import` statements and checks each module path against `allowed_imports`. Matching is exact string equality.

```grapheme
import core from "grapheme/core"   // ✓ allowed by default
import text from "grapheme/text"   // ✗ rejected unless added to allowed_imports
```

### Execution model

Scripts run inside `tokio::task::spawn_blocking` to prevent blocking the async runtime. The `tokio::time::timeout` wrapper enforces `execution_timeout` at the async boundary — the underlying synchronous engine call is cancelled if the deadline is exceeded.

---

## Source Resolution

`GraphemeJobHandler` resolves the script source from `job.payload_ref` using a prefix convention:

| Prefix | Resolution |
|---|---|
| `grapheme:inline:<source>` | Uses the text after the prefix as the script source |
| `grapheme:file:<path>` | Reads the script from the filesystem path |
| _(no prefix)_ | Treated as a bare inline source string |

**Examples:**

```rust
// Inline source
job.payload_ref = "grapheme:inline:import core from \"grapheme/core\"\nquery Ping { core.echo(message: \"ping\") { state { current } } }".to_string();

// File source
job.payload_ref = "grapheme:file:/opt/workflows/summarize.gql".to_string();

// Bare inline (no prefix)
job.payload_ref = "import core from \"grapheme/core\"\nquery Ping { core.echo(message: \"ok\") { state { current } } }".to_string();
```

Specialised handlers (`GraphemeEchoJobHandler`, `GraphemeHealthcheckJobHandler`, `GraphemeTextOpsJobHandler`) synthesise a `grapheme:inline:` source internally — callers submit a structured JSON payload, not raw source.

---

## Handler Reference

### GraphemeJobHandler

**Job type:** `workflow.grapheme`

The base Grapheme handler. Accepts `payload_ref` directly (inline, file, or bare). Used when the caller constructs the Grapheme source or file path themselves.

**Payload:** raw `payload_ref` string (source or path, see Source Resolution above)

**When to use:** Integration points that produce Grapheme scripts at runtime or need to reference a script file on disk.

---

### GraphemeEchoJobHandler

**Job type:** `workflow.grapheme.echo`

Accepts a JSON payload with a `message` field, synthesises an inline `core.echo` Grapheme script, and delegates to `GraphemeJobHandler`. Used for integration tests, health verification, and pipeline smoke checks.

**Payload:**

```json
{
  "message": "string"
}
```

| Field | Constraint |
|---|---|
| `message` | Required. Non-empty. Maximum 512 characters. |

**Guardrail behavior:**
- Empty or whitespace `message` → `POLICY_VIOLATION` / FatalFailure (no retry)
- `message` over 512 characters → `POLICY_VIOLATION` / FatalFailure

**Generated source:**

```grapheme
import core from "grapheme/core"

query Echo {
  core.echo(message: "<sanitised message>") {
    state { current }
  }
}
```

Quote characters in the message are replaced with single quotes. Newlines and carriage returns are replaced with spaces before source generation.

**Example job submission:**

```rust
use stasis::application::orchestration::stasis_workflow_job_builder::StasisWorkflowJobBuilder;

let job = StasisWorkflowJobBuilder::new("job-echo-1")
    .with_job_type("workflow.grapheme.echo")
    .with_payload_ref(serde_json::to_string(&serde_json::json!({
        "message": "hello from grapheme"
    }))?)
    .with_scheduled_at(Utc::now())
    .build();

runtime.enqueue(job).await?;
```

---

### GraphemeHealthcheckJobHandler

**Job type:** `workflow.grapheme.healthcheck`

Validates that the Grapheme engine is operational by executing an inline echo script. Accepts an optional message string in `payload_ref`. Falls back to `"stasis grapheme healthcheck"` when the payload is empty.

**Payload:** plain string in `payload_ref` (not JSON). Empty payload is valid.

**When to use:** Kubernetes/platform liveness probes, runtime startup verification, deployment validation.

**Example job submission:**

```rust
let job = StasisWorkflowJobBuilder::new("job-healthcheck-1")
    .with_job_type("workflow.grapheme.healthcheck")
    .with_payload_ref("deployment check".to_string())
    .with_scheduled_at(Utc::now())
    .build();
```

---

### GraphemeTextOpsJobHandler

**Job type:** `workflow.grapheme.textops`

Runs a text transformation pipeline (`summarize` or `extract_keywords`) over input text using a Grapheme-backed engine execution. Validates payload structure and limits before reaching the engine.

**Payload:**

```json
{
  "mode": "summarize" | "extract_keywords",
  "text": "string",
  "max_items": "number | null"
}
```

| Field | Constraint |
|---|---|
| `mode` | Required. One of `summarize` or `extract_keywords`. |
| `text` | Required. Non-empty. Maximum 4,096 characters. |
| `max_items` | Optional. Integer 1–10. Defaults to `3` when omitted. |

**Guardrail behavior:**
- Empty or whitespace `text` → `POLICY_VIOLATION` / FatalFailure
- `text` over 4,096 characters → `POLICY_VIOLATION` / FatalFailure
- `max_items` outside 1–10 range → `POLICY_VIOLATION` / FatalFailure
- Invalid JSON payload → `POLICY_VIOLATION` / FatalFailure

**Mode semantics:**

- `summarize` — extracts the first `max_items` sentences (`.`, `!`, `?` boundaries). If no sentence boundaries are found, returns the first 24 whitespace-delimited tokens.
- `extract_keywords` — tokenises by non-alphanumeric boundaries, drops stop words and tokens shorter than 4 characters, ranks by frequency (descending), then alphabetically, and returns the top `max_items` keywords as a comma-separated string.

**Example job submission:**

```rust
let job = StasisWorkflowJobBuilder::new("job-textops-1")
    .with_job_type("workflow.grapheme.textops")
    .with_payload_ref(serde_json::to_string(&serde_json::json!({
        "mode": "extract_keywords",
        "text": "Stasis provides durable orchestration for AI-driven workflows with policy enforcement.",
        "max_items": 5
    }))?)
    .with_scheduled_at(Utc::now())
    .build();
```

---

## Diagnostics and Tracing

Every Grapheme job attempt records a structured JSON `diagnostics` blob in the attempt record. This applies to both success and failure outcomes.

### Success diagnostics

```json
{
  "provider": "grapheme-sdk",
  "status": "success",
  "duration_ms": 45,
  "execution_id": "grapheme:abc123def456"
}
```

| Field | Description |
|---|---|
| `provider` | Always `"grapheme-sdk"` |
| `status` | Always `"success"` |
| `duration_ms` | Wall-clock milliseconds from engine invocation to completion |
| `execution_id` | `grapheme:<artifact_id>` returned by the engine |

### Failure diagnostics

```json
{
  "provider": "grapheme-sdk",
  "status": "failure",
  "duration_ms": 12,
  "guardrail_code": "IMPORT_NOT_ALLOWLISTED",
  "policy_reason": "grapheme policy violation: import 'grapheme/text' is not allowlisted"
}
```

| Field | Description |
|---|---|
| `provider` | Always `"grapheme-sdk"` |
| `status` | Always `"failure"` |
| `duration_ms` | Wall-clock milliseconds until failure was detected |
| `guardrail_code` | Classified failure code (see table below) |
| `policy_reason` | Full error message — only present when `guardrail_code` indicates a policy violation |

### Guardrail codes

| Code | Trigger condition |
|---|---|
| `IMPORT_NOT_ALLOWLISTED` | An `import` statement references a module not in `allowed_imports` |
| `SOURCE_TOO_LARGE` | Source byte length exceeds `max_source_bytes` |
| `EXECUTION_TIMEOUT` | Engine execution did not complete within `execution_timeout` |
| `INVALID_TIMEOUT_CONFIG` | `execution_timeout` is zero (misconfiguration guard) |
| `POLICY_VIOLATION` | Payload-level validation failure in specialised handlers, or generic engine policy error |
| `EXECUTION_ERROR` | Engine returned a non-policy error (e.g. parse error, runtime panic) |

### Lineage

All Grapheme job attempts are recorded in the durable attempt store and are reachable via `InvestigateRuntimeLineage`. The `execution_id` (`grapheme:<artifact_id>`) from success diagnostics is the stable reference to the engine artifact for a given execution.

```rust
use stasis::application::use_cases::investigate_runtime_lineage::InvestigateRuntimeLineage;

let lineage = investigate.for_job("job-echo-1").await?;
for attempt in &lineage.attempts {
    println!("attempt {} diagnostics: {}", attempt.attempt_number, attempt.diagnostics.as_deref().unwrap_or("none"));
}
```

---

## Extending the WorkflowEngine Port

The `WorkflowEngine` port in `src/ports/outbound/runtime/workflow_engine.rs` decouples all handlers from `grapheme-sdk`. In-process test doubles can substitute the real engine:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use stasis::ports::outbound::runtime::workflow_engine::{WorkflowEngine, WorkflowExecutionOutput};
use stasis::domain::errors::Result;

struct StubWorkflowEngine;

#[async_trait]
impl WorkflowEngine for StubWorkflowEngine {
    async fn execute_grapheme_source(&self, _source: &str) -> Result<WorkflowExecutionOutput> {
        Ok(WorkflowExecutionOutput {
            run_id: "grapheme:stub-001".to_string(),
        })
    }
}
```

Pass it to any handler directly:

```rust
let engine = Arc::new(StubWorkflowEngine);
let handler = GraphemeEchoJobHandler::new(engine);
```

This is the pattern used in tests that call `.without_grapheme_handlers()` and register a custom handler instead of the default `GraphemeSdkWorkflowEngine`.
