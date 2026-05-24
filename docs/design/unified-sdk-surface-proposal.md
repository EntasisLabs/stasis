# Unified SDK Surface Proposal

Status: Draft
Date: 2026-05-24
Owner: Stasis Core
Audience: SDK, Runtime, DX, Architecture

## 1. Purpose

Define a single, beginner-friendly SDK entrypoint that:

1. Hides composition complexity for common usage.
2. Preserves advanced extension capability for power users.
3. Creates a stable API contract that can be wrapped in future language SDKs (Python first).

## 2. Problem Statement

Current runtime integration uses two concepts:

1. `StasisRuntimeBuilder` for composition and wiring.
2. `RuntimeSdk` for backend-agnostic runtime operations.

Naming issue:

1. The public name `RuntimeSdk` feels implementation-oriented.
2. The desired premium public surface is `StasisRuntime`.

This is flexible but introduces first-run friction:

1. New users must understand composition internals before completing simple tasks.
2. The `RuntimeComposition` type leaks into application-level code too early.
3. Documentation and onboarding require explaining architecture before enabling useful outcomes.

## 3. Goals

1. One primary public runtime surface for 80-90% of users.
2. One-line in-memory startup for local/testing workflows.
3. One-line durable startup path with explicit config.
4. Backend-neutral runtime operations API.
5. Compatibility with current runtime builder and runtime operations behavior.
6. Language-wrapper readiness via stable config and operation contracts.
7. Adopt `StasisRuntime` as the clean public name.

## 4. Non-Goals

1. Removing advanced composition controls from Rust.
2. Rewriting runtime internals.
3. Solving distributed deployment topology in this phase.
4. Defining full polyglot transport right now (only contract readiness is required).

## 5. Proposed Public API Shape

## 5.1 Primary Entrypoint

Make the existing runtime facade the primary user entrypoint, with clean naming:

```rust
pub type StasisRuntime = RuntimeSdk;

impl RuntimeSdk {
  pub async fn in_memory() -> Result<Self>;
  pub async fn surreal_mem(namespace: impl Into<String>, database: impl Into<String>) -> Result<Self>;
  pub async fn surreal_ws(endpoint: impl Into<String>, namespace: impl Into<String>, database: impl Into<String>) -> Result<Self>;
  pub async fn surreal_kv(path: impl Into<String>, namespace: impl Into<String>, database: impl Into<String>) -> Result<Self>;

  pub async fn from_builder(builder: StasisRuntimeBuilder) -> Result<Self>;
}
```

## 5.2 Single Operational Handle

`StasisRuntime` (alias over `RuntimeSdk`) is the single runtime interaction surface:

```rust
#[derive(Clone)]
pub struct RuntimeSdk {
    pub async fn enqueue(&self, job: NewJob) -> Result<()>;
    pub async fn process_once(&self, queue: &str, worker_id: &str) -> Result<Option<String>>;
    pub async fn register_recurring(&self, definition: RecurringDefinition) -> Result<()>;
    pub async fn publish_pending_events(&self, limit: usize) -> Result<usize>;
    pub async fn stats_snapshot(&self, pending_limit: usize) -> Result<RuntimeStatsSnapshot>;
}
```

Notes:

1. No new top-level runtime struct is required.
2. Advanced composition remains available through `StasisRuntimeBuilder`.

## 5.3 Convenience Constructors

Provide minimal task-first constructors directly on `RuntimeSdk`/`StasisRuntime`.

## 6. Internal Mapping (No Runtime Rewrite)

Implementation should be an extension of existing components with no new runtime wrapper type:

1. Add constructors on `RuntimeSdk` that internally call `StasisRuntimeBuilder`.
2. Add a public `type StasisRuntime = RuntimeSdk` alias.
3. Existing runtime behavior, handler registration, and stores remain unchanged.

This keeps risk low and preserves test parity.

## 7. Backward Compatibility and Deprecation

## Phase 1: Additive

1. Add `RuntimeSdk` convenience constructors (`in_memory`, `surreal_mem`, `surreal_ws`, `surreal_kv`, `from_builder`).
2. Add `StasisRuntime` alias as public preferred naming.
2. Keep `StasisRuntimeBuilder` and `RuntimeSdk` fully supported.
3. Update docs and quickstarts to prefer unified SDK path.

## Phase 2: Soft Guidance

1. Mark `StasisRuntimeBuilder` and direct `RuntimeComposition` usage as advanced APIs in docs.
2. Add lint/docs guidance steering new users to `StasisRuntime`.

## Phase 3: Optional Deprecation Review

Evaluate deprecation only after:

1. SDK parity is complete.
2. Internal and external migration is low-risk.
3. Language wrapper contract is stable.

## 8. Configuration Contract for Polyglot Wrappers

Define a stable, language-neutral configuration model now.

Example shape:

```json
{
  "backend": {
    "kind": "in_memory | surreal_mem | surreal_ws | surreal_kv",
    "endpoint": "ws://127.0.0.1:8000/rpc",
    "path": "./data/stasis-runtime",
    "namespace": "stasis",
    "database": "runtime"
  },
  "memory": {
    "enable_locus": true
  },
  "chat": {
    "model": "openai::gpt-4o-mini"
  },
  "features": {
    "grapheme_handlers": true,
    "agent_handlers": true,
    "memory_operation_handlers": true
  }
}
```

Guidance:

1. Keep config fields explicit, versioned, and forward-compatible.
2. Avoid exposing Rust-specific trait wiring in cross-language APIs.
3. Represent operations as request/response DTO contracts where possible.

## 9. Python Wrapper Readiness Strategy

Recommended progression:

1. Stabilize Rust runtime API first (`StasisRuntime` / `RuntimeSdk`).
2. Freeze minimal config and operation contracts.
3. Expose an embedding boundary:
   - Option A: FFI boundary for direct embedding.
   - Option B: Sidecar process with JSON-RPC/gRPC bridge.
4. Implement Python wrapper against stable contract, not runtime internals.

Design rule:

1. Python SDK should map to the same single-handle mental model.
2. Keep naming and lifecycle consistent across Rust and Python.

## 10. DX Acceptance Criteria

The DX target is met when:

1. A beginner can enqueue and process a job in under 20 lines.
2. A beginner does not need to understand `RuntimeComposition` to get started.
3. One official quickstart path exists and is consistent across docs.
4. Advanced extension points remain available without regressions.
5. Future wrapper authors can consume a stable contract without learning Rust internals.

## 11. Suggested Implementation Slices

1. Add convenience constructors on `RuntimeSdk` (`in_memory`, `surreal_mem`, `surreal_ws`, `surreal_kv`, `from_builder`).
2. Add and export `StasisRuntime` alias.
3. Keep `RuntimeSdk` as compatibility name for transition period.
4. Update docs-book examples to unified SDK path.
5. Add architecture conformance/tests for API parity behavior.

## 12. Open Questions

1. Should control-plane methods be flattened onto `StasisRuntime` or remain separate?
2. Should `RuntimeSdk::from_builder` be the only advanced bridge, or do we need more adapter injection on runtime constructors?
3. Do we want one feature-flagged crate surface (`stasis`) or a separate slim crate (`stasis-sdk`)?
4. Which polyglot bridge is preferred for Python first: FFI or sidecar RPC?

## 13. Recommendation

Adopt a `StasisRuntime`-first public story now (implemented via `RuntimeSdk` + alias), keep the current builder as the advanced composition path, and shift official docs to one beginner-first runtime entrypoint. This maximizes immediate DX while minimizing migration risk and sets up a clean contract for future Python and multi-language SDK wrappers.
