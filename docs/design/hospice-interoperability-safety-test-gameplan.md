# Hospice Interoperability Safety Test Analysis and Gameplan

## Purpose

This document captures the current safety-critical test gap analysis and turns it into an executable gameplan for Stasis runtime hardening under hospice interoperability expectations.

Scope:

- Protect against unauthorized operator actions.
- Protect outbound delivery integrity.
- Preserve policy-governed identity mutation correctness.
- Guarantee deterministic behavior under failure and load for critical delivery paths.

## Context Snapshot (2026-05-24)

Recent implementation streams completed:

- Runtime backend expansion: `InMemory`, `SurrealMem`, `SurrealWs`, `SurrealKv`.
- Runtime SDK surface expansion (`RuntimeSdk` constructors + alias `StasisRuntime`).
- Locus compatibility coverage for new backends.
- Identity memory schema moved to `SCHEMAFULL` with explicit fields.
- Architecture and runtime docs updated for backend/schema changes.

Current status:

- Compile and targeted tests are passing for implemented changes.
- High-risk, safety-focused negative-path and abuse-case tests are still missing.

## Safety-Critical Risk Analysis

Severity ordering is based on hospice interoperability expectations where correctness, auditability, and controlled actions are non-negotiable.

### P0 - Unauthorized or Unsafe Actions

Risk:

- Dashboard mutation/action routes may allow unsafe behavior if authentication/authorization boundaries are incomplete or untested.

Primary impact:

- Unauthorized control-plane actions, accidental state mutation, and weak operator trust boundaries.

Priority targets:

- `src/dashboard/handlers.rs`
- Any route paths that trigger scheduling, endpoint inspection, event inspection, or action execution.

### P0 - Outbound Delivery Integrity and Transport Trust

Risk:

- Webhook publisher security contract coverage is minimal for auth headers, failure policy, retry semantics, and target policy checks.

Primary impact:

- Delivery to untrusted targets, silent failures, accidental data disclosure, and weak assurance in cross-system interoperability.

Priority targets:

- `src/infrastructure/runtime/http_webhook_event_publisher.rs`
- Runtime publish/replay paths and endpoint publisher adapters.

### P0 - Identity Mutation Safety Under Adverse Paths

Risk:

- Surreal-backed identity mutation flows need stronger parity assertions on failure outcomes and policy enforcement transitions.

Primary impact:

- Incorrect identity state transitions, non-deterministic approval handling, and policy bypass/regression risk.

Priority targets:

- `src/infrastructure/memory/surreal_identity_memory_store.rs`
- Identity service/domain transition orchestration paths.

### P1 - Endpoint Routing Failure Policy

Risk:

- Routing currently appears fail-fast on first endpoint failure; policy behavior for multi-endpoint fan-out is not fully locked by tests.

Primary impact:

- Partial delivery, starvation of healthy endpoints, and brittle behavior under mixed endpoint health.

Priority targets:

- Runtime endpoint routing and delivery orchestration modules.

### P1 - Outbox Query Fairness and Starvation Resistance

Risk:

- Broad scans (`type::table` style patterns) in critical query paths may degrade fairness/ordering under load.

Primary impact:

- Due-item starvation, retry skew, delayed patient-care signaling under sustained backlog.

Priority targets:

- Outbox store due-order selection and replay logic.

## Phase Plan

## Phase 0 - Backlog Lock and Test Harness Baseline

Outcome:

- Create deterministic, reproducible harness preconditions for P0 test implementation.

Deliverables:

- Finalized test naming and ownership.
- Shared fixtures for dashboard auth contexts, webhook mock servers, and identity state setup.
- Environment contract documented (internal-only test env vars remain internal docs only).

Exit criteria:

- Every P0 test has a concrete test name, file target, and expected assertion contract.

## Phase 1 - Dashboard Authn/Authz Hardening Tests (P0)

Outcome:

- Prove unsafe dashboard actions are denied without explicit authorization.

Planned tests:

1. Unauthenticated requests to action endpoints return denial status and cause no side effects.
2. Authenticated but unauthorized roles are denied for mutation endpoints.
3. Authorized role executes action successfully with expected audit/reply contract.
4. Malformed payloads are rejected with no downstream mutation.
5. Invalid bearer token is denied for mutation endpoints.
6. Non-action dashboard routes remain accessible when action-route auth is enabled.

Suggested placement:

- New dashboard-focused test module under `tests/` (for example, `tests/dashboard_authorization.rs`).

Acceptance criteria:

- Negative-path assertions verify unchanged system state.
- Positive-path assertions verify exactly-once intended mutation.
- Architecture conformance still passes.

## Phase 2 - Webhook Security Contract Tests (P0)

Outcome:

- Lock transport integrity behavior and failure semantics for outbound webhook delivery.

Planned tests:

1. Required auth header present and correct for signed/authenticated targets.
2. Missing/invalid auth data fails closed (no false success).
3. Non-2xx responses follow expected retry/failure path.
4. Network timeout/connection errors map to deterministic retry/backoff outcomes.
5. Disallowed target policy is rejected before dispatch.

Implemented in kickoff:

- Bearer authorization header is attached when configured.
- Missing auth against auth-required endpoint fails closed with non-success status error.
- Non-2xx responses return deterministic publish failure.
- Unreachable endpoint returns deterministic request failure.
- Webhook target policy now rejects non-absolute and non-http(s) targets before dispatch.
- Surreal runtime delivery diagnostics now assert retry backoff scheduling and post-retry recovery (failure then success counters and outbox state transitions).
- Surreal runtime delivery diagnostics now assert terminal failure at max attempts (`OutboxStatus::Failed`, no further `next_attempt_at`).

Suggested placement:

- `tests/control_plane_delivery_diagnostics.rs` expansion and/or dedicated `tests/webhook_security_contract.rs`.

Acceptance criteria:

- No silent success on failed sends.
- Retry counters and failure state transitions are deterministic.
- Delivery diagnostics expose enough signal for operator audit.

## Phase 3 - Identity Adverse-Path Parity Tests (P0)

Outcome:

- Confirm policy and state-machine correctness under failure and concurrent/aging scenarios.

Planned tests:

1. `ApprovalRequired` path correctness.
2. `StaleState` rejection path correctness.
3. Expired proposal rejection path correctness.
4. `PolicyDenied` path correctness.
5. `NotFound` and invalid transition handling without partial mutation.

Implemented in kickoff:

- Surreal commit parity now covers `StaleState`, `ApprovalRequired`, `ExpiredProposal`, `PolicyDenied`, and `NotFound` outcomes.
- Policy-denied commit path now asserts no transition events were persisted (no partial mutation side effects).
- Adverse-path tests now also assert proposal lifecycle persistence in history (`Rejected` for stale/policy-denied, `Expired` for expired proposal).
- Denied and expired commit paths now assert relationship version snapshot count is unchanged (no hidden version writes on non-commit outcomes).

Suggested placement:

- Extend tests in `src/infrastructure/memory/surreal_identity_memory_store.rs` plus integration coverage in `tests/locus_memory_adapters.rs` where relevant.

Acceptance criteria:

- Every negative path asserts unchanged canonical entity state.
- Proposal/version invariants remain intact after rejection.
- Memory adapter parity holds across `SurrealMem`, `SurrealWs` (env-gated), and `SurrealKv` where practical.

## Phase 4 - Routing and Outbox Reliability Tests (P1)

Outcome:

- Prove robust behavior under mixed endpoint health and high outbox pressure.

Planned tests:

1. Multi-endpoint routing policy under partial failures (fail-fast vs continue) is explicit and verified.
2. Due-order processing remains fair and starvation resistant.
3. Retry/backoff ordering remains stable under concurrent due items.
4. Replay and publish loops do not permanently starve healthy work.

Suggested placement:

- `tests/runtime_backend_parity.rs` and delivery diagnostics/replay-focused integration suites.

Acceptance criteria:

- Policy is explicit in assertions (no implicit behavior).
- Under synthetic load, due work completes within bounded attempts/time in test conditions.

Implemented in kickoff:

- Endpoint routing semantics are now explicit and tested for both fail-fast and continue-on-error modes.
- Repeated mixed-health endpoint runs now assert non-starvation for healthy endpoints under continue-on-error mode.
- Outbox pending selection now prioritizes due retry timestamps (`next_attempt_at`), with regression coverage that a future-scheduled retry cannot starve due pending events under low publish limits.
- A bounded-tick backlog stress test now validates that mixed publish failures still drain pending outbox work without starvation under constrained publish limits.

## Execution Sequence

Recommended implementation order:

1. Dashboard authn/authz tests (Phase 1).
2. Webhook security contract tests (Phase 2).
3. Identity adverse-path parity tests (Phase 3).
4. Routing and outbox reliability tests (Phase 4).

Rationale:

- This sequence first secures highest-impact external and operator-facing control points, then hardens internal reliability semantics.

## Working Agreement

For each phase:

1. Implement tests.
2. Run targeted test file(s).
3. Run `cargo test --lib`.
4. Run architecture conformance: `cargo test --test architecture_conformance`.
5. Run `cargo check`.

Completion rule:

- A phase is complete only when all new tests are green and no architecture conformance regressions are introduced.

## Definition of Done (Safety Hardening Milestone)

The hardening effort is complete when:

- P0 test suites exist and pass in CI.
- P1 reliability suites exist and pass in CI.
- Failure policies are explicit and asserted (not inferred).
- Critical negative paths prove no partial mutation or silent-success behavior.
- Internal docs remain authoritative for test-only env vars.

## Tracking Table

| Phase | Priority | Status | Owner | Notes |
| --- | --- | --- | --- | --- |
| Phase 0: Backlog lock and harness baseline | P0 | Planned | Runtime team | Define fixtures and naming |
| Phase 1: Dashboard authn/authz tests | P0 | Complete | Runtime + Dashboard | Added bearer + role-claim authz coverage, including denied unauthorized-role and allowed authorized-role action routes |
| Phase 2: Webhook security contract tests | P0 | In Progress | Runtime delivery | Added terminal max-attempt failure assertion on top of auth, target policy, and retry/recovery coverage |
| Phase 3: Identity adverse-path parity tests | P0 | In Progress | Memory + Identity | Added Surreal parity tests for all primary commit outcomes plus proposal-state persistence checks |
| Phase 4: Routing and outbox reliability tests | P1 | In Progress | Runtime delivery | Added bounded backlog drain stress test on top of fail-fast/continue policy and outbox non-starvation coverage |
| Phase 5: Dashboard productionization and grapheme depth | P1 | Planned | Runtime + Dashboard + Orchestration | Replace demo-seeded in-memory dashboard wiring with production runtime composition and deeper grapheme workflow control surfaces |

## Notes

- This document is internal planning and should evolve as tests are implemented.
- Public docs-book content should continue to expose production env variables only.
- Dashboard action routes now support internal auth gates via `STASIS_DASHBOARD_ACTION_AUTH_BEARER` and optional role claims (`STASIS_DASHBOARD_ACTION_REQUIRED_ROLE`, `STASIS_DASHBOARD_ACTION_ROLE_CLAIM_HEADER`).

## Phase 5 - Dashboard Productionization and Grapheme Depth (P1)

Outcome:

- Promote dashboard runtime behavior from demo-only to production-grade wiring.
- Replace superficial grapheme controls with runtime-backed workflow operations and diagnostics.

Current gaps (confirmed):

- Dashboard binary still boots `InMemoryRuntime` and seeds demo jobs/endpoints/nodes at startup.
- Dashboard query service is currently in-memory-specific (`InMemoryDashboardQueryService`) rather than backend-agnostic.
- Workflow actions in dashboard handlers are confirmation-level only and do not persist/execute full grapheme workflow lifecycle semantics.

Planned tasks:

1. Add dashboard runtime backend selection (`in-memory`, `surreal-mem`, `surreal-ws`, `surreal-kv`) from environment, with explicit default and startup diagnostics.
2. Introduce a backend-agnostic dashboard query/action service interface implemented for both in-memory and Surreal compositions.
3. Gate demo seed data behind an explicit opt-in flag; production startup path must be seed-free by default.
4. Replace placeholder workflow save/execute action behavior with persisted workflow definitions and runtime-backed execution receipts.
5. Add grapheme-specific dashboard diagnostics panels: guardrail violation counts, execution latency percentiles, timeout counts, and top failing imports.
6. Add end-to-end integration tests proving dashboard actions mutate real runtime stores (not seeded scaffolding) across at least in-memory + one Surreal backend.

Exit criteria:

- Dashboard starts without demo seeding by default and reflects live runtime/control-plane state.
- Workflow action endpoints return durable IDs/statuses from runtime stores.
- Grapheme sections display runtime-derived metrics and failure diagnostics.
- Release-gate suites include dashboard productionization integration coverage.
