# Runtime Demo Program Plan

## Objective

Build a runnable demo program that proves the runtime can:
- Accept workflow jobs across multiple Grapheme-backed workflow classes.
- Process jobs and publish outbox events.
- Surface replay, lineage, and diagnostics for successful and failed executions.
- Run on both in-memory and SurrealMem backends with no behavior drift.

## Demo Success Criteria

The demo is considered complete when all items below are true:
- A single command launches a demo run from a clean workspace.
- The demo enqueues and processes at least one job for each class:
  - workflow.grapheme.healthcheck
  - workflow.grapheme.echo
  - workflow.grapheme.textops
- The demo includes one intentional policy failure and shows:
  - guardrail_code
  - policy_reason
  - attempt/outbox lineage visibility
- The same scenario works on both backends (InMemory and SurrealMem).
- The demo prints or exports a concise run summary including outcomes and execution ids.

## Scope

In scope:
- Add a runnable runtime demo entrypoint in src/bin.
- Add deterministic fixture payloads and orchestration flow.
- Add human-readable output for:
  - job states
  - attempt diagnostics
  - lineage investigation queries
- Provide command examples and expected output snippets in docs.

Out of scope for this first demo:
- External service dependencies.
- Full UI/dashboard.
- Multi-process worker orchestration.
- Production-grade performance profiling.

## Delivery Phases

### Phase D0: Demo Skeleton

Goal:
- Create a bin target that boots runtime composition and registers handlers.

Tasks:
- Add src/bin/runtime_demo.rs.
- Add backend selector flag with values in-memory and surreal-mem.
- Register handlers:
  - GraphemeJobHandler
  - GraphemeHealthcheckJobHandler
  - GraphemeEchoJobHandler
  - GraphemeTextOpsJobHandler

Acceptance:
- Binary starts and exits cleanly with no jobs.

### Phase D1: Golden Scenario

Goal:
- Execute one deterministic, end-to-end happy path per workflow class.

Tasks:
- Enqueue fixture jobs with fixed ids and trace metadata.
- Process queue until empty using process_once loop.
- Publish outbox events.

Acceptance:
- All fixture jobs reach Succeeded.
- Output includes execution ids for Grapheme-backed jobs.

### Phase D2: Forensics Scenario

Goal:
- Prove diagnosability and lineage on failures.

Tasks:
- Enqueue one invalid payload job (for example malformed textops/echo payload).
- Capture dead-letter result.
- Query and print:
  - list_attempts_by_guardrail_code
  - list_attempts_by_execution_id when available
  - investigate_lineage by guardrail_code and by execution_id
  - get_replay_report by job id

Acceptance:
- Failure path outputs policy diagnostics and lineage events in demo output.

### Phase D3: Backend Parity Demo Run

Goal:
- Show same demo flow under both backends.

Tasks:
- Run full scenario once with in-memory backend.
- Run full scenario once with SurrealMem backend.
- Compare summary metrics and outcome counts.

Acceptance:
- No behavior differences in job outcomes or lineage shape.

### Phase D4: Demo Readme And Operator Script

Goal:
- Make demo easy to run and repeat.

Tasks:
- Add run instructions to README and docs-book.
- Add lightweight shell script or documented commands for:
  - run in-memory
  - run surreal-mem
- Include troubleshooting notes for common failures.

Acceptance:
- A new contributor can run demo in under 5 minutes.

## Minimal Implementation Blueprint

Proposed executable flow in runtime_demo:
1. Build runtime via RuntimeFactory.
2. Register optional event publisher that logs event type and job id.
3. Register four workflow handlers.
4. Enqueue deterministic fixtures.
5. Loop process_once until no more leased jobs.
6. publish_pending_events in batches.
7. Query and print replay and lineage slices.
8. Print final summary table.

## Demo Fixture Matrix

Happy path fixtures:
- workflow.grapheme.healthcheck with simple text payload.
- workflow.grapheme.echo with typed JSON payload.
- workflow.grapheme.textops summarize mode payload.
- workflow.grapheme.textops extract_keywords mode payload.

Failure fixture:
- workflow.grapheme.echo with invalid schema payload.

## Risk Register

Risk: output is too noisy to inspect quickly.
Mitigation: provide concise summary first, detailed diagnostics only on demand.

Risk: backend setup drift in local runs.
Mitigation: default backend remains in-memory; SurrealMem path is optional flag.

Risk: nondeterministic ids/time reduce clarity.
Mitigation: use injected clock and id generator option for deterministic demo mode.

## Exit Criteria For Demo-Ready Claim

Declare demo-ready when:
- All D0-D4 acceptance points are complete.
- cargo test remains green.
- A documented command pair runs the demo on both backends without manual code edits.

## Immediate Next Execution Slice

Recommended next slice:
- Implement D0 and D1 first (runtime_demo binary + golden scenario).
- Then add D2 for diagnostics showcase in same binary.
- D3 and D4 follow as polish and packaging.
