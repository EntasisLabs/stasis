# Medousa Roadmap

## Product Identity

Name: Medousa

Positioning:
- Medousa is a continuous web researcher agent powered by the Stasis orchestration runtime.
- The first release is also the canonical demonstration of runtime orchestration capabilities.
- The goal is a real operator tool, not a throwaway sample.

Tagline options:
- Medousa: Continuous Research, Orchestrated.
- Medousa: Ask Once, Stay Updated.
- Medousa: Runtime-Native Web Intelligence.

## Product Vision

Given a user question such as:
- Can you give me a report on the latest Rust trends?

Medousa should:
- Plan the research workflow.
- Execute guarded fetch and synthesis jobs.
- Return a report with evidence and citations.
- Persist lineage, diagnostics, and replay state.
- Support recurring refreshes without rebuilding pipelines.

## North-Star Outcomes

- Reliable long-running research workflows with auditability.
- Human-usable CLI and daemon workflows.
- Strong policy safety around source access and generated workflow logic.
- Backend parity across in-memory and SurrealMem runtime modes.

## Release Plan

### M0: Product Skeleton

Scope:
- Introduce Medousa identity and roadmap in repo docs.
- Define first runnable entrypoints:
  - medousa-daemon
  - medousa-cli

Acceptance:
- Documentation clearly treats Medousa as a product initiative.
- Entry points are identified and implementation plan is explicit.

### M1: Vertical Slice (Ask And Report)

Scope:
- CLI command to submit a research prompt.
- Runtime job graph with planner, fetch, and synth stages.
- Report output with citations and run summary.

Acceptance:
- A user can run one command and receive a structured report.
- Lineage and attempt diagnostics are queryable for the run.

### M2: Continuous Research

Scope:
- Recurring scheduling per topic.
- Refresh runs produce updated reports and execution lineage.

Acceptance:
- A configured topic can auto-refresh on schedule.
- Users can compare latest vs previous run summaries.

### M3: Guarded Custom Workflow Authoring

Scope:
- Model-suggested Grapheme code for specialized workflows.
- Validation and policy enforcement before execution.

Acceptance:
- Unsafe or invalid generated workflows fail with policy diagnostics.
- Safe generated workflows execute with full runtime observability.

### M4: Operator Experience

Scope:
- Better CLI ergonomics.
- Run inspection commands for replay and lineage investigation.
- Clear run summaries and failure explanations.

Acceptance:
- Operators can investigate any run without raw database inspection.

## Initial CLI Surface

- medousa ask <prompt>
- medousa run <run_id>
- medousa report <run_id>
- medousa lineage <run_id>
- medousa watch add <topic> --cron <expr> --tz <tz>
- medousa watch list

## Architecture Mapping To Existing Runtime

Already available and reusable:
- Runtime orchestration and retries.
- Outbox publishing and lineage persistence.
- Grapheme execution guardrails and diagnostics.
- Recurring scheduling and retention primitives.
- Runtime parity test suite.

Net new for Medousa:
- Product-level binaries and command UX.
- Web research workflow handlers and report assembly.
- Topic/watch management and report presentation.

## Immediate Next Build Slice

1. Create src/bin/medousa_daemon.rs and src/bin/medousa_cli.rs.
2. Implement medousa ask with one deterministic vertical slice:
   - planner
   - fetch
   - synth
3. Print report summary plus run diagnostics references.
4. Add one integration test for end-to-end ask flow.

## Definition Of Ready For Public Demo

Medousa is demo-ready when:
- A single command sequence starts daemon, submits ask, and prints report.
- One success and one policy-failure path are both demonstrated.
- Replay and lineage inspection are shown in CLI output.
- Core tests remain green.
