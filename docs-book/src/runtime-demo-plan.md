# Runtime Demo Program Plan

## Objective

Build a runnable demo program that proves runtime execution, observability, and lineage flows across in-memory and SurrealMem backends.

## Demo Success Criteria

- One command launches the demo scenario.
- Demo runs workflow.grapheme.healthcheck, workflow.grapheme.echo, and workflow.grapheme.textops.
- Demo includes one intentional policy failure and prints diagnostics and lineage.
- Scenario can be run against both in-memory and SurrealMem backends.

## Delivery Phases

## D0: Demo Skeleton

- Add src/bin/runtime_demo.rs.
- Add backend selector for in-memory or surreal-mem.
- Register Grapheme handlers.

## D1: Golden Scenario

- Enqueue deterministic happy-path jobs.
- Run process loop and publish outbox.
- Print summary with states and execution ids.

## D2: Forensics Scenario

- Add one invalid payload job.
- Print replay report and lineage investigation output.

## D3: Backend Parity Demo Run

- Run full scenario on both backends.
- Confirm parity in outcome shape.

## D4: Demo Docs And Runbook

- Add command examples and expected output snippets.
- Add quick troubleshooting for local runs.

## Recommended Next Slice

Implement D0 and D1 first, then immediately layer D2 into the same binary.
