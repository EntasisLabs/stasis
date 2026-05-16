# Medousa Grapheme CLI-Mimic Tooling Plan

Status: Active
Owner: Medousa
Last updated: 2026-05-16

## Objective

Expose Grapheme capabilities to Medousa agents through tools that mirror terminal CLI workflows. The agent should discover modules, inspect operations, run scripts, and then promote successful flows into durable Stasis jobs.

## Why This Matters

1. Agents can learn unknown tools at runtime using discovery-first behavior.
2. Tool usage stays aligned with real CLI semantics.
3. Successful exploratory runs can be upgraded into recurring runtime automation.

## Target Workflow

1. Discover capability:
   1. `grapheme modules search <query>`
   2. `grapheme modules info <module>`
   3. `grapheme modules ops <query>`
   4. `grapheme examples list/show`
2. Draft script from discovered syntax.
3. Execute script via `grapheme run`.
4. Observe result and refine.
5. Enqueue and schedule recurring jobs in Stasis runtime.

## Phase Plan

## Phase A: Discovery + Run (Now)

Scope:
1. Add Medousa tools that wrap:
   1. `grapheme modules search`
   2. `grapheme modules info`
   3. `grapheme modules ops`
   4. `grapheme examples list/show`
   5. `grapheme run <tempfile>`
2. Return structured output containing command, args, exit code, stdout, stderr.
3. Parse JSON output from `grapheme run --json` when available.

Acceptance:
1. Agent can discover web modules and ops from tools only.
2. Agent can execute a script source string via tool input.
3. Tool output includes enough diagnostics for self-correction loops.

## Phase B: Runtime Promotion

Scope:
1. Convert successful grapheme run into `workflow.grapheme.run` enqueue payload.
2. Add helpers for recurring schedule creation and updates.
3. Add schedule templates (hourly, daily digest, custom cron).

Acceptance:
1. Agent can take a script and schedule it without user manual glue code.
2. Recurring jobs are inspectable by job ID and schedule ID.

## Phase C: Guardrails + Memory

Scope:
1. Add policy filters for risky command flags and imports.
2. Persist successful script templates and repair traces in memory.
3. Provide deterministic retries with bounded fallback strategy.

Acceptance:
1. Agent avoids unsafe grapheme command usage.
2. Agent reuses prior successful scripts for similar intents.

## Risks

1. CLI output format drift may break parsing assumptions.
2. Runtime environment differences (plugins/native modules) can cause false failures.
3. Long-running runs may block interaction without timeout controls.

## Mitigations

1. Always return raw stdout/stderr in addition to parsed payloads.
2. Prefer non-native mode by default, require explicit opt-in for native modules.
3. Add execution timeouts and clear failure classification.

## Execution Log

- 2026-05-16: Plan created.
- 2026-05-16: Phase A implementation started.
- 2026-05-16: Phase A implemented (modules search/info/ops, examples list/show, cli run tool).
- 2026-05-16: Phase B started and implemented (promote to one-off job + recurring schedule tools).
