# ADR-0005 Agentic Workflow Skill Graph Contract

## Document Metadata

- Document Type: Architecture Decision Record
- Status: Accepted
- Date: 2026-05-25
- Owners: Product, Runtime Engineering, Dashboard Engineering

## Context

Workflow authoring has drifted between UI-only graph metaphors and source-first implementation details.

This drift creates ambiguity in product behavior and runtime guarantees:
1. Node meaning is unclear (visual card vs executable behavior).
2. Workflow persistence is source-centric without canonical guided graph semantics.
3. Job execution lacks explicit trigger-to-workflow-revision provenance in product framing.

Stasis is an agentic orchestrator. The builder must model AI skills, not generic DAG plumbing.

## Decision

Adopt the following canonical contract:

1. Node = Grapheme function step.
2. Edge = piped function output -> next function input.
3. Workflow = versioned AI skill graph compiled into Grapheme source.
4. Grapheme source = compiled artifact for execution (advanced mode may inspect/override explicitly).
5. Job = trigger binding (HTTP/Kafka/Queue/Schedule) that executes a specific workflow revision.
6. Workflow output = explicit handoff into model context/tool loop input.

Guided mode must use product-facing action labels while preserving Grapheme function identity in metadata.

## Consequences

Positive:
1. Single source of truth for guided authoring and runtime execution semantics.
2. Deterministic graph-to-source compile path reduces visual/runtime drift.
3. Clear workflow revision provenance across trigger and job lifecycle.

Tradeoffs:
1. Requires graph schema/versioning and migration handling.
2. Requires deterministic compiler and parity tests for canonical templates.
3. Advanced-mode source edits need explicit round-trip or override policy.

## Guardrails

1. No non-functional node types in default catalog.
2. No runtime/parser/reflection terminology in guided-mode copy.
3. Every job execution record must reference immutable workflow revision ID.
4. Every release gate must include graph->source->execute parity evidence.

## Implementation Notes

Near-term first slice:
1. Add graph serialization field to workflow revisions.
2. Add deterministic compiler for one canonical chain template.
3. Bind one trigger type to workflow revision job creation path.
4. Add integration tests for compile parity and revision provenance.