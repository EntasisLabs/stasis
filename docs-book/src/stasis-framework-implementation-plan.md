# Stasis Framework Implementation Plan

Companion to the framework RFC.

Primary source:
- [docs/design/stasis-framework-implementation-plan.md](../../docs/design/stasis-framework-implementation-plan.md)

## Focus

- Move to one Stasis-owned orchestration pipeline.
- Keep product crates in consumer mode only.
- Hide provider adapters behind Stasis ports.

## Phases

- P-A: Freeze and guardrails
- P-B: Canonical AI pipeline contract
- P-C: Tool registration unification
- P-D: Agent flow unification
- P-E: Medousa consumer migration
- P-F: Hardening and drift tests
