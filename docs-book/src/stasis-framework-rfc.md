# Stasis Framework RFC

This page tracks the canonical architecture contract for Stasis as a framework-first orchestration system.

Primary source:
- [docs/design/stasis-framework-rfc.md](../../docs/design/stasis-framework-rfc.md)

## Key Alignment

- Stasis owns AI abstraction, tool routing, agent orchestration, and runtime lifecycle semantics.
- Product crates (for example Medousa) consume Stasis APIs and do not directly own provider orchestration logic.
- Infrastructure adapters (including genai-backed adapters) remain implementation detail behind Stasis ports.

## PR Guardrail

For architecture-impacting changes, reference the RFC and verify boundary rules before merge.
