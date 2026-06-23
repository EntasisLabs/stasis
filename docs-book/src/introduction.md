# Introduction

## Document Metadata

- Document Type: Book Overview
- Audience: Engineer, Architect, Operator
- Stability: Stable
- Last Verified: 2026-06-23
- Verified Against: Stasis 0.7.0 release

Stasis is an agentic framework SDK with a durable job orchestration runtime.

If you are new to Stasis, start with [Getting Started](./getting-started.md) before diving into architecture details.

This book is the architecture reference for the runtime and covers:

1. System architecture and decision boundaries.
2. Runtime behavior and reliability semantics.
3. SurrealDB schema contracts.
4. Architecture Decision Records (ADRs).

## How to Read This Book

1. Start with Getting Started for a first end-to-end run.
2. Read Runtime Builder and Job Runtime Design for runtime behavior.
3. Read Architecture Overview to understand system boundaries and flow.
4. Use SurrealDB Schema as the source of truth for data contracts.
5. Review Extension Points for integration contracts and ADRs for rationale.

## Scope

This book documents the Stasis 0.7.0 runtime architecture and operational semantics. Reference pages marked **Stable** are verified against the current release train. It does not define production deployment topology beyond the cookbook recipes.
