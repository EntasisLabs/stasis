# Documentation Transformation Program

Status: In Progress
Date: 2026-05-14
Owner: Stasis Core
Audience: Architecture, Platform, Security, Operations, Developer Experience

## 1. Purpose

Define a phased program to turn Stasis documentation into enterprise-grade official documentation:
- stable
- reviewable
- code-traceable
- operationally actionable

This program intentionally separates official product documentation from internal planning artifacts.

## 2. Target Documentation Standard

Official docs must answer:
1. What the system does now.
2. How it is designed and why.
3. How to operate it safely.
4. How to review trust, reliability, and controls.
5. Which contracts are stable vs evolving.

Official docs must avoid:
- roadmap-first narratives
- historical implementation logs as primary content
- mixed audience pages (architecture + sprint planning in one page)

## 3. Documentation Lanes

Lane A: Official Documentation (customer-facing trust surface)
- Architecture Overview
- Runtime Behavior and Contracts
- Data Schema and Invariants
- Operations and Reliability
- Security and Compliance
- Upgrade and Versioning Policy
- Validation and Verification Guides

Lane B: Internal Engineering Planning (team execution surface)
- roadmaps
- phase plans
- implementation plans
- migration sequences

Rule: Lane B can reference Lane A, but Lane A must not depend on Lane B for critical understanding.

## 4. Phased Delivery Plan

## Phase D0: Information Architecture Reset

Objective:
- split the docs surface into Official and Internal lanes.

Deliverables:
- docs-book navigation organized by lane.
- clear labels on every page: `Document Type`, `Audience`, `Stability`.
- planning documents moved under an internal section in the book.

Exit criteria:
- no planning/roadmap pages in official docs navigation path.
- reviewers can find architecture/runtime/schema without reading execution plans.

## Phase D1: Canonical Reference Baseline

Objective:
- make official pages self-contained and deterministic.

Deliverables:
- Architecture overview rewritten as current-state reference.
- Runtime design rewritten with explicit invariants and failure semantics.
- Schema specification aligned to code-level contracts.
- RFC converted into standards language where applicable.

Exit criteria:
- each official page includes: Purpose, Scope, Invariants, Interfaces, Failure Modes, Non-goals.
- cross-links are local and non-broken.

## Phase D2: Code-Truth Traceability

Objective:
- ensure every key statement can be reviewed against code.

Deliverables:
- add `Code References` sections to official pages.
- map contracts to specific source modules, adapters, and tests.
- publish traceability matrix from docs claims to tests.

Exit criteria:
- high-risk claims (retries, dead-letter, lineage, guardrails, thread ancestry) each have at least one code reference and one test reference.
- docs review checklist enforces evidence links.

## Phase D3: Operational Excellence Documentation

Objective:
- provide runbooks and operational procedures expected by enterprise teams.

Deliverables:
- operations guide: startup, shutdown, replay, retention, incident flow.
- reliability guide: SLO assumptions, failure domains, recovery semantics.
- observability guide: metrics, traces, diagnostics interpretation.

Exit criteria:
- on-call reviewer can resolve common incidents from docs without code deep-dive.
- replay and dead-letter handling documented as procedures.

## Phase D4: Security, Risk, and Compliance Surface

Objective:
- document controls, boundaries, and audit semantics.

Deliverables:
- security architecture page (auth boundaries, secrets posture, data handling).
- compliance and retention policy page.
- threat and misuse assumptions page.

Exit criteria:
- security reviewer can identify trust boundaries and audit points from docs alone.

## Phase D5: Versioning, Support, and Change Policy

Objective:
- make contract stability explicit for enterprise adoption.

Deliverables:
- support matrix (stable/beta/experimental contracts).
- versioning and deprecation policy.
- release documentation checklist.

Exit criteria:
- each documented API/contract has a declared stability level.
- change process is explicit and reviewable.

## 5. Sprint Execution Model

For each documentation sprint:
1. Select one phase objective and bounded page set.
2. Gather code references first.
3. Update docs to current-state truth only.
4. Validate links/build/tests.
5. Run documentation review checklist.

Definition of Done for a sprint:
- pages compile in docs-book.
- links resolve.
- code references are present.
- no roadmap language introduced in official pages.

## 6. Mandatory Documentation Metadata

Each official page must include:
- Document Type: `Reference Standard` | `Architecture Standard` | `Operational Runbook`
- Audience: `Engineer` | `SRE` | `Security` | `Architect`
- Stability: `Stable` | `Evolving` | `Experimental`
- Last Verified Date
- Verified Against: code modules and test suites

## 7. Review Gates

Gate 1: Technical Accuracy
- claims match current source behavior.

Gate 2: Operational Completeness
- procedures exist for failure and recovery paths.

Gate 3: Enterprise Readability
- concise, deterministic language with low ambiguity.

Gate 4: Traceability
- critical claims point to code and tests.

## 8. Immediate Next Sprint (Recommended)

Sprint D0.1 (Information Architecture)
- introduce lane separation in docs-book summary.
- tag pages with metadata header template.
- move planning pages into explicit Internal Planning section.

Sprint D1.1 (Canonical Baseline)
- normalize architecture/runtime/schema/RFC pages to reference-standard structure.

Sprint D2.1 (Traceability)
- add code and test reference blocks for runtime lineage, threading, and diagnostics contracts.
