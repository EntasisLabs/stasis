# Identity Model 0.4.0 Roadmap and Internal Plan

Status: **Approved — In Implementation**
Date: 2026-06-04
Owner: Stasis Core
Target Release: **0.4.0**
Feedback source: Post-0.3.0 operator and integrator review

Depends on:

- [identity-memory-layer.md](../../docs-book/src/identity-memory-layer.md)
- `src/ports/outbound/memory/identity_memory_models.rs`
- `src/ports/outbound/memory/identity_memory_store.rs`
- `src/infrastructure/memory/in_memory_identity_memory_store.rs`
- `src/infrastructure/memory/surreal_identity_memory_store.rs`
- `src/application/runtime/identity_context_compiler.rs`

## 1. Purpose

Evolve the identity memory layer from a single mixed context blob into a **typed, mode-aware model** that separates:

1. **Cognitive context** — who the actor is, who they know, lightweight preferences
2. **Policy context** — trust, autonomy, approval profiles, flattened governance claims
3. **Structural wiring** — persona→user and user→channel runtime bindings (0.3.0 compat)

This release delivers four coordinated additions:

| Addition | Benefit |
|---|---|
| `UserEntity.preferences: Map<String, Value>` | Simple scalar prefs without graph edges |
| `ContactEntity` table | First-class people with `display_name`, `aliases` |
| `RelationshipKind` enum | Typed `knows`, `prefers`, `delegation`, `colleague` (+ structural kinds) |
| `GetIdentityContextRequest.mode` | Clean policy vs cognitive separation |

## 2. Problem Statement

Today:

1. User state beyond timezone/language requires awkward relationship edges.
2. People are not first-class entities — names live in free text or ad-hoc refs.
3. `relationship_kind` is an untyped string (`assistant_user`, etc.) with no semantic guardrails.
4. `get_identity_context` returns policy enforcement data and personalization data in one response — callers cannot ask for one slice without post-filtering.

## 3. Architecture

### 3.1 Unified model

```text
┌──────────────── Cognitive slice (mode=Cognitive) ────────────────┐
│  PersonaEntity ──assistant_user──► UserEntity (+ preferences)    │
│       │                              │                           │
│       │                              ├──knows/prefers/…──► ContactEntity
│       └── (social edges) ────────────┘                           │
└──────────────────────────────────────────────────────────────────┘

┌──────────────── Policy slice (mode=Policy) ──────────────────────┐
│  PersonaEntity ──assistant_user──► UserEntity (anchor only)      │
│  UserEntity ──user_channel──► ChannelProfileEntity               │
│  Relationships with autonomy / approval / policy_tags            │
│  PolicyProfileEntity + flattened_claims                          │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 New types (contract)

```rust
// Scalar prefs — no graph edge required
pub struct UserEntity {
    // existing fields…
    #[serde(default)]
    pub preferences: BTreeMap<String, Value>,
}

// First-class people
pub struct ContactEntity {
    pub contact_id: String,
    pub display_name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub status: String,
    pub version: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RelationshipKind {
    AssistantUser,   // structural (0.3.0)
    UserChannel,     // structural (0.3.0)
    Knows,
    Prefers,
    Delegation,
    Colleague,
    Legacy(String),  // unknown persisted strings
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IdentityContextMode {
    #[default]
    Full,
    Policy,
    Cognitive,
}

pub struct GetIdentityContextRequest {
    // existing fields…
    #[serde(default)]
    pub mode: IdentityContextMode,
}

pub struct GetIdentityContextResponse {
    // existing fields…
    #[serde(default)]
    pub contacts: Vec<ContactEntity>,
}
```

### 3.3 Mode semantics

| Field | `Full` | `Policy` | `Cognitive` |
|---|---|---|---|
| persona, user, channel | ✓ | anchor only | ✓ |
| `user.preferences` | ✓ | stripped | ✓ |
| `contacts` | ✓ | empty | ✓ |
| structural rels | ✓ | ✓ | excluded |
| social rels | ✓ | excluded | ✓ |
| `policy_profiles` | ✓ | ✓ | empty |
| `flattened_claims` | ✓ | ✓ | empty |

Implementation: shared `apply_identity_context_mode()` in `identity_context_filter.rs`, called by both store adapters after building the full graph.

### 3.4 Governance tiers (preferences)

| Path | Tier |
|---|---|
| `preferences.*` (default) | AutoCommit |
| `preferences.policy.*` | ConfirmRequired (reserved) |

### 3.5 Serde and migration

- `RelationshipKind` serializes as snake_case string (`assistant_user`, `knows`, …).
- Unknown persisted strings deserialize to `Legacy(String)` — no data loss.
- `mode` defaults to `Full`; `preferences` and `contacts` default empty.
- Surreal schema adds `identity_contact` table and `preferences` field on `identity_user`.

## 4. Implementation Phases

### Phase A — Models + serde compat ✅ (this branch)

- [x] Extend `identity_memory_models.rs`
- [x] Add `RelationshipKind` parse/as_str/is_structural/is_social
- [x] Add `IdentityEntityType::ContactEntity`

### Phase B — Store parity

- [x] `InMemoryIdentityMemoryStore`: contacts map, `upsert_contact`, contact resolution
- [x] `SurrealIdentityMemoryStore`: `identity_contact` schema, row types, load path
- [x] Patch paths for `relationship_kind`, `preferences`, contact fields

### Phase C — Mode filtering

- [x] `identity_context_filter.rs` with unit tests
- [x] Both stores call filter after full context assembly

### Phase D — Runtime integration

- [x] `identity_context_compiler` uses `mode: Cognitive`
- [x] Summary includes `contacts=` and `preferences=` counts
- [ ] Future: dedicated policy consumer with `mode: Policy`

### Phase E — Docs and release

- [x] Update `docs-book/src/identity-memory-layer.md`
- [x] Update `docs/architecture/surrealdb-schema.md`
- [x] Update `docs-book/src/surrealdb-schema.md`
- [x] Update cookbook examples
- [ ] CHANGELOG `[Unreleased]` → `[0.4.0]` (at release tag)

## 5. Test Plan

| Test | Validates |
|---|---|
| `relationship_kind_deserializes_legacy_strings` | `"assistant_user"` → `AssistantUser` |
| `identity_context_cognitive_mode_excludes_policy` | No policy_profiles in Cognitive |
| `identity_context_policy_mode_strips_preferences` | preferences empty in Policy |
| `contact_loaded_via_knows_relationship` | ContactEntity in Cognitive response |
| `user_preferences_round_trip` | preferences persist without relationships |
| `runtime_backend_parity` identity fixtures | Updated enum kinds compile |

## 6. Non-Goals (0.4.0)

- Full `ContactEntity` commit/rollback path (propose returns InvalidPatch until Phase 2)
- Alias resolution in runtime handlers (mention → contact_id)
- UserEntity commit path beyond RelationshipEntity (unchanged from 0.3.0)
- Breaking removal of `Legacy(String)` kind variant

## 7. Release Gate

1. All identity store unit tests pass (in-memory + surreal module tests).
2. `runtime_backend_parity` identity continuity tests pass.
3. Default `mode: Full` preserves 0.3.0 response shape (plus empty `contacts`).
4. [x] Docs book identity-memory-layer updated and `mdbook build` succeeds.

## 8. Acceptance Criteria

- [x] Operator can store `user.preferences.theme` without creating a relationship
- [x] Operator can create `ContactEntity` and link via `RelationshipKind::Knows`
- [x] Prompt path uses Cognitive mode and does not leak policy profiles into snapshot summary counts incorrectly
- [x] Policy consumer can call `mode: Policy` and receive structural + governance slice only
