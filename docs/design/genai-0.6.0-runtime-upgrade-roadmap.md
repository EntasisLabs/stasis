# genai 0.6.0 Runtime Upgrade — 0.6.0 Roadmap and Internal Plan

Status: **Shipped (0.6.0)**
Date: 2026-06-02
Owner: Stasis Core
Target Release: **0.6.0**
Feedback source: Post-0.5.0 internal review — genai stable line, reasoning effort, provider expansion

**Release decisions (2026-06-02):**

- **No `STASIS_LLM_REASONING_EFFORT`** — use genai env / model suffix for global defaults; payload `reasoning_effort` for per-job control
- **WebSearch built-in tools deferred** — Slice 6 skipped; custom `StasisTool` registry remains default search path

Depends on:

- [concurrent-capabilities-0.5.0-roadmap.md](./concurrent-capabilities-0.5.0-roadmap.md) (shipped branch overrides, memory on tool branches)
- [orchestration-patterns.md](../../docs-book/src/orchestration-patterns.md)
- [environment-configuration.md](../../docs-book/src/environment-configuration.md)
- [chat-middleware.md](../../docs-book/src/chat-middleware.md)
- `src/infrastructure/llm/genai_chat_client.rs`
- `src/application/orchestration/prompt_pipeline.rs`
- `src/application/orchestration/runtime_job_payloads.rs`
- Upstream: [genai CHANGELOG](https://github.com/jeremychone/rust-genai/blob/HEAD/CHANGELOG.md) (v0.5.3 → v0.6.5)

## 1. Purpose

Upgrade Stasis from **genai 0.5.3 → 0.6.5** and wire **first-class chat generation options** (starting with **reasoning effort**) through the existing runtime orchestration stack — without breaking integrators who already ship JSON job payloads and env-based LLM config.

This release delivers three coordinated outcomes:

| Outcome | Benefit |
|---|---|
| **genai 0.6.x baseline** | New providers (Bedrock, Vertex, OpenRouter, …), GPT-5 / Responses API path, prompt cache hooks, improved streaming |
| **Reasoning effort via runtime payloads** | Per-job / per-branch compute depth — natural fit with 0.5.0 concurrent lanes |
| **Provider-ready operator surface** | Documented env auth for new adapters; optional built-in provider tools (WebSearch) behind explicit opt-in |

**Public API principle:** additive, optional fields and builder methods only. No signature changes on `AiChatClient`, `StasisSdk`, or existing payload required fields.

## 2. Problem Statement

Today:

1. **`genai` is pinned at 0.5.3** — missing 0.6.x providers, `ReasoningEffort::XHigh` / `::Max`, built-in WebSearch, prompt cache, and OpenAI Responses improvements.
2. **`ChatOptions` is never built from runtime context** — `PromptExecutionPipeline` and tool-loop rounds call `complete(..., None)`; reasoning effort, verbosity, and cache keys never reach the model.
3. **`model_hint` is metadata-only** — cookbooks use values like `"fast-reasoning"` but nothing maps them to genai.
4. **Provider expansion is implicit** — genai supports many adapters, but Stasis docs/env only cover a narrow OpenAI-centric path (`STASIS_LLM_*`).
5. **Built-in WebSearch exists in genai 0.6** but Stasis only exposes custom `StasisTool` registry tools — no documented path for provider-native search.

## 3. Design Principles (minimal breakage)

### 3.1 What stays stable

| Surface | Guarantee |
|---|---|
| `AiChatClient` trait | Unchanged method signatures |
| Existing job payload JSON | Deserializes unchanged; new fields are optional with serde defaults |
| `StasisRuntimeBuilder` defaults | Same handlers, same middleware order, same tool registry model |
| `STASIS_LLM_PROVIDER` / `STASIS_LLM_MODEL` / `STASIS_LLM_API_KEY` | Unchanged semantics |
| `runtime_prelude` / `sdk_prelude` | No removals; optional new exports only if clearly additive |

### 3.2 What is additive

| Addition | Scope |
|---|---|
| `reasoning_effort: Option<String>` on runtime job payloads + `PromptExecutionContext` | Opt-in per job / pattern / branch |
| `GenaiChatClient::with_default_chat_options(...)` or builder-level client defaults | Opt-in at composition time |
| `StasisRuntimeBuilder::with_builtin_provider_tools(...)` | **Off by default** — explicit opt-in for WebSearch |
| Env aliases for new providers | Documented in environment-configuration; follows existing `STASIS_{ADAPTER}_API_KEY` pattern |
| Diagnostics fields | `reasoning_effort_resolved`, `stop_reason` (when genai exposes) — additive JSON keys |

### 3.3 What we deliberately defer (0.6.x or later)

- Full **multi-model routing** from `model_hint` (concurrent roadmap Track B)
- Per-agent model overrides on `RegisterAgentRequest`
- Dynamic model switching inside a single `GenaiChatClient` instance (separate clients or resolver hook instead)
- Replacing custom `StasisTool` search tools with built-in WebSearch by default

## 4. Architecture

### 4.1 Chat options resolution pipeline

Introduce an internal module (no new public prelude requirement):

```text
Job payload (optional reasoning_effort)
        ↓
Handler maps → PromptExecutionContext
        ↓
resolve_chat_options(context, client_defaults) → ChatOptions
        ↓
PromptExecutionPipeline / ToolLoopPipeline → AiChatClient::complete(req, Some(&options))
        ↓
GenaiChatClient → genai 0.6 exec_chat / exec_chat_stream
```

**Resolution order** (same pattern as 0.5.0 `memory_policy` / `tool_call_mode`):

```text
branch.reasoning_effort
  ?? pattern.reasoning_effort
  ?? parse from model target suffix (genai ReasoningEffort::from_model_name)
  ?? client default ChatOptions
  ?? none (provider default)
```

### 4.2 Reasoning effort contract

**Payload field (all LLM job types — additive):**

```json
{
  "reasoning_effort": "high"
}
```

Accepted keywords (mapped internally to genai `ReasoningEffort`):

| Keyword | genai (0.6) | Typical use |
|---|---|---|
| `none` | `None` | Fast lanes, merge/summary branches |
| `minimal` | `Minimal` | Legacy OpenAI o-series |
| `low` | `Low` | Cheap reasoning |
| `medium` | `Medium` | Balanced |
| `high` | `High` | Deep analysis / tool loops |
| `xhigh` | `XHigh` | OpenAI-only (0.6+) |
| `max` | `Max` | Anthropic-only (0.6+) |
| `budget:N` | `Budget(N)` | Gemini thinking budget |

**Concurrent branch example (0.5.0 + 0.6.0):**

```json
{
  "branch_id": "research",
  "execution_mode": "tool_loop",
  "reasoning_effort": "high",
  "tool_name": "stasis.web.search.mock",
  "user_prompt_template": "Research {{input}}"
}
```

Invalid keywords → policy violation at handler parse (consistent with existing guardrails), not silent ignore.

### 4.3 Model target suffix (zero JSON change path)

genai 0.6 resolves effort from model names like `openai::gpt-5-mini-high`. Stasis already builds targets via `GenaiChatClient::build_model_target`.

**Operator path without new JSON fields:**

```bash
STASIS_LLM_MODEL=gpt-5-mini-high   # genai strips suffix and applies effort
```

Document this alongside explicit `reasoning_effort` — explicit field wins when both are present.

### 4.4 Provider auth and new adapters

No new public Rust types required. Extend **documented env** using existing resolver in `GenaiChatClient::auth_env_candidates`:

```text
STASIS_{ADAPTER}_API_KEY     # e.g. STASIS_BEDROCK_API_KEY, STASIS_VERTEX_API_KEY
STASIS_LLM_API_KEY           # global fallback (unchanged)
```

Model targets use genai namespaces:

```text
bedrock::anthropic.claude-3-5-sonnet-...
vertex::gemini-2.5-pro
open_router::anthropic/claude-sonnet-4
groq::llama-3.3-70b-versatile    # breaking in 0.6 — document migration
```

**StasisRuntimeBuilder** — no new required methods. Optional:

```rust
.with_default_model("bedrock::...")  // if not using env
```

### 4.5 Built-in provider tools (WebSearch) — opt-in

genai 0.6 normalizes built-in tools (`ToolName::WebSearch`, etc.). Stasis keeps **`StasisTool` registry as primary**.

**Phase 1 (0.6.0):** runtime builder flag, default **off**:

```rust
StasisRuntimeBuilder::new(backend)
    .with_builtin_provider_tools(BuiltinProviderToolsConfig {
        web_search: true,  // registers genai WebSearch into tool loop exposure
        ..Default::default()
    })
```

When enabled, tool-loop / agent / concurrent tool branches can reference a **well-known tool name** (e.g. `stasis.provider.web_search`) that maps to genai's built-in — parallel to custom `StasisTool` entries.

**Rationale:** "meh for now" — ship the plumbing and docs, not product push. Custom registry tools remain the recommended path for deterministic tests and mock parity.

### 4.6 Streaming and reasoning capture

`GenaiChatClient` already handles `ReasoningChunk` and `capture_reasoning_content`. genai 0.6 changes:

- Gemini / OpenAI Resp: reasoning capture more **opt-in**
- Review stream defaults in `complete_stream` after bump

**Action:** audit `with_capture_reasoning_content(true)` against 0.6 behavior; add parity test if tool-loop regressions appear.

### 4.7 Middleware and cache interaction

`deterministic_cache_key` already hashes `ChatOptions`. Once options include `reasoning_effort`, cache keys automatically differentiate effort levels — **no middleware API change**.

Document that cache hits require matching reasoning effort in options.

## 5. genai Upgrade Impact Matrix

### 5.1 Dependency bump

```toml
# Cargo.toml
genai = "0.6.5"
```

Run full test suite + examples smoke. No expected changes to `reqwest` direct usage in Stasis (genai owns HTTP).

### 5.2 Known upstream behavior changes

| Change | Stasis impact | Mitigation |
|---|---|---|
| Groq requires `groq::` namespace | Breaks bare model names | Document in CHANGELOG + env-configuration; no code change if models already namespaced |
| Gemini reasoning opt-in | Fewer implicit reasoning fields in responses | Keep explicit capture flags; update tests |
| OpenAI GPT-5 → Responses API | Transparent via genai | Integration test with mock/scripted client |
| `AuthData::None` variant | Rare | No change unless custom auth resolver added |
| New TLS feature flags on genai | Build feature passthrough if needed | Default genai features usually sufficient |

### 5.3 Providers to document first (0.6.0)

Priority for operator docs and smoke examples:

| Provider | Model target example | Auth env |
|---|---|---|
| OpenAI / OpenAI Resp | `openai::gpt-5-mini` | `STASIS_OPENAI_API_KEY` |
| Anthropic | `anthropic::claude-sonnet-4-20250514` | `STASIS_ANTHROPIC_API_KEY` |
| Gemini | `gemini::gemini-2.5-flash` | `STASIS_GEMINI_API_KEY` |
| Bedrock | `bedrock::...` | `STASIS_BEDROCK_API_KEY` (+ region env TBD) |
| Vertex | `vertex::gemini-2.5-pro` | `STASIS_VERTEX_API_KEY` / ADC doc |
| OpenRouter | `open_router::...` | `STASIS_OPEN_ROUTER_API_KEY` |
| Ollama (native 0.6) | `ollama::gemma3:4b` | none / local |

Lower priority: Moonshot, MiniMax, Baidu, Aliyun — document namespace only.

## 6. Payload Changes (additive)

Extend **optional** field on shared runtime payloads (serde default `None`):

| Payload | New field |
|---|---|
| `PromptJobPayload` | `reasoning_effort` |
| `ToolLoopJobPayload` | `reasoning_effort` |
| `AgentTurnJobPayload` | `reasoning_effort` |
| `AgentSessionJobPayload` | `reasoning_effort` (session default) |
| `SequentialPatternJobPayload` | `reasoning_effort` |
| `SequentialStageJobPayload` | `reasoning_effort` |
| `ConcurrentPatternJobPayload` | `reasoning_effort` |
| `ConcurrentBranchJobPayload` | `reasoning_effort` |
| `HandoffPatternJobPayload` | `reasoning_effort` |
| `HandoffTurnJobPayload` | `reasoning_effort` |
| `OrchestratorPatternJobPayload` | `reasoning_effort` |
| `OrchestratorRouteJobPayload` | `reasoning_effort` |

**Internal:** `PromptExecutionContext.reasoning_effort: Option<String>` — not necessarily exported in prelude.

**Public Rust API:** payload structs in `runtime_job_payloads.rs` are already used via `runtime_prelude_ext` — adding optional fields is semver-compatible for consumers constructing structs with `..Default::default()` or partial updates; consumers using struct literals will need new fields (document in CHANGELOG).

Prefer helper methods where they exist:

```rust
ConcurrentBranchJobPayload::tool_loop(...).with_reasoning_effort("high")
// optional builder methods — additive API
```

## 7. Implementation Slices

Each slice lands independently; CI green after each.

### Slice 1 — genai 0.6.5 bump (foundation)

- [ ] Bump `genai = "0.6.5"` in `Cargo.toml`
- [ ] Fix compile errors (if any) in `genai_chat_client.rs`, tests
- [ ] Audit Groq / Gemini / stream capture behavior
- [ ] `cargo test` full suite + `examples/*` smoke

**Exit:** no behavior change yet except upstream fixes.

### Slice 2 — ChatOptions builder (internal)

- [ ] Add `src/application/runtime/chat_options_resolver.rs` (or `llm/chat_options_builder.rs`)
- [ ] Map keyword → `genai::chat::ReasoningEffort` with validation errors
- [ ] Wire `PromptExecutionPipeline` + tool-loop chat calls to pass resolved options
- [ ] Unit tests: resolution order, invalid keyword rejection

**Exit:** reasoning effort works when passed via context; payloads not yet extended.

### Slice 3 — Payload + handler wiring

- [ ] Add optional `reasoning_effort` to payloads (Slice 6 table)
- [ ] Handlers map payload → `PromptExecutionContext` (pattern/branch override)
- [ ] Diagnostics: `reasoning_effort_resolved` on prompt, tool-loop, orchestration jobs
- [ ] Parity test: concurrent branch `high` vs `none` sends different cache keys / options

**Exit:** JSON job API supports reasoning effort end-to-end.

### Slice 4 — Client defaults + model suffix

- [ ] `GenaiChatClient` / builder: optional default `ChatOptions`
- [ ] Document `STASIS_LLM_MODEL=gpt-5-mini-high` path
- [ ] Optional: `STASIS_LLM_REASONING_EFFORT` env for global default

**Exit:** operators can configure effort without JSON changes.

### Slice 5 — Provider documentation + env

- [ ] Update `docs-book/src/environment-configuration.md` with 0.6 provider table
- [ ] Add `docs-book/src/llm-providers.md` (new reference page)
- [ ] Example: `examples/multi_provider_env.rs` (or extend existing production example)
- [ ] `.env.example` additions for Bedrock / Vertex / OpenRouter (keys commented)

**Exit:** users can run new providers via env + model target only.

### Slice 6 — Built-in WebSearch (opt-in)

- [ ] `BuiltinProviderToolsConfig` + builder method (default off)
- [ ] Map `stasis.provider.web_search` → genai built-in in tool loop tool list
- [ ] Mock/scripted test behind feature flag or ignored integration test
- [ ] Document vs custom `StasisTool` tradeoffs

**Exit:** runtime-ready path exists; not enabled by default.

### Slice 7 — Docs, CHANGELOG, release

- [ ] Update orchestration-patterns, agent-coordination, chat-middleware notes
- [ ] Cross-link from concurrent 0.5.0 roadmap deferred Track B
- [ ] `CHANGELOG [Unreleased]` → `[0.6.0]`
- [ ] `mdbook build`
- [ ] Mark this roadmap **Shipped**

## 8. Test Plan

| Test | Validates |
|---|---|
| `chat_options_resolver_*` unit tests | Keyword mapping, resolution order |
| `runtime_backend_parity` tool-loop streaming | No regression after genai bump |
| `chat_middleware_pipeline` cache key | Different effort → different cache key |
| `in_memory_concurrent_branch_reasoning_effort` | Branch override reaches options |
| `architecture_conformance` | No infrastructure leak in application layer |
| `production_examples_smoke` | Examples compile with optional fields |
| Optional live smoke (ignored CI) | One provider per adapter family |

## 9. Operator Migration Guide (summary)

### 9.1 No action required if:

- Using env-based OpenAI/Anthropic/Gemini with unchanged model names
- Job payloads omit new fields (defaults preserve prior behavior)

### 9.2 Recommended actions:

1. **Pin Stasis 0.6.0** and bump lockfile.
2. **Groq users:** prefix models with `groq::`.
3. **Reasoning models:** set `reasoning_effort` on heavy branches or use `-high` model suffix.
4. **New providers:** set `STASIS_{ADAPTER}_API_KEY` and namespace model in `STASIS_LLM_MODEL` or per-job `model_hint` (routing still metadata-only in 0.6.0 unless suffix parse applies at client).

### 9.3 Breaking changes (explicit)

| Change | Severity |
|---|---|
| Groq bare model names | Medium — documented namespace fix |
| Struct literal payload construction in Rust | Low — add `reasoning_effort: None` or use helpers |
| Gemini reasoning in diagnostics if capture flags changed | Low — observability only |

No HTTP API version bump required — job JSON is backward compatible.

## 10. Non-Goals (0.6.0)

- `model_hint` → dynamic model router (deferred Track B)
- Agent registry model overrides
- Prompt cache key automation (can follow 0.6.1 — manual via future `ChatOptions` extension)
- Replacing StasisTool registry with built-in tools as default
- Exposing raw `genai::Client` in public prelude
- re-exporting entire genai crate from Stasis

## 11. Release Gate

1. `genai 0.6.5` compiles; full `cargo test` green.
2. Tool-loop + concurrent + agent parity tests pass (in-memory + surreal where applicable).
3. No changes to `AiChatClient` trait signatures.
4. Existing JSON fixtures deserialize without `reasoning_effort`.
5. `mdbook build` succeeds.
6. CHANGELOG documents genai bump, additive fields, Groq namespace, opt-in WebSearch.

## 12. Open Decisions (discuss before Slice 3)

| # | Question | Recommendation |
|---|---|---|
| 1 | String vs enum in JSON payloads? | **String keywords** — matches genai `from_keyword`, easier for dashboard JSON editors |
| 2 | Global env `STASIS_LLM_REASONING_EFFORT`? | Yes — mirrors `STASIS_LLM_MODEL`; optional Slice 4 |
| 3 | Built-in WebSearch tool name? | `stasis.provider.web_search` — stable, namespaced, distinct from user tools |
| 4 | Export `ReasoningEffort` in prelude? | **No** — keep genai as implementation detail; Stasis strings at boundary |
| 5 | Slice order vs parallel work? | Slice 1+2 first (bump + internal wiring), then 3+5 (payload + docs), WebSearch last |

## 13. Relationship to 0.5.0 Concurrent Work

0.5.0 established the **override pattern** (pattern default → branch override) for `tool_call_mode` and `memory_policy`. 0.6.0 applies the same pattern to **`reasoning_effort`**, enabling:

```text
Concurrent job
├── summary  (prompt,     reasoning: none)
├── research (tool_loop,  reasoning: high,  memory: on)
└── validate (prompt,     reasoning: low)
```

Deferred from [concurrent-capabilities-0.5.0-roadmap.md](./concurrent-capabilities-0.5.0-roadmap.md) §8:

- Full model routing → **0.7.0 candidate** (after effort + provider docs prove out)

---

**Next step:** Review open decisions (§12), then execute Slice 1 (genai bump) on a branch.
