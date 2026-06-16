# LLM Providers (genai 0.6.x)

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Operator
- Stability: Stable
- Last Verified: 2026-06-02
- Verified Against:
  - `Cargo.toml` (`genai = "0.6.5"`)
  - `src/infrastructure/llm/genai_chat_client.rs`
  - [genai CHANGELOG](https://github.com/jeremychone/rust-genai/blob/HEAD/CHANGELOG.md)

## Purpose

Stasis routes chat completions through **genai 0.6.x**. This page documents how to configure providers at deploy time and how per-job **`reasoning_effort`** overrides interact with env-based defaults.

## Default wiring

| Variable | Purpose |
|---|---|
| `STASIS_LLM_PROVIDER` | Adapter id passed to genai (`openai`, `anthropic`, `gemini`, …) |
| `STASIS_LLM_MODEL` | Default model target (supports genai namespaced targets) |
| `STASIS_LLM_API_KEY` | Global API key fallback when adapter-specific key is unset |

Adapter-specific keys follow the existing pattern: `STASIS_OPENAI_API_KEY`, `STASIS_ANTHROPIC_API_KEY`, `STASIS_GEMINI_API_KEY`, and so on. See `.env.example` for the full list.

## Namespaced model targets

genai 0.6.x expects **namespaced** model targets for most adapters:

| Provider | Example model target | Auth env |
|---|---|---|
| OpenAI / OpenAI Responses | `openai::gpt-5-mini` | `STASIS_OPENAI_API_KEY` |
| Anthropic | `anthropic::claude-sonnet-4-20250514` | `STASIS_ANTHROPIC_API_KEY` |
| Gemini | `gemini::gemini-2.5-flash` | `STASIS_GEMINI_API_KEY` |
| Groq | `groq::llama-3.3-70b-versatile` | `STASIS_GROQ_API_KEY` |
| Bedrock | `bedrock::anthropic.claude-3-5-sonnet-20241022-v2:0` | `STASIS_BEDROCK_API_KEY` (+ region) |
| Vertex | `vertex::gemini-2.5-pro` | `STASIS_VERTEX_API_KEY` / ADC |
| OpenRouter | `open_router::anthropic/claude-3.5-sonnet` | `STASIS_OPEN_ROUTER_API_KEY` |
| Ollama (local) | `ollama::gemma3:4b` | none |

Set `STASIS_LLM_MODEL` to the namespaced target, or pass a hint via job payload `model_hint` (metadata-only in 0.6.0 — routing still uses the configured client model).

## Reasoning effort

### Per-job override (recommended)

All LLM job payloads accept optional `reasoning_effort` as a string keyword:

| Keyword | Meaning |
|---|---|
| `none` | Disable extra reasoning budget where supported |
| `minimal`, `low`, `medium`, `high`, `xhigh`, `max` | Provider-supported effort levels |
| `budget:N` | Token budget style hint (e.g. `budget:8192`) |

Orchestration patterns resolve **branch / stage / turn / route override → pattern default** (same semantics as `memory_policy` and `tool_call_mode` on concurrent branches).

Example concurrent branch mix:

```json
{
  "reasoning_effort": "low",
  "branches": [
    { "branch_id": "research", "reasoning_effort": "high", "execution_mode": "tool_loop", "tool_name": "stasis.web.search" },
    { "branch_id": "summary", "execution_mode": "prompt" }
  ]
}
```

The `research` branch runs at `high`; `summary` inherits pattern default `low`.

### Model suffix fallback

When no payload override is set, `GenaiChatClient` parses reasoning effort from the configured model name suffix (genai convention), e.g. `gpt-5-mini-high` → `high`.

### Env-level defaults

Stasis does **not** add a separate `STASIS_LLM_REASONING_EFFORT` variable in 0.6.0. Use genai client env configuration or model suffix naming for global defaults; use payload fields for per-job control.

## Groq migration note

Bare Groq model names from genai 0.5.x must be prefixed: `groq::model-name`.

## Deferred (not in 0.6.0)

- Built-in provider **WebSearch** tools — custom `StasisTool` registry remains the supported search path
- Dynamic **model_hint → model router** — planned for a follow-on release
