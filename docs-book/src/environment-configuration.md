# Environment Configuration

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Operator
- Stability: Stable
- Last Verified: 2026-09-17
- Verified Against:
  - src/application/config/env.rs
  - src/application/config/secrets.rs
  - src/application/composition/surreal_backend_config.rs
  - Cargo.toml
  - tests/wasm_kernel_smoke.rs
  - src/infrastructure/llm/openai_http_gateway.rs
  - src/infrastructure/memory/locus_node_store_factory.rs
  - stasis-wasm/src/lib.rs

## Purpose

Provide a safe, consistent way to load Stasis configuration from process environment, local `.env` files, and file-based secret mounts (including Vault Agent sidecars).

## Quick start

1. Copy [`.env.example`](https://github.com/EntasisLabs/stasis/blob/main/.env.example) to `.env`.
2. Call `bootstrap()` once near process entry (before reading config):

```rust
use stasis::config_prelude::{bootstrap, non_empty, required, with_default};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    bootstrap()?;
    let model = with_default("STASIS_LLM_MODEL", "gpt-4o-mini");
    let api_key = required("STASIS_LLM_API_KEY")?;
    Ok(())
}
```

The dashboard binary calls `bootstrap()` automatically on startup.

## Resolution order

When the global resolver is installed via `bootstrap()`:

1. **Process environment** — explicit exports, container/Kubernetes env, Vault Agent env injection
2. **File secrets** — one file per key under `STASIS_SECRETS_DIR`

During bootstrap, Stasis also loads a dotenv file **without overriding existing process env**:

- `STASIS_ENV_FILE` when set, otherwise `.env` in the working directory

## API surface (`stasis::config_prelude`)

| Function | Description |
|---|---|
| `bootstrap()` | Load dotenv + secrets dir, install resolver |
| `bootstrap_with(EnvBootstrapOptions)` | Same with explicit paths / skip flags |
| `non_empty(key)` | Trimmed non-empty value or `None` |
| `with_default(key, default)` | Value or default string |
| `first_non_empty(&[keys])` | First matching non-empty value |
| `required(key)` | Value or `EnvError` naming the missing key only |
| `truthy(key)` | Parses `1`, `true`, `yes`, `on` (case-insensitive) |
| `load_dotenv_from(path)` | Load dotenv without installing resolver |

Secret-safe errors never include secret values — only variable names.

## Vault and production secret mounts

Stasis does not embed a Vault HTTP client. Production setups typically inject secrets in one of two ways:

### 1. Environment injection (recommended for 12-factor apps)

Configure Vault Agent, External Secrets Operator, or your platform to inject secrets as environment variables before Stasis starts. These always win over `.env` and file mounts.

### 2. File mounts (`STASIS_SECRETS_DIR`)

Point `STASIS_SECRETS_DIR` at a directory where each secret is a file named after the variable:

```text
/vault/secrets/
  STASIS_LLM_API_KEY
  STASIS_DASHBOARD_SURREAL_PASSWORD
```

File contents are trimmed; empty files are ignored.

### Custom vault clients

Implement [`SecretsSource`](https://docs.rs/stasis-rs/latest/stasis/application/config/secrets/trait.SecretsSource.html) and compose it with `ChainedSecretsSource` if you need direct Vault API access in-process.

## Common variables

See `.env.example` for a full local template. Frequently used keys:

| Variable | Purpose |
|---|---|
| `STASIS_LLM_PROVIDER` | LLM adapter id (`openai`, `anthropic`, …) |
| `STASIS_LLM_MODEL` | Default model id (genai namespaced targets — see [LLM Providers](./llm-providers.md)) |
| `STASIS_LLM_API_KEY` | Global LLM API key fallback |
| `STASIS_DASHBOARD_RUNTIME_BACKEND` | Dashboard persistence backend |
| `STASIS_DASHBOARD_SURREAL_*` | Surreal connection settings |
| `STASIS_SECRETS_DIR` | Directory of file-backed secrets |

Surreal helpers in `surreal_backend_config` (`resolve_surreal_namespace_from_env`, `resolve_surreal_auth_from_env`, …) use the same resolver when `bootstrap()` has run.

For provider namespaces, reasoning effort keywords, and Groq migration notes, see [LLM Providers](./llm-providers.md).

## WASM profile

The kernel compiles to `wasm32-unknown-unknown` without default features:

```bash
rustup target add wasm32-unknown-unknown
cargo check -p stasis-rs --target wasm32-unknown-unknown --no-default-features
cargo test -p stasis-rs --target wasm32-unknown-unknown --no-default-features --test wasm_kernel_smoke
```

On that profile:

- `bootstrap()` still installs an OS-env resolver; it does **not** load `.env` or `STASIS_SECRETS_DIR` (those need the `env-fs` feature).
- Do not enable `native` / `dashboard` / `surreal-native` / `llm-genai` / `grapheme` for wasm32 — the crate `compile_error`s if `native` is on.
- `--no-default-features` on native hosts is the same slim kernel (intentional).
- In-memory `StasisSdk` + `RuntimeSdk` run under the W2 Node `wasm-bindgen-test` harness (`tests/wasm_kernel_smoke.rs`). Wall time uses `web-time` / chrono `wasmbind`. Locus store/recall jobs are covered by the same harness.
- Opt-in `llm-openai-http` adds `OpenAiHttpGateway`: OpenAI Chat Completions over `reqwest`/`fetch`. Set `STASIS_OPENAI_API_KEY` (or `OPENAI_API_KEY` / `STASIS_LLM_API_KEY`) and optionally `STASIS_LLM_BASE_URL` for Groq/OpenRouter/Ollama `/v1`. Browser hosts should pass the key into `OpenAiHttpGateway::new` (no process env) and often need a same-origin proxy because of CORS.
- Opt-in `http-wasm` compiles webhook and cluster HTTP forwarders (`fetch`). Native webhook/TCP paths stay on the default native graph.
- Opt-in `surreal-ws` compiles remote `wss://` / `ws://` job adapters. `surreal-native` / SurrealKV `compile_error` on wasm32.
- Opt-in `locus-persist` wires `locus-surreal-adapter` (`indxdb://`, `mem://`, `wss://`). Stasis does not reimplement STTP; `locus-wasm` remains the browser persistence implementation.
- Optional workspace crate `stasis-wasm` exposes `wasm-bindgen` `StasisWasmClient` (in-memory register/invoke + ping job). `npm --prefix stasis-wasm run build` produces the npm package (`pkg/` bundler + `pkg-node/` Node). Publish with `./scripts/publish-npm.sh`.

See [ADR-0009](https://github.com/EntasisLabs/stasis/blob/main/docs/adr/ADR-0009-wasm-target-profile.md) and the [browser embed cookbook](./cookbook/embed-stasis-browser-host.md).

## Safety notes

- `.env` is gitignored — never commit secrets.
- Dotenv never overrides existing process env (safe for prod + local `.env` together).
- Use `required()` for mandatory secrets; avoid logging return values.
- Prefer platform/Vault injection in production over long-lived `.env` files.
