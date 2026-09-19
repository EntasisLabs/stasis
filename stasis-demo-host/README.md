# stasis-demo-host

Thin process that powers the [runstasis.io](https://runstasis.io) live demo
without pasting an OpenAI key into the browser.

```text
Public runstasis.io shell
  └─ @urspace/client
         generateSessionKey → POST {public mint}/bootstrap
         connectMinted(token, secretKey)
         session.fetch POST /v1/chat/completions
                │ Iroh (no key)
                ▼
Site::serve("127.0.0.1:PROXY")   // urspace 0.5.x, production relay
                │
                ▼
axum proxy → OpenAI (OPENAI_API_KEY in env)
```

This crate does **not** embed `stasis-rs`, the dashboard, or `stasisd`. The
browser runs `stasis-wasm`. This binary is proxy + mint + quota only.

## Listeners

| Bind (env) | Default | Exposure | Role |
| --- | --- | --- | --- |
| `PROXY_BIND` | `127.0.0.1:8787` | Loopback, shared only via `urspace::Site` | `POST /v1/chat/completions`, `GET /v1/models` stub |
| `MINT_BIND` | `0.0.0.0:8788` | Public (or a tunnel) | `POST /bootstrap`, `GET /health` |

Nothing else is served on the mint port. The proxy strips inbound
`Authorization` / `x-api-key` / cookies and injects the server key.

## Run

Requires Rust 1.85+ (edition 2024). `urspace` is pulled from
[EntasisLabs/urspace](https://github.com/EntasisLabs/urspace) (0.5.x on `main`);
it is not published on crates.io.

```bash
cp stasis-demo-host/.env.example stasis-demo-host/.env
# set OPENAI_API_KEY

export $(grep -v '^#' stasis-demo-host/.env | xargs)
cargo run -p stasis-demo-host
```

`Site::serve` / `Site::serve_with` contacts the production Iroh relay so a
browser can reach the loopback proxy. The proxy must be listening first; the
binary binds it before opening the site.

```bash
cargo check -p stasis-demo-host
cargo test -p stasis-demo-host
```

## How the site connects

Matches [`@urspace/client`](https://github.com/EntasisLabs/urspace/blob/main/packages/client/README.md)
and [`docs/sdk.md`](https://github.com/EntasisLabs/urspace/blob/main/docs/sdk.md).

`POST /bootstrap` body is `{ "public_key": number[32] }` (the 32 bytes from
`generateSessionKey().publicKey`). The response is **plain text**: a `usi1.`
token (`response.text()`), not JSON.

```js
import { connectMinted, generateSessionKey } from "@urspace/client";

const keys = generateSessionKey();
const token = await fetch("https://mint-host.example/bootstrap", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ public_key: Array.from(keys.publicKey) }),
}).then((response) => response.text());

const session = await connectMinted(token, keys.secretKey);

const subjectHex = [...keys.publicKey]
  .map((b) => b.toString(16).padStart(2, "0"))
  .join("");

const reply = await session.fetch(
  "POST",
  "/v1/chat/completions",
  [
    { name: "content-type", value: "application/json" },
    { name: "x-urspace-subject", value: subjectHex },
  ],
  new TextEncoder().encode(
    JSON.stringify({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: "hello" }],
    }),
  ),
);
```

Urspace forwards loopback HTTP as-is and does **not** inject the session
public key. The demo host therefore requires `x-urspace-subject` (or
`x-stasis-subject`) on `POST /v1/chat/completions` so quota can key on
`{subject hex}:{YYYY-MM-DD}` UTC. That header is the public identity, not a
secret; it is stripped before the request is sent upstream.

`GET /v1/models` is a read-only stub of `ALLOWED_MODELS`. Streaming
(`stream: true`) is rejected. Other `/v1/*` paths return 404.

## Environment

See [`.env.example`](.env.example).

| Variable | Default | Notes |
| --- | --- | --- |
| `OPENAI_API_KEY` | required | Never logged |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible `/v1` root |
| `PROXY_BIND` | `127.0.0.1:8787` | Must stay loopback in production |
| `MINT_BIND` | `0.0.0.0:8788` | Public mint + health |
| `DAILY_COMPLETIONS` | `5` | `0` = unlimited |
| `DAILY_TOKENS` | unset | Prompt+completion tokens when the upstream body includes `usage` |
| `ALLOWED_MODELS` | `gpt-4o-mini` | Comma-separated |
| `MINT_TTL_SECS` | `30` | `MintOptions.ttl` |
| `MINT_MAX_SESSIONS` | `1` | Single browser key |
| `MINT_PER_IP` / `MINT_PER_SUBJECT` | `10` / `5` | Sliding window |
| `MINT_WINDOW_SECS` | `600` | |
| `TRUST_PROXY` | off | Honor `X-Forwarded-For` only behind a trusted proxy |
| `CORS_ORIGIN` | `*` | Mint listener only |
| `URSPACE_BOOTSTRAP_ORIGIN` | `https://urspace.online` | Signed into minted tokens |
| `RUST_LOG` | `stasis_demo_host=info` | |

## Security notes

- The browser never sees `OPENAI_API_KEY`. Do not put the key in JS, WASM, or
  the public site.
- Subject-bound mint: stealing a `usi1.` token is useless without `secretKey`.
  Tokens expire quickly (`MintOptions { ttl: ~30s, max_sessions: 1 }`).
- `POST /bootstrap` is **not** authentication. It is rate-limited by IP and by
  subject. Anyone who can hit mint can ask for a ticket.
- Daily caps are per browser identity (session public key) and UTC date.
  Counts live in memory; a restart resets them.
- Do not log invite URLs, mint tokens, session keys, or `OPENAI_API_KEY`.
- Keep `PROXY_BIND` on loopback. The public surface is mint + health only.
- Operators can kick a live session with `Site::kick("session-…")` from a
  future admin path; this binary does not expose kick on the network.
- Binding mint on `0.0.0.0` without a tunnel still needs host firewall /
  TLS termination in front of it for a public demo.

## Out of scope

- Wiring `stasis-web` / `stasis-wasm` (follow-up)
- Dual upstream site+agent in the urspace protocol
- Deploying to a droplet
