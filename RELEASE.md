# Publishing Stasis to crates.io

This workspace already publishes under the `stasis-rs*` names because [`stasis`](https://crates.io/crates/stasis) is a different project (Wayland idle manager).

## What is published

| Crate | Version in tree | Registry (as of 2026-09-22) | Action |
| --- | --- | --- | --- |
| `stasis-rs-macros` | 0.1.0 | crates.io **0.1.0** | Do **not** republish unless the macros crate is bumped |
| `stasis-rs` | 0.12.0 | crates.io latest **0.11.0** (2026-09-18) | Publish **0.12.0** |
| `stasisd` | 0.1.0 | not published | `publish = false` (workspace binary only) |
| `stasis-wasm` | 0.12.0 | crates.io `publish = false`; **npm `stasis-wasm@0.12.0` already uploaded** (2026-09-18) | Skip npm unless you bump to **0.13.0** (0.12.0 tarball was built against kernel 0.11.0) |

Owner of both crates.io packages: [`theelevators`](https://github.com/theelevators) (Tom Vazquez). Publishing requires that account (or a new owner) plus a crates.io API token and 2FA.

## Ordered publish sequence

1. **`stasis-rs-macros` 0.1.0** — skip upload. Already on crates.io; source is unchanged. `stasis-rs 0.12.0` still depends on `stasis-rs-macros = "0.1.0"`.
2. **`stasis-rs` 0.12.0** — this is the release to upload. crates.io already has **0.11.0**; a second 0.11.0 upload will be rejected.

If macros ever change, bump `stasis-macros/Cargo.toml` first (for example `0.1.1`), publish that version, then point `stasis-rs` at the new macros version and publish `stasis-rs`.

## Commands (token required for the real upload)

crates.io will not accept a publish from this environment without **your** token. Do not paste tokens into chat, git, or CI logs. Create a token at <https://crates.io/settings/tokens> with `publish-update` on `stasis-rs` (and `stasis-rs-macros` only if you bump it). 2FA / trusted publishing must be enabled on the owner account.

```bash
# 1. Auth (once per machine). Prefer a token via stdin, not argv.
cargo login

# 2. Dry-run from a clean git tree (no token needed):
./scripts/publish-crates.sh

# Equivalent manual dry-run, dependency order:
cargo publish -p stasis-rs-macros --dry-run
cargo publish -p stasis-rs --dry-run

# 3. Real upload (only after dry-run is green). Macros 0.1.0 must be skipped.
cargo publish -p stasis-rs
```

`cargo publish` rewrites the `stasis-macros` path dependency to crates.io `stasis-rs-macros ^0.1.0`. Macros must remain available at that version (they already are).

To yank a mistaken upload (does not delete; only hides from new resolvers):

```bash
cargo yank --vers 0.12.0 stasis-rs
```

## Blockers that require the crate owner

- **API token** with `publish-update` for `stasis-rs`, stored via `cargo login` or `CARGO_REGISTRY_TOKEN` — never committed.
- **2FA** on the crates.io owner account (`theelevators`).
- **Do not republish `stasis-rs-macros` 0.1.0** — crates.io rejects duplicate versions. Yank is not a rename.
- **Do not publish as `stasis`** — that name is owned by another project. Keep `stasis-rs`.
- **`stasisd`** stays unpublished on crates.io until `publish = false` is removed.
- **`stasis-wasm`** is published to **npm**, not crates.io. See [npm](#npm-stasis-wasm).
- **No GitHub Actions publish workflow** is configured. Adding one later needs a repository secret (`CARGO_REGISTRY_TOKEN`) or crates.io [trusted publishing](https://crates.io/docs/trusted-publishing).
- Root junk (`showtest.md`, `*.gr` scratch files) is already omitted by the `stasis-rs` `include` list; do not add those paths.

## Local verification

```bash
rustc --version   # need 1.85+ (edition 2024); MSRV is declared as rust-version = "1.85"
./scripts/publish-crates.sh
```

`cargo publish --dry-run` packages the crate and builds it from the tarball. It does not upload and does not consume a publish token.

## Dry-run results (2026-09-22, rustc 1.98.1, 0.12.0)

Pending. Run `./scripts/publish-crates.sh` on this branch after the version bump. No crates.io token is required for `--dry-run`.

## Dry-run results (2026-09-17, rustc 1.98.1, 0.11.0)

Historical. 0.11.0 was uploaded to crates.io on 2026-09-18.

| Crate | Result | Notes |
| --- | --- | --- |
| `stasis-rs-macros` 0.1.0 | **dry-run OK** | Warns `crate stasis-rs-macros@0.1.0 already exists on crates.io index`. Packaged 20 files, 39.3KiB (11.0KiB compressed). Verify compile 4.5s. **Do not upload.** |
| `stasis-rs` 0.11.0 | **dry-run OK**, then **published** | Packaged 326 files, 3.7MiB (588.7KiB compressed). Verify compile 6m 14s from the tarball. `build.rs` fell back to prebuilt `dashboard.css` (no local Tailwind). |

Scratch files (`showtest.md`, `*.gr`) were not in the tarball. `stasisd` was not packaged (`publish = false`). `stasis-wasm` is an npm package, not a crates.io crate.

## npm (`stasis-wasm`)

The registry name `stasis-wasm` is taken by this project at **0.12.0**. The Rust crate stays `publish = false`. npm 0.12.0 is already uploaded; the kernel crate for this release is `stasis-rs` **0.12.0**. A new npm tarball needs `stasis-wasm` **0.13.0**.

`pkg/` and `pkg-node/` are gitignored; they are produced on the machine that publishes. `prepack` / `prepublishOnly` build them if missing, then run the Node smoke test.

### Commands (token required for the real upload)

Create a granular npm token (or log in interactively) at <https://www.npmjs.com/settings/~/tokens>. 2FA must be enabled. Do not paste tokens into chat, git, or CI logs.

```bash
# 1. Auth (once per machine)
npm login

# 2. Dry-run from the repo root (no token needed for pack; publish --dry-run may ask you to be logged in)
./scripts/publish-npm.sh

# Equivalent manual:
cd stasis-wasm
npm run build
npm test
npm publish --access public --dry-run

# 3. Real upload (only after dry-run is green)
CONFIRM_PUBLISH=yes ./scripts/publish-npm.sh --execute
# or: cd stasis-wasm && npm publish --access public
```

`publishConfig.access` is `public`, so an unscoped first publish does not become a 402/private-package error.

To unpublish within 72 hours (npm policy; prefer deprecate for anything already downloaded):

```bash
npm unpublish stasis-wasm@0.12.0
npm deprecate stasis-wasm@0.12.0 "reason"
```

### Blockers that require you

- **npm login** on the account that should own `stasis-wasm`
- **2FA** on that npm account
- Actual `npm publish` (this repo only dry-runs)
- `wasm32-unknown-unknown` target and `wasm-bindgen-cli` matching `Cargo.lock` (0.2.121 as of this writing)

## npm dry-run results (2026-09-18, 0.12.0)

Run on this branch with `./scripts/publish-npm.sh`. No npm token was used. `npm publish --dry-run` printed `+ stasis-wasm@0.12.0` and warned that a real publish requires login.

| Check | Result |
| --- | --- |
| `wasm-bindgen` 0.2.121 + `wasm32-unknown-unknown` | OK |
| Node smoke (mock, OpenAI construct, memory, identity, tools, Grapheme, replay) | pass (10 tests) |
| Tarball | `stasis-wasm-0.12.0.tgz`, 13 files, ~3.8 MB packed / 13.1 MB unpacked |
| Contents | `LICENSE-*`, `README.md`, `package.json`, `pkg/` (bundler), `pkg-node/` (Node) |
| `CONFIRM_PUBLISH` guard | `--execute` without the env var refused |

`pkg/` and `pkg-node/` remain gitignored. Rebuild on the machine that publishes. Grapheme in the guest grows the wasm (~6.5 MB) vs 0.11.0.

