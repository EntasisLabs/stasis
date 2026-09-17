# Publishing Stasis to crates.io

This workspace already publishes under the `stasis-rs*` names because [`stasis`](https://crates.io/crates/stasis) is a different project (Wayland idle manager).

## What is published

| Crate | Version in tree | crates.io (as of 2026-09-17) | Action |
| --- | --- | --- | --- |
| `stasis-rs-macros` | 0.1.0 | **0.1.0 already published** | Do **not** republish unless the macros crate is bumped |
| `stasis-rs` | 0.11.0 | latest **0.10.0** | Publish **0.11.0** |
| `stasisd` | 0.1.0 | not published | `publish = false` (workspace binary only) |
| `stasis-wasm` | 0.10.0 | not published | `publish = false` (npm packaging is separate and still private) |

Owner of both crates.io packages: [`theelevators`](https://github.com/theelevators) (Tom Vazquez). Publishing requires that account (or a new owner) plus a crates.io API token and 2FA.

## Ordered publish sequence

1. **`stasis-rs-macros` 0.1.0** — skip upload. Already on crates.io; source is unchanged. `stasis-rs 0.11.0` still depends on `stasis-rs-macros = "0.1.0"`.
2. **`stasis-rs` 0.11.0** — this is the release to upload.

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
cargo yank --vers 0.11.0 stasis-rs
```

## Blockers that require the crate owner

- **API token** with `publish-update` for `stasis-rs`, stored via `cargo login` or `CARGO_REGISTRY_TOKEN` — never committed.
- **2FA** on the crates.io owner account (`theelevators`).
- **Do not republish `stasis-rs-macros` 0.1.0** — crates.io rejects duplicate versions. Yank is not a rename.
- **Do not publish as `stasis`** — that name is owned by another project. Keep `stasis-rs`.
- **`stasisd` / `stasis-wasm`** stay unpublished until `publish = false` is removed and versions/metadata are decided.
- **No GitHub Actions publish workflow** is configured. Adding one later needs a repository secret (`CARGO_REGISTRY_TOKEN`) or crates.io [trusted publishing](https://crates.io/docs/trusted-publishing).
- Root junk (`showtest.md`, `*.gr` scratch files) is already omitted by the `stasis-rs` `include` list; do not add those paths.

## Local verification

```bash
rustc --version   # need 1.85+ (edition 2024); MSRV is declared as rust-version = "1.85"
./scripts/publish-crates.sh
```

`cargo publish --dry-run` packages the crate and builds it from the tarball. It does not upload and does not consume a publish token.
