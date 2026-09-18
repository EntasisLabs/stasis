#!/usr/bin/env bash
# Build wasm-bindgen npm artifacts for bundlers (`pkg/`) and Node (`pkg-node/`).
set -euo pipefail

crate_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="$(cd "$crate_root/.." && pwd)"
cd "$repo_root"

CARGO=(cargo)

rustc_minor() {
	local ver
	ver="$("$@" --version 2>/dev/null | awk '{print $2}')"
	echo "${ver:-0}" | cut -d. -f2
}

if [[ "$(rustc_minor rustc)" -lt 85 ]]; then
	if command -v rustup >/dev/null && [[ "$(rustc_minor rustup run stable rustc)" -ge 85 ]]; then
		echo "default rustc is < 1.85; using rustup run stable"
		CARGO=(rustup run stable cargo)
	else
		echo "Rust 1.85+ is required (edition 2024). Current: $(rustc --version 2>/dev/null || echo missing)" >&2
		exit 1
	fi
fi

if ! command -v wasm-bindgen >/dev/null 2>&1; then
  version="$(awk '/name = "wasm-bindgen"$/{getline; if ($1=="version") {gsub(/"/, "", $3); print $3; exit}}' Cargo.lock)"
  echo "wasm-bindgen CLI is required (lockfile version: ${version:-unknown})" >&2
  echo "Install with: cargo install wasm-bindgen-cli --locked --version \"\$version\"" >&2
  exit 1
fi

if command -v rustup >/dev/null; then
	rustup target add wasm32-unknown-unknown >/dev/null
	if [[ "${CARGO[*]}" == "rustup run stable cargo" ]]; then
		rustup target add wasm32-unknown-unknown --toolchain stable >/dev/null
	fi
fi

"${CARGO[@]}" build -p stasis-wasm --target wasm32-unknown-unknown --release

wasm_bin="target/wasm32-unknown-unknown/release/stasis_wasm.wasm"
if [[ ! -f "$wasm_bin" ]]; then
  echo "expected $wasm_bin after cargo build" >&2
  ls -la target/wasm32-unknown-unknown/release/*.wasm 2>/dev/null || true
  exit 1
fi

bindgen_target() {
  local target="$1"
  local out_dir="$2"
  rm -rf "$out_dir"
  mkdir -p "$out_dir"
  wasm-bindgen "$wasm_bin" \
    --out-dir "$out_dir" \
    --out-name stasis_wasm \
    --target "$target" \
    --typescript
}

bindgen_target bundler "$crate_root/pkg"
bindgen_target nodejs "$crate_root/pkg-node"

echo "built npm artifacts:"
ls -la "$crate_root/pkg" "$crate_root/pkg-node"
