#!/usr/bin/env bash
# Package and verify crates.io publishes for this workspace.
#
# Default: dry-run only (no upload, no token).
# Real upload: CONFIRM_PUBLISH=yes ./scripts/publish-crates.sh --execute
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="dry-run"
if [[ "${1:-}" == "--execute" ]]; then
	MODE="execute"
elif [[ -n "${1:-}" ]]; then
	echo "usage: $0 [--execute]" >&2
	exit 2
fi

CARGO=(cargo)

rustc_minor() {
	local ver
	ver="$("$@" --version 2>/dev/null | awk '{print $2}')"
	echo "${ver:-0}" | cut -d. -f2
}

if ! command -v cargo >/dev/null; then
	echo "cargo not found on PATH" >&2
	exit 1
fi

if [[ "$(rustc_minor rustc)" -lt 85 ]]; then
	if command -v rustup >/dev/null && [[ "$(rustc_minor rustup run stable rustc)" -ge 85 ]]; then
		echo "default rustc is < 1.85; using rustup run stable"
		CARGO=(rustup run stable cargo)
	else
		echo "Rust 1.85+ is required (edition 2024). Current: $(rustc --version 2>/dev/null || echo missing)" >&2
		exit 1
	fi
fi

publish_crate() {
	local pkg="$1"
	local extra=()
	if [[ "$MODE" == "dry-run" ]]; then
		extra+=(--dry-run)
	fi
	echo "==> ${CARGO[*]} publish -p ${pkg} ${extra[*]:-}"
	"${CARGO[@]}" publish -p "$pkg" "${extra[@]}"
}

if [[ "$MODE" == "execute" && "${CONFIRM_PUBLISH:-}" != "yes" ]]; then
	echo "Refusing real publish. Re-run with CONFIRM_PUBLISH=yes $0 --execute after cargo login." >&2
	exit 1
fi

# Macros 0.1.0 is already on crates.io. Dry-run is still useful; never re-upload 0.1.0.
if [[ "$MODE" == "dry-run" ]]; then
	publish_crate stasis-rs-macros
else
	echo "==> skipping stasis-rs-macros (0.1.0 already published; bump first if macros changed)"
fi

publish_crate stasis-rs

echo "==> done ($MODE)"
echo "Not published (publish = false): stasisd, stasis-wasm"
