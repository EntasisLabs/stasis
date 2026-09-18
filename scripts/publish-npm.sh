#!/usr/bin/env bash
# Build, test, and pack (or publish) the stasis-wasm npm package.
#
# Default: dry-run only (no upload, no token).
# Real upload: CONFIRM_PUBLISH=yes ./scripts/publish-npm.sh --execute
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/stasis-wasm"

MODE="dry-run"
if [[ "${1:-}" == "--execute" ]]; then
	MODE="execute"
elif [[ -n "${1:-}" ]]; then
	echo "usage: $0 [--execute]" >&2
	exit 2
fi

if [[ "$MODE" == "execute" && "${CONFIRM_PUBLISH:-}" != "yes" ]]; then
	echo "Refusing real npm publish. Re-run with CONFIRM_PUBLISH=yes $0 --execute after npm login." >&2
	exit 1
fi

if ! command -v npm >/dev/null; then
	echo "npm not found on PATH" >&2
	exit 1
fi

npm run build
npm test

if [[ "$MODE" == "dry-run" ]]; then
	echo "==> npm publish --access public --dry-run"
	npm publish --access public --dry-run
else
	echo "==> npm publish --access public"
	npm publish --access public
fi

echo "==> done ($MODE)"
echo "Rust crate stasis-wasm stays unpublished on crates.io (publish = false)."
