#!/usr/bin/env bash
# Build wasm-bindgen artifacts if `npm pack` / `npm publish` is run without a prior build.
set -euo pipefail

crate_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$crate_root"

if [[ -f pkg/stasis_wasm.js && -f pkg/stasis_wasm_bg.wasm && -f pkg-node/stasis_wasm.js && -f pkg-node/stasis_wasm_bg.wasm ]]; then
	echo "using existing stasis-wasm pkg/ and pkg-node/"
	exit 0
fi

exec bash "$crate_root/scripts/build-npm.sh"
