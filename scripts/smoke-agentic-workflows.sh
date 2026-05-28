#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[smoke] cargo check --examples"
cargo check --examples

echo "[smoke] production agentic workflows (dry run, all packs)"
STASIS_EXAMPLE_DRY_RUN=1 cargo run --example agentic_workflows_production

echo "[smoke] backend profile example (in-memory)"
STASIS_EXAMPLE_RUNTIME_BACKEND=in-memory cargo run --example runtime_backends_profiles

echo "[smoke] team role examples (dry run)"
for profile in sre product support; do
  STASIS_EXAMPLE_DRY_RUN=1 STASIS_EXAMPLE_TEAM_PROFILE="$profile" cargo run --example team_role_workflows
done

echo "[smoke] targeted integration harness"
cargo test --test production_examples_smoke

if command -v mdbook >/dev/null 2>&1; then
  echo "[smoke] mdbook build docs-book"
  mdbook build docs-book
else
  echo "[smoke] mdbook not installed, skipping docs build"
fi

echo "[smoke] completed successfully"
