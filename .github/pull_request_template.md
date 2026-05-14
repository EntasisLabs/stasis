## Architecture Checklist

- [ ] This PR references the architecture RFC and implementation plan:
  - docs/design/stasis-framework-rfc.md
  - docs/design/stasis-framework-implementation-plan.md
- [ ] Layer boundaries are preserved (app -> stasis APIs -> ports -> adapters)
- [ ] No direct adapter orchestration is introduced in consumer apps
- [ ] Runtime diagnostics/lineage behavior is preserved or explicitly updated
- [ ] Tests were added/updated for architecture-impacting changes

## Summary

Describe the change and why it aligns with the roadmap.

## Validation

List commands and key outcomes (for example: cargo test -p stasis).
