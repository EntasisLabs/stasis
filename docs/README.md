# Documentation Index

## Official Documentation

- Architecture overview: [architecture/overview.md](architecture/overview.md)
- Runtime v1 draft: [v1-runtime-draft.md](v1-runtime-draft.md)
- Job runtime design: [design/job-runtime-design.md](design/job-runtime-design.md)
- SurrealDB schema: [architecture/surrealdb-schema.md](architecture/surrealdb-schema.md)
- Stasis framework RFC: [design/stasis-framework-rfc.md](design/stasis-framework-rfc.md)
- ADRs: [adr/README.md](adr/README.md)

## Documentation Program

- Documentation transformation program: [design/documentation-transformation-program.md](design/documentation-transformation-program.md)

## Validation

- Metadata gate: `./scripts/check-doc-metadata.sh`

## Internal Planning

- Distributed command center plan: [design/distributed-command-center-phase-plan.md](design/distributed-command-center-phase-plan.md)
- API and SDK layer design: [design/stasis-api-sdk-layer-design.md](design/stasis-api-sdk-layer-design.md)
- Unified SDK surface proposal: [design/unified-sdk-surface-proposal.md](design/unified-sdk-surface-proposal.md)
- Locus integration RFC and delivery plan: [design/locus-integration-rfc-plan.md](design/locus-integration-rfc-plan.md)
- OpenTelemetry integration RFC and delivery plan (frozen contract, 0.3.0): [design/opentelemetry-integration-rfc-plan.md](design/opentelemetry-integration-rfc-plan.md)
- Agent platform runtime contracts plan (comms, translation, MCP bridge): [design/agent-platform-runtime-contracts-plan.md](design/agent-platform-runtime-contracts-plan.md)
- `stasisd` declarative engine plan (YAML/TOML desired state): [design/stasisd-declarative-engine-plan.md](design/stasisd-declarative-engine-plan.md)
- Agent platform + `stasisd` phased epic (execution board): [design/agent-platform-and-stasisd-epic.md](design/agent-platform-and-stasisd-epic.md)
- WASM target profile epic (kernel on `wasm32-unknown-unknown`): [design/wasm-target-phase-plan.md](design/wasm-target-phase-plan.md)
- ADR-0009 WASM target profile (Proposed): [adr/ADR-0009-wasm-target-profile.md](adr/ADR-0009-wasm-target-profile.md)
- Hospice interoperability safety test analysis and gameplan: [design/hospice-interoperability-safety-test-gameplan.md](design/hospice-interoperability-safety-test-gameplan.md)
- Grapheme reflection and LSP delivery plan: [design/grapheme-reflection-lsp-delivery-plan.md](design/grapheme-reflection-lsp-delivery-plan.md)

## Internal Testing Environment Variables

- `STASIS_TEST_SURREAL_WS_ENDPOINT`
	- Used only by the runtime backend parity test that validates `RuntimeBackend::SurrealWs` with Locus memory wiring.
	- When unset, that websocket compatibility test exits early so local and CI runs can stay deterministic without a running SurrealDB websocket endpoint.
