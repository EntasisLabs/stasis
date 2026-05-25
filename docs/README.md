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
- Hospice interoperability safety test analysis and gameplan: [design/hospice-interoperability-safety-test-gameplan.md](design/hospice-interoperability-safety-test-gameplan.md)
- Grapheme reflection and LSP delivery plan: [design/grapheme-reflection-lsp-delivery-plan.md](design/grapheme-reflection-lsp-delivery-plan.md)

## Internal Testing Environment Variables

- `STASIS_TEST_SURREAL_WS_ENDPOINT`
	- Used only by the runtime backend parity test that validates `RuntimeBackend::SurrealWs` with Locus memory wiring.
	- When unset, that websocket compatibility test exits early so local and CI runs can stay deterministic without a running SurrealDB websocket endpoint.
