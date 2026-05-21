# Component: medousa-cli

## Entry

- Binary: `medousa/src/bin/medousa_cli.rs`

## Command Families

1. Local runtime commands
- `ask`
- `llm`

2. Daemon API commands
- `daemon-health`
- `daemon-stats`
- `daemon-ask`
- `daemon-watch-add`

## Local Runtime Path

For `ask`/`llm`:

1. Parse args and resolve backend/provider/model/base-url.
2. Build runtime via `build_runtime(...)`.
3. Create workflow payload (`AgentSessionJobPayload` or `PromptJobPayload`).
4. Build `NewJob` via `StasisWorkflowJobBuilder`.
5. Enqueue in runtime.
6. Call `process_once(...)`.
7. Read attempts/diagnostics and print result.

## Daemon Client Path

CLI acts as HTTP client (reqwest) against daemon endpoints:

- `/health`
- `/v1/stats`
- `/v1/jobs/ask`
- `/v1/recurring/prompt`

## State Handling

CLI is mostly stateless:

- no long-lived in-process state between commands
- no local session persistence owned by CLI
- all durable state lives in runtime backend and/or daemon-side stores

## Failure/Visibility Notes

- local commands surface diagnostics from job attempts
- daemon commands surface HTTP errors and typed response payloads
