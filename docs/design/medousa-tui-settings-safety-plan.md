# Medousa TUI Settings Safety Plan

Status: Active
Owner: Medousa
Last updated: 2026-05-19

## Goal

Make runtime settings manageable from the TUI while keeping secrets and policy-sensitive settings safe by default.

## Scope

This plan covers:
1. In-TUI management for API key and allowed Grapheme modules.
2. Validation and rejection of unsafe settings input.
3. Redaction of sensitive values in observability surfaces.
4. Durable and safer persistence semantics.

This plan does not yet cover:
1. Multi-profile settings.
2. Per-session policy overrides.

## Safety Principles

1. Never render raw secret values in UI surfaces outside direct edit buffers.
2. Never include secrets in observability payload logs.
3. Fail closed for malformed allowlist entries.
4. Persist secrets separately from normal defaults data.
5. Use atomic writes for settings persistence paths.

## Settings Model

Non-secret settings:
1. backend
2. provider
3. model
4. base_url
5. tool_call_mode
6. max_tool_rounds
7. thinking_capture
8. thinking_max_lines
9. allowed_modules

Secret settings:
1. api_key

Persistence split:
1. Non-secret defaults live in medousa data defaults JSON.
2. API key lives in a dedicated secret file path with restricted permissions where possible.

## First Slice (Implemented)

1. Add API key and allowed module fields in the Settings overlay.
2. Mask API key in rendering with only suffix visibility.
3. Validate allowed modules as dotted IDs with strict character constraints.
4. Reject apply on invalid allowlist and show operator-facing reason.
5. Redact sensitive keys and bearer-like values in tool payload observability events.
6. Move defaults writes to an atomic write path.
7. Persist API key in dedicated secret storage path rather than defaults JSON.

## Next Slices

Slice 2: Runtime enforcement
1. Apply allowlist to tool execution dispatch. Status: Implemented.
2. Block disallowed module invocations with explicit diagnostics. Status: Implemented.
3. Add an allowlist preview panel in command palette. Status: Implemented.

Slice 3: Secret backend hardening
1. Add OS keychain adapter with file fallback. Status: Implemented.
2. Add key rotation and clear-key affordance in TUI. Status: Implemented.
3. Add explicit "redaction mode" indicator in observability panel. Status: Implemented.

Slice 4: Transactional UX
1. Introduce staged editing (draft vs applied). Status: Implemented.
2. Add validation summary before save. Status: Implemented.
3. Add revert-to-last-known-good action. Status: Implemented.

## Acceptance Criteria

1. Operator can set or clear API key from TUI without exposing raw key in logs.
2. Invalid allowlist values are rejected and not persisted.
3. Tool payload observability renders redacted sensitive fields.
4. Settings writes are atomic for defaults and secret paths.

## Risks

1. Secret file storage is still weaker than OS keychain on shared hosts.
2. Existing payload schemas may include novel sensitive field names not yet captured.
3. Some promotion flows that rely on remembered source now require explicit source when allowlist enforcement is active.

## Mitigations

1. Prioritize keychain adapter in Slice 3.
2. Expand sensitive key dictionary and add regression tests per incident.
3. Keep explicit source requirement under allowlist mode, and add a source-preview confirmation in a follow-up UX pass.

## Execution Log

- 2026-05-19: Slice 1 implemented (masked API key field, module allowlist validation, payload redaction, atomic persistence).
- 2026-05-19: Slice 2 implemented (policy-aware tool registry blocks disallowed Grapheme module operations at runtime).
- 2026-05-19: Added explicit clear-key affordance in TUI settings and command palette (/clear-key).
- 2026-05-19: Slice 4 implemented (draft/apply settings workflow, pre-apply validation summary, revert-to-last-applied action).
- 2026-05-19: Slice 3 implemented (keychain-first secret storage with file fallback, rotate-key action, observability redaction-mode indicator).
- 2026-05-19: Added allowlist preview panel (slash + command palette) with multiline source editing plus quick actions to replace/append detected ops into draft allowlist.
