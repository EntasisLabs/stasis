# Component: medousa-daemon

## Entry

- Binary: `medousa/src/bin/medousa_daemon.rs`

## Process Model

Daemon has two concurrent loops:

1. HTTP server (Axum)
2. Scheduler loop (`run_scheduler_loop`)

Scheduler tick (`tick_runtime`):

1. materialize recurring jobs (`materialize_recurring_now`)
2. process one queued job (`process_once`)
3. publish pending outbox events (`publish_pending`)

## API Surface

From shared types in `medousa/src/daemon_api.rs`:

- `GET /health`
- `GET /v1/stats`
- `POST /v1/jobs/ask`
- `POST /v1/jobs/prompt`
- `POST /v1/recurring/prompt`

Optional dashboard mount for in-memory backend:

- `/dashboard`

## AppState Ownership

`AppState` stores:

- `runtime: Arc<RuntimeComposition>`
- backend label
- worker id
- `last_tick_at` (RwLock)

## Request Handling Pattern

Typical write endpoint flow:

1. validate request payload
2. build workflow payload + `NewJob`
3. enqueue into runtime
4. return accepted response with `job_id`

Recurring registration additionally computes and stores `next_run_at`.

## State and Durability

Daemon itself persists no custom files.
State durability is delegated to runtime backend stores:

- jobs
- attempts
- outbox
- recurring definitions

## Operational Notes

- `--once` runs a single scheduler tick and exits
- `--interval-ms` controls scheduler cadence
- graceful shutdown via Ctrl+C + watch signal
