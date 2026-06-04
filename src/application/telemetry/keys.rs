//! Stable runtime metric instrument keys (OpenTelemetry contract §5).

// Job lifecycle
pub const JOB_SUCCEEDED_TOTAL: &str = "runtime.job.succeeded.total";
pub const JOB_RETRYABLE_FAILURE_TOTAL: &str = "runtime.job.retryable_failure.total";
pub const JOB_FATAL_FAILURE_TOTAL: &str = "runtime.job.fatal_failure.total";
pub const JOB_DEAD_LETTER_TOTAL: &str = "runtime.job.dead_letter.total";
pub const JOB_RETRY_SCHEDULED_TOTAL: &str = "runtime.job.retry_scheduled.total";
pub const JOB_PROCESS_DURATION_MS: &str = "runtime.job.process.duration_ms";

// Outbox
pub const OUTBOX_PUBLISH_SUCCESS_TOTAL: &str = "runtime.outbox.publish.success.total";
pub const OUTBOX_PUBLISH_FAILURE_TOTAL: &str = "runtime.outbox.publish.failure.total";

// Grapheme
pub const GRAPHEME_GUARDRAIL_FAILURE_TOTAL: &str = "runtime.grapheme.guardrail_failure.total";

// Chat middleware
pub const CHAT_REQUESTS_TOTAL: &str = "runtime.chat.requests.total";
pub const CHAT_ERRORS_TOTAL: &str = "runtime.chat.errors.total";
pub const CHAT_DURATION_MS: &str = "runtime.chat.duration_ms";
pub const CHAT_CACHE_HIT_TOTAL: &str = "runtime.chat.cache.hit.total";
pub const CHAT_CACHE_MISS_TOTAL: &str = "runtime.chat.cache.miss.total";
pub const CHAT_TOOL_CALLS_TOTAL: &str = "runtime.chat.tool_calls.total";

// Memory (0.3.0)
pub const MEMORY_RECALL_TOTAL: &str = "runtime.memory.recall.total";
pub const MEMORY_RECALL_ERRORS_TOTAL: &str = "runtime.memory.recall.errors.total";
pub const MEMORY_RECALL_DURATION_MS: &str = "runtime.memory.recall.duration_ms";
pub const MEMORY_STORE_TOTAL: &str = "runtime.memory.store.total";
pub const MEMORY_STORE_ERRORS_TOTAL: &str = "runtime.memory.store.errors.total";
pub const MEMORY_STORE_DURATION_MS: &str = "runtime.memory.store.duration_ms";

// Worker
pub const WORKER_PROCESS_ONCE_TOTAL: &str = "runtime.worker.process_once.total";
pub const WORKER_PROCESS_ONCE_DURATION_MS: &str = "runtime.worker.process_once.duration_ms";
