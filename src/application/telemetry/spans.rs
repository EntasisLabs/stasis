//! Stable OpenTelemetry span names (contract §6.1).

pub const WORKER_PROCESS_ONCE: &str = "stasis.worker.process_once";
pub const JOB_EXECUTE: &str = "stasis.job.execute";
pub const CHAT_COMPLETE: &str = "stasis.chat.complete";
pub const MEMORY_RECALL: &str = "stasis.memory.recall";
pub const MEMORY_STORE: &str = "stasis.memory.store";
pub const OUTBOX_PUBLISH: &str = "stasis.outbox.publish";
pub const GRAPHEME_EXECUTE: &str = "stasis.grapheme.execute";
