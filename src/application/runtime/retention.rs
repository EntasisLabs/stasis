#[derive(Clone, Debug)]
pub struct RetentionPolicy {
    pub terminal_ttl_days: i64,
    /// Completion and control receipts stay queryable for duplicate completion
    /// until this age. Independent of terminal job deletion.
    pub receipt_ttl_days: i64,
    /// Cleanup observations and acknowledgements use the same clock.
    pub cleanup_ttl_days: i64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            terminal_ttl_days: 30,
            receipt_ttl_days: 30,
            cleanup_ttl_days: 30,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionPruneReport {
    pub jobs_pruned: usize,
    pub attempts_pruned: usize,
    pub outbox_events_pruned: usize,
    pub receipts_pruned: usize,
    pub cleanups_pruned: usize,
}
