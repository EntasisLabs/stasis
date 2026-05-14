#[derive(Clone, Debug)]
pub struct RetentionPolicy {
    pub terminal_ttl_days: i64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            terminal_ttl_days: 30,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionPruneReport {
    pub jobs_pruned: usize,
    pub attempts_pruned: usize,
    pub outbox_events_pruned: usize,
}
