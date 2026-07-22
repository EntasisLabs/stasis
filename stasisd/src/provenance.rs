//! Ownership metadata for `stasisd`-managed runtime objects (ADR-0008).

/// Marker written onto managed recurring definitions.
pub const MANAGED_BY: &str = "stasisd";

/// Prefix applied to managed recurring definition ids: `stasisd:<id>`.
pub const MANAGED_ID_PREFIX: &str = "stasisd:";

/// Build the runtime recurring id for a config schedule id.
pub fn managed_recurring_id(schedule_id: &str) -> String {
    format!("{MANAGED_ID_PREFIX}{schedule_id}")
}

pub fn is_managed_recurring_id(runtime_id: &str) -> bool {
    runtime_id.starts_with(MANAGED_ID_PREFIX)
}

pub fn strip_managed_prefix(runtime_id: &str) -> Option<&str> {
    runtime_id.strip_prefix(MANAGED_ID_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_ids_use_frozen_prefix() {
        assert_eq!(managed_recurring_id("nightly-review"), "stasisd:nightly-review");
        assert!(is_managed_recurring_id("stasisd:nightly-review"));
        assert!(!is_managed_recurring_id("manual"));
        assert_eq!(strip_managed_prefix("stasisd:nightly-review"), Some("nightly-review"));
        assert_eq!(MANAGED_BY, "stasisd");
    }
}
