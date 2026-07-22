//! Ownership metadata for `stasisd`-managed runtime objects (ADR-0008).

/// Marker written onto managed recurring definitions.
pub const MANAGED_BY: &str = "stasisd";

/// Prefix applied to managed recurring definition ids: `stasisd:<id>`.
pub const MANAGED_ID_PREFIX: &str = "stasisd:";

/// Prefix for managed delivery endpoints: `stasisd:endpoint:<id>`.
pub const MANAGED_ENDPOINT_ID_PREFIX: &str = "stasisd:endpoint:";

/// Build the runtime recurring id for a config schedule id.
pub fn managed_recurring_id(schedule_id: &str) -> String {
    format!("{MANAGED_ID_PREFIX}{schedule_id}")
}

pub fn is_managed_recurring_id(runtime_id: &str) -> bool {
    runtime_id.starts_with(MANAGED_ID_PREFIX) && !runtime_id.starts_with(MANAGED_ENDPOINT_ID_PREFIX)
}

pub fn strip_managed_prefix(runtime_id: &str) -> Option<&str> {
    runtime_id.strip_prefix(MANAGED_ID_PREFIX)
}

pub fn managed_endpoint_id(endpoint_id: &str) -> String {
    format!("{MANAGED_ENDPOINT_ID_PREFIX}{endpoint_id}")
}

pub fn is_managed_endpoint_id(runtime_id: &str) -> bool {
    runtime_id.starts_with(MANAGED_ENDPOINT_ID_PREFIX)
}

pub fn strip_managed_endpoint_prefix(runtime_id: &str) -> Option<&str> {
    runtime_id.strip_prefix(MANAGED_ENDPOINT_ID_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_ids_use_frozen_prefix() {
        assert_eq!(managed_recurring_id("nightly-review"), "stasisd:nightly-review");
        assert!(is_managed_recurring_id("stasisd:nightly-review"));
        assert!(!is_managed_recurring_id("manual"));
        assert!(!is_managed_recurring_id("stasisd:endpoint:fake"));
        assert_eq!(strip_managed_prefix("stasisd:nightly-review"), Some("nightly-review"));
        assert_eq!(managed_endpoint_id("fake"), "stasisd:endpoint:fake");
        assert!(is_managed_endpoint_id("stasisd:endpoint:fake"));
        assert_eq!(MANAGED_BY, "stasisd");
    }
}
