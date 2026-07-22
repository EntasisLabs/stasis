//! Ownership metadata for `stasisd`-managed runtime objects (ADR-0008).

/// Marker written onto managed recurring definitions.
pub const MANAGED_BY: &str = "stasisd";

/// Prefix applied to managed recurring definition ids: `stasisd:<id>`.
pub const MANAGED_ID_PREFIX: &str = "stasisd:";

/// Build the runtime recurring id for a config schedule id.
pub fn managed_recurring_id(schedule_id: &str) -> String {
    format!("{MANAGED_ID_PREFIX}{schedule_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_ids_use_frozen_prefix() {
        assert_eq!(managed_recurring_id("nightly-review"), "stasisd:nightly-review");
        assert_eq!(MANAGED_BY, "stasisd");
    }
}
