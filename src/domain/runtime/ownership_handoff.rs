use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey};

/// Two-phase fenced ownership handoff so only one node executes a given generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipHandoffPhase {
    Reserved,
    Committed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OwnershipHandoff {
    pub handoff_id: String,
    pub resource: ResourceKey,
    pub from_owner: OwnerId,
    pub to_owner: OwnerId,
    /// Generation that must remain exclusive for the duration of the handoff.
    pub generation: u64,
    pub fencing_token: FencingToken,
    pub phase: OwnershipHandoffPhase,
    pub reserved_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl OwnershipHandoff {
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.phase == OwnershipHandoffPhase::Reserved && !self.is_expired(now)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OwnershipHandoffReservation {
    pub handoff: OwnershipHandoff,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn reserved_handoff_is_active_until_expiry() {
        let now = Utc::now();
        let handoff = OwnershipHandoff {
            handoff_id: "ho-1".into(),
            resource: ResourceKey("res-1".into()),
            from_owner: OwnerId("a".into()),
            to_owner: OwnerId("b".into()),
            generation: 3,
            fencing_token: FencingToken(3),
            phase: OwnershipHandoffPhase::Reserved,
            reserved_at: now,
            updated_at: now,
            expires_at: now + Duration::seconds(30),
        };
        assert!(handoff.is_active(now));
        assert!(!handoff.is_active(now + Duration::seconds(31)));
    }
}
