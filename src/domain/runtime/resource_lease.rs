use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ResourceKey(pub String);

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct OwnerId(pub String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FencingToken(pub u64);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceLease {
    pub resource: ResourceKey,
    pub owner: OwnerId,
    pub generation: u64,
    pub fencing_token: FencingToken,
    pub expires_at: DateTime<Utc>,
}

impl ResourceLease {
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now
    }
}
