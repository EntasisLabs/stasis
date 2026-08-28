use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::ownership_handoff::{OwnershipHandoff, OwnershipHandoffReservation};
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};

#[derive(Clone, Debug)]
pub struct ReserveOwnershipHandoff {
    pub resource: ResourceKey,
    pub from_owner: OwnerId,
    pub to_owner: OwnerId,
    pub fencing_token: FencingToken,
    pub ttl: Duration,
    pub now: DateTime<Utc>,
    pub handoff_id: String,
}

/// Fenced ownership handoff: reserve → transfer generation → commit|abort.
///
/// Prevents two nodes from executing the same resource generation.
#[async_trait]
pub trait OwnershipHandoffStore: Send + Sync {
    async fn reserve(
        &self,
        request: ReserveOwnershipHandoff,
    ) -> Result<OwnershipHandoffReservation>;

    async fn commit(
        &self,
        handoff_id: &str,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease>;

    async fn abort(
        &self,
        handoff_id: &str,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<OwnershipHandoff>;

    async fn get(&self, handoff_id: &str) -> Result<Option<OwnershipHandoff>>;

    async fn get_active_for_resource(
        &self,
        resource: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<Option<OwnershipHandoff>>;
}
