use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::store_bounds::StoreBound;

crate::async_store! {
pub trait ResourceLeaseStore: StoreBound {
    async fn get(&self, resource: &ResourceKey) -> Result<Option<ResourceLease>>;
    async fn acquire(
        &self,
        resource: ResourceKey,
        owner: OwnerId,
        ttl: Duration,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease>;
    async fn renew(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease>;
    async fn release(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool>;
    async fn transfer(
        &self,
        resource: &ResourceKey,
        from: &OwnerId,
        to: OwnerId,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease>;
    async fn validate_fence(
        &self,
        resource: &ResourceKey,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool>;
}
}
