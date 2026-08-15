use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;

#[derive(Clone, Default)]
pub struct InMemoryResourceLeaseStore {
    leases: Arc<RwLock<HashMap<String, ResourceLease>>>,
}

fn lock_err() -> StasisError {
    StasisError::PortFailure("resource lease store lock poisoned".into())
}

fn key(resource: &ResourceKey) -> &str {
    resource.0.as_str()
}

fn live<'a>(lease: &'a ResourceLease, now: DateTime<Utc>) -> Option<&'a ResourceLease> {
    if lease.is_expired(now) {
        None
    } else {
        Some(lease)
    }
}

#[async_trait]
impl ResourceLeaseStore for InMemoryResourceLeaseStore {
    async fn get(&self, resource: &ResourceKey) -> Result<Option<ResourceLease>> {
        let leases = self.leases.read().map_err(|_| lock_err())?;
        Ok(leases.get(key(resource)).cloned())
    }

    async fn acquire(
        &self,
        resource: ResourceKey,
        owner: OwnerId,
        ttl: Duration,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease> {
        let mut leases = self.leases.write().map_err(|_| lock_err())?;
        let existing = leases.get(key(&resource)).cloned();
        if let Some(existing) = existing {
            if live(&existing, now).is_some() && !force {
                return Err(StasisError::PortFailure(format!(
                    "resource already leased: {}",
                    resource.0
                )));
            }
            let generation = existing.generation.saturating_add(1);
            let lease = ResourceLease {
                resource,
                owner,
                generation,
                fencing_token: FencingToken(generation),
                expires_at: now + ttl,
            };
            leases.insert(lease.resource.0.clone(), lease.clone());
            return Ok(lease);
        }

        let lease = ResourceLease {
            resource,
            owner,
            generation: 1,
            fencing_token: FencingToken(1),
            expires_at: now + ttl,
        };
        leases.insert(lease.resource.0.clone(), lease.clone());
        Ok(lease)
    }

    async fn renew(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease> {
        let mut leases = self.leases.write().map_err(|_| lock_err())?;
        let Some(lease) = leases.get_mut(key(resource)) else {
            return Err(StasisError::PortFailure(format!(
                "resource lease not found: {}",
                resource.0
            )));
        };
        if lease.is_expired(now) || lease.owner != *owner || lease.fencing_token != fencing_token {
            return Err(StasisError::PortFailure(
                "resource lease renew rejected: stale fence or owner".into(),
            ));
        }
        lease.expires_at = now + ttl;
        Ok(lease.clone())
    }

    async fn release(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let mut leases = self.leases.write().map_err(|_| lock_err())?;
        let Some(lease) = leases.get(key(resource)) else {
            return Ok(false);
        };
        if lease.is_expired(now) || lease.owner != *owner || lease.fencing_token != fencing_token {
            return Ok(false);
        }
        leases.remove(key(resource));
        Ok(true)
    }

    async fn transfer(
        &self,
        resource: &ResourceKey,
        from: &OwnerId,
        to: OwnerId,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease> {
        let mut leases = self.leases.write().map_err(|_| lock_err())?;
        let Some(existing) = leases.get(key(resource)).cloned() else {
            return Err(StasisError::PortFailure(format!(
                "resource lease not found: {}",
                resource.0
            )));
        };
        if existing.is_expired(now)
            || existing.owner != *from
            || existing.fencing_token != fencing_token
        {
            return Err(StasisError::PortFailure(
                "resource lease transfer rejected: stale fence or owner".into(),
            ));
        }
        let generation = existing.generation.saturating_add(1);
        let lease = ResourceLease {
            resource: resource.clone(),
            owner: to,
            generation,
            fencing_token: FencingToken(generation),
            expires_at: now + ttl,
        };
        leases.insert(key(resource).to_string(), lease.clone());
        Ok(lease)
    }

    async fn validate_fence(
        &self,
        resource: &ResourceKey,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let leases = self.leases.read().map_err(|_| lock_err())?;
        Ok(leases
            .get(key(resource))
            .map(|lease| !lease.is_expired(now) && lease.fencing_token == fencing_token)
            .unwrap_or(false))
    }
}
