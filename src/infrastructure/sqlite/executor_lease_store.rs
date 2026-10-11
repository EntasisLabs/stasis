use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;

use super::codec::{decode_time, encode_time};
use super::executor::{SqlExecutor, SqlValue, query_one, transaction_result};

#[derive(Clone)]
pub struct ExecutorResourceLeaseStore {
    executor: Arc<dyn SqlExecutor>,
}

impl ExecutorResourceLeaseStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }

    fn load(&self, resource: &str) -> Result<Option<ResourceLease>> {
        let row = query_one(
            self.executor.as_ref(),
            "SELECT resource, owner, CAST(generation AS TEXT), CAST(fencing_token AS TEXT), \
             CAST(expires_at AS TEXT)
             FROM resource_lease WHERE resource = ?1",
            &[SqlValue::text(resource)],
        )?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(ResourceLease {
            resource: ResourceKey(row.text(0)?),
            owner: OwnerId(row.text(1)?),
            generation: u64::try_from(row.i64(2)?).unwrap_or(0),
            fencing_token: FencingToken(u64::try_from(row.i64(3)?).unwrap_or(0)),
            expires_at: decode_time(row.i64(4)?)?,
        }))
    }

    fn write(&self, lease: &ResourceLease) -> Result<()> {
        self.executor.execute(
            "INSERT INTO resource_lease (resource, owner, generation, fencing_token, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(resource) DO UPDATE SET
                owner = excluded.owner,
                generation = excluded.generation,
                fencing_token = excluded.fencing_token,
                expires_at = excluded.expires_at",
            &[
                SqlValue::from(lease.resource.0.clone()),
                SqlValue::from(lease.owner.0.clone()),
                SqlValue::from(lease.generation as i64),
                SqlValue::from(lease.fencing_token.0 as i64),
                SqlValue::from(encode_time(lease.expires_at)),
            ],
        )?;
        Ok(())
    }
}

crate::async_store! {
impl ResourceLeaseStore for ExecutorResourceLeaseStore {
    async fn get(&self, resource: &ResourceKey) -> Result<Option<ResourceLease>> {
        self.load(&resource.0)
    }

    async fn acquire(
        &self,
        resource: ResourceKey,
        owner: OwnerId,
        ttl: Duration,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease> {
        transaction_result(self.executor.as_ref(), &mut || {
            let existing = self.load(&resource.0)?;
            let lease = if let Some(existing) = existing {
                if !existing.is_expired(now) && !force {
                    return Err(StasisError::PortFailure(format!(
                        "resource already leased: {}",
                        resource.0
                    )));
                }
                let generation = existing.generation.saturating_add(1);
                ResourceLease {
                    resource: resource.clone(),
                    owner: owner.clone(),
                    generation,
                    fencing_token: FencingToken(generation),
                    expires_at: now + ttl,
                }
            } else {
                ResourceLease {
                    resource: resource.clone(),
                    owner: owner.clone(),
                    generation: 1,
                    fencing_token: FencingToken(1),
                    expires_at: now + ttl,
                }
            };
            self.write(&lease)?;
            Ok(lease)
        })
    }

    async fn renew(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease> {
        transaction_result(self.executor.as_ref(), &mut || {
            let Some(mut lease) = self.load(&resource.0)? else {
                return Err(StasisError::PortFailure(format!(
                    "resource lease not found: {}",
                    resource.0
                )));
            };
            if lease.is_expired(now)
                || lease.owner != *owner
                || lease.fencing_token != fencing_token
            {
                return Err(StasisError::PortFailure(
                    "resource lease renew rejected: stale fence or owner".into(),
                ));
            }
            lease.expires_at = now + ttl;
            self.write(&lease)?;
            Ok(lease)
        })
    }

    async fn release(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        transaction_result(self.executor.as_ref(), &mut || {
            let Some(lease) = self.load(&resource.0)? else {
                return Ok(false);
            };
            if lease.is_expired(now)
                || lease.owner != *owner
                || lease.fencing_token != fencing_token
            {
                return Ok(false);
            }
            self.executor.execute(
                "DELETE FROM resource_lease WHERE resource = ?1",
                &[SqlValue::text(&resource.0)],
            )?;
            Ok(true)
        })
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
        transaction_result(self.executor.as_ref(), &mut || {
            let Some(existing) = self.load(&resource.0)? else {
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
                owner: to.clone(),
                generation,
                fencing_token: FencingToken(generation),
                expires_at: now + ttl,
            };
            self.write(&lease)?;
            Ok(lease)
        })
    }

    async fn validate_fence(
        &self,
        resource: &ResourceKey,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self
            .load(&resource.0)?
            .is_some_and(|lease| !lease.is_expired(now) && lease.fencing_token == fencing_token))
    }
}
}
