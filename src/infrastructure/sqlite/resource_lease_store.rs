use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;

use super::codec::{decode_time, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteResourceLeaseStore {
    session: SqliteSession,
}

impl SqliteResourceLeaseStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    fn load(tx: &rusqlite::Transaction<'_>, resource: &str) -> Result<Option<ResourceLease>> {
        let row = match tx.query_row(
            "SELECT resource, owner, generation, fencing_token, expires_at FROM resource_lease WHERE resource = ?1",
            [resource],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        ) {
            Ok(row) => Some(row),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(err) => return Err(sql_err("get resource lease", err)),
        };
        let Some((resource, owner, generation, fencing_token, expires_at)) = row else {
            return Ok(None);
        };
        Ok(Some(ResourceLease {
            resource: ResourceKey(resource),
            owner: OwnerId(owner),
            generation: u64::try_from(generation).unwrap_or(0),
            fencing_token: FencingToken(u64::try_from(fencing_token).unwrap_or(0)),
            expires_at: decode_time(expires_at)?,
        }))
    }

    fn write(tx: &rusqlite::Transaction<'_>, lease: &ResourceLease) -> Result<()> {
        tx.execute(
            "INSERT INTO resource_lease (resource, owner, generation, fencing_token, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(resource) DO UPDATE SET
                owner = excluded.owner,
                generation = excluded.generation,
                fencing_token = excluded.fencing_token,
                expires_at = excluded.expires_at",
            rusqlite::params![
                lease.resource.0,
                lease.owner.0,
                lease.generation as i64,
                lease.fencing_token.0 as i64,
                encode_time(lease.expires_at),
            ],
        )
        .map_err(|err| sql_err("save resource lease", err))?;
        Ok(())
    }
}

crate::async_store! {
impl ResourceLeaseStore for SqliteResourceLeaseStore {
    async fn get(&self, resource: &ResourceKey) -> Result<Option<ResourceLease>> {
        self.session
            .with_tx(|tx| Self::load(tx, &resource.0))
    }

    async fn acquire(
        &self,
        resource: ResourceKey,
        owner: OwnerId,
        ttl: Duration,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease> {
        self.session.with_tx(|tx| {
            let existing = Self::load(tx, &resource.0)?;
            let lease = if let Some(existing) = existing {
                if !existing.is_expired(now) && !force {
                    return Err(StasisError::PortFailure(format!(
                        "resource already leased: {}",
                        resource.0
                    )));
                }
                let generation = existing.generation.saturating_add(1);
                ResourceLease {
                    resource,
                    owner,
                    generation,
                    fencing_token: FencingToken(generation),
                    expires_at: now + ttl,
                }
            } else {
                ResourceLease {
                    resource,
                    owner,
                    generation: 1,
                    fencing_token: FencingToken(1),
                    expires_at: now + ttl,
                }
            };
            Self::write(tx, &lease)?;
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
        self.session.with_tx(|tx| {
            let Some(mut lease) = Self::load(tx, &resource.0)? else {
                return Err(StasisError::PortFailure(format!(
                    "resource lease not found: {}",
                    resource.0
                )));
            };
            if lease.is_expired(now) || lease.owner != *owner || lease.fencing_token != fencing_token
            {
                return Err(StasisError::PortFailure(
                    "resource lease renew rejected: stale fence or owner".into(),
                ));
            }
            lease.expires_at = now + ttl;
            Self::write(tx, &lease)?;
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
        self.session.with_tx(|tx| {
            let Some(lease) = Self::load(tx, &resource.0)? else {
                return Ok(false);
            };
            if lease.is_expired(now) || lease.owner != *owner || lease.fencing_token != fencing_token
            {
                return Ok(false);
            }
            tx.execute(
                "DELETE FROM resource_lease WHERE resource = ?1",
                [&resource.0],
            )
            .map_err(|err| sql_err("delete resource lease", err))?;
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
        self.session.with_tx(|tx| {
            let Some(existing) = Self::load(tx, &resource.0)? else {
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
            Self::write(tx, &lease)?;
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
            .get(resource)
            .await?
            .is_some_and(|lease| !lease.is_expired(now) && lease.fencing_token == fencing_token))
    }
}
}
