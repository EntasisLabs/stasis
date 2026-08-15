use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::any::Any};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;

#[derive(Clone)]
pub struct SurrealResourceLeaseStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealResourceLeaseStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "resource_lease".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    fn missing_table(err: &impl std::fmt::Display, table: &str) -> bool {
        let message = err.to_string();
        message.contains("does not exist") && message.contains(table)
    }

    async fn save(&self, lease: &ResourceLease) -> Result<()> {
        let row = LeaseRow::from(lease.clone());
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", row.resource.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("save resource lease", e))?;
        Ok(())
    }

    async fn delete(&self, resource: &ResourceKey) -> Result<()> {
        self.db
            .query("DELETE type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", resource.0.clone()))
            .await
            .map_err(|e| Self::port_err("delete resource lease", e))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct LeaseRow {
    resource: String,
    owner: String,
    generation: u64,
    fencing_token: u64,
    expires_at: DateTime<Utc>,
}

impl From<ResourceLease> for LeaseRow {
    fn from(value: ResourceLease) -> Self {
        Self {
            resource: value.resource.0,
            owner: value.owner.0,
            generation: value.generation,
            fencing_token: value.fencing_token.0,
            expires_at: value.expires_at,
        }
    }
}

impl From<LeaseRow> for ResourceLease {
    fn from(value: LeaseRow) -> Self {
        Self {
            resource: ResourceKey(value.resource),
            owner: OwnerId(value.owner),
            generation: value.generation,
            fencing_token: FencingToken(value.fencing_token),
            expires_at: value.expires_at,
        }
    }
}

#[async_trait]
impl ResourceLeaseStore for SurrealResourceLeaseStore {
    async fn get(&self, resource: &ResourceKey) -> Result<Option<ResourceLease>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", resource.0.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err, &self.table) => return Ok(None),
            Err(err) => return Err(Self::port_err("get resource lease", err)),
        };
        let rows: Vec<LeaseRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err, &self.table) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode resource lease", err)),
        };
        Ok(rows.into_iter().next().map(ResourceLease::from))
    }

    async fn acquire(
        &self,
        resource: ResourceKey,
        owner: OwnerId,
        ttl: Duration,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<ResourceLease> {
        let existing = self.get(&resource).await?;
        if let Some(existing) = existing {
            if !existing.is_expired(now) && !force {
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
            self.save(&lease).await?;
            return Ok(lease);
        }
        let lease = ResourceLease {
            resource,
            owner,
            generation: 1,
            fencing_token: FencingToken(1),
            expires_at: now + ttl,
        };
        self.save(&lease).await?;
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
        let Some(mut lease) = self.get(resource).await? else {
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
        self.save(&lease).await?;
        Ok(lease)
    }

    async fn release(
        &self,
        resource: &ResourceKey,
        owner: &OwnerId,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(lease) = self.get(resource).await? else {
            return Ok(false);
        };
        if lease.is_expired(now) || lease.owner != *owner || lease.fencing_token != fencing_token {
            return Ok(false);
        }
        self.delete(resource).await?;
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
        let Some(existing) = self.get(resource).await? else {
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
        self.save(&lease).await?;
        Ok(lease)
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
            .map(|lease| !lease.is_expired(now) && lease.fencing_token == fencing_token)
            .unwrap_or(false))
    }
}
