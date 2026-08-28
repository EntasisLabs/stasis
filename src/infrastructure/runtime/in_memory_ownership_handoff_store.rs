use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::ownership_handoff::{
    OwnershipHandoff, OwnershipHandoffPhase, OwnershipHandoffReservation,
};
use crate::domain::runtime::resource_lease::{FencingToken, OwnerId, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::ownership_handoff_store::OwnershipHandoffStore;
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;

fn lock_err() -> StasisError {
    StasisError::PortFailure("ownership handoff store lock poisoned".into())
}

/// Fenced handoff store backed by an in-memory [`ResourceLeaseStore`].
#[derive(Clone)]
pub struct InMemoryOwnershipHandoffStore {
    leases: Arc<dyn ResourceLeaseStore>,
    handoffs: Arc<RwLock<HashMap<String, OwnershipHandoff>>>,
    by_resource: Arc<RwLock<HashMap<String, String>>>,
}

impl InMemoryOwnershipHandoffStore {
    pub fn new(leases: Arc<dyn ResourceLeaseStore>) -> Self {
        Self {
            leases,
            handoffs: Arc::new(RwLock::new(HashMap::new())),
            by_resource: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl OwnershipHandoffStore for InMemoryOwnershipHandoffStore {
    async fn reserve(
        &self,
        resource: &ResourceKey,
        from: &OwnerId,
        to: OwnerId,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
        handoff_id: String,
    ) -> Result<OwnershipHandoffReservation> {
        if !self
            .leases
            .validate_fence(resource, fencing_token, now)
            .await?
        {
            return Err(StasisError::PortFailure(
                "ownership handoff reserve rejected: stale fence".into(),
            ));
        }

        let lease = self
            .leases
            .get(resource)
            .await?
            .ok_or_else(|| StasisError::PortFailure("resource lease not found".into()))?;
        if lease.owner != *from || lease.fencing_token != fencing_token {
            return Err(StasisError::PortFailure(
                "ownership handoff reserve rejected: owner/fence mismatch".into(),
            ));
        }

        {
            let by_resource = self.by_resource.read().map_err(|_| lock_err())?;
            if let Some(existing_id) = by_resource.get(&resource.0) {
                let handoffs = self.handoffs.read().map_err(|_| lock_err())?;
                if let Some(existing) = handoffs.get(existing_id) {
                    if existing.is_active(now) {
                        return Err(StasisError::PortFailure(format!(
                            "ownership handoff already reserved for generation {}: {}",
                            existing.generation, existing.handoff_id
                        )));
                    }
                }
            }
        }

        let handoff = OwnershipHandoff {
            handoff_id: handoff_id.clone(),
            resource: resource.clone(),
            from_owner: from.clone(),
            to_owner: to,
            generation: lease.generation,
            fencing_token,
            phase: OwnershipHandoffPhase::Reserved,
            reserved_at: now,
            updated_at: now,
            expires_at: now + ttl,
        };

        let mut handoffs = self.handoffs.write().map_err(|_| lock_err())?;
        let mut by_resource = self.by_resource.write().map_err(|_| lock_err())?;
        handoffs.insert(handoff_id, handoff.clone());
        by_resource.insert(resource.0.clone(), handoff.handoff_id.clone());

        Ok(OwnershipHandoffReservation { handoff })
    }

    async fn commit(
        &self,
        handoff_id: &str,
        fencing_token: FencingToken,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<ResourceLease> {
        let handoff = {
            let handoffs = self.handoffs.read().map_err(|_| lock_err())?;
            handoffs
                .get(handoff_id)
                .cloned()
                .ok_or_else(|| StasisError::PortFailure("ownership handoff not found".into()))?
        };

        if handoff.fencing_token != fencing_token {
            return Err(StasisError::PortFailure(
                "ownership handoff commit rejected: stale fence".into(),
            ));
        }
        if !handoff.is_active(now) {
            return Err(StasisError::PortFailure(
                "ownership handoff commit rejected: not active".into(),
            ));
        }

        // Re-validate that no other node advanced the generation during the reserve window.
        let current = self
            .leases
            .get(&handoff.resource)
            .await?
            .ok_or_else(|| StasisError::PortFailure("resource lease not found".into()))?;
        if current.generation != handoff.generation || current.fencing_token != fencing_token {
            return Err(StasisError::PortFailure(
                "ownership handoff commit rejected: generation conflict".into(),
            ));
        }

        let transferred = self
            .leases
            .transfer(
                &handoff.resource,
                &handoff.from_owner,
                handoff.to_owner.clone(),
                fencing_token,
                ttl,
                now,
            )
            .await?;

        {
            let mut handoffs = self.handoffs.write().map_err(|_| lock_err())?;
            let mut by_resource = self.by_resource.write().map_err(|_| lock_err())?;
            if let Some(entry) = handoffs.get_mut(handoff_id) {
                entry.phase = OwnershipHandoffPhase::Committed;
                entry.updated_at = now;
            }
            by_resource.remove(&handoff.resource.0);
        }

        Ok(transferred)
    }

    async fn abort(
        &self,
        handoff_id: &str,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<OwnershipHandoff> {
        let mut handoffs = self.handoffs.write().map_err(|_| lock_err())?;
        let mut by_resource = self.by_resource.write().map_err(|_| lock_err())?;
        let Some(handoff) = handoffs.get_mut(handoff_id) else {
            return Err(StasisError::PortFailure("ownership handoff not found".into()));
        };
        if handoff.fencing_token != fencing_token {
            return Err(StasisError::PortFailure(
                "ownership handoff abort rejected: stale fence".into(),
            ));
        }
        if handoff.phase != OwnershipHandoffPhase::Reserved {
            return Err(StasisError::PortFailure(
                "ownership handoff abort rejected: not reserved".into(),
            ));
        }
        handoff.phase = OwnershipHandoffPhase::Aborted;
        handoff.updated_at = now;
        by_resource.remove(&handoff.resource.0);
        Ok(handoff.clone())
    }

    async fn get(&self, handoff_id: &str) -> Result<Option<OwnershipHandoff>> {
        let handoffs = self.handoffs.read().map_err(|_| lock_err())?;
        Ok(handoffs.get(handoff_id).cloned())
    }

    async fn get_active_for_resource(
        &self,
        resource: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<Option<OwnershipHandoff>> {
        let by_resource = self.by_resource.read().map_err(|_| lock_err())?;
        let Some(handoff_id) = by_resource.get(&resource.0) else {
            return Ok(None);
        };
        let handoffs = self.handoffs.read().map_err(|_| lock_err())?;
        Ok(handoffs
            .get(handoff_id)
            .filter(|h| h.is_active(now))
            .cloned())
    }
}
