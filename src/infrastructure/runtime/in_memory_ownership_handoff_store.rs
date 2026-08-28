use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::ownership_handoff::{
    OwnershipHandoff, OwnershipHandoffPhase, OwnershipHandoffReservation,
};
use crate::domain::runtime::resource_lease::{FencingToken, ResourceKey, ResourceLease};
use crate::ports::outbound::runtime::ownership_handoff_store::{
    OwnershipHandoffStore, ReserveOwnershipHandoff,
};
use crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;

fn lock_err() -> StasisError {
    StasisError::PortFailure("ownership handoff store lock poisoned".into())
}

/// Fenced handoff store backed by an in-memory [`ResourceLeaseStore`].
#[derive(Clone)]
pub struct InMemoryOwnershipHandoffStore {
    leases: Arc<dyn ResourceLeaseStore>,
    state: Arc<RwLock<OwnershipHandoffState>>,
}

#[derive(Default)]
struct OwnershipHandoffState {
    handoffs: HashMap<String, OwnershipHandoff>,
    by_resource: HashMap<String, String>,
}

impl InMemoryOwnershipHandoffStore {
    pub fn new(leases: Arc<dyn ResourceLeaseStore>) -> Self {
        Self {
            leases,
            state: Arc::new(RwLock::new(OwnershipHandoffState::default())),
        }
    }
}

#[async_trait]
impl OwnershipHandoffStore for InMemoryOwnershipHandoffStore {
    async fn reserve(
        &self,
        request: ReserveOwnershipHandoff,
    ) -> Result<OwnershipHandoffReservation> {
        let ReserveOwnershipHandoff {
            resource,
            from_owner,
            to_owner,
            fencing_token,
            ttl,
            now,
            handoff_id,
        } = request;
        if !self
            .leases
            .validate_fence(&resource, fencing_token, now)
            .await?
        {
            return Err(StasisError::PortFailure(
                "ownership handoff reserve rejected: stale fence".into(),
            ));
        }

        let lease = self
            .leases
            .get(&resource)
            .await?
            .ok_or_else(|| StasisError::PortFailure("resource lease not found".into()))?;
        if lease.owner != from_owner || lease.fencing_token != fencing_token {
            return Err(StasisError::PortFailure(
                "ownership handoff reserve rejected: owner/fence mismatch".into(),
            ));
        }

        let handoff = OwnershipHandoff {
            handoff_id: handoff_id.clone(),
            resource: resource.clone(),
            from_owner,
            to_owner,
            generation: lease.generation,
            fencing_token,
            phase: OwnershipHandoffPhase::Reserved,
            reserved_at: now,
            updated_at: now,
            expires_at: now + ttl,
        };

        let mut state = self.state.write().map_err(|_| lock_err())?;
        if let Some(existing_id) = state.by_resource.get(&resource.0)
            && let Some(existing) = state.handoffs.get(existing_id)
            && existing.is_active(now)
        {
            return Err(StasisError::PortFailure(format!(
                "ownership handoff already reserved for generation {}: {}",
                existing.generation, existing.handoff_id
            )));
        }
        state.handoffs.insert(handoff_id, handoff.clone());
        state
            .by_resource
            .insert(resource.0, handoff.handoff_id.clone());

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
            let state = self.state.read().map_err(|_| lock_err())?;
            state
                .handoffs
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
            let mut state = self.state.write().map_err(|_| lock_err())?;
            if let Some(entry) = state.handoffs.get_mut(handoff_id) {
                entry.phase = OwnershipHandoffPhase::Committed;
                entry.updated_at = now;
            }
            state.by_resource.remove(&handoff.resource.0);
        }

        Ok(transferred)
    }

    async fn abort(
        &self,
        handoff_id: &str,
        fencing_token: FencingToken,
        now: DateTime<Utc>,
    ) -> Result<OwnershipHandoff> {
        let mut state = self.state.write().map_err(|_| lock_err())?;
        let Some(handoff) = state.handoffs.get_mut(handoff_id) else {
            return Err(StasisError::PortFailure(
                "ownership handoff not found".into(),
            ));
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
        let resource = handoff.resource.0.clone();
        let handoff = handoff.clone();
        state.by_resource.remove(&resource);
        Ok(handoff)
    }

    async fn get(&self, handoff_id: &str) -> Result<Option<OwnershipHandoff>> {
        let state = self.state.read().map_err(|_| lock_err())?;
        Ok(state.handoffs.get(handoff_id).cloned())
    }

    async fn get_active_for_resource(
        &self,
        resource: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<Option<OwnershipHandoff>> {
        let state = self.state.read().map_err(|_| lock_err())?;
        let Some(handoff_id) = state.by_resource.get(&resource.0) else {
            return Ok(None);
        };
        Ok(state
            .handoffs
            .get(handoff_id)
            .filter(|h| h.is_active(now))
            .cloned())
    }
}
