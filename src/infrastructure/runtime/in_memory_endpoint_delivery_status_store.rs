use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;

#[derive(Clone, Default)]
pub struct InMemoryEndpointDeliveryStatusStore {
    statuses: Arc<RwLock<HashMap<String, EndpointDeliveryStatus>>>,
}

impl InMemoryEndpointDeliveryStatusStore {
    fn upsert(&self, endpoint_id: &str, at: DateTime<Utc>) -> Result<EndpointDeliveryStatus> {
        let mut statuses = self.statuses.write().map_err(|_| {
            StasisError::PortFailure("endpoint delivery status store lock poisoned".to_string())
        })?;

        let entry = statuses
            .entry(endpoint_id.to_string())
            .or_insert_with(|| EndpointDeliveryStatus::new(endpoint_id, at));
        Ok(entry.clone())
    }
}

#[async_trait]
impl EndpointDeliveryStatusStore for InMemoryEndpointDeliveryStatusStore {
    async fn record_success(
        &self,
        endpoint_id: &str,
        event_id: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut status = self.upsert(endpoint_id, at)?;
        status.success_count = status.success_count.saturating_add(1);
        status.last_event_id = Some(event_id.to_string());
        status.last_error = None;
        status.last_success_at = Some(at);
        status.updated_at = at;

        let mut statuses = self.statuses.write().map_err(|_| {
            StasisError::PortFailure("endpoint delivery status store lock poisoned".to_string())
        })?;
        statuses.insert(endpoint_id.to_string(), status);
        Ok(())
    }

    async fn record_failure(
        &self,
        endpoint_id: &str,
        event_id: &str,
        error: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut status = self.upsert(endpoint_id, at)?;
        status.failure_count = status.failure_count.saturating_add(1);
        status.last_event_id = Some(event_id.to_string());
        status.last_error = Some(error.to_string());
        status.last_failure_at = Some(at);
        status.updated_at = at;

        let mut statuses = self.statuses.write().map_err(|_| {
            StasisError::PortFailure("endpoint delivery status store lock poisoned".to_string())
        })?;
        statuses.insert(endpoint_id.to_string(), status);
        Ok(())
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatus>> {
        let statuses = self.statuses.read().map_err(|_| {
            StasisError::PortFailure("endpoint delivery status store lock poisoned".to_string())
        })?;
        Ok(statuses.get(endpoint_id).cloned())
    }

    async fn list(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        let statuses = self.statuses.read().map_err(|_| {
            StasisError::PortFailure("endpoint delivery status store lock poisoned".to_string())
        })?;

        let mut out = statuses.values().cloned().collect::<Vec<_>>();
        out.sort_by(|a, b| a.endpoint_id.cmp(&b.endpoint_id));
        Ok(out)
    }

    async fn prune_updated_before(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let mut statuses = self.statuses.write().map_err(|_| {
            StasisError::PortFailure("endpoint delivery status store lock poisoned".to_string())
        })?;

        let before = statuses.len();
        statuses.retain(|_, status| status.updated_at >= cutoff);
        Ok((before - statuses.len()) as u64)
    }
}
