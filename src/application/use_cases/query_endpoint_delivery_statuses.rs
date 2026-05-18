use std::collections::{HashMap, HashSet};

use chrono::{Duration, Utc};

use crate::application::dto::{
    EndpointDiagnosticsReadModelRow, EndpointFailureRateTrendRow,
    EndpointFailureTrendDirection, ListEndpointDiagnosticsReadModelRequest,
    ListEndpointFailureRateTrendsRequest, ListTopUnhealthyEndpointsRequest,
};
use crate::domain::errors::Result;
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;

#[derive(Clone)]
pub struct GetEndpointDeliveryStatus<S>
where
    S: EndpointDeliveryStatusStore,
{
    store: S,
}

impl<S> GetEndpointDeliveryStatus<S>
where
    S: EndpointDeliveryStatusStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatus>> {
        self.store.get(endpoint_id).await
    }
}

#[derive(Clone)]
pub struct ListEndpointDeliveryStatuses<S>
where
    S: EndpointDeliveryStatusStore,
{
    store: S,
}

impl<S> ListEndpointDeliveryStatuses<S>
where
    S: EndpointDeliveryStatusStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        self.store.list().await
    }
}

#[derive(Clone)]
pub struct PruneEndpointDeliveryStatuses<S>
where
    S: EndpointDeliveryStatusStore,
{
    store: S,
}

impl<S> PruneEndpointDeliveryStatuses<S>
where
    S: EndpointDeliveryStatusStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self, updated_before: chrono::DateTime<chrono::Utc>) -> Result<u64> {
        self.store.prune_updated_before(updated_before).await
    }
}

#[derive(Clone)]
pub struct ListEndpointDiagnosticsReadModel<E, S>
where
    E: DeliveryEndpointStore,
    S: EndpointDeliveryStatusStore,
{
    endpoint_store: E,
    status_store: S,
}

impl<E, S> ListEndpointDiagnosticsReadModel<E, S>
where
    E: DeliveryEndpointStore,
    S: EndpointDeliveryStatusStore,
{
    pub fn new(endpoint_store: E, status_store: S) -> Self {
        Self {
            endpoint_store,
            status_store,
        }
    }

    pub async fn execute(
        &self,
        request: &ListEndpointDiagnosticsReadModelRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        let mut rows = self.build_rows(request).await?;
        let offset = request.offset;
        if offset >= rows.len() {
            return Ok(Vec::new());
        }

        let limit = request.limit.unwrap_or(rows.len().saturating_sub(offset));
        rows = rows.into_iter().skip(offset).take(limit).collect();
        Ok(rows)
    }

    async fn build_rows(
        &self,
        request: &ListEndpointDiagnosticsReadModelRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        let now = Utc::now();
        let endpoints = self.endpoint_store.list().await?;
        let statuses = self.status_store.list().await?;

        let status_by_endpoint = statuses
            .into_iter()
            .map(|status| (status.endpoint_id.clone(), status))
            .collect::<HashMap<_, _>>();

        let requested_ids = request.endpoint_ids.as_ref().map(|ids| {
            ids.iter()
                .map(|id| id.to_string())
                .collect::<HashSet<String>>()
        });

        let stale_cutoff = request
            .stale_after_seconds
            .filter(|seconds| *seconds > 0)
            .map(|seconds| now - Duration::seconds(seconds));

        let mut rows: Vec<EndpointDiagnosticsReadModelRow> = Vec::new();
        for endpoint in endpoints {
            if !request.include_disabled && !endpoint.enabled {
                continue;
            }

            if let Some(ids) = &requested_ids {
                if !ids.contains(&endpoint.endpoint_id) {
                    continue;
                }
            }

            if let Some(protocol) = &request.protocol {
                if &endpoint.protocol != protocol {
                    continue;
                }
            }

            let status = status_by_endpoint.get(&endpoint.endpoint_id);
            let success_count = status.map(|s| s.success_count).unwrap_or(0);
            let failure_count = status.map(|s| s.failure_count).unwrap_or(0);
            let last_success_at = status.and_then(|s| s.last_success_at);
            let last_failure_at = status.and_then(|s| s.last_failure_at);

            if let Some(min_failures) = request.min_failure_count {
                if failure_count < min_failures {
                    continue;
                }
            }

            let unhealthy = is_unhealthy(last_success_at, last_failure_at, stale_cutoff);

            if request.unhealthy_only && !unhealthy {
                continue;
            }

            rows.push(EndpointDiagnosticsReadModelRow {
                endpoint_id: endpoint.endpoint_id,
                endpoint_name: endpoint.name,
                protocol: endpoint.protocol,
                target: endpoint.target,
                enabled: endpoint.enabled,
                success_count,
                failure_count,
                last_event_id: status.and_then(|s| s.last_event_id.clone()),
                last_error: status.and_then(|s| s.last_error.clone()),
                last_success_at,
                last_failure_at,
                updated_at: status.map(|s| s.updated_at).unwrap_or(now),
                unhealthy,
            });
        }

        rows.sort_by(|left, right| {
            right
                .unhealthy
                .cmp(&left.unhealthy)
                .then_with(|| right.failure_count.cmp(&left.failure_count))
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.endpoint_id.cmp(&right.endpoint_id))
        });

        Ok(rows)
    }
}

#[derive(Clone)]
pub struct ListTopUnhealthyEndpoints<E, S>
where
    E: DeliveryEndpointStore,
    S: EndpointDeliveryStatusStore,
{
    read_model: ListEndpointDiagnosticsReadModel<E, S>,
}

impl<E, S> ListTopUnhealthyEndpoints<E, S>
where
    E: DeliveryEndpointStore,
    S: EndpointDeliveryStatusStore,
{
    pub fn new(endpoint_store: E, status_store: S) -> Self {
        Self {
            read_model: ListEndpointDiagnosticsReadModel::new(endpoint_store, status_store),
        }
    }

    pub async fn execute(
        &self,
        request: &ListTopUnhealthyEndpointsRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        let limit = if request.limit == 0 { 10 } else { request.limit };
        self.read_model
            .execute(&ListEndpointDiagnosticsReadModelRequest {
                endpoint_ids: None,
                protocol: request.protocol.clone(),
                min_failure_count: Some(1),
                stale_after_seconds: None,
                unhealthy_only: true,
                include_disabled: request.include_disabled,
                offset: 0,
                limit: Some(limit),
            })
            .await
    }
}

#[derive(Clone)]
pub struct ListEndpointFailureRateTrends<E, S>
where
    E: DeliveryEndpointStore,
    S: EndpointDeliveryStatusStore,
{
    endpoint_store: E,
    status_store: S,
}

impl<E, S> ListEndpointFailureRateTrends<E, S>
where
    E: DeliveryEndpointStore,
    S: EndpointDeliveryStatusStore,
{
    pub fn new(endpoint_store: E, status_store: S) -> Self {
        Self {
            endpoint_store,
            status_store,
        }
    }

    pub async fn execute(
        &self,
        request: &ListEndpointFailureRateTrendsRequest,
    ) -> Result<Vec<EndpointFailureRateTrendRow>> {
        let endpoints = self.endpoint_store.list().await?;
        let statuses = self.status_store.list().await?;
        let status_by_endpoint = statuses
            .into_iter()
            .map(|status| (status.endpoint_id.clone(), status))
            .collect::<HashMap<_, _>>();

        let mut rows = Vec::new();
        for endpoint in endpoints {
            if !request.include_disabled && !endpoint.enabled {
                continue;
            }

            if let Some(protocol) = &request.protocol {
                if &endpoint.protocol != protocol {
                    continue;
                }
            }

            let Some(status) = status_by_endpoint.get(&endpoint.endpoint_id) else {
                continue;
            };

            let total_attempts = status.success_count.saturating_add(status.failure_count);
            if let Some(min_total_attempts) = request.min_total_attempts {
                if total_attempts < min_total_attempts {
                    continue;
                }
            }

            let failure_rate = if total_attempts == 0 {
                0.0
            } else {
                status.failure_count as f64 / total_attempts as f64
            };

            rows.push(EndpointFailureRateTrendRow {
                endpoint_id: endpoint.endpoint_id,
                endpoint_name: endpoint.name,
                protocol: endpoint.protocol,
                enabled: endpoint.enabled,
                success_count: status.success_count,
                failure_count: status.failure_count,
                total_attempts,
                failure_rate,
                trend: classify_trend(status.last_success_at, status.last_failure_at),
                last_success_at: status.last_success_at,
                last_failure_at: status.last_failure_at,
                updated_at: status.updated_at,
            });
        }

        rows.sort_by(|left, right| {
            right
                .failure_rate
                .partial_cmp(&left.failure_rate)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.failure_count.cmp(&left.failure_count))
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.endpoint_id.cmp(&right.endpoint_id))
        });

        let limit = if request.limit == 0 {
            20
        } else {
            request.limit
        };
        rows.truncate(limit);
        Ok(rows)
    }
}

fn is_unhealthy(
    last_success_at: Option<chrono::DateTime<chrono::Utc>>,
    last_failure_at: Option<chrono::DateTime<chrono::Utc>>,
    stale_cutoff: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    let stale = stale_cutoff
        .map(|cutoff| match last_success_at {
            Some(last_success) => last_success < cutoff,
            None => true,
        })
        .unwrap_or(false);
    let failed_after_success = match (last_failure_at, last_success_at) {
        (Some(last_failure), Some(last_success)) => last_failure >= last_success,
        (Some(_), None) => true,
        _ => false,
    };
    failed_after_success || stale
}

fn classify_trend(
    last_success_at: Option<chrono::DateTime<chrono::Utc>>,
    last_failure_at: Option<chrono::DateTime<chrono::Utc>>,
) -> EndpointFailureTrendDirection {
    match (last_success_at, last_failure_at) {
        (Some(last_success), Some(last_failure)) if last_success > last_failure => {
            EndpointFailureTrendDirection::Improving
        }
        (Some(_), Some(_)) => EndpointFailureTrendDirection::Worsening,
        (None, Some(_)) => EndpointFailureTrendDirection::Worsening,
        _ => EndpointFailureTrendDirection::Stable,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use crate::application::dto::{
        EndpointFailureTrendDirection, ListEndpointDiagnosticsReadModelRequest,
        ListEndpointFailureRateTrendsRequest, ListTopUnhealthyEndpointsRequest,
    };
    use crate::domain::runtime::delivery_endpoint::{DeliveryProtocol, NewDeliveryEndpoint};
    use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
    use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
    use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;

    use super::{
        ListEndpointDiagnosticsReadModel, ListEndpointFailureRateTrends,
        ListTopUnhealthyEndpoints, PruneEndpointDeliveryStatuses,
    };

    #[tokio::test]
    async fn read_model_returns_unhealthy_first_with_filters() {
        let endpoint_store = InMemoryDeliveryEndpointStore::default();
        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let now = Utc::now();

        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.http.unhealthy".to_string(),
                name: "Unhealthy HTTP".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/unhealthy".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");
        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.http.healthy".to_string(),
                name: "Healthy HTTP".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/healthy".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        status_store
            .record_failure(
                "endpoint.http.unhealthy",
                "evt-1",
                "boom",
                now - Duration::minutes(1),
            )
            .await
            .expect("failure record should succeed");
        status_store
            .record_success(
                "endpoint.http.healthy",
                "evt-2",
                now - Duration::seconds(10),
            )
            .await
            .expect("success record should succeed");

        let use_case = ListEndpointDiagnosticsReadModel::new(endpoint_store, status_store);
        let rows = use_case
            .execute(&ListEndpointDiagnosticsReadModelRequest {
                protocol: Some(DeliveryProtocol::HttpWebhook),
                unhealthy_only: false,
                include_disabled: true,
                offset: 0,
                limit: None,
                ..Default::default()
            })
            .await
            .expect("read model should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].endpoint_id, "endpoint.http.unhealthy");
        assert!(rows[0].unhealthy);
    }

    #[tokio::test]
    async fn read_model_applies_offset_and_limit() {
        let endpoint_store = InMemoryDeliveryEndpointStore::default();
        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let now = Utc::now();

        for id in ["a", "b", "c"] {
            endpoint_store
                .insert(NewDeliveryEndpoint {
                    endpoint_id: format!("endpoint.{id}"),
                    name: format!("Endpoint {id}"),
                    protocol: DeliveryProtocol::HttpWebhook,
                    target: "https://example.com/hook".to_string(),
                    metadata: None,
                    created_at: now,
                })
                .await
                .expect("insert should succeed");
            status_store
                .record_failure(&format!("endpoint.{id}"), "evt-1", "boom", now)
                .await
                .expect("record should succeed");
        }

        let use_case = ListEndpointDiagnosticsReadModel::new(endpoint_store, status_store);
        let rows = use_case
            .execute(&ListEndpointDiagnosticsReadModelRequest {
                include_disabled: true,
                offset: 1,
                limit: Some(1),
                ..Default::default()
            })
            .await
            .expect("read model should succeed");

        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn top_unhealthy_returns_only_unhealthy_endpoints() {
        let endpoint_store = InMemoryDeliveryEndpointStore::default();
        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let now = Utc::now();

        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.unhealthy".to_string(),
                name: "Unhealthy".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/unhealthy".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");
        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.healthy".to_string(),
                name: "Healthy".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/healthy".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        status_store
            .record_failure("endpoint.unhealthy", "evt-f", "boom", now)
            .await
            .expect("failure should succeed");
        status_store
            .record_success("endpoint.healthy", "evt-s", now)
            .await
            .expect("success should succeed");

        let use_case = ListTopUnhealthyEndpoints::new(endpoint_store, status_store);
        let rows = use_case
            .execute(&ListTopUnhealthyEndpointsRequest {
                protocol: None,
                include_disabled: true,
                limit: 10,
            })
            .await
            .expect("top unhealthy should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].endpoint_id, "endpoint.unhealthy");
    }

    #[tokio::test]
    async fn failure_rate_trends_are_ranked_and_classified() {
        let endpoint_store = InMemoryDeliveryEndpointStore::default();
        let status_store = InMemoryEndpointDeliveryStatusStore::default();
        let now = Utc::now();

        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.trend.worse".to_string(),
                name: "Worse".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/worse".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");
        endpoint_store
            .insert(NewDeliveryEndpoint {
                endpoint_id: "endpoint.trend.improve".to_string(),
                name: "Improve".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://example.com/improve".to_string(),
                metadata: None,
                created_at: now,
            })
            .await
            .expect("insert should succeed");

        status_store
            .record_failure("endpoint.trend.worse", "evt-1", "boom", now - Duration::minutes(2))
            .await
            .expect("record should succeed");
        status_store
            .record_failure("endpoint.trend.worse", "evt-2", "boom", now - Duration::minutes(1))
            .await
            .expect("record should succeed");

        status_store
            .record_failure("endpoint.trend.improve", "evt-3", "oops", now - Duration::minutes(2))
            .await
            .expect("record should succeed");
        status_store
            .record_success("endpoint.trend.improve", "evt-4", now - Duration::seconds(30))
            .await
            .expect("record should succeed");

        let use_case = ListEndpointFailureRateTrends::new(endpoint_store, status_store);
        let rows = use_case
            .execute(&ListEndpointFailureRateTrendsRequest {
                protocol: None,
                include_disabled: true,
                min_total_attempts: Some(1),
                limit: 10,
            })
            .await
            .expect("trend query should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].endpoint_id, "endpoint.trend.worse");
        assert_eq!(rows[0].trend, EndpointFailureTrendDirection::Worsening);
        assert_eq!(rows[1].trend, EndpointFailureTrendDirection::Improving);
    }

    #[tokio::test]
    async fn prune_use_case_deletes_old_status_rows() {
        let store = InMemoryEndpointDeliveryStatusStore::default();
        let now = Utc::now();

        store
            .record_success("endpoint.recent", "evt-r", now)
            .await
            .expect("record should succeed");
        store
            .record_success("endpoint.old", "evt-o", now - Duration::days(10))
            .await
            .expect("record should succeed");

        let prune = PruneEndpointDeliveryStatuses::new(store.clone());
        let deleted = prune
            .execute(now - Duration::days(1))
            .await
            .expect("prune should succeed");

        assert_eq!(deleted, 1);
        let remaining = store.list().await.expect("list should succeed");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].endpoint_id, "endpoint.recent");
    }
}
