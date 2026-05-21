use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterNodeHeartbeat;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;

#[derive(Clone)]
pub struct CoordinatorFailoverJobHandler {
    cluster_store: Arc<dyn ClusterNodeStore>,
}

#[derive(Deserialize)]
struct CoordinatorFailoverPayload {
    coordinator_node_id: String,
    failover_to_node_id: Option<String>,
    queue_scope: Option<Vec<String>>,
    reason: Option<String>,
}

impl CoordinatorFailoverJobHandler {
    pub fn new(cluster_store: Arc<dyn ClusterNodeStore>) -> Self {
        Self { cluster_store }
    }

    fn parse_payload(raw: &str) -> std::result::Result<CoordinatorFailoverPayload, String> {
        let payload: CoordinatorFailoverPayload = serde_json::from_str(raw)
            .map_err(|err| format!("invalid coordinator failover payload json: {err}"))?;

        if payload.coordinator_node_id.trim().is_empty() {
            return Err("coordinator_node_id must not be empty".to_string());
        }

        if let Some(target) = &payload.failover_to_node_id {
            if target.trim().is_empty() {
                return Err("failover_to_node_id must not be empty when provided".to_string());
            }
        }

        Ok(payload)
    }

    fn failure(message: impl Into<String>) -> JobExecutionOutcome {
        JobExecutionOutcome::FatalFailure {
            message: message.into(),
            execution_id: None,
            diagnostics: None,
        }
    }

    fn retryable(message: impl Into<String>) -> JobExecutionOutcome {
        JobExecutionOutcome::RetryableFailure {
            message: message.into(),
            execution_id: None,
            diagnostics: None,
        }
    }

    fn remaining_ttl_seconds(
        lease_expires_at: chrono::DateTime<Utc>,
        now: chrono::DateTime<Utc>,
    ) -> i64 {
        lease_expires_at
            .signed_duration_since(now)
            .num_seconds()
            .max(1)
    }
}

#[async_trait]
impl JobHandler for CoordinatorFailoverJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.cluster.coordinator_failover"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::failure(message)),
        };

        let now = Utc::now();
        let nodes = match self.cluster_store.list().await {
            Ok(nodes) => nodes,
            Err(err) => {
                return Ok(Self::retryable(format!(
                    "failed to load cluster nodes for failover: {err}"
                )));
            }
        };

        let Some(source) = nodes
            .iter()
            .find(|node| node.node_id == payload.coordinator_node_id)
            .cloned()
        else {
            return Ok(Self::failure(format!(
                "coordinator node not found: {}",
                payload.coordinator_node_id
            )));
        };

        if source.lease_expires_at < now {
            return Ok(Self::failure(format!(
                "coordinator node is not active: {}",
                source.node_id
            )));
        }

        let target_node_id = payload.failover_to_node_id.clone().or_else(|| {
            nodes
                .iter()
                .filter(|node| node.node_id != source.node_id)
                .filter(|node| node.lease_expires_at >= now)
                .find(|node| node.region == source.region)
                .map(|node| node.node_id.clone())
        });

        let Some(target_node_id) = target_node_id else {
            return Ok(Self::failure("no active failover target available"));
        };

        if target_node_id == source.node_id {
            return Ok(Self::failure(
                "failover target must differ from coordinator node",
            ));
        }

        let Some(target) = nodes
            .iter()
            .find(|node| node.node_id == target_node_id)
            .cloned()
        else {
            return Ok(Self::failure(format!(
                "failover target node not found: {}",
                target_node_id
            )));
        };

        if target.lease_expires_at < now {
            return Ok(Self::failure(format!(
                "failover target node is not active: {}",
                target.node_id
            )));
        }

        let moved_queues = if let Some(scope) = payload.queue_scope {
            scope.into_iter().collect::<BTreeSet<_>>()
        } else {
            source
                .queue_ownership
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        };

        let source_queues = source
            .queue_ownership
            .into_iter()
            .filter(|queue| !moved_queues.contains(queue))
            .collect::<Vec<_>>();

        let mut target_queue_set = target.queue_ownership.into_iter().collect::<BTreeSet<_>>();
        for queue in &moved_queues {
            target_queue_set.insert(queue.clone());
        }

        let source_ttl = Self::remaining_ttl_seconds(source.lease_expires_at, now);
        let target_ttl = Self::remaining_ttl_seconds(target.lease_expires_at, now);

        let source_update = self
            .cluster_store
            .heartbeat(ClusterNodeHeartbeat {
                node_id: source.node_id.clone(),
                heartbeat_at: now,
                lease_ttl_seconds: source_ttl,
                queue_ownership: Some(source_queues),
                capability_tags: None,
                metadata: Some(
                    json!({
                        "action": "coordinator_failover_source",
                        "reason": payload.reason,
                    })
                    .to_string(),
                ),
            })
            .await;

        if source_update.is_err() {
            return Ok(Self::retryable(
                "failed to update source node during failover",
            ));
        }

        let target_update = self
            .cluster_store
            .heartbeat(ClusterNodeHeartbeat {
                node_id: target.node_id.clone(),
                heartbeat_at: now,
                lease_ttl_seconds: target_ttl,
                queue_ownership: Some(target_queue_set.into_iter().collect::<Vec<_>>()),
                capability_tags: None,
                metadata: Some(
                    json!({
                        "action": "coordinator_failover_target",
                        "from_node": source.node_id,
                        "reason": payload.reason,
                    })
                    .to_string(),
                ),
            })
            .await;

        if target_update.is_err() {
            return Ok(Self::retryable(
                "failed to update target node during failover",
            ));
        }

        let diagnostics = json!({
            "status": "success",
            "source_node": payload.coordinator_node_id,
            "target_node": target.node_id,
            "moved_queues": moved_queues.into_iter().collect::<Vec<_>>(),
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:stasis:cluster:coordinator_failover".to_string(),
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use crate::application::runtime::coordinator_failover_job_handler::CoordinatorFailoverJobHandler;
    use crate::application::runtime::in_memory_runtime::JobHandler;
    use crate::domain::runtime::cluster_node::{ClusterNodeRole, NewClusterNode};
    use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
    use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
    use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
    use std::sync::Arc;

    fn sample_job(payload_ref: String) -> Job {
        Job {
            id: "job.cluster.failover.1".to_string(),
            queue: "cluster-control".to_string(),
            job_type: "workflow.stasis.cluster.coordinator_failover".to_string(),
            payload_ref,
            state: JobState::Enqueued,
            priority: 100,
            attempts: 0,
            max_attempts: 3,
            backoff_policy: BackoffPolicy::default(),
            idempotency_key: "idem-failover".to_string(),
            correlation_id: "corr-failover".to_string(),
            causation_id: "cause-failover".to_string(),
            trace_id: "trace-failover".to_string(),
            sttp_input_node_id: "sttp:in:cluster:failover".to_string(),
            sttp_output_node_id: None,
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            scheduled_at: Utc::now(),
            started_at: None,
            finished_at: None,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn handler_moves_queues_to_target_node() {
        let store = InMemoryClusterNodeStore::default();
        let now = Utc::now();

        store
            .register(NewClusterNode {
                node_id: "node.coord.a".to_string(),
                role: ClusterNodeRole::Coordinator,
                region: "us-east".to_string(),
                queue_ownership: vec!["default".to_string(), "priority".to_string()],
                capability_tags: vec![],
                heartbeat_at: now,
                lease_ttl_seconds: 60,
                metadata: None,
            })
            .await
            .expect("register source should succeed");

        store
            .register(NewClusterNode {
                node_id: "node.coord.b".to_string(),
                role: ClusterNodeRole::Coordinator,
                region: "us-east".to_string(),
                queue_ownership: vec![],
                capability_tags: vec![],
                heartbeat_at: now,
                lease_ttl_seconds: 60,
                metadata: None,
            })
            .await
            .expect("register target should succeed");

        let handler = CoordinatorFailoverJobHandler::new(Arc::new(store.clone()));
        let payload = serde_json::json!({
            "coordinator_node_id": "node.coord.a",
            "failover_to_node_id": "node.coord.b",
            "queue_scope": ["priority"],
            "reason": "planned",
        })
        .to_string();

        let outcome = handler
            .execute(&sample_job(payload))
            .await
            .expect("handler execution should succeed");

        assert!(matches!(
            outcome,
            crate::application::runtime::in_memory_runtime::JobExecutionOutcome::Success { .. }
        ));

        let source = store
            .get("node.coord.a")
            .await
            .expect("source get should succeed")
            .expect("source should exist");
        let target = store
            .get("node.coord.b")
            .await
            .expect("target get should succeed")
            .expect("target should exist");

        assert_eq!(source.queue_ownership, vec!["default".to_string()]);
        assert!(target.queue_ownership.iter().any(|q| q == "priority"));
    }

    #[tokio::test]
    async fn handler_returns_fatal_failure_for_missing_target() {
        let store = InMemoryClusterNodeStore::default();
        let now = Utc::now();

        store
            .register(NewClusterNode {
                node_id: "node.coord.a".to_string(),
                role: ClusterNodeRole::Coordinator,
                region: "us-east".to_string(),
                queue_ownership: vec!["default".to_string()],
                capability_tags: vec![],
                heartbeat_at: now - Duration::seconds(1),
                lease_ttl_seconds: 60,
                metadata: None,
            })
            .await
            .expect("register source should succeed");

        let handler = CoordinatorFailoverJobHandler::new(Arc::new(store));
        let payload = serde_json::json!({
            "coordinator_node_id": "node.coord.a",
            "failover_to_node_id": "missing",
        })
        .to_string();

        let outcome = handler
            .execute(&sample_job(payload))
            .await
            .expect("handler execution should succeed");

        assert!(matches!(
            outcome,
            crate::application::runtime::in_memory_runtime::JobExecutionOutcome::FatalFailure { .. }
        ));
    }
}
