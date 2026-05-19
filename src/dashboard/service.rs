use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::application::dto::{
    ListClusterNodeHealthRequest, ListEndpointDiagnosticsReadModelRequest,
    ListEndpointFailureRateTrendsRequest,
};
use crate::application::runtime::in_memory_runtime::InMemoryRuntime;
use crate::dashboard::dto::{
    AttemptInspectorDto, ClusterMapDto, DashboardDto, EndpointRowDto, EventInspectorDto,
    InspectorView, JobInspectorDto, JobRowDto, OutboxEventRowDto, RecurringDefinitionRowDto,
    SystemKpiDto, UiListPanel,
};
use crate::dashboard::mappers::{
    map_cluster_health_row, map_endpoint_inspector, map_endpoint_row, map_job_to_row,
    map_node_inspector, map_outbox_to_row, map_recurring_definition_row,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::JobState;
use crate::domain::runtime::outbox::OutboxEvent;
use crate::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;
use crate::sdk::control_plane_sdk::ControlPlaneSdk;

type DashboardControlStore =
    CompositeControlPlaneStore<InMemoryDeliveryEndpointStore, InMemoryClusterNodeStore>;
type DashboardControlPlane = ControlPlaneSdk<DashboardControlStore>;

#[derive(Clone, Debug)]
pub enum InspectEntity {
    Job(String),
    Attempt(String),
    Node(String),
    Endpoint(String),
    Event(String),
}

#[async_trait]
pub trait DashboardQueryService: Send + Sync {
    async fn dashboard(&self, inspect: Option<InspectEntity>) -> Result<DashboardDto>;
    async fn jobs_stream(&self) -> Result<UiListPanel<JobRowDto>>;
    async fn outbox_stream(&self) -> Result<UiListPanel<OutboxEventRowDto>>;
    async fn endpoint_stream(&self) -> Result<UiListPanel<EndpointRowDto>>;
    async fn recurring_stream(&self) -> Result<UiListPanel<RecurringDefinitionRowDto>>;
    async fn cluster_stream(&self) -> Result<ClusterMapDto>;
    async fn scheduler_materialize_now(&self, scheduler_id: &str) -> Result<usize>;
    async fn scheduler_process_queue_once(
        &self,
        queue: &str,
        worker_id: &str,
    ) -> Result<Option<String>>;
    async fn scheduler_publish_pending_now(&self, limit: usize) -> Result<usize>;
    async fn scheduler_replay_dead_letter_now(&self, job_id: &str) -> Result<bool>;
    async fn inspect(&self, entity: InspectEntity) -> Result<InspectorView>;
}

#[derive(Clone)]
pub struct InMemoryDashboardQueryService {
    runtime: Arc<InMemoryRuntime>,
    control_plane: DashboardControlPlane,
}

impl InMemoryDashboardQueryService {
    pub fn new(runtime: Arc<InMemoryRuntime>, control_plane: DashboardControlPlane) -> Self {
        Self { runtime, control_plane }
    }

    async fn list_all_jobs(&self) -> Result<Vec<crate::domain::runtime::job::Job>> {
        let states = [
            JobState::Enqueued,
            JobState::Leased,
            JobState::Running,
            JobState::Succeeded,
            JobState::Failed,
            JobState::DeadLetter,
            JobState::Canceled,
        ];

        let mut jobs = Vec::new();
        for state in states {
            jobs.extend(self.runtime.job_store.list_by_state(state).await?);
        }

        jobs.sort_by(|left, right| {
            right
                .scheduled_at
                .cmp(&left.scheduled_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(jobs)
    }

    async fn list_all_outbox_events(&self) -> Result<Vec<OutboxEvent>> {
        let jobs = self.list_all_jobs().await?;
        let mut seen = HashSet::new();
        let mut out = Vec::new();

        for job in jobs {
            for event in self.runtime.list_lineage_events(&job.id).await? {
                if seen.insert(event.event_id.clone()) {
                    out.push(event);
                }
            }
        }

        out.sort_by(|left, right| {
            right
                .event
                .occurred_at
                .cmp(&left.event.occurred_at)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        Ok(out)
    }
}

#[async_trait]
impl DashboardQueryService for InMemoryDashboardQueryService {
    async fn dashboard(&self, inspect: Option<InspectEntity>) -> Result<DashboardDto> {
        let jobs = self.jobs_stream().await?;
        let outbox = self.outbox_stream().await?;
        let cluster = self.cluster_stream().await?;
        let healthy_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.health == "Healthy")
            .count();
        let degraded_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.health == "Degraded")
            .count();
        let offline_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.health == "Offline")
            .count();

        let running_jobs = jobs.items.iter().filter(|job| job.status == "running").count();
        let enqueued_jobs = jobs.items.iter().filter(|job| job.status == "enqueued").count();
        let succeeded_jobs = jobs.items.iter().filter(|job| job.status == "succeeded").count();
        let failed_jobs = jobs
            .items
            .iter()
            .filter(|job| job.status == "failed" || job.status == "dead_letter")
            .count();

        let pending_outbox = outbox
            .items
            .iter()
            .filter(|event| event.delivery_state == "pending")
            .count();
        let failed_outbox = outbox
            .items
            .iter()
            .filter(|event| event.delivery_state == "failed")
            .count();

        let endpoint_trends = self
            .control_plane
            .list_endpoint_failure_rate_trends(ListEndpointFailureRateTrendsRequest {
                protocol: None,
                include_disabled: true,
                min_total_attempts: None,
                limit: 100,
            })
            .await
            .unwrap_or_default();

        let avg_failure_rate = if endpoint_trends.is_empty() {
            0.0
        } else {
            endpoint_trends
                .iter()
                .map(|row| row.failure_rate)
                .sum::<f64>()
                / endpoint_trends.len() as f64
        };

        let inspector = match inspect {
            Some(entity) => self.inspect(entity).await?,
            None => InspectorView::None,
        };

        Ok(DashboardDto {
            kpis: SystemKpiDto {
                succeeded_jobs,
                failed_jobs,
                enqueued_jobs,
                running_jobs,
                pending_outbox,
                failed_outbox,
                healthy_nodes,
                degraded_nodes,
                offline_nodes,
                endpoint_failure_rate: format!("{:.1}%", avg_failure_rate * 100.0),
            },
            job_stream: jobs,
            outbox_stream: outbox,
            cluster_map: cluster,
            inspector,
        })
    }

    async fn jobs_stream(&self) -> Result<UiListPanel<JobRowDto>> {
        let jobs = self.list_all_jobs().await?;
        let mapped = jobs.iter().map(map_job_to_row).collect::<Vec<_>>();

        Ok(UiListPanel {
            items: mapped.clone(),
            total: Some(mapped.len() as u64),
            cursor: None,
        })
    }

    async fn outbox_stream(&self) -> Result<UiListPanel<OutboxEventRowDto>> {
        let events = self.list_all_outbox_events().await?;
        let mapped = events.iter().take(200).map(map_outbox_to_row).collect::<Vec<_>>();

        Ok(UiListPanel {
            items: mapped.clone(),
            total: Some(mapped.len() as u64),
            cursor: None,
        })
    }

    async fn endpoint_stream(&self) -> Result<UiListPanel<EndpointRowDto>> {
        let rows = self
            .control_plane
            .list_endpoint_diagnostics_read_model(ListEndpointDiagnosticsReadModelRequest {
                endpoint_ids: None,
                protocol: None,
                min_failure_count: None,
                stale_after_seconds: None,
                unhealthy_only: false,
                include_disabled: true,
                offset: 0,
                limit: Some(200),
            })
            .await?;

        let mapped = rows.iter().map(map_endpoint_row).collect::<Vec<_>>();

        Ok(UiListPanel {
            items: mapped.clone(),
            total: Some(mapped.len() as u64),
            cursor: None,
        })
    }

    async fn recurring_stream(&self) -> Result<UiListPanel<RecurringDefinitionRowDto>> {
        let definitions = self.runtime.recurring_store.list().await?;
        let mut rows = definitions
            .iter()
            .map(map_recurring_definition_row)
            .collect::<Vec<_>>();

        rows.sort_by(|left, right| {
            left.next_run_at
                .cmp(&right.next_run_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        Ok(UiListPanel {
            items: rows.clone(),
            total: Some(rows.len() as u64),
            cursor: None,
        })
    }

    async fn cluster_stream(&self) -> Result<ClusterMapDto> {
        let rows = self
            .control_plane
            .list_cluster_node_health(ListClusterNodeHealthRequest {
                role: None,
                region: None,
                capability_tag: None,
                queue: None,
                health: None,
                offset: 0,
                limit: Some(200),
            })
            .await?;

        let nodes = rows.iter().map(map_cluster_health_row).collect();

        Ok(ClusterMapDto { nodes })
    }

    async fn scheduler_materialize_now(&self, scheduler_id: &str) -> Result<usize> {
        self.runtime.materialize_recurring_now(scheduler_id).await
    }

    async fn scheduler_process_queue_once(
        &self,
        queue: &str,
        worker_id: &str,
    ) -> Result<Option<String>> {
        self.runtime.process_once_now(queue, worker_id).await
    }

    async fn scheduler_publish_pending_now(&self, limit: usize) -> Result<usize> {
        self.runtime.publish_pending_events_now(limit).await
    }

    async fn scheduler_replay_dead_letter_now(&self, job_id: &str) -> Result<bool> {
        self.runtime.replay_dead_letter_now(job_id).await
    }

    async fn inspect(&self, entity: InspectEntity) -> Result<InspectorView> {
        let inspector = match entity {
            InspectEntity::Job(id) => {
                let jobs = self.list_all_jobs().await?;
                let Some(job) = jobs.iter().find(|job| job.id == id) else {
                    return Ok(InspectorView::None);
                };

                InspectorView::Job(JobInspectorDto {
                    id: job.id.clone(),
                    status: format!("{:?}", job.state),
                    queue: job.queue.clone(),
                    trace_id: job.trace_id.clone(),
                    correlation_id: job.correlation_id.clone(),
                    causation_id: job.causation_id.clone(),
                    last_error: job.last_error.clone(),
                })
            }
            InspectEntity::Attempt(id) => {
                let jobs = self.list_all_jobs().await?;
                let mut found = None;
                for job in jobs {
                    for attempt in self.runtime.list_job_attempts(&job.id).await? {
                        if attempt.attempt_id == id {
                            found = Some(attempt);
                            break;
                        }
                    }
                    if found.is_some() {
                        break;
                    }
                }

                let Some(attempt) = found else {
                    return Ok(InspectorView::None);
                };

                InspectorView::Attempt(AttemptInspectorDto {
                    attempt_id: attempt.attempt_id,
                    job_id: attempt.job_id,
                    outcome: format!("{:?}", attempt.outcome),
                    worker_id: attempt.worker_id,
                    duration_ms: attempt.duration_ms,
                    guardrail_code: attempt.guardrail_code,
                    policy_reason: attempt.policy_reason,
                })
            }
            InspectEntity::Node(id) => {
                let rows = self
                    .control_plane
                    .list_cluster_node_health(ListClusterNodeHealthRequest {
                        role: None,
                        region: None,
                        capability_tag: None,
                        queue: None,
                        health: None,
                        offset: 0,
                        limit: Some(200),
                    })
                    .await?;

                let Some(node) = rows.iter().find(|row| row.snapshot.node.node_id == id) else {
                    return Ok(InspectorView::None);
                };
                InspectorView::Node(map_node_inspector(node))
            }
            InspectEntity::Endpoint(id) => {
                let rows = self
                    .control_plane
                    .list_endpoint_diagnostics_read_model(ListEndpointDiagnosticsReadModelRequest {
                        endpoint_ids: Some(vec![id.clone()]),
                        protocol: None,
                        min_failure_count: None,
                        stale_after_seconds: None,
                        unhealthy_only: false,
                        include_disabled: true,
                        offset: 0,
                        limit: Some(1),
                    })
                    .await?;

                let Some(endpoint) = rows.first() else {
                    return Ok(InspectorView::None);
                };
                InspectorView::Endpoint(map_endpoint_inspector(endpoint))
            }
            InspectEntity::Event(id) => {
                let events = self.list_all_outbox_events().await?;
                let Some(event) = events.iter().find(|event| event.event_id == id) else {
                    return Ok(InspectorView::None);
                };

                InspectorView::Event(EventInspectorDto {
                    event_id: event.event_id.clone(),
                    event_type: format!("{:?}", event.event.event_type),
                    job_id: event.event.job_id.clone(),
                    correlation_id: event.event.correlation_id.clone(),
                    trace_id: event.event.trace_id.clone(),
                    status: format!("{:?}", event.status),
                })
            }
        };

        Ok(inspector)
    }
}
