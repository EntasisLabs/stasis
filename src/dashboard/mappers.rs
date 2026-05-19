use crate::application::dto::{ClusterNodeHealthRow, EndpointDiagnosticsReadModelRow};
use crate::dashboard::dto::{
    ClusterNodeCardDto, EndpointInspectorDto, EndpointRowDto, JobRowDto, NodeInspectorDto,
    OutboxEventRowDto, RecurringDefinitionRowDto,
};
use crate::domain::runtime::job::{Job, JobState};
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus};

pub fn map_job_to_row(job: &Job) -> JobRowDto {
    JobRowDto {
        id: job.id.clone(),
        job_type: job.job_type.clone(),
        queue: job.queue.clone(),
        status: map_job_state(job.state.clone()),
        priority: job.priority,
        attempts: job.attempts,
        lease_owner: job.lease_owner.clone(),
        trace_id: job.trace_id.clone(),
        updated_at: job.heartbeat_at.or(job.finished_at).unwrap_or(job.scheduled_at),
    }
}

pub fn map_outbox_to_row(event: &OutboxEvent) -> OutboxEventRowDto {
    OutboxEventRowDto {
        event_id: event.event_id.clone(),
        event_type: format!("{:?}", event.event.event_type),
        correlation_id: event.event.correlation_id.clone(),
        delivery_state: map_outbox_state(event.status.clone()),
        retry_attempts: event.publish_attempts,
        occurred_at: event.event.occurred_at,
    }
}

pub fn map_cluster_health_row(row: &ClusterNodeHealthRow) -> ClusterNodeCardDto {
    ClusterNodeCardDto {
        node_id: row.snapshot.node.node_id.clone(),
        role: format!("{:?}", row.snapshot.node.role),
        region: row.snapshot.node.region.clone(),
        health: format!("{:?}", row.snapshot.health),
        queue_ownership_count: row.snapshot.node.queue_ownership.len(),
        capability_count: row.snapshot.node.capability_tags.len(),
        lease_expires_at: row.snapshot.node.lease_expires_at,
    }
}

pub fn map_node_inspector(row: &ClusterNodeHealthRow) -> NodeInspectorDto {
    NodeInspectorDto {
        node_id: row.snapshot.node.node_id.clone(),
        region: row.snapshot.node.region.clone(),
        role: format!("{:?}", row.snapshot.node.role),
        health: format!("{:?}", row.snapshot.health),
        queue_ownership: row.snapshot.node.queue_ownership.clone(),
        capability_tags: row.snapshot.node.capability_tags.clone(),
    }
}

pub fn map_endpoint_inspector(row: &EndpointDiagnosticsReadModelRow) -> EndpointInspectorDto {
    EndpointInspectorDto {
        endpoint_id: row.endpoint_id.clone(),
        protocol: format!("{:?}", row.protocol),
        target: row.target.clone(),
        enabled: row.enabled,
        success_count: row.success_count,
        failure_count: row.failure_count,
        last_error: row.last_error.clone(),
    }
}

pub fn map_endpoint_row(row: &EndpointDiagnosticsReadModelRow) -> EndpointRowDto {
    let total_attempts = row.success_count + row.failure_count;
    let failure_rate = if total_attempts == 0 {
        0.0
    } else {
        (row.failure_count as f64) / (total_attempts as f64)
    };

    EndpointRowDto {
        endpoint_id: row.endpoint_id.clone(),
        endpoint_name: row.endpoint_name.clone(),
        protocol: format!("{:?}", row.protocol),
        target: row.target.clone(),
        enabled: row.enabled,
        success_count: row.success_count,
        failure_count: row.failure_count,
        failure_rate,
        failure_rate_percent: failure_rate * 100.0,
        unhealthy: row.unhealthy,
        last_error: row.last_error.clone(),
    }
}

pub fn map_recurring_definition_row(
    definition: &crate::domain::runtime::recurring::RecurringDefinition,
) -> RecurringDefinitionRowDto {
    RecurringDefinitionRowDto {
        id: definition.id.clone(),
        queue: definition.queue.clone(),
        job_type: definition.job_type.clone(),
        cron_expr: definition.cron_expr.clone(),
        timezone: definition.timezone.clone(),
        enabled: definition.enabled,
        next_run_at: definition.next_run_at,
        last_run_at: definition.last_run_at,
    }
}

fn map_job_state(state: JobState) -> String {
    match state {
        JobState::Enqueued => "enqueued",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::DeadLetter => "dead_letter",
        JobState::Canceled => "canceled",
    }
    .to_string()
}

fn map_outbox_state(status: OutboxStatus) -> String {
    match status {
        OutboxStatus::Pending => "pending",
        OutboxStatus::Published => "published",
        OutboxStatus::Failed => "failed",
    }
    .to_string()
}
