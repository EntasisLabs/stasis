//! Native execution authority. No application/resource lease participates in this boundary.
use crate::application::runtime::in_memory_runtime::JobExecutionOutcome;
use crate::application::runtime::job_lifecycle::{JobLifecycleEvent, apply_retryable_failure};
use crate::domain::runtime::job::{Job, JobState};
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType};
use chrono::{DateTime, Utc};

pub use crate::domain::runtime::execution::{
    CleanupRecord, CommitResult, ExecutionGrant, ExecutionReceipt,
};

/// Digest the outcome, not caller-provided digest text or the commit timestamp.
pub fn outcome_digest(outcome: &JobExecutionOutcome) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(outcome).expect("serializable native outcome"))
    )
}

pub struct Publication {
    pub job: Job,
    pub attempt: JobAttempt,
    pub outbox: OutboxEvent,
    pub receipt: ExecutionReceipt,
}

pub fn publication(
    snapshot: &Job,
    outcome: JobExecutionOutcome,
    now: DateTime<Utc>,
) -> Publication {
    let grant = ExecutionGrant::from_job(snapshot).expect("leased native snapshot");
    let digest = outcome_digest(&outcome);
    let mut job = snapshot.clone();
    job.wakeup_pending = false;
    let (attempt_outcome, execution_id, diagnostics, lifecycle, event_type) = match outcome {
        JobExecutionOutcome::Success {
            output_provenance,
            execution_id,
            diagnostics,
        } => {
            job.state = JobState::Succeeded;
            job.output_provenance = output_provenance;
            job.finished_at = Some(now);
            (
                JobAttemptOutcome::Succeeded,
                execution_id,
                diagnostics,
                JobLifecycleEvent::Succeeded,
                RuntimeEventType::JobSucceeded,
            )
        }
        JobExecutionOutcome::RetryableFailure {
            message,
            execution_id,
            diagnostics,
        } => {
            let dead = apply_retryable_failure(&mut job, now, message.clone());
            let lifecycle = if dead {
                JobLifecycleEvent::DeadLettered { message }
            } else {
                JobLifecycleEvent::RetryScheduled {
                    attempt: job.attempts,
                    message,
                }
            };
            (
                JobAttemptOutcome::RetryableFailure,
                execution_id,
                diagnostics,
                lifecycle,
                if dead {
                    RuntimeEventType::JobDeadLettered
                } else {
                    RuntimeEventType::JobRetryScheduled
                },
            )
        }
        JobExecutionOutcome::FatalFailure {
            message,
            execution_id,
            diagnostics,
        } => {
            job.attempts = job.attempts.saturating_add(1);
            job.state = JobState::DeadLetter;
            job.last_error = Some(message.clone());
            job.finished_at = Some(now);
            (
                JobAttemptOutcome::FatalFailure,
                execution_id,
                diagnostics,
                JobLifecycleEvent::DeadLettered { message },
                RuntimeEventType::JobDeadLettered,
            )
        }
        JobExecutionOutcome::Deferred {
            scheduled_at,
            message,
            execution_id,
            diagnostics,
        } => {
            job.state = JobState::Enqueued;
            job.scheduled_at = scheduled_at;
            job.last_error = Some(message.clone());
            (
                JobAttemptOutcome::Deferred,
                execution_id,
                diagnostics,
                JobLifecycleEvent::Deferred {
                    scheduled_at,
                    message,
                },
                RuntimeEventType::JobRetryScheduled,
            )
        }
    };
    job.version += 1;
    job.lease_owner = None;
    job.lease_expires_at = None;
    job.heartbeat_at = None;
    let key = format!("{}:execution:{}", job.id, grant.epoch);
    let envelope = crate::application::runtime::runtime_diagnostics_helpers::extract_runtime_diagnostics_envelope(diagnostics.as_deref());
    let error_message = if matches!(attempt_outcome, JobAttemptOutcome::Succeeded) {
        None
    } else {
        job.last_error.clone()
    };
    let attempt = JobAttempt {
        attempt_id: key.clone(),
        job_id: job.id.clone(),
        attempt_number: snapshot.attempts.saturating_add(1),
        worker_id: grant.owner.clone(),
        started_at: snapshot.started_at.unwrap_or(now),
        finished_at: now,
        outcome: attempt_outcome,
        error_message: error_message.clone(),
        output_provenance: job.output_provenance.clone(),
        execution_id: execution_id.clone(),
        guardrail_code: envelope.guardrail_code.clone(),
        policy_reason: envelope.policy_reason.clone(),
        duration_ms: envelope.duration_ms,
        diagnostics: diagnostics.clone(),
    };
    let outbox = OutboxEvent {
        event_id: key,
        status: OutboxStatus::Pending,
        publish_attempts: 0,
        published_at: None,
        next_attempt_at: None,
        last_publish_error: None,
        event: RuntimeEvent {
            event_type,
            job_id: job.id.clone(),
            thread_id: envelope.thread_id.clone(),
            correlation_id: job.correlation_id.clone(),
            causation_id: job.causation_id.clone(),
            trace_id: job.trace_id.clone(),
            input_provenance: job.input_provenance.clone(),
            output_provenance: job.output_provenance.clone(),
            execution_id,
            input_memory_query_id: envelope.input_memory_query_id.clone(),
            input_memory_query_fingerprint: envelope.input_memory_query_fingerprint.clone(),
            output_memory_node_id: envelope.output_memory_node_id.clone(),
            retrieval_path: envelope.retrieval_path.clone(),
            occurred_at: now,
            message: error_message,
        },
    };
    let receipt = ExecutionReceipt {
        grant,
        digest,
        job_json: serde_json::to_string(&job).expect("native job serializable"),
        lifecycle,
        output_json: diagnostics,
        delivered: false,
        retained_at: Some(now),
    };
    Publication {
        job,
        attempt,
        outbox,
        receipt,
    }
}

pub fn cancellation(current: &Job, now: DateTime<Utc>) -> (Job, OutboxEvent, ExecutionReceipt) {
    let mut snapshot = current.clone();
    // Enqueued cancellation has no execution owner; it grants no cleanup authority.
    snapshot.lease_owner = Some(current.lease_owner.clone().unwrap_or_default());
    let mut p = publication(
        &snapshot,
        JobExecutionOutcome::FatalFailure {
            message: "job cancelled".into(),
            execution_id: None,
            diagnostics: None,
        },
        now,
    );
    p.job.state = JobState::Canceled;
    p.job.attempts = current.attempts;
    p.job.output_provenance = None;
    p.outbox.event_id = format!(
        "{}:incarnation:{}:cancel:{}",
        current.id, current.incarnation, current.version
    );
    p.outbox.event.event_type = RuntimeEventType::JobCanceled;
    p.outbox.event.output_provenance = None;
    p.receipt.lifecycle = JobLifecycleEvent::Canceled {
        reason: "job cancelled".into(),
    };
    p.receipt.job_json = serde_json::to_string(&p.job).expect("serializable native canceled job");
    (p.job, p.outbox, p.receipt)
}

pub fn operator_failure(current: &Job, now: DateTime<Utc>) -> Publication {
    let mut snapshot = current.clone();
    snapshot.lease_owner = Some(
        current
            .lease_owner
            .clone()
            .unwrap_or_else(|| "native-control".into()),
    );
    let mut p = publication(
        &snapshot,
        JobExecutionOutcome::FatalFailure {
            message: "operator fail".into(),
            execution_id: Some(current.id.clone()),
            diagnostics: None,
        },
        now,
    );
    let key = format!(
        "{}:incarnation:{}:operator-fail:{}",
        current.id, current.incarnation, current.version
    );
    p.attempt.attempt_id = key.clone();
    p.outbox.event_id = key;
    p.receipt.job_json = serde_json::to_string(&p.job).expect("serializable native failure");
    p
}
