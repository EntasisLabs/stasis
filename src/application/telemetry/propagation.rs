use crate::application::config::env::{non_empty, with_default};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::runtime_tracing::{OtelAttribute, TraceContext};

/// How job traces link to OpenTelemetry parent contexts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TracePropagationMode {
    Legacy,
    W3c,
    Both,
}

pub fn trace_propagation_mode() -> TracePropagationMode {
    match non_empty("STASIS_OTEL_TRACE_PROPAGATION")
        .unwrap_or_else(|| with_default("STASIS_OTEL_TRACE_PROPAGATION", "w3c"))
        .to_ascii_lowercase()
        .as_str()
    {
        "legacy" => TracePropagationMode::Legacy,
        "both" => TracePropagationMode::Both,
        _ => TracePropagationMode::W3c,
    }
}

pub fn is_w3c_trace_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Parses a W3C `traceparent` header (`version-trace_id-parent_id-trace_flags`).
pub fn parse_traceparent(header: &str) -> Result<TraceContext> {
    let trimmed = header.trim();
    let parts: Vec<&str> = trimmed.split('-').collect();
    if parts.len() != 4 {
        return Err(StasisError::PortFailure(format!(
            "invalid traceparent format: expected 4 segments, got {}",
            parts.len()
        )));
    }

    if parts[0] != "00" {
        return Err(StasisError::PortFailure(format!(
            "unsupported traceparent version: {}",
            parts[0]
        )));
    }

    let trace_id = parts[1].to_ascii_lowercase();
    if trace_id.len() != 32 || !is_w3c_trace_id(&trace_id) {
        return Err(StasisError::PortFailure(
            "invalid traceparent trace-id segment".to_string(),
        ));
    }

    let span_id = parts[2].to_ascii_lowercase();
    if span_id.len() != 16 || !span_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StasisError::PortFailure(
            "invalid traceparent parent-id segment".to_string(),
        ));
    }

    let trace_flags = u8::from_str_radix(parts[3], 16).map_err(|err| {
        StasisError::PortFailure(format!("invalid traceparent trace-flags segment: {err}"))
    })?;

    Ok(TraceContext {
        trace_id,
        span_id,
        trace_flags,
    })
}

pub fn generate_w3c_trace_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static TRACE_COUNTER: AtomicU64 = AtomicU64::new(1);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0) as u64;
    let counter = TRACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}{:016x}", nanos, counter)
}

pub fn parent_trace_context(job_trace_id: &str) -> Option<TraceContext> {
    match trace_propagation_mode() {
        TracePropagationMode::Legacy => None,
        TracePropagationMode::W3c | TracePropagationMode::Both if is_w3c_trace_id(job_trace_id) => {
            Some(TraceContext {
                trace_id: job_trace_id.to_ascii_lowercase(),
                span_id: "0000000000000000".to_string(),
                trace_flags: 1,
            })
        }
        TracePropagationMode::Both => None,
        TracePropagationMode::W3c => None,
    }
}

pub fn job_execute_span_attributes(job: &Job) -> Vec<OtelAttribute> {
    let mut attributes = vec![
        OtelAttribute::string("stasis.job.id", job.id.clone()),
        OtelAttribute::string("stasis.job.type", job.job_type.clone()),
        OtelAttribute::string("stasis.trace_id", job.trace_id.clone()),
        OtelAttribute::string("stasis.queue", job.queue.clone()),
        OtelAttribute::string("stasis.correlation_id", job.correlation_id.clone()),
    ];

    if !is_w3c_trace_id(&job.trace_id) {
        attributes.push(OtelAttribute::bool("stasis.legacy_trace_id", true));
    }

    attributes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_traceparent_extracts_trace_context() {
        let context = parse_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .expect("traceparent should parse");

        assert_eq!(context.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(context.span_id, "00f067aa0ba902b7");
        assert_eq!(context.trace_flags, 1);
    }

    #[test]
    fn parse_traceparent_rejects_invalid_segments() {
        assert!(parse_traceparent("00-short-00f067aa0ba902b7-01").is_err());
        assert!(parse_traceparent("not-a-traceparent").is_err());
    }

    #[test]
    fn parent_trace_context_honors_w3c_mode() {
        unsafe {
            std::env::set_var("STASIS_OTEL_TRACE_PROPAGATION", "w3c");
        }

        let parent = parent_trace_context("4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(parent.is_some());

        unsafe {
            std::env::set_var("STASIS_OTEL_TRACE_PROPAGATION", "legacy");
        }
        assert!(parent_trace_context("4bf92f3577b34da6a3ce929d0e0e4736").is_none());

        unsafe {
            std::env::remove_var("STASIS_OTEL_TRACE_PROPAGATION");
        }
    }

    #[test]
    fn job_execute_span_attributes_marks_legacy_trace_ids() {
        let job = crate::domain::runtime::job::NewJob {
            id: "job-1".to_string(),
            queue: "default".to_string(),
            job_type: "test".to_string(),
            payload_ref: "payload".to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem".to_string(),
            correlation_id: "corr".to_string(),
            causation_id: "cause".to_string(),
            trace_id: "legacy-trace".to_string(),
            sttp_input_node_id: "sttp:in".to_string(),
            scheduled_at: chrono::Utc::now(),
            backoff_policy: crate::domain::runtime::job::BackoffPolicy::default(),
        }
        .into_job();

        let attributes = job_execute_span_attributes(&job);
        assert!(
            attributes
                .iter()
                .any(|attribute| attribute.key == "stasis.legacy_trace_id")
        );
    }
}
