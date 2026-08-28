use chrono::Utc;
use stasis::sdk_prelude::{BackoffPolicy, NewJob, RuntimeBackend, RuntimeFactory, RuntimeSdk};

#[tokio::main]
async fn main() -> stasis::sdk_prelude::Result<()> {
    let runtime = RuntimeFactory::build(RuntimeBackend::InMemory).await?;
    let sdk = RuntimeSdk::new(runtime);

    sdk.enqueue(NewJob {
        id: "job-1".into(),
        queue: "default".into(),
        job_type: "demo.job".into(),
        payload_ref: "demo.payload".into(),
        priority: 0,
        max_attempts: 3,
        idempotency_key: "idem-1".into(),
        correlation_id: "corr-1".into(),
        causation_id: "cause-1".into(),
        trace_id: "trace-1".into(),
        input_provenance: Some(stasis::domain::runtime::provenance::ProvenanceRef::sttp("sttp:demo:1")),
        placement: stasis::domain::runtime::placement::PlacementConstraints::default(),
        scheduled_at: Utc::now(),
        backoff_policy: BackoffPolicy::default(),
    })
    .await?;

    let stats = sdk.stats_snapshot(100).await?;
    println!("enqueued_jobs={}", stats.enqueued_jobs);
    Ok(())
}
