#![cfg(feature = "sqlite")]

//! SQLite runtime coverage that the shared in-memory suites do not exercise:
//! file reopen, migration replay, and inbound idempotency on a private database.

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;

use stasis::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use stasis::application::runtime::sqlite_runtime::SqliteRuntime;
use stasis::domain::errors::Result;
use stasis::domain::runtime::inbound_trigger::{InboundDisposition, InboundProtocol};
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::typed_contract::StasisJob;
use stasis::infrastructure::sqlite::session::SqliteSession;
use stasis::ports::outbound::runtime::job_store::JobStore;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Ping {
    n: u32,
}

impl StasisJob for Ping {
    const NAME: &'static str = "sqlite_ping";
    const VERSION: u32 = 1;
    type Output = ();
}

struct OkHandler;

#[async_trait]
impl JobHandler for OkHandler {
    fn job_type(&self) -> &'static str {
        "sqlite_ping"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            output_provenance: None,
            execution_id: None,
            diagnostics: None,
        })
    }
}

fn raw_job(id: &str, now: chrono::DateTime<Utc>) -> NewJob {
    NewJob {
        id: id.to_string(),
        queue: "default".to_string(),
        job_type: "sqlite_ping".to_string(),
        payload_ref: "{}".to_string(),
        priority: 100,
        max_attempts: 3,
        idempotency_key: format!("idem-{id}"),
        correlation_id: format!("corr-{id}"),
        causation_id: "cause".to_string(),
        trace_id: "trace".to_string(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy::default(),
    }
}

#[test]
fn sqlite_migration_is_idempotent_in_memory_and_on_disk() {
    let memory = SqliteSession::open(":memory:").expect("memory");
    memory.migrate().expect("second migrate");

    let path = std::env::temp_dir().join(format!(
        "stasis-sqlite-migrate-{}-{}.sqlite",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    ));
    let path_str = path.to_string_lossy().to_string();
    let first = SqliteSession::open(&path_str).expect("create file");
    drop(first);
    let second = SqliteSession::open(&path_str).expect("reopen file");
    second.migrate().expect("replay migrate");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_file_reopen_keeps_processed_job() {
    let path = std::env::temp_dir().join(format!(
        "stasis-sqlite-runtime-{}-{}.sqlite",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    ));
    let path_str = path.to_string_lossy().to_string();

    let rt = SqliteRuntime::open_path(&path_str).expect("open");
    rt.register_handler(OkHandler).expect("handler");
    let now = Utc::now();
    rt.enqueue(raw_job("job-file", now)).await.expect("enqueue");
    let processed = rt
        .process_once("default", "worker-file", now)
        .await
        .expect("process");
    assert_eq!(processed.as_deref(), Some("job-file"));
    drop(rt);

    let reopened = SqliteRuntime::open_path(&path_str).expect("reopen");
    let job = reopened
        .job_store
        .get("job-file")
        .await
        .expect("get")
        .expect("job persisted");
    assert_eq!(job.state, JobState::Succeeded);
    assert!(job.finished_at.is_some());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_memory_runtime_dedupes_inbound_and_fences_leases() {
    let rt = SqliteRuntime::open_memory().expect("memory runtime");
    let first = rt
        .accept_inbound_job(InboundProtocol::HttpWebhook, "idem-ping", Ping { n: 1 })
        .await
        .expect("accept");
    assert_eq!(first.disposition, InboundDisposition::Accepted);

    let second = rt
        .accept_inbound_job(InboundProtocol::HttpWebhook, "idem-ping", Ping { n: 2 })
        .await
        .expect("duplicate");
    assert_eq!(second.disposition, InboundDisposition::Duplicate);
    assert_eq!(second.job_id, first.job_id);

    let lease = rt
        .acquire_lease("widget", "owner-a", Duration::from_secs(30))
        .await
        .expect("acquire");
    let renewed = rt
        .renew_lease(
            "widget",
            "owner-a",
            lease.fencing_token,
            Duration::from_secs(30),
        )
        .await
        .expect("renew");
    assert!(renewed.fencing_token.0 >= lease.fencing_token.0);

    let stale = rt
        .renew_lease(
            "widget",
            "owner-a",
            stasis::domain::runtime::resource_lease::FencingToken(lease.fencing_token.0.saturating_sub(1)),
            Duration::from_secs(30),
        )
        .await;
    assert!(stale.is_err());
}
