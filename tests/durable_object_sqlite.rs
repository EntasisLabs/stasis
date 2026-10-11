#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]

//! Durable Object store logic against the shared schema, using rusqlite as the
//! executor. The JavaScript bridge is covered by `stasis-wasm/test/durable-sqlite.mjs`.

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};

use stasis::domain::runtime::inbound_trigger::InboundDisposition;
use stasis::domain::runtime::job::JobState;
use stasis::domain::runtime::job_attempt::JobAttemptOutcome;
use stasis::infrastructure::sqlite::schema::MIGRATIONS;
use stasis::infrastructure::sqlite::{DurableSqliteKernel, RusqliteExecutor, SqlExecutor};

fn kernel() -> (Arc<RusqliteExecutor>, DurableSqliteKernel) {
    let executor = Arc::new(RusqliteExecutor::open_in_memory().expect("sqlite"));
    let kernel = DurableSqliteKernel::open(executor.clone()).expect("schema");
    (executor, kernel)
}

#[test]
fn schema_migration_is_idempotent_and_reuses_shared_ddl() {
    let (executor, kernel) = kernel();
    assert_eq!(MIGRATIONS[0].0, 1);
    DurableSqliteKernel::open(executor.clone()).expect("second open");
    let version = executor
        .query(
            "SELECT CAST(version AS TEXT) FROM stasis_schema_migration",
            &[],
        )
        .expect("version");
    assert_eq!(version.len(), 1);
    assert_eq!(version[0].text(0).unwrap(), "1");
    let tables = executor
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'job'",
            &[],
        )
        .expect("tables");
    assert_eq!(tables.len(), 1);
    drop(kernel);
}

#[test]
fn inbound_key_dedupes_and_survives_reopen() {
    let (executor, first) = kernel();
    let accepted = first.accept_job("order-17", "ship-it").expect("accept");
    assert_eq!(accepted.disposition, InboundDisposition::Accepted);
    let duplicate = first.accept_job("order-17", "other-payload").expect("dup");
    assert_eq!(duplicate.disposition, InboundDisposition::Duplicate);
    assert_eq!(duplicate.job_id, accepted.job_id);

    let processed = first
        .process_once("default", "worker-a")
        .expect("process")
        .expect("job");
    assert_eq!(processed.job_id, accepted.job_id);
    assert_eq!(processed.state, "succeeded");
    assert!(
        first
            .process_once("default", "worker-a")
            .expect("second")
            .is_none()
    );

    drop(first);
    let reopened = DurableSqliteKernel::open(executor).expect("reopen");
    let job = reopened
        .get_job(&accepted.job_id)
        .expect("get")
        .expect("row");
    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(job.payload_ref, "ship-it");
    assert_eq!(job.idempotency_key, "order-17");
    assert_eq!(job.attempts, 1);
    assert!(job.lease_owner.is_none());
    let attempts = reopened.attempts_for(&job.id).expect("attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, JobAttemptOutcome::Succeeded);
    assert_eq!(attempts[0].worker_id, "worker-a");
    assert!(attempts[0].diagnostics.as_deref() == Some("ship-it"));

    let again = reopened
        .accept_job("order-17", "ship-it")
        .expect("dup after reopen");
    assert_eq!(again.disposition, InboundDisposition::Duplicate);
    assert_eq!(again.job_id, accepted.job_id);
}

#[test]
fn grapheme_state_round_trips_across_reopen() {
    let (executor, first) = kernel();
    let payload = r#"grapheme:json:{"source":"op main()","workflow_id":"wf-1","state_current":{"step":2,"cursor":"abc"}}"#;
    let accepted = first.accept_job("wf-1", payload).expect("accept");
    first
        .process_once("default", "worker-a")
        .expect("process")
        .expect("processed");
    drop(first);
    let reopened = DurableSqliteKernel::open(executor).expect("reopen");
    let state = reopened
        .load_grapheme_state("wf-1")
        .expect("state")
        .expect("present");
    assert!(state.contains("\"step\":2"));
    assert!(state.contains("abc"));
    let job = reopened
        .get_job(&accepted.job_id)
        .expect("job")
        .expect("row");
    assert_eq!(job.job_type, "grapheme.workflow");
    assert_eq!(job.state, JobState::Succeeded);
}

#[test]
fn job_heartbeat_ignores_stale_owner() {
    let (_executor, kernel) = kernel();
    let accepted = kernel.accept_job("lease-1", "payload").expect("accept");
    let now = Utc::now();
    let leased = kernel
        .lease_next("default", "owner-a", now)
        .expect("lease")
        .expect("job");
    assert_eq!(leased.state, JobState::Leased);
    assert_eq!(leased.lease_owner.as_deref(), Some("owner-a"));
    let expiry = leased.lease_expires_at.expect("expiry");
    let later = now + Duration::seconds(5);
    assert!(
        !kernel
            .heartbeat(&accepted.job_id, "owner-b", later, 60)
            .expect("stale heartbeat")
    );
    let unchanged = kernel.get_job(&accepted.job_id).unwrap().unwrap();
    assert_eq!(unchanged.lease_expires_at, Some(expiry));
    assert!(
        kernel
            .heartbeat(&accepted.job_id, "owner-a", later, 60)
            .expect("owner heartbeat")
    );
    let extended = kernel.get_job(&accepted.job_id).unwrap().unwrap();
    assert!(extended.lease_expires_at.expect("extended") > expiry);
    assert_eq!(extended.heartbeat_at, Some(later));
}

#[test]
fn resource_lease_fence_rejects_stale_renew_and_allows_expiry() {
    let (_executor, kernel) = kernel();
    let now = Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap();
    let lease = kernel
        .acquire_lease("queue:default", "worker-a", 30, now, false)
        .expect("acquire");
    assert_eq!(lease.fencing_token.0, 1);
    let renewed = kernel
        .renew_lease(
            "queue:default",
            "worker-a",
            1,
            30,
            now + Duration::seconds(10),
        )
        .expect("renew");
    assert_eq!(renewed.fencing_token.0, 1);
    assert!(renewed.expires_at > lease.expires_at);
    let stale = kernel.renew_lease(
        "queue:default",
        "worker-a",
        0,
        30,
        now + Duration::seconds(11),
    );
    assert!(stale.is_err());
    let stolen = kernel.acquire_lease(
        "queue:default",
        "worker-b",
        30,
        now + Duration::seconds(12),
        false,
    );
    assert!(stolen.is_err());
    let after_expiry = kernel
        .acquire_lease(
            "queue:default",
            "worker-b",
            30,
            renewed.expires_at + Duration::seconds(1),
            false,
        )
        .expect("expired acquire");
    assert_eq!(after_expiry.fencing_token.0, 2);
    assert_eq!(after_expiry.owner.0, "worker-b");
}
