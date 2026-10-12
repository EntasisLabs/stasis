//! Open real pre-upgrade databases and run a job after the one-shot import.
use chrono::Utc;
use rusqlite::Connection;

use crate::application::runtime::sqlite_runtime::SqliteRuntime;
use crate::domain::runtime::execution::JobExecutionOutcome;
use crate::domain::runtime::job::JobState;
use crate::infrastructure::sqlite::migrate::apply_schema;
use crate::infrastructure::sqlite::schema::MIGRATIONS;
use crate::infrastructure::sqlite::{RusqliteExecutor, SqlExecutor};
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore;
use crate::ports::outbound::runtime::job_store::JobStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const WHEN: i64 = 1_700_000_000_000_000_000;
const CHILD_SPEC: &str = r#"{"job_type":"child","payload_ref":"{}","queue":"default","priority":0,"max_attempts":1,"backoff":{"base_delay_seconds":1,"max_delay_seconds":2},"idempotency_key":null,"correlation_id":null,"placement":{}}"#;

fn temp_db(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "stasis-{label}-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn apply_until(conn: &Connection, through: i64) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS stasis_schema_migration (
            version INTEGER PRIMARY KEY,
            applied_at INTEGER NOT NULL
        );",
    )
    .unwrap();
    for (version, sql) in MIGRATIONS {
        if *version > through {
            break;
        }
        conn.execute_batch(sql).unwrap();
        conn.execute(
            "INSERT INTO stasis_schema_migration (version, applied_at) VALUES (?1, 0)",
            [*version],
        )
        .unwrap();
    }
}

fn seed_v1(conn: &Connection) {
    conn.execute(
        "INSERT INTO job (job_id, queue, job_type, payload_ref, state, priority, attempts, max_attempts, backoff_base_delay_seconds, backoff_max_delay_seconds, idempotency_key, correlation_id, causation_id, trace_id, sttp_input_node_id, lease_owner, scheduled_at) VALUES ('ready','default','work','{}','enqueued',0,0,3,1,10,'ready','c','c','t','',NULL,?1)",
        [WHEN],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO job (job_id, queue, job_type, payload_ref, state, priority, attempts, max_attempts, backoff_base_delay_seconds, backoff_max_delay_seconds, idempotency_key, correlation_id, causation_id, trace_id, sttp_input_node_id, lease_owner, lease_expires_at, scheduled_at, started_at) VALUES ('inflight','default','work','{}','running',10,1,3,1,10,'inflight','c','c','t','','worker',?1,?2,?2)",
        [WHEN + 1, WHEN + 60_000_000_000],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO job (job_id, queue, job_type, payload_ref, state, priority, attempts, max_attempts, backoff_base_delay_seconds, backoff_max_delay_seconds, idempotency_key, correlation_id, causation_id, trace_id, sttp_input_node_id, scheduled_at, finished_at) VALUES ('done','default','work','{}','succeeded',0,1,3,1,10,'done','c','c','t','',?1,?1)",
        [WHEN],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO job_attempt (attempt_id, job_id, attempt_number, worker_id, started_at, finished_at, outcome) VALUES ('inflight-attempt-1','inflight',1,'worker',?1,?1,'retryable_failure')",
        [WHEN],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO outbox_event (event_id, status, publish_attempts, event_type, job_id, correlation_id, causation_id, trace_id, sttp_input_node_id, occurred_at, message) VALUES ('evt-1','pending',0,'job_published','ready','c','c','t','',?1,'hello')",
        [WHEN],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO durable_wait (wait_id, job_id, signal_type, correlation_key, status, created_at, updated_at, consumed_signal_ids_json) VALUES ('wait-1','ready','kind','key','pending',?1,?1,'[]')",
        [WHEN],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO durable_signal (signal_id, signal_type, correlation_key, payload_json, created_at) VALUES ('sig-1','kind','key','{}',?1)",
        [WHEN],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO job_continuation (continuation_id, parent_job_id, trigger, status, child_spec_json, created_at) VALUES ('edge-1','ready','succeeded','pending',?1,?2)",
        (CHILD_SPEC, WHEN),
    )
    .unwrap();
    conn.execute(
        "INSERT INTO recurring_definition (recurring_id, queue, job_type, payload_template_ref, cron_expr, timezone, jitter_seconds, enabled, max_attempts, next_run_at) VALUES ('every','default','work','{}','0 * * * * *','UTC',0,1,3,?1)",
        [WHEN],
    )
    .unwrap();
}

fn success() -> JobExecutionOutcome {
    JobExecutionOutcome::Success {
        output_provenance: None,
        execution_id: None,
        diagnostics: None,
    }
}

async fn assert_upgraded_runtime(rt: &SqliteRuntime) {
    let inflight = rt.job_store.get("inflight").await.unwrap().unwrap();
    assert_eq!(inflight.state, JobState::Enqueued);
    assert!(inflight.lease_owner.is_none());
    assert_eq!(inflight.execution_epoch, 0);
    assert_eq!(inflight.incarnation, 1);
    assert_eq!(inflight.terminal_cycle, 1);
    assert_eq!(inflight.version, 0);
    assert_eq!(inflight.attempts, 1);
    assert_eq!(
        rt.job_attempt_store
            .list_by_job_id("inflight")
            .await
            .unwrap()
            .len(),
        1
    );
    let done = rt.job_store.get("done").await.unwrap().unwrap();
    assert_eq!(done.state, JobState::Succeeded);
    assert!(
        rt.job_store
            .execution_receipt("done", 0)
            .await
            .unwrap()
            .is_none()
    );
    let wait = rt.wait_store.get_wait("wait-1").await.unwrap().unwrap();
    assert_eq!(wait.incarnation, 1);
    let signal = rt.wait_store.get_signal("sig-1").await.unwrap().unwrap();
    assert!(signal.target_incarnations.is_none());
    let edge = rt.continuation_store.get("edge-1").await.unwrap().unwrap();
    assert_eq!(edge.parent_incarnation, 1);
    assert_eq!(edge.parent_terminal_cycle, 1);
    assert_eq!(edge.parent_version_floor, Some(0));
    assert_eq!(
        rt.outbox_store.list_by_job_id("ready").await.unwrap().len(),
        1
    );
    assert_eq!(rt.recurring_store.list().await.unwrap().len(), 1);

    let leased = rt
        .job_store
        .lease_due("default", "worker", Utc::now(), 120, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.id, "ready");
    assert!(leased.execution_epoch > 0);
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, success())
            .await
            .unwrap(),
        crate::domain::runtime::execution::CommitResult::Committed
    );
    assert_eq!(
        rt.job_store.get("ready").await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    assert!(
        rt.job_store
            .execution_receipt("ready", leased.execution_epoch)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn v1_file_upgrades_and_runs_end_to_end() {
    let path = temp_db("v1");
    {
        let conn = Connection::open(&path).unwrap();
        apply_until(&conn, 1);
        seed_v1(&conn);
    }
    let rt = SqliteRuntime::open_path(path.to_str().unwrap()).unwrap();
    assert_upgraded_runtime(&rt).await;
    assert_eq!(count_table(rt.job_store.session(), "job"), 0);
    assert_eq!(count_table(rt.job_store.session(), "durable_wait"), 0);
    drop(rt);
    let rt = SqliteRuntime::open_path(path.to_str().unwrap()).unwrap();
    assert_eq!(
        rt.job_store.get("ready").await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn ledger_file_keeps_grant_and_sequence() {
    let path = temp_db("ledger");
    {
        let conn = Connection::open(&path).unwrap();
        apply_until(&conn, 2);
        let payload = ledger_payload();
        conn.execute(
            "INSERT INTO runtime_safety_ledger (id, format, payload) VALUES (1, 1, ?1)",
            [payload],
        )
        .unwrap();
    }
    let rt = SqliteRuntime::open_path(path.to_str().unwrap()).unwrap();
    let job = rt.job_store.get("kept").await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Leased);
    assert_eq!(job.lease_owner.as_deref(), Some("worker"));
    assert_eq!(job.execution_epoch, 2);
    assert_eq!(job.version, 4);
    assert_eq!(job.incarnation, 3);
    assert_eq!(job.terminal_cycle, 1);
    assert_eq!(
        rt.job_store
            .commit_execution(&job, success())
            .await
            .unwrap(),
        crate::domain::runtime::execution::CommitResult::Committed
    );
    let next: i64 = rt
        .job_store
        .session()
        .query_opt("SELECT next FROM runtime_id_alloc", [], |row| row.get(0))
        .unwrap()
        .unwrap();
    assert!(next >= 40, "imported sequence was {next}");
    assert_eq!(
        count_table(rt.job_store.session(), "runtime_safety_ledger"),
        0
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn executor_v1_database_upgrades_inside_apply_schema() {
    let exec = Arc::new(RusqliteExecutor::open_in_memory().unwrap());
    seed_executor_v1(&exec);
    apply_schema(exec.as_ref()).unwrap();
    let store =
        crate::infrastructure::sqlite::executor_job_store::ExecutorJobStore::new(exec.clone());
    let inflight = store.get("inflight").await.unwrap().unwrap();
    assert_eq!(inflight.state, JobState::Enqueued);
    assert_eq!(inflight.incarnation, 1);
    assert!(inflight.lease_owner.is_none());
    let leased = store
        .lease_due("default", "worker", Utc::now(), 120, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.id, "ready");
    store.commit_execution(&leased, success()).await.unwrap();
    assert_eq!(
        store.get("ready").await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    let version = exec
        .query(
            "SELECT CAST(COALESCE(MAX(version), 0) AS TEXT) FROM stasis_schema_migration",
            &[],
        )
        .unwrap();
    assert_eq!(version[0].i64(0).unwrap(), 3);
    let leftover = exec
        .query("SELECT CAST(COUNT(*) AS TEXT) FROM job", &[])
        .unwrap();
    assert_eq!(leftover[0].i64(0).unwrap(), 0);
}

#[test]
fn upgrade_refuses_unreconcilable_sources() {
    assert_open_refuses("both", |conn| {
        apply_until(conn, 2);
        seed_v1(conn);
        conn.execute(
            "INSERT INTO runtime_safety_ledger (id, format, payload) VALUES (1, 1, ?1)",
            [ledger_payload()],
        )
        .unwrap();
    });
    assert_open_refuses("format", |conn| {
        apply_until(conn, 2);
        conn.execute(
            "INSERT INTO runtime_safety_ledger (id, format, payload) VALUES (1, 9, '{}')",
            [],
        )
        .unwrap();
    });
    assert_open_refuses("json", |conn| {
        apply_until(conn, 2);
        conn.execute(
            "INSERT INTO runtime_safety_ledger (id, format, payload) VALUES (1, 1, '{')",
            [],
        )
        .unwrap();
    });
    assert_open_refuses("orphan-wait", |conn| {
        apply_until(conn, 1);
        conn.execute(
            "INSERT INTO durable_wait (wait_id, job_id, signal_type, correlation_key, status, created_at, updated_at, consumed_signal_ids_json) VALUES ('w','missing','k','c','pending',0,0,'[]')",
            [],
        )
        .unwrap();
    });
    assert_open_refuses("orphan-edge", |conn| {
        apply_until(conn, 1);
        conn.execute(
            "INSERT INTO job_continuation (continuation_id, parent_job_id, trigger, status, child_spec_json, created_at) VALUES ('e','missing','succeeded','pending',?1,0)",
            [CHILD_SPEC],
        )
        .unwrap();
    });
    assert_open_refuses("state", |conn| {
        apply_until(conn, 1);
        conn.execute(
            "INSERT INTO job (job_id, queue, job_type, payload_ref, state, priority, attempts, max_attempts, backoff_base_delay_seconds, backoff_max_delay_seconds, idempotency_key, correlation_id, causation_id, trace_id, sttp_input_node_id, scheduled_at) VALUES ('bad','default','work','{}','nope',0,0,1,1,1,'bad','c','c','t','',0)",
            [],
        )
        .unwrap();
    });
}

#[test]
fn executor_refuses_dual_sources_without_writing_version_three() {
    let exec = Arc::new(RusqliteExecutor::open_in_memory().unwrap());
    seed_executor_v1(&exec);
    exec.execute(
        "INSERT INTO runtime_safety_ledger (id, format, payload) VALUES (1, 1, ?1)",
        &[crate::infrastructure::sqlite::SqlValue::text(
            ledger_payload(),
        )],
    )
    .unwrap();
    let error = apply_schema(exec.as_ref()).unwrap_err().to_string();
    assert!(
        error.contains("refusing"),
        "unexpected migration error: {error}"
    );
    let version = exec
        .query(
            "SELECT CAST(COALESCE(MAX(version), 0) AS TEXT) FROM stasis_schema_migration",
            &[],
        )
        .unwrap();
    assert_eq!(version[0].i64(0).unwrap(), 2);
    let jobs = exec
        .query("SELECT CAST(COUNT(*) AS TEXT) FROM job", &[])
        .unwrap();
    assert_eq!(jobs[0].i64(0).unwrap(), 3);
}

fn assert_open_refuses(label: &str, seed: impl FnOnce(&Connection)) {
    let path = temp_db(label);
    {
        let conn = Connection::open(&path).unwrap();
        seed(&conn);
    }
    let error = match SqliteRuntime::open_path(path.to_str().unwrap()) {
        Ok(_) => panic!("{label} database was accepted"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("refusing") || error.contains("unknown") || error.contains("not readable"),
        "{label}: {error}"
    );
    let conn = Connection::open(&path).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM stasis_schema_migration",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(version < 3, "{label} recorded version {version}");
    let _ = std::fs::remove_file(Path::new(&path));
}

fn count_table(session: crate::infrastructure::sqlite::SqliteSession, table: &str) -> i64 {
    session
        .query_opt(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
        .unwrap()
}

fn seed_executor_v1(exec: &RusqliteExecutor) {
    exec.execute(
        "CREATE TABLE IF NOT EXISTS stasis_schema_migration (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)",
        &[],
    )
    .unwrap();
    for (version, sql) in MIGRATIONS {
        if *version > 2 {
            break;
        }
        for statement in crate::infrastructure::sqlite::script::split_sql_script(sql).unwrap() {
            exec.execute(&statement, &[]).unwrap();
        }
        exec.execute(
            "INSERT INTO stasis_schema_migration (version, applied_at) VALUES (?1, 0)",
            &[crate::infrastructure::sqlite::SqlValue::from(*version)],
        )
        .unwrap();
    }
    let file = temp_db("copy-v1");
    {
        let conn = Connection::open(&file).unwrap();
        apply_until(&conn, 1);
        seed_v1(&conn);
        for table in [
            "job",
            "job_attempt",
            "outbox_event",
            "durable_wait",
            "durable_signal",
            "job_continuation",
            "recurring_definition",
        ] {
            let mut stmt = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let columns = stmt.column_count();
            let names: Vec<String> = stmt
                .column_names()
                .iter()
                .map(|name| (*name).to_string())
                .collect();
            let rows = stmt
                .query_map([], |row| {
                    let mut values = Vec::new();
                    for index in 0..columns {
                        values.push(row.get::<_, rusqlite::types::Value>(index)?);
                    }
                    Ok(values)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            drop(stmt);
            for values in rows {
                let placeholders = (1..=columns)
                    .map(|n| format!("?{n}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "INSERT INTO {table} ({}) VALUES ({placeholders})",
                    names.join(",")
                );
                let params = values
                    .into_iter()
                    .map(|value| match value {
                        rusqlite::types::Value::Null => {
                            crate::infrastructure::sqlite::SqlValue::Null
                        }
                        rusqlite::types::Value::Integer(value) => {
                            crate::infrastructure::sqlite::SqlValue::Integer(value)
                        }
                        rusqlite::types::Value::Real(value) => {
                            crate::infrastructure::sqlite::SqlValue::Text(value.to_string())
                        }
                        rusqlite::types::Value::Text(value) => {
                            crate::infrastructure::sqlite::SqlValue::Text(value)
                        }
                        rusqlite::types::Value::Blob(value) => {
                            crate::infrastructure::sqlite::SqlValue::Text(
                                String::from_utf8_lossy(&value).into_owned(),
                            )
                        }
                    })
                    .collect::<Vec<_>>();
                exec.execute(&sql, &params).unwrap();
            }
        }
    }
    let _ = std::fs::remove_file(file);
}

fn ledger_payload() -> String {
    r#"{"state":{"jobs":{"kept":{"id":"kept","queue":"default","job_type":"work","payload_ref":"{}","state":"Leased","priority":0,"attempts":0,"max_attempts":3,"backoff_policy":{"base_delay_seconds":1,"max_delay_seconds":10},"idempotency_key":"kept","correlation_id":"c","causation_id":"c","trace_id":"t","input_provenance":null,"output_provenance":null,"placement":{},"execution_epoch":2,"incarnation":3,"terminal_cycle":1,"wakeup_pending":false,"version":4,"lease_owner":"worker","lease_expires_at":"2099-01-01T00:00:00Z","heartbeat_at":"2024-01-01T00:00:00Z","scheduled_at":"2024-01-01T00:00:00Z","started_at":"2024-01-01T00:00:00Z","finished_at":null,"last_error":null,"progress_json":null},"job-12":{"id":"job-12","queue":"other","job_type":"work","payload_ref":"{}","state":"Succeeded","priority":0,"attempts":1,"max_attempts":3,"backoff_policy":{"base_delay_seconds":1,"max_delay_seconds":10},"idempotency_key":"job-12","correlation_id":"c","causation_id":"c","trace_id":"t","input_provenance":null,"output_provenance":null,"placement":{},"execution_epoch":1,"incarnation":1,"terminal_cycle":1,"wakeup_pending":false,"version":2,"lease_owner":null,"lease_expires_at":null,"heartbeat_at":null,"scheduled_at":"2024-01-01T00:00:00Z","started_at":"2024-01-01T00:00:00Z","finished_at":"2024-01-01T00:00:01Z","last_error":null,"progress_json":null}},"continuations":{},"staged":[],"waits":{},"signals":{},"epochs":{"kept":2,"job-12":1},"attempts":{},"events":{},"receipts":[],"cleanup":[],"cancellations":{}},"recurring":{},"sequence":40}"#.to_string()
}
