//! Apply [`super::schema::MIGRATIONS`] through a [`SqlExecutor`].
//!
//! No `PRAGMA` and no SQL `BEGIN`: the executor's `transaction` method is the
//! only transaction boundary. An id sequence table is created beside the shared
//! schema because a Durable Object isolate loses process-local counters when it
//! is evicted.

use chrono::Utc;

use crate::domain::errors::Result;

use super::codec::{encode_time, sql_err};
use super::executor::{SqlExecutor, SqlValue, query_one};
use super::schema::MIGRATIONS;
use super::script::split_sql_script;

const MIGRATION_TABLE: &str = "CREATE TABLE IF NOT EXISTS stasis_schema_migration (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
)";

/// Allocate ids that survive isolate eviction. Not part of `MIGRATIONS`: the
/// native runtime keeps using its process-local generator.
const ID_SEQUENCE_TABLE: &str = "CREATE TABLE IF NOT EXISTS stasis_id_seq (
    prefix TEXT PRIMARY KEY,
    next INTEGER NOT NULL
)";

pub fn apply_schema(executor: &dyn SqlExecutor) -> Result<()> {
    executor.transaction(&mut || {
        executor.execute(MIGRATION_TABLE, &[])?;
        let current = schema_version(executor)?;
        for (version, sql) in MIGRATIONS {
            if *version <= current {
                continue;
            }
            for statement in split_sql_script(sql)? {
                executor.execute(&statement, &[]).map_err(|err| {
                    sql_err("apply sqlite migration", format!("v{version}: {err}"))
                })?;
            }
            executor.execute(
                "INSERT INTO stasis_schema_migration (version, applied_at) VALUES (?1, ?2)",
                &[
                    SqlValue::from(*version),
                    SqlValue::from(encode_time(Utc::now())),
                ],
            )?;
        }
        executor.execute(ID_SEQUENCE_TABLE, &[])?;
        Ok(())
    })
}

pub fn schema_version(executor: &dyn SqlExecutor) -> Result<i64> {
    let row = query_one(
        executor,
        "SELECT CAST(COALESCE(MAX(version), 0) AS TEXT) FROM stasis_schema_migration",
        &[],
    )?;
    match row {
        Some(row) => row.i64(0),
        None => Ok(0),
    }
}

pub fn next_sequence(executor: &dyn SqlExecutor, prefix: &str) -> Result<u64> {
    let row = query_one(
        executor,
        "INSERT INTO stasis_id_seq (prefix, next) VALUES (?1, 1)
         ON CONFLICT(prefix) DO UPDATE SET next = next + 1
         RETURNING CAST(next AS TEXT)",
        &[SqlValue::text(prefix)],
    )?;
    let Some(row) = row else {
        return Err(sql_err("sqlite id sequence", "insert returned no row"));
    };
    let value = row.i64(0)?;
    u64::try_from(value).map_err(|_| sql_err("sqlite id sequence", format!("negative id {value}")))
}
