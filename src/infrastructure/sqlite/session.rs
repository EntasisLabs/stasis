use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior};

use crate::domain::errors::Result;

use super::codec::{encode_time, sql_err};
use super::schema::MIGRATIONS;

/// Shared native SQLite connection.
///
/// Stores clone this handle. One mutex serializes statements on the connection.
/// Lease and claim updates still use `BEGIN IMMEDIATE` so a second process opening
/// the same file cannot observe a torn compare-and-swap.
#[derive(Clone)]
pub struct SqliteSession {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteSession {
    pub fn open(path: &str) -> Result<Self> {
        if path != ":memory:"
            && let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|err| sql_err("create sqlite directory", err))?;
        }

        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(path)
        }
        .map_err(|err| sql_err("open sqlite", err))?;

        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|err| sql_err("set sqlite busy timeout", err))?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(|err| sql_err("enable sqlite foreign keys", err))?;
        if path != ":memory:" {
            conn.execute_batch("PRAGMA journal_mode = WAL;")
                .map_err(|err| sql_err("enable sqlite wal", err))?;
        }

        let session = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        session.migrate()?;
        Ok(session)
    }

    pub fn migrate(&self) -> Result<()> {
        self.with_tx(|tx| {
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS stasis_schema_migration (
                    version INTEGER PRIMARY KEY,
                    applied_at INTEGER NOT NULL
                );",
            )
            .map_err(|err| sql_err("create schema migration table", err))?;

            let current: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(version), 0) FROM stasis_schema_migration",
                    [],
                    |row| row.get(0),
                )
                .map_err(|err| sql_err("read schema version", err))?;

            for (version, sql) in MIGRATIONS {
                if *version <= current {
                    continue;
                }
                tx.execute_batch(sql)
                    .map_err(|err| sql_err(format!("apply sqlite migration {version}").as_str(), err))?;
                tx.execute(
                    "INSERT INTO stasis_schema_migration (version, applied_at) VALUES (?1, ?2)",
                    rusqlite::params![version, encode_time(Utc::now())],
                )
                .map_err(|err| sql_err("record schema migration", err))?;
            }
            Ok(())
        })
    }

    pub fn execute(&self, sql: &str, params: impl rusqlite::Params) -> Result<usize> {
        let conn = self.lock()?;
        conn.execute(sql, params)
            .map_err(|err| sql_err("sqlite execute", err))
    }

    pub fn query_map<T, P, F>(&self, sql: &str, params: P, mut map: F) -> Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(sql)
            .map_err(|err| sql_err("sqlite prepare", err))?;
        let rows = stmt
            .query_map(params, |row| map(row))
            .map_err(|err| sql_err("sqlite query", err))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|err| sql_err("sqlite read row", err))?);
        }
        Ok(out)
    }

    pub fn query_opt<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<Option<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.lock()?;
        conn.query_row(sql, params, map)
            .optional()
            .map_err(|err| sql_err("sqlite query", err))
    }

    pub fn with_tx<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| sql_err("begin sqlite transaction", err))?;
        let value = f(&tx)?;
        tx.commit()
            .map_err(|err| sql_err("commit sqlite transaction", err))?;
        Ok(value)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| sql_err("sqlite connection", "lock poisoned"))
    }
}
