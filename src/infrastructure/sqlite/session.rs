use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior};

use crate::domain::errors::Result;

use super::codec::{encode_time, sql_err};
use super::executor::{SqlExecutor, SqlRow, SqlValue};
use super::row_engine::SqlIo;
use super::runtime_import;
use super::schema::MIGRATIONS;

/// Shared native SQLite connection.
///
/// Stores clone this handle. One mutex serializes statements on the connection.
/// Lease and claim updates still use `BEGIN IMMEDIATE` so a second process opening
/// the same file cannot observe a torn compare-and-swap.
#[derive(Clone)]
pub struct SqliteSession {
    conn: Arc<Mutex<Connection>>,
    /// Filesystem path, or a `file:…?mode=memory&cache=shared` URI.
    ///
    /// `:memory:` is rewritten to a named shared-cache URI so
    /// `locus-sqlite-adapter` can open a second connection to the same
    /// database. The adapter owns that connection; it cannot take this one.
    path: String,
}

static MEMORY_NAMES: AtomicU64 = AtomicU64::new(1);

fn open_path(requested: &str) -> String {
    if requested == ":memory:" {
        let id = MEMORY_NAMES.fetch_add(1, Ordering::Relaxed);
        format!("file:stasis-mem-{id}?mode=memory&cache=shared")
    } else {
        requested.to_string()
    }
}

fn is_memory_path(path: &str) -> bool {
    path == ":memory:" || path.contains("mode=memory")
}

impl SqliteSession {
    pub fn open(path: &str) -> Result<Self> {
        let path = open_path(path);
        let memory = is_memory_path(&path);
        if !memory
            && let Some(parent) = Path::new(&path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|err| sql_err("create sqlite directory", err))?;
        }

        let conn = Connection::open(&path).map_err(|err| sql_err("open sqlite", err))?;

        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|err| sql_err("set sqlite busy timeout", err))?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(|err| sql_err("enable sqlite foreign keys", err))?;
        if !memory {
            conn.execute_batch("PRAGMA journal_mode = WAL;")
                .map_err(|err| sql_err("enable sqlite wal", err))?;
        }

        let session = Self {
            conn: Arc::new(Mutex::new(conn)),
            path,
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
                tx.execute_batch(sql).map_err(|err| {
                    sql_err(format!("apply sqlite migration {version}").as_str(), err)
                })?;
                if *version == 3 {
                    let mut io = TxIo { tx };
                    runtime_import::import_legacy_sources(&mut io)?;
                }
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

    /// Path passed to SQLite. In-memory sessions use a shared-cache URI.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Driver-free view of this connection for stores that also run on Durable Objects.
    pub fn executor(&self) -> Arc<dyn SqlExecutor> {
        Arc::new(super::rusqlite_executor::RusqliteExecutor::from_shared(
            self.conn.clone(),
        ))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| sql_err("sqlite connection", "lock poisoned"))
    }
}

/// Statement IO on an open rusqlite transaction. `Transaction` is not `Sync`,
/// so this is not a [`super::executor::SqlExecutor`].
pub(crate) struct TxIo<'a, 'conn> {
    pub tx: &'a Transaction<'conn>,
}

impl SqlIo for TxIo<'_, '_> {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let bound = bind(params);
        let refs: Vec<&dyn rusqlite::types::ToSql> =
            bound.iter().map(|value| value.as_ref()).collect();
        let changed = self
            .tx
            .execute(sql, rusqlite::params_from_iter(refs))
            .map_err(|err| sql_err("sqlite execute", err))?;
        Ok(u64::try_from(changed).unwrap_or(0))
    }

    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        let mut statement = self
            .tx
            .prepare(sql)
            .map_err(|err| sql_err("sqlite prepare", err))?;
        let bound = bind(params);
        let refs: Vec<&dyn rusqlite::types::ToSql> =
            bound.iter().map(|value| value.as_ref()).collect();
        let mut rows = statement
            .query(rusqlite::params_from_iter(refs))
            .map_err(|err| sql_err("sqlite query", err))?;
        let mut out = Vec::new();
        loop {
            match rows.next() {
                Ok(Some(row)) => out.push(read_row(row)?),
                Ok(None) => break,
                Err(err) => return Err(sql_err("sqlite read row", err)),
            }
        }
        Ok(out)
    }
}

fn bind(params: &[SqlValue]) -> Vec<Box<dyn rusqlite::types::ToSql>> {
    params
        .iter()
        .map(|value| -> Box<dyn rusqlite::types::ToSql> {
            match value {
                SqlValue::Null => Box::new(rusqlite::types::Null),
                SqlValue::Integer(value) => Box::new(*value),
                SqlValue::Text(value) => Box::new(value.clone()),
            }
        })
        .collect()
}

fn read_row(row: &rusqlite::Row<'_>) -> Result<SqlRow> {
    use rusqlite::types::ValueRef;
    let count = row.as_ref().column_count();
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let value = row
            .get_ref(index)
            .map_err(|err| sql_err("sqlite column", err))?;
        values.push(match value {
            ValueRef::Null => SqlValue::Null,
            ValueRef::Integer(value) => SqlValue::Integer(value),
            ValueRef::Real(value) => SqlValue::Text(value.to_string()),
            ValueRef::Text(bytes) | ValueRef::Blob(bytes) => {
                SqlValue::Text(String::from_utf8_lossy(bytes).into_owned())
            }
        });
    }
    Ok(SqlRow { values })
}
