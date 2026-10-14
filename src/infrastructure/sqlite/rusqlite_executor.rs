//! rusqlite-backed [`SqlExecutor`] for native tests of the Durable Object stores.
//!
//! The transaction callback re-enters `execute` / `query` on the same
//! connection. A thread-local pointer joins that callback to the open
//! `BEGIN IMMEDIATE` so the mutex is not locked twice.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, Row, TransactionBehavior, params_from_iter, types::ValueRef};

use crate::domain::errors::Result;

use super::codec::sql_err;
use super::executor::{SqlExecutor, SqlRow, SqlValue};

static NEXT_EXECUTOR_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct TxEnter {
    id: u64,
    conn: *const Connection,
}

thread_local! {
    static OPEN_TX: Cell<Option<TxEnter>> = const { Cell::new(None) };
}

struct ClearTx {
    id: u64,
}

impl Drop for ClearTx {
    fn drop(&mut self) {
        OPEN_TX.with(|slot| {
            if slot.get().is_some_and(|enter| enter.id == self.id) {
                slot.set(None);
            }
        });
    }
}

pub struct RusqliteExecutor {
    id: u64,
    conn: Arc<Mutex<Connection>>,
}

impl RusqliteExecutor {
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|err| sql_err("open sqlite", err))?;
        Ok(Self::from_shared(Arc::new(Mutex::new(conn))))
    }

    /// Share a connection that [`SqliteSession`](super::session::SqliteSession) already owns.
    pub fn from_shared(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            id: NEXT_EXECUTOR_ID.fetch_add(1, Ordering::Relaxed),
            conn,
        }
    }

    fn with_conn<T>(&self, body: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        if let Some(enter) = OPEN_TX.with(|slot| slot.get())
            && enter.id == self.id
        {
            // Safety: `enter.conn` points at the `Transaction` connection created
            // by `transaction` on this thread. `ClearTx` clears the slot before
            // that transaction is dropped, and the callback does not send the
            // pointer to another thread. Only a shared reference is formed.
            return body(unsafe { &*enter.conn });
        }
        let guard = self
            .conn
            .lock()
            .map_err(|_| sql_err("sqlite connection", "lock poisoned"))?;
        body(&guard)
    }
}

impl SqlExecutor for RusqliteExecutor {
    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        self.with_conn(|conn| {
            let bound = bind(params);
            let refs: Vec<&dyn rusqlite::types::ToSql> =
                bound.iter().map(|value| value.as_ref()).collect();
            let changed = conn
                .execute(sql, params_from_iter(refs))
                .map_err(|err| sql_err("sqlite execute", err))?;
            Ok(u64::try_from(changed).unwrap_or(0))
        })
    }

    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        self.with_conn(|conn| {
            let mut statement = conn
                .prepare(sql)
                .map_err(|err| sql_err("sqlite prepare", err))?;
            let bound = bind(params);
            let refs: Vec<&dyn rusqlite::types::ToSql> =
                bound.iter().map(|value| value.as_ref()).collect();
            let mut rows = statement
                .query(params_from_iter(refs))
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
        })
    }

    fn transaction(&self, body: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        if OPEN_TX
            .with(|slot| slot.get())
            .is_some_and(|enter| enter.id == self.id)
        {
            return body();
        }
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| sql_err("sqlite connection", "lock poisoned"))?;
        let tx = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| sql_err("begin sqlite transaction", err))?;
        let conn_ptr: *const Connection = &*tx;
        OPEN_TX.with(|slot| {
            slot.set(Some(TxEnter {
                id: self.id,
                conn: conn_ptr,
            }))
        });
        let _clear = ClearTx { id: self.id };
        let result = body();
        drop(_clear);
        match result {
            Ok(()) => tx
                .commit()
                .map_err(|err| sql_err("commit sqlite transaction", err)),
            Err(err) => Err(err),
        }
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

fn read_row(row: &Row<'_>) -> Result<SqlRow> {
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
