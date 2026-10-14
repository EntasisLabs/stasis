//! Driver-free SQLite session used by the Durable Object stores.
//!
//! Native rusqlite and the Workers JavaScript bridge both implement
//! [`SqlExecutor`]. Statements are synchronous: Durable Object `sql.exec` does
//! not return a promise, and the store futures therefore stay `Send` without
//! holding a JavaScript handle across an await. The store impls use
//! `stasis::async_store!` (`?Send` on wasm32) so they match `StoreRuntime`.

use crate::domain::errors::Result;

use super::codec::sql_err;

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Text(String),
}

impl SqlValue {
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }
}

impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<Option<i64>> for SqlValue {
    fn from(value: Option<i64>) -> Self {
        match value {
            Some(value) => Self::Integer(value),
            None => Self::Null,
        }
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<Option<String>> for SqlValue {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(value) => Self::Text(value),
            None => Self::Null,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SqlRow {
    pub values: Vec<SqlValue>,
}

impl SqlRow {
    pub fn get(&self, index: usize) -> Result<&SqlValue> {
        self.values.get(index).ok_or_else(|| {
            sql_err(
                "sqlite row",
                format!("column {index} missing (len {})", self.values.len()),
            )
        })
    }

    pub fn text(&self, index: usize) -> Result<String> {
        match self.get(index)? {
            SqlValue::Text(value) => Ok(value.clone()),
            SqlValue::Integer(value) => Ok(value.to_string()),
            SqlValue::Null => Err(sql_err("sqlite text", format!("column {index} is null"))),
        }
    }

    pub fn text_opt(&self, index: usize) -> Result<Option<String>> {
        match self.get(index)? {
            SqlValue::Null => Ok(None),
            SqlValue::Text(value) => Ok(Some(value.clone())),
            SqlValue::Integer(value) => Ok(Some(value.to_string())),
        }
    }

    pub fn i64(&self, index: usize) -> Result<i64> {
        match self.get(index)? {
            SqlValue::Integer(value) => Ok(*value),
            SqlValue::Text(value) => value
                .parse::<i64>()
                .map_err(|err| sql_err("sqlite integer", err)),
            SqlValue::Null => Err(sql_err("sqlite integer", format!("column {index} is null"))),
        }
    }

    pub fn i64_opt(&self, index: usize) -> Result<Option<i64>> {
        match self.get(index)? {
            SqlValue::Null => Ok(None),
            SqlValue::Text(value) if value.is_empty() => Ok(None),
            other => {
                let row = SqlRow {
                    values: vec![other.clone()],
                };
                Ok(Some(row.i64(0)?))
            }
        }
    }

    pub fn f64(&self, index: usize) -> Result<f64> {
        match self.get(index)? {
            SqlValue::Integer(value) => Ok(*value as f64),
            SqlValue::Text(value) => value
                .parse::<f64>()
                .map_err(|err| sql_err("sqlite real", err)),
            SqlValue::Null => Err(sql_err("sqlite real", format!("column {index} is null"))),
        }
    }

    pub fn f64_opt(&self, index: usize) -> Result<Option<f64>> {
        match self.get(index)? {
            SqlValue::Null => Ok(None),
            SqlValue::Text(value) if value.is_empty() => Ok(None),
            other => {
                let row = SqlRow {
                    values: vec![other.clone()],
                };
                Ok(Some(row.f64(0)?))
            }
        }
    }
}

/// Synchronous SQL session.
///
/// `transaction` must run `body` on the same thread before returning, and must
/// roll the transaction back when `body` returns an error. Nested calls join
/// the transaction already in progress. Durable Object hosts map this onto
/// `ctx.storage.transactionSync` rather than SQL `BEGIN`.
pub trait SqlExecutor: Send + Sync {
    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64>;
    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>>;
    fn transaction(&self, body: &mut dyn FnMut() -> Result<()>) -> Result<()>;
}

pub fn query_one(
    executor: &dyn SqlExecutor,
    sql: &str,
    params: &[SqlValue],
) -> Result<Option<SqlRow>> {
    Ok(executor.query(sql, params)?.into_iter().next())
}

/// Run `body` inside [`SqlExecutor::transaction`] and keep its value.
///
/// The closure's error is the one returned to the caller. The transaction
/// itself only sees a rollback signal.
pub fn transaction_result<T>(
    executor: &dyn SqlExecutor,
    body: &mut dyn FnMut() -> Result<T>,
) -> Result<T> {
    let mut outcome: Option<Result<T>> = None;
    let tx = executor.transaction(&mut || match body() {
        Ok(value) => {
            outcome = Some(Ok(value));
            Ok(())
        }
        Err(err) => {
            outcome = Some(Err(err));
            Err(sql_err("sqlite transaction", "aborted"))
        }
    });
    match (tx, outcome) {
        // A commit failure must never masquerade as a successful publication.
        (Err(_), Some(Err(body_error))) => Err(body_error),
        (Err(commit_error), _) => Err(commit_error),
        (Ok(()), Some(result)) => result,
        (Ok(()), None) => Err(sql_err("sqlite transaction", "body was not invoked")),
    }
}
