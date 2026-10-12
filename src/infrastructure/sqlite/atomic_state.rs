//! Durable safety ledger. The reviewed memory transitions run synchronously inside
//! the SQL transaction; no job/effect becomes visible before its ledger commits.
//! This correctness-first adapter serializes the ledger, so its cost is linear in
//! retained state. It is not an unbounded/high-throughput storage implementation.
use super::executor::{SqlExecutor, SqlValue, transaction_result};
use crate::application::runtime::in_memory_runtime::{
    InMemoryJobStore, InMemoryRecurringStore, NativeState,
};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::infrastructure::runtime::system_clock::SystemClock;
use crate::ports::outbound::runtime::clock::Clock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};

pub(crate) mod map_entries {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::{collections::HashMap, hash::Hash};
    pub fn serialize<K: Serialize, V: Serialize, S: Serializer>(
        m: &HashMap<K, V>,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let mut entries = m
            .iter()
            .map(|(k, v)| Ok((serde_json::to_string(k).map_err(S::Error::custom)?, k, v)))
            .collect::<std::result::Result<Vec<_>, S::Error>>()?;
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
            .into_iter()
            .map(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .serialize(s)
    }
    pub fn deserialize<
        'de,
        K: Deserialize<'de> + Eq + Hash,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    >(
        d: D,
    ) -> std::result::Result<HashMap<K, V>, D::Error> {
        Ok(Vec::<(K, V)>::deserialize(d)?.into_iter().collect())
    }
}

const LOAD: &str = "SELECT payload FROM runtime_safety_ledger WHERE id = 1";
const SAVE: &str = "INSERT INTO runtime_safety_ledger(id,format,payload) VALUES(1,1,?1) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload";
const LEGACY: &str = "SELECT (SELECT COUNT(*) FROM job) + (SELECT COUNT(*) FROM job_attempt) + (SELECT COUNT(*) FROM outbox_event) + (SELECT COUNT(*) FROM durable_wait) + (SELECT COUNT(*) FROM durable_signal) + (SELECT COUNT(*) FROM job_continuation)";

#[derive(Default, Serialize, Deserialize)]
struct Ledger {
    state: NativeState,
    recurring: HashMap<String, RecurringDefinition>,
    sequence: u64,
}

#[derive(Clone)]
pub(crate) enum Backend {
    Executor(Arc<dyn SqlExecutor>),
    #[cfg(feature = "sqlite")]
    Native(super::session::SqliteSession),
}

#[derive(Clone)]
pub(crate) struct AtomicState {
    backend: Backend,
    clock: Arc<dyn Clock>,
}

pub(crate) struct Views {
    pub jobs: InMemoryJobStore,
    pub recurring: InMemoryRecurringStore,
}

fn err(e: impl std::fmt::Display) -> StasisError {
    StasisError::PortFailure(format!("runtime safety ledger: {e}"))
}

/// Async reference store operations must be immediately ready. Never block an
/// executor or suspend a SQL transaction around arbitrary user/handler futures.
pub(crate) fn ready<T>(f: impl Future<Output = Result<T>>) -> Result<T> {
    let mut f = Box::pin(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => Err(err("reference transition suspended; unsupported")),
    }
}

impl AtomicState {
    pub fn executor(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            backend: Backend::Executor(executor),
            clock: Arc::new(SystemClock),
        }
    }
    #[cfg(feature = "sqlite")]
    pub fn native(session: super::session::SqliteSession) -> Self {
        Self {
            backend: Backend::Native(session),
            clock: Arc::new(SystemClock),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    #[cfg(feature = "sqlite")]
    pub fn session(&self) -> &super::session::SqliteSession {
        match &self.backend {
            Backend::Native(s) => s,
            _ => panic!("not a native session"),
        }
    }

    fn transition<T>(
        &self,
        payload: Option<String>,
        legacy_count: i64,
        operation: impl FnOnce(&Views, &mut u64) -> Result<T>,
    ) -> Result<(T, String)> {
        if legacy_count != 0 {
            return Err(err(
                "legacy v1 runtime rows require explicit offline reconciliation; refusing to hide or reinterpret them",
            ));
        }
        let ledger: Ledger = match payload {
            Some(s) => serde_json::from_str(&s).map_err(err)?,
            None => Ledger::default(),
        };
        let mut sequence = ledger.sequence;
        let views = Views {
            jobs: InMemoryJobStore {
                state: Arc::new(RwLock::new(ledger.state)),
                clock: self.clock.clone(),
            },
            recurring: InMemoryRecurringStore {
                defs: Arc::new(RwLock::new(ledger.recurring)),
            },
        };
        let result = operation(&views, &mut sequence)?;
        let state = views.jobs.state.read().map_err(err)?;
        let recurring = views.recurring.defs.read().map_err(err)?;
        #[derive(Serialize)]
        struct Snapshot<'a> {
            state: &'a NativeState,
            recurring: &'a HashMap<String, RecurringDefinition>,
            sequence: u64,
        }
        let payload = serde_json::to_value(&Snapshot {
            state: &state,
            recurring: &recurring,
            sequence,
        })
        .map_err(err)?
        .to_string();
        Ok((result, payload))
    }

    pub fn run<T>(&self, operation: impl FnOnce(&Views) -> Result<T>) -> Result<T> {
        self.run_sequence(|v, _| operation(v))
    }
    /// Recheck the canonical admission after the SQL write, immediately before
    /// committing. An expiry at that boundary rolls back the entire publication.
    pub fn run_completion(
        &self,
        snapshot: &crate::domain::runtime::job::Job,
        operation: impl FnOnce(&Views) -> Result<crate::domain::runtime::execution::CommitResult>,
    ) -> Result<crate::domain::runtime::execution::CommitResult> {
        use crate::domain::runtime::execution::{CommitResult, ExecutionGrant};
        use crate::ports::outbound::runtime::job_store::JobStore;
        let admission = std::cell::RefCell::new(None);
        self.run_checked(
            |v, _| {
                let canonical = ready(v.jobs.get(&snapshot.id))?;
                let result = operation(v)?;
                if result == CommitResult::Committed {
                    *admission.borrow_mut() = canonical;
                }
                Ok(result)
            },
            || {
                if let Some(job) = admission.borrow().as_ref() {
                    let grant = ExecutionGrant::from_job(snapshot)
                        .ok_or_else(|| err("missing completion grant"))?;
                    if !grant.permits(&job, self.clock.now()) {
                        return Err(err(
                            "execution lease expired at commit; all effects rolled back",
                        ));
                    }
                }
                Ok(())
            },
        )
    }
    fn run_sequence<T>(&self, operation: impl FnOnce(&Views, &mut u64) -> Result<T>) -> Result<T> {
        self.run_checked(operation, || Ok(()))
    }
    fn run_checked<T>(
        &self,
        operation: impl FnOnce(&Views, &mut u64) -> Result<T>,
        check: impl FnOnce() -> Result<()>,
    ) -> Result<T> {
        match &self.backend {
            Backend::Executor(e) => {
                let mut operation = Some(operation);
                let mut check = Some(check);
                transaction_result(e.as_ref(), &mut || {
                    let format =
                        e.query("SELECT format FROM runtime_safety_ledger WHERE id=1", &[])?;
                    if let Some(row) = format.first() {
                        if row.i64(0)? != 1 {
                            return Err(err("unsupported ledger format"));
                        }
                    }
                    let rows = e.query(LOAD, &[])?;
                    let payload = rows.first().map(|r| r.text(0)).transpose()?;
                    let legacy = e
                        .query(LEGACY, &[])?
                        .first()
                        .ok_or_else(|| err("legacy count missing"))?
                        .i64(0)?;
                    let op = operation
                        .take()
                        .ok_or_else(|| err("transaction callback invoked twice"))?;
                    let original = payload.clone();
                    let (result, payload) = self.transition(payload, legacy, op)?;
                    if payload.len() > 1024 * 1024 {
                        return Err(err(
                            "executor ledger exceeds supported 1 MiB profile; no volatile fallback",
                        ));
                    }
                    if original.as_deref() != Some(payload.as_str()) {
                        e.execute(SAVE, &[SqlValue::Text(payload)])?;
                    }
                    check
                        .take()
                        .ok_or_else(|| err("transaction callback invoked twice"))?(
                    )?;
                    Ok(result)
                })
            }
            #[cfg(feature = "sqlite")]
            Backend::Native(s) => s.with_tx(|tx| {
                use rusqlite::OptionalExtension;
                let format: Option<i64> = tx
                    .query_row(
                        "SELECT format FROM runtime_safety_ledger WHERE id=1",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(err)?;
                if format.is_some_and(|f| f != 1) {
                    return Err(err("unsupported ledger format"));
                }
                let payload: Option<String> = tx
                    .query_row(LOAD, [], |r| r.get(0))
                    .optional()
                    .map_err(err)?;
                let legacy: i64 = tx.query_row(LEGACY, [], |r| r.get(0)).map_err(err)?;
                let original = payload.clone();
                let (result, payload) = self.transition(payload, legacy, operation)?;
                if original.as_deref() != Some(payload.as_str()) {
                    tx.execute(SAVE, [payload]).map_err(err)?;
                }
                check()?;
                Ok(result)
            }),
        }
    }
    pub fn next_id(&self, prefix: &str) -> Result<String> {
        self.run_sequence(|_, sequence| {
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| err("identifier sequence exhausted"))?;
            Ok(format!("{prefix}-{}", *sequence))
        })
    }
}
