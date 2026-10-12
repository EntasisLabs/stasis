//! Durable row store. The reviewed memory transitions run synchronously inside one
//! SQL transaction against the rows that operation needs. Nothing is visible
//! before that transaction commits.
use super::executor::{SqlExecutor, transaction_result};
use super::row_engine::{self, ExecIo, Scope};
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

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub(crate) state: NativeState,
    pub(crate) recurring: HashMap<String, RecurringDefinition>,
    pub(crate) sequence: u64,
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
    StasisError::PortFailure(format!("sqlite row store: {e}"))
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

    fn apply<T>(
        &self,
        io: &mut dyn row_engine::SqlIo,
        scope: &Scope,
        operation: impl FnOnce(&Views) -> Result<T>,
    ) -> Result<T> {
        let loaded = row_engine::load(io, scope)?;
        let jobs = InMemoryJobStore {
            state: Arc::new(RwLock::new(loaded.state)),
            clock: self.clock.clone(),
        };
        let recurring = InMemoryRecurringStore {
            defs: Arc::new(RwLock::new(loaded.recurring)),
        };
        let views = Views {
            jobs: jobs.clone(),
            recurring: recurring.clone(),
        };
        let result = operation(&views)?;
        let state = jobs.state.read().map_err(err)?.clone();
        let recurring = recurring.defs.read().map_err(err)?.clone();
        row_engine::flush(
            io,
            &row_engine::WorkingSet {
                before: loaded.before,
                state,
                recurring,
            },
        )?;
        Ok(result)
    }

    pub fn run<T>(&self, scope: Scope, operation: impl FnOnce(&Views) -> Result<T>) -> Result<T> {
        self.run_checked(scope, operation, || Ok(()))
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
            Scope::Commit {
                job_id: snapshot.id.clone(),
                epoch: snapshot.execution_epoch,
            },
            |v| {
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
    fn run_checked<T>(
        &self,
        scope: Scope,
        operation: impl FnOnce(&Views) -> Result<T>,
        check: impl FnOnce() -> Result<()>,
    ) -> Result<T> {
        match &self.backend {
            Backend::Executor(executor) => {
                let mut operation = Some(operation);
                let mut check = Some(check);
                transaction_result(executor.as_ref(), &mut || {
                    let op = operation
                        .take()
                        .ok_or_else(|| err("transaction callback invoked twice"))?;
                    let mut io = ExecIo(executor.as_ref());
                    let result = self.apply(&mut io, &scope, op)?;
                    check
                        .take()
                        .ok_or_else(|| err("transaction callback invoked twice"))?(
                    )?;
                    Ok(result)
                })
            }
            #[cfg(feature = "sqlite")]
            Backend::Native(session) => session.with_tx(|tx| {
                let mut io = super::session::TxIo { tx };
                let result = self.apply(&mut io, &scope, operation)?;
                check()?;
                Ok(result)
            }),
        }
    }
    pub fn next_id(&self, prefix: &str) -> Result<String> {
        match &self.backend {
            Backend::Executor(executor) => {
                let prefix = prefix.to_string();
                transaction_result(executor.as_ref(), &mut || {
                    let mut io = ExecIo(executor.as_ref());
                    row_engine::allocate_id(&mut io, &prefix)
                })
            }
            #[cfg(feature = "sqlite")]
            Backend::Native(session) => session.with_tx(|tx| {
                let mut io = super::session::TxIo { tx };
                row_engine::allocate_id(&mut io, prefix)
            }),
        }
    }
    pub fn prune_retained(
        &self,
        receipt_cutoff: i64,
        cleanup_cutoff: i64,
    ) -> Result<(usize, usize)> {
        match &self.backend {
            Backend::Executor(executor) => transaction_result(executor.as_ref(), &mut || {
                let mut io = ExecIo(executor.as_ref());
                row_engine::prune_retained(&mut io, receipt_cutoff, cleanup_cutoff)
            }),
            #[cfg(feature = "sqlite")]
            Backend::Native(session) => session.with_tx(|tx| {
                let mut io = super::session::TxIo { tx };
                row_engine::prune_retained(&mut io, receipt_cutoff, cleanup_cutoff)
            }),
        }
    }
}
