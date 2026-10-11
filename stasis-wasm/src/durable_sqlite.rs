//! JavaScript bridge from a Durable Object's `ctx.storage.sql` into the
//! driver-free SQLite kernel.
//!
//! `worker::SqlStorage` (and any other `JsValue` handle) is `!Send`. The
//! handle lives in a thread-local and the executor type itself is a unit
//! struct, so it satisfies `StoreRuntime` on native and on wasm32. `sql.exec`
//! is synchronous, so the store futures never hold a JavaScript value across
//! an await.

use std::cell::RefCell;
use std::sync::Arc;

use serde::Serialize;
use wasm_bindgen::prelude::*;

use stasis::domain::errors::Result as StasisResult;
use stasis::domain::runtime::job::JobState;
use stasis::infrastructure::sqlite::bridge_codec::{decode_response, encode_params};
use stasis::infrastructure::sqlite::codec::sql_err;
use stasis::infrastructure::sqlite::{DurableSqliteKernel, SqlExecutor, SqlRow, SqlValue};

struct SqlBridge {
    exec: js_sys::Function,
    transaction: js_sys::Function,
}

thread_local! {
    static BRIDGE: RefCell<Option<SqlBridge>> = const { RefCell::new(None) };
}

/// Install the host functions that talk to `ctx.storage.sql`.
///
/// `exec(sql, paramsJson)` returns a JSON string `{rowsWritten, rows}`.
/// `transaction(callback)` runs `callback` inside `ctx.storage.transactionSync`
/// and throws when `callback` returns `false`, so the storage call rolls back.
#[wasm_bindgen]
pub fn install_durable_sql_bridge(exec: &js_sys::Function, transaction: &js_sys::Function) {
    BRIDGE.with(|slot| {
        *slot.borrow_mut() = Some(SqlBridge {
            exec: exec.clone(),
            transaction: transaction.clone(),
        });
    });
}

#[derive(Clone, Copy)]
struct JsSqlExecutor;

impl SqlExecutor for JsSqlExecutor {
    fn execute(&self, sql: &str, params: &[SqlValue]) -> StasisResult<u64> {
        Ok(call_exec(sql, params)?.rows_written)
    }

    fn query(&self, sql: &str, params: &[SqlValue]) -> StasisResult<Vec<SqlRow>> {
        Ok(call_exec(sql, params)?.rows)
    }

    fn transaction(&self, body: &mut dyn FnMut() -> StasisResult<()>) -> StasisResult<()> {
        let transaction = BRIDGE.with(|slot| {
            slot.borrow()
                .as_ref()
                .map(|bridge| bridge.transaction.clone())
        });
        let Some(transaction) = transaction else {
            return Err(sql_err(
                "durable sql",
                "install_durable_sql_bridge was not called",
            ));
        };
        let mut outcome: Option<StasisResult<()>> = None;
        let outcome_ptr: *mut Option<StasisResult<()>> = &mut outcome;
        let body_ptr: *mut dyn FnMut() -> StasisResult<()> = body;
        let closure = Closure::wrap(Box::new(move || {
            // Safety: `transactionSync` invokes this closure before returning,
            // on the same thread. The pointers stay inside this stack frame.
            let result = unsafe { (*body_ptr)() };
            let ok = result.is_ok();
            unsafe { *outcome_ptr = Some(result) };
            ok
        }) as Box<dyn FnMut() -> bool>);
        let called = transaction.call1(&JsValue::NULL, closure.as_ref().unchecked_ref());
        drop(closure);
        match (outcome, called) {
            (Some(result), Ok(_)) => result,
            (Some(Err(err)), Err(_)) => Err(err),
            (Some(Ok(())), Err(err)) => Err(sql_err("durable sql transaction", js_error(err))),
            (None, Err(err)) => Err(sql_err("durable sql transaction", js_error(err))),
            (None, Ok(_)) => Ok(()),
        }
    }
}

fn call_exec(
    sql: &str,
    params: &[SqlValue],
) -> StasisResult<stasis::infrastructure::sqlite::bridge_codec::ExecResponse> {
    let exec = BRIDGE.with(|slot| slot.borrow().as_ref().map(|bridge| bridge.exec.clone()));
    let Some(exec) = exec else {
        return Err(sql_err(
            "durable sql",
            "install_durable_sql_bridge was not called",
        ));
    };
    let params_json = encode_params(params)?;
    let result = exec
        .call2(
            &JsValue::NULL,
            &JsValue::from_str(sql),
            &JsValue::from_str(&params_json),
        )
        .map_err(|err| sql_err("durable sql exec", js_error(err)))?;
    let text = result
        .as_string()
        .ok_or_else(|| sql_err("durable sql exec", "bridge must return a JSON string"))?;
    decode_response(&text)
}

fn js_error(err: JsValue) -> String {
    err.as_string()
        .or_else(|| {
            js_sys::Reflect::get(&err, &JsValue::from_str("message"))
                .ok()?
                .as_string()
        })
        .unwrap_or_else(|| "javascript exception".to_string())
}

fn js_err(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AcceptView {
    disposition: &'static str,
    job_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessView {
    processed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobView {
    job_id: String,
    queue: String,
    job_type: String,
    payload: String,
    state: &'static str,
    attempts: u32,
    idempotency_key: String,
    lease_owner: Option<String>,
    heartbeat_at: Option<String>,
}

fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Enqueued => "enqueued",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::DeadLetter => "dead_letter",
        JobState::Canceled => "canceled",
    }
}

/// Kernel bound to the installed Durable Object SQL bridge.
#[wasm_bindgen]
pub struct StasisDurableSqlite {
    inner: DurableSqliteKernel,
}

#[wasm_bindgen]
impl StasisDurableSqlite {
    #[wasm_bindgen(constructor)]
    pub fn open() -> Result<StasisDurableSqlite, JsValue> {
        let inner = DurableSqliteKernel::open(Arc::new(JsSqlExecutor)).map_err(js_err)?;
        Ok(Self { inner })
    }

    pub fn accept_job(&self, idempotency_key: &str, payload: &str) -> Result<String, JsValue> {
        let accept = self
            .inner
            .accept_job(idempotency_key, payload)
            .map_err(js_err)?;
        json_view(&AcceptView {
            disposition: if DurableSqliteKernel::is_duplicate(&accept) {
                "duplicate"
            } else {
                "accepted"
            },
            job_id: accept.job_id,
        })
    }

    pub fn process_once(&self, queue: &str, worker_id: &str) -> Result<String, JsValue> {
        let processed = self.inner.process_once(queue, worker_id).map_err(js_err)?;
        json_view(&ProcessView {
            processed: processed.is_some(),
            job_id: processed.as_ref().map(|job| job.job_id.clone()),
            state: processed.map(|job| job.state),
        })
    }

    pub fn get_job(&self, job_id: &str) -> Result<String, JsValue> {
        let job = self.inner.get_job(job_id).map_err(js_err)?;
        let Some(job) = job else {
            return Ok("null".to_string());
        };
        json_view(&JobView {
            job_id: job.id,
            queue: job.queue,
            job_type: job.job_type,
            payload: job.payload_ref,
            state: state_name(job.state),
            attempts: job.attempts,
            idempotency_key: job.idempotency_key,
            lease_owner: job.lease_owner,
            heartbeat_at: job
                .heartbeat_at
                .map(|stamp| stamp.timestamp_nanos_opt().unwrap_or(0).to_string()),
        })
    }

    pub fn save_workflow_state(
        &self,
        workflow_id: &str,
        source: &str,
        state_json: &str,
    ) -> Result<(), JsValue> {
        self.inner
            .save_grapheme_state(workflow_id, source, state_json)
            .map_err(js_err)
    }

    pub fn load_workflow_state(&self, workflow_id: &str) -> Result<String, JsValue> {
        match self
            .inner
            .load_grapheme_state(workflow_id)
            .map_err(js_err)?
        {
            Some(state) => Ok(state),
            None => Ok("null".to_string()),
        }
    }
}

fn json_view(value: &impl Serialize) -> Result<String, JsValue> {
    serde_json::to_string(value).map_err(|err| js_err(err))
}
