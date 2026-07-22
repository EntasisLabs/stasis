//! `stasisd` — declarative Stasis engine (ADR-0008).

mod config;
mod error;
mod job_types;
mod model;
mod parse;
mod provenance;
mod reconcile;
mod validate;

use std::env;
use std::process::ExitCode;

use stasis::domain::agent::AGENT_ENVELOPE_SCHEMA_VERSION_V1;
use stasis::sdk::runtime_sdk::RuntimeSdk;

use crate::config::{load_desired_state, CliArgs};
use crate::error::StasisdError;
use crate::model::API_VERSION;
use crate::provenance::{managed_recurring_id, MANAGED_BY, MANAGED_ID_PREFIX};
use crate::reconcile::reconcile;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(StasisdError::Usage(message)) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
        Err(StasisdError::Validation(message)) => {
            eprintln!("stasisd validation error: {message}");
            ExitCode::from(2)
        }
        Err(err) => {
            eprintln!("stasisd error: {err}");
            ExitCode::from(3)
        }
    }
}

async fn run() -> Result<(), StasisdError> {
    let args = CliArgs::parse(env::args().skip(1))?;
    let desired = load_desired_state(&args.config_path)?;

    if args.strict && !desired.diagnostics.is_empty() {
        return Err(StasisdError::Validation(desired.diagnostics.join("; ")));
    }

    for diagnostic in &desired.diagnostics {
        eprintln!("stasisd warning: {diagnostic}");
    }

    println!(
        "stasisd: api={API_VERSION} managed_by={MANAGED_BY} managed_id_prefix={MANAGED_ID_PREFIX} example_id={} agent_envelope_schema_v{AGENT_ENVELOPE_SCHEMA_VERSION_V1}",
        managed_recurring_id("example")
    );
    println!(
        "stasisd: loaded config from {} (schedules={}, sources={}, diagnostics={}, once={})",
        args.config_path.display(),
        desired.schedules.len(),
        desired.sources.len(),
        desired.diagnostics.len(),
        args.once
    );

    let runtime = RuntimeSdk::in_memory()
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;

    let report = reconcile(&runtime, &desired).await?;
    println!(
        "stasisd: reconcile created={} updated={} drained={} orphaned={} unchanged={} cancel_skipped={}",
        report.created.len(),
        report.updated.len(),
        report.drained.len(),
        report.orphaned.len(),
        report.unchanged.len(),
        report.skipped_cancel.len()
    );

    if !args.once {
        eprintln!("stasisd: watch/tick loop not implemented yet; use --once for Phase 1");
    }

    Ok(())
}
