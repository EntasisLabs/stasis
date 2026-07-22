//! `stasisd` — declarative Stasis engine (ADR-0008).
//!
//! Phase 0 skeleton: parse CLI flags and load an empty config path successfully.

mod config;
mod error;
mod provenance;

use std::env;
use std::process::ExitCode;

use stasis::domain::agent::AGENT_ENVELOPE_SCHEMA_VERSION_V1;

use crate::config::{load_desired_state, API_VERSION, CliArgs};
use crate::error::StasisdError;
use crate::provenance::{managed_recurring_id, MANAGED_BY, MANAGED_ID_PREFIX};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
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

fn run() -> Result<(), StasisdError> {
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
        "stasisd: loaded config from {} (schedules={}, sources={}, once={})",
        args.config_path.display(),
        desired.schedules.len(),
        desired.sources.len(),
        args.once
    );

    if !args.once {
        // Phase 2 will run the watch/tick host. Phase 0 exits after one load.
        eprintln!("stasisd: watch/tick loop not implemented yet; use --once for Phase 0");
    }

    Ok(())
}
