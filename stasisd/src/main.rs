//! `stasisd` — declarative Stasis engine (ADR-0008).

mod config;
mod error;
mod host;
mod job_types;
mod model;
mod parse;
mod provenance;
mod reconcile;
mod runtime_config;
mod tick;
mod validate;
mod watch;

use std::env;
use std::process::ExitCode;

use stasis::config_prelude::bootstrap;
use stasis::domain::agent::AGENT_ENVELOPE_SCHEMA_VERSION_V1;
use stasis::prelude::StasisRuntimeBuilder;
use stasis::sdk::runtime_sdk::RuntimeSdk;

use crate::config::CliArgs;
use crate::error::StasisdError;
use crate::host::{run_host, HostOptions};
use crate::model::API_VERSION;
use crate::provenance::{managed_recurring_id, MANAGED_BY, MANAGED_ID_PREFIX};
use crate::runtime_config::resolve_stasisd_runtime_backend_from_env;

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
    let _ = bootstrap();
    let args = CliArgs::parse(env::args().skip(1))?;

    println!(
        "stasisd: api={API_VERSION} managed_by={MANAGED_BY} managed_id_prefix={MANAGED_ID_PREFIX} example_id={} agent_envelope_schema_v{AGENT_ENVELOPE_SCHEMA_VERSION_V1}",
        managed_recurring_id("example")
    );

    let backend = resolve_stasisd_runtime_backend_from_env()?;
    let runtime = RuntimeSdk::from_builder(StasisRuntimeBuilder::new(backend))
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;

    let host_options = HostOptions {
        config_path: args.config_path.clone(),
        strict: args.strict,
        watch: args.watch && !args.once,
        tick_interval: args.tick_interval,
        reconcile_interval: args.reconcile_interval,
        debounce: args.debounce,
        tick: args.tick.clone(),
        max_ticks: if args.once {
            Some(1)
        } else {
            args.max_ticks
        },
        run_for: args.run_for,
    };

    let report = run_host(&runtime, host_options).await?;
    println!(
        "stasisd: host ticks={} reconciles={} last_tick={:?} last_reconcile_created={:?}",
        report.ticks,
        report.reconciles,
        report.last_tick,
        report
            .last_reconcile
            .as_ref()
            .map(|r| r.created.len())
    );

    Ok(())
}
