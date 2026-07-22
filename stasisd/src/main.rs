//! `stasisd` — declarative Stasis engine (ADR-0008).

mod config;
mod error;
mod health;
mod host;
mod join_e2e;
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
use std::sync::Arc;

use stasis::config_prelude::bootstrap;
use stasis::domain::agent::AGENT_ENVELOPE_SCHEMA_VERSION_V1;
use stasis::infrastructure::agent::{
    InMemoryAgentEventIngress, InMemoryAgentTransport, InMemoryTurnWaitStore,
    JsonAgentMessageCodec, WaitCorrelatingAgentEventIngress,
};
use stasis::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
use stasis::ports::outbound::agent::AgentEventIngress;
use stasis::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
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
    let endpoint_store: Arc<dyn DeliveryEndpointStore> =
        Arc::new(InMemoryDeliveryEndpointStore::default());
    let wait_store = Arc::new(InMemoryTurnWaitStore::new());
    let base_ingress = Arc::new(InMemoryAgentEventIngress::new());
    let ingress: Arc<dyn AgentEventIngress> = Arc::new(WaitCorrelatingAgentEventIngress::new(
        base_ingress,
        wait_store.clone(),
    ));
    let transport = Arc::new(InMemoryAgentTransport::new());

    let runtime = RuntimeSdk::from_builder(
        StasisRuntimeBuilder::new(backend)
            .with_delivery_endpoint_store(endpoint_store.clone())
            .with_turn_wait_store(wait_store)
            .with_agent_message_codec(Arc::new(JsonAgentMessageCodec::v1()))
            .with_agent_event_ingress(ingress)
            .with_agent_transport(transport),
    )
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
        healthz_addr: args.healthz_addr,
        endpoint_store: Some(endpoint_store),
    };

    let report = run_host(&runtime, host_options).await?;
    println!(
        "stasisd: host ticks={} reconciles={} last_tick={:?} last_reconcile_created={:?} endpoints_created={:?}",
        report.ticks,
        report.reconciles,
        report.last_tick,
        report
            .last_reconcile
            .as_ref()
            .map(|r| r.created.len()),
        report
            .last_reconcile
            .as_ref()
            .map(|r| r.endpoint_created.len())
    );

    Ok(())
}
