use stasis::application::composition::surreal_backend_config::{
    resolve_surreal_auth_from_env, resolve_surreal_database_from_env, resolve_surreal_namespace_from_env,
};
use stasis::domain::errors::{Result, StasisError};
use stasis::prelude::{RuntimeBackend, RuntimeSdk, StasisRuntimeBuilder, SurrealAuth};

fn resolve_surreal_namespace() -> String {
    resolve_surreal_namespace_from_env(
        "STASIS_EXAMPLE_SURREAL_NAMESPACE",
        Some("STASIS_DASHBOARD_SURREAL_NAMESPACE"),
        "stasis",
    )
}

fn resolve_surreal_database() -> String {
    resolve_surreal_database_from_env(
        "STASIS_EXAMPLE_SURREAL_DATABASE",
        Some("STASIS_DASHBOARD_SURREAL_DATABASE"),
        "runtime",
    )
}

fn resolve_surreal_auth() -> Option<SurrealAuth> {
    resolve_surreal_auth_from_env(
        "STASIS_EXAMPLE_SURREAL_USERNAME",
        "STASIS_EXAMPLE_SURREAL_PASSWORD",
        Some("STASIS_DASHBOARD_SURREAL_USERNAME"),
        Some("STASIS_DASHBOARD_SURREAL_PASSWORD"),
    )
}

fn apply_surreal_auth(backend: RuntimeBackend) -> RuntimeBackend {
    match resolve_surreal_auth() {
        Some(auth) => backend.with_surreal_auth(auth),
        None => backend,
    }
}

fn resolve_runtime_backend_from_env() -> Result<RuntimeBackend> {
    let backend = std::env::var("STASIS_EXAMPLE_RUNTIME_BACKEND")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "in-memory".to_string());

    match backend.as_str() {
        "in-memory" | "inmemory" => Ok(RuntimeBackend::InMemory),
        "surreal-mem" | "mem" => Ok(apply_surreal_auth(RuntimeBackend::surreal_mem(
            resolve_surreal_namespace(),
            resolve_surreal_database(),
        ))),
        "surreal-ws" | "ws" => {
            let endpoint = std::env::var("STASIS_EXAMPLE_SURREAL_ENDPOINT")
                .ok()
                .or_else(|| std::env::var("STASIS_DASHBOARD_SURREAL_ENDPOINT").ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    StasisError::PortFailure(
                        "STASIS_EXAMPLE_SURREAL_ENDPOINT is required when STASIS_EXAMPLE_RUNTIME_BACKEND=surreal-ws"
                            .to_string(),
                    )
                })?;

            Ok(apply_surreal_auth(RuntimeBackend::surreal_ws(
                endpoint,
                resolve_surreal_namespace(),
                resolve_surreal_database(),
            )))
        }
        "surreal-kv" | "kv" => {
            let path = std::env::var("STASIS_EXAMPLE_SURREAL_KV_PATH")
                .ok()
                .or_else(|| std::env::var("STASIS_DASHBOARD_SURREAL_KV_PATH").ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    StasisError::PortFailure(
                        "STASIS_EXAMPLE_SURREAL_KV_PATH is required when STASIS_EXAMPLE_RUNTIME_BACKEND=surreal-kv"
                            .to_string(),
                    )
                })?;

            Ok(apply_surreal_auth(RuntimeBackend::surreal_kv(
                path,
                resolve_surreal_namespace(),
                resolve_surreal_database(),
            )))
        }
        other => Err(StasisError::PortFailure(format!(
            "unsupported STASIS_EXAMPLE_RUNTIME_BACKEND='{other}'"
        ))),
    }
}

fn describe_backend(backend: &RuntimeBackend) -> String {
    match backend {
        RuntimeBackend::InMemory => "in-memory".to_string(),
        RuntimeBackend::SurrealMem {
            namespace,
            database,
            ..
        } => {
            format!("surreal-mem ns={namespace} db={database}")
        }
        RuntimeBackend::SurrealWs {
            endpoint,
            namespace,
            database,
            ..
        } => {
            format!("surreal-ws endpoint={endpoint} ns={namespace} db={database}")
        }
        RuntimeBackend::SurrealKv {
            path,
            namespace,
            database,
            ..
        } => {
            format!("surreal-kv path={path} ns={namespace} db={database}")
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let backend = resolve_runtime_backend_from_env()?;
    let backend_summary = describe_backend(&backend);

    let runtime = RuntimeSdk::from_builder(StasisRuntimeBuilder::new(backend).with_locus_memory()).await?;

    println!("runtime backend profile initialized: {backend_summary}");

    match runtime.stats_snapshot(20).await {
        Ok(stats) => {
            println!(
                "runtime stats enqueued={} running={} succeeded={} failed={} dead_letter={} pending_outbox={} recurring={}",
                stats.enqueued_jobs,
                stats.running_jobs,
                stats.succeeded_jobs,
                stats.failed_jobs,
                stats.dead_letter_jobs,
                stats.pending_outbox_events,
                stats.recurring_definitions
            );
        }
        Err(err) => {
            println!(
                "runtime initialized but stats snapshot is not available yet: {}",
                err
            );
        }
    }

    Ok(())
}
