use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use stasis::sdk::runtime_sdk::RuntimeSdk;
use tokio::time::{interval, MissedTickBehavior};

use crate::config::load_desired_state;
use crate::error::StasisdError;
use crate::health::{serve_health_endpoints, HealthState};
use crate::reconcile::{reconcile, ReconcileReport};
use crate::tick::{tick_once, TickOptions, TickReport};
use crate::watch::ConfigWatcher;

#[derive(Clone, Debug)]
pub struct HostOptions {
    pub config_path: std::path::PathBuf,
    pub strict: bool,
    pub watch: bool,
    pub tick_interval: Duration,
    pub reconcile_interval: Duration,
    pub debounce: Duration,
    pub tick: TickOptions,
    pub max_ticks: Option<u64>,
    pub run_for: Option<Duration>,
    pub healthz_addr: Option<SocketAddr>,
}

#[derive(Clone, Debug, Default)]
pub struct HostReport {
    pub ticks: u64,
    pub reconciles: u64,
    pub last_tick: Option<TickReport>,
    pub last_reconcile: Option<ReconcileReport>,
}

pub async fn run_host(runtime: &RuntimeSdk, options: HostOptions) -> Result<HostReport, StasisdError> {
    let health = HealthState::new();
    if let Some(addr) = options.healthz_addr {
        let health = health.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_health_endpoints(addr, health).await {
                eprintln!("stasisd healthz error: {err}");
            }
        });
    }

    let mut report = HostReport::default();
    report.last_reconcile =
        Some(reconcile_from_path(runtime, &options.config_path, options.strict).await?);
    report.reconciles = 1;
    health.set_ready(true);

    let watcher = if options.watch {
        Some(ConfigWatcher::start(&options.config_path)?)
    } else {
        None
    };

    let mut tick_timer = interval(options.tick_interval);
    tick_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut reconcile_timer = interval(options.reconcile_interval);
    reconcile_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick_timer.tick().await;
    reconcile_timer.tick().await;

    let deadline = options.run_for.map(|d| tokio::time::Instant::now() + d);

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|err| StasisdError::Runtime(format!("failed to listen for SIGTERM: {err}")))?;

    loop {
        if let Some(max) = options.max_ticks {
            if report.ticks >= max {
                break;
            }
        }
        if let Some(deadline) = deadline {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }

        let shutdown = async {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = async {
                    #[cfg(unix)]
                    {
                        let _ = sigterm.recv().await;
                    }
                    #[cfg(not(unix))]
                    {
                        std::future::pending::<()>().await;
                    }
                } => {}
            }
        };

        tokio::select! {
            _ = tick_timer.tick() => {
                if let Some(watcher) = &watcher {
                    if watcher.try_recv_debounced(options.debounce)? {
                        match reconcile_from_path(runtime, &options.config_path, options.strict).await {
                            Ok(reconcile_report) => {
                                report.last_reconcile = Some(reconcile_report);
                                report.reconciles += 1;
                                health.set_ready(true);
                            }
                            Err(err) => {
                                health.set_ready(false);
                                return Err(err);
                            }
                        }
                    }
                }
                report.last_tick = Some(tick_once(runtime, &options.tick).await?);
                report.ticks += 1;
            }
            _ = reconcile_timer.tick() => {
                match reconcile_from_path(runtime, &options.config_path, options.strict).await {
                    Ok(reconcile_report) => {
                        report.last_reconcile = Some(reconcile_report);
                        report.reconciles += 1;
                        health.set_ready(true);
                    }
                    Err(err) => {
                        health.set_ready(false);
                        return Err(err);
                    }
                }
            }
            _ = shutdown => {
                break;
            }
        }
    }

    health.set_ready(false);
    Ok(report)
}

pub async fn reconcile_from_path(
    runtime: &RuntimeSdk,
    config_path: &Path,
    strict: bool,
) -> Result<ReconcileReport, StasisdError> {
    let desired = load_desired_state(config_path)?;
    if strict && !desired.diagnostics.is_empty() {
        return Err(StasisdError::Validation(desired.diagnostics.join("; ")));
    }
    for diagnostic in &desired.diagnostics {
        eprintln!("stasisd warning: {diagnostic}");
    }
    reconcile(runtime, &desired).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provenance::managed_recurring_id;
    use crate::tick::TickOptions;
    use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    use stasis::prelude::RuntimeBackend;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("stasisd-host-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn cookbook_drop_toml_ticks_then_remove_drains() {
        let dir = temp_dir("cookbook");
        let file = dir.join("nightly.toml");
        fs::write(
            &file,
            r#"
api_version = "stasisd/v1"
[[schedule]]
id = "nightly"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 2 * * * *"
payload = { user_prompt = "review open work" }
"#,
        )
        .unwrap();

        let runtime = RuntimeSdk::from_builder(StasisRuntimeBuilder::new(RuntimeBackend::InMemory))
            .await
            .unwrap();

        let report = run_host(
            &runtime,
            HostOptions {
                config_path: dir.clone(),
                strict: true,
                watch: false,
                tick_interval: Duration::from_millis(10),
                reconcile_interval: Duration::from_secs(60),
                debounce: Duration::from_millis(10),
                tick: TickOptions {
                    queues: vec!["agents".into()],
                    process_limit: 1,
                    ..TickOptions::default()
                },
                max_ticks: Some(1),
                run_for: None,
                healthz_addr: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.ticks, 1);
        assert_eq!(report.reconciles, 1);
        let defs = runtime.list_recurring().await.unwrap();
        assert!(defs
            .iter()
            .any(|d| d.id == managed_recurring_id("nightly")));

        fs::remove_file(&file).unwrap();
        let drained = reconcile_from_path(&runtime, &dir, true).await.unwrap();
        assert!(drained.drained.contains(&managed_recurring_id("nightly")));
        let defs = runtime.list_recurring().await.unwrap();
        let nightly = defs
            .iter()
            .find(|d| d.id == managed_recurring_id("nightly"))
            .expect("drained def remains disabled");
        assert!(!nightly.enabled);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn strict_fails_when_sibling_is_invalid_non_strict_applies_good() {
        let dir = temp_dir("strict-sibling");
        fs::write(
            dir.join("good.toml"),
            r#"
api_version = "stasisd/v1"
[[schedule]]
id = "good"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 2 * * * *"
"#,
        )
        .unwrap();
        fs::write(dir.join("bad.toml"), "api_version = \"nope\"\n").unwrap();

        let runtime = RuntimeSdk::from_builder(StasisRuntimeBuilder::new(RuntimeBackend::InMemory))
            .await
            .unwrap();

        let err = reconcile_from_path(&runtime, &dir, true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("api_version") || err.to_string().contains("validation"));

        let report = reconcile_from_path(&runtime, &dir, false).await.unwrap();
        assert!(report.created.contains(&managed_recurring_id("good")));
        let _ = fs::remove_dir_all(dir);
    }
}
