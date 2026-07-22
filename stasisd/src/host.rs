use std::path::Path;
use std::time::Duration;

use stasis::sdk::runtime_sdk::RuntimeSdk;
use tokio::time::{interval, MissedTickBehavior};

use crate::config::load_desired_state;
use crate::error::StasisdError;
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
}

#[derive(Clone, Debug, Default)]
pub struct HostReport {
    pub ticks: u64,
    pub reconciles: u64,
    pub last_tick: Option<TickReport>,
    pub last_reconcile: Option<ReconcileReport>,
}

pub async fn run_host(runtime: &RuntimeSdk, options: HostOptions) -> Result<HostReport, StasisdError> {
    let mut report = HostReport::default();
    report.last_reconcile =
        Some(reconcile_from_path(runtime, &options.config_path, options.strict).await?);
    report.reconciles = 1;

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

        tokio::select! {
            _ = tick_timer.tick() => {
                if let Some(watcher) = &watcher {
                    if watcher.try_recv_debounced(options.debounce)? {
                        report.last_reconcile = Some(
                            reconcile_from_path(runtime, &options.config_path, options.strict).await?,
                        );
                        report.reconciles += 1;
                    }
                }
                report.last_tick = Some(tick_once(runtime, &options.tick).await?);
                report.ticks += 1;
            }
            _ = reconcile_timer.tick() => {
                report.last_reconcile = Some(
                    reconcile_from_path(runtime, &options.config_path, options.strict).await?,
                );
                report.reconciles += 1;
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

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
}
