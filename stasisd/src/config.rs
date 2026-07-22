//! Config discovery + load for `stasisd/v1`.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::StasisdError;
use crate::model::{DesiredState, StasisdDocument};
use crate::parse::parse_config_bytes;
use crate::tick::TickOptions;
use crate::validate::validate_desired_state;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
    pub once: bool,
    pub strict: bool,
    pub watch: bool,
    pub tick_interval: Duration,
    pub reconcile_interval: Duration,
    pub debounce: Duration,
    pub max_ticks: Option<u64>,
    pub run_for: Option<Duration>,
    pub healthz_addr: Option<SocketAddr>,
    pub tick: TickOptions,
}

impl CliArgs {
    pub fn parse<I, S>(args: I) -> Result<Self, StasisdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut config_path = None;
        let mut once = false;
        let mut strict = false;
        let mut watch = true;
        let mut tick_interval = Duration::from_secs(1);
        let mut reconcile_interval = Duration::from_secs(60);
        let mut debounce = Duration::from_millis(250);
        let mut max_ticks = None;
        let mut run_for = None;
        let mut healthz_addr = None;
        let mut tick = TickOptions::default();
        let mut queues: Vec<String> = Vec::new();

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            let arg = arg.as_ref();
            match arg {
                "--config" | "-c" => {
                    let value = next_value(&mut iter, "--config")?;
                    config_path = Some(PathBuf::from(value));
                }
                "--once" => {
                    once = true;
                    watch = false;
                }
                "--strict" => strict = true,
                "--watch" => watch = true,
                "--no-watch" => watch = false,
                "--tick-interval" => {
                    tick_interval = parse_duration(&next_value(&mut iter, "--tick-interval")?)?;
                }
                "--reconcile-interval" => {
                    reconcile_interval =
                        parse_duration(&next_value(&mut iter, "--reconcile-interval")?)?;
                }
                "--debounce" => {
                    debounce = parse_duration(&next_value(&mut iter, "--debounce")?)?;
                }
                "--queue" => {
                    queues.push(next_value(&mut iter, "--queue")?);
                }
                "--worker-id" => {
                    tick.worker_id = next_value(&mut iter, "--worker-id")?;
                }
                "--scheduler-id" => {
                    tick.scheduler_id = next_value(&mut iter, "--scheduler-id")?;
                }
                "--process-limit" => {
                    tick.process_limit = next_value(&mut iter, "--process-limit")?
                        .parse()
                        .map_err(|_| {
                            StasisdError::Usage("--process-limit must be an integer".into())
                        })?;
                }
                "--publish-limit" => {
                    tick.publish_limit = next_value(&mut iter, "--publish-limit")?
                        .parse()
                        .map_err(|_| {
                            StasisdError::Usage("--publish-limit must be an integer".into())
                        })?;
                }
                "--max-ticks" => {
                    max_ticks = Some(next_value(&mut iter, "--max-ticks")?.parse().map_err(
                        |_| StasisdError::Usage("--max-ticks must be an integer".into()),
                    )?);
                }
                "--run-for" => {
                    run_for = Some(parse_duration(&next_value(&mut iter, "--run-for")?)?);
                }
                "--healthz-addr" => {
                    let value = next_value(&mut iter, "--healthz-addr")?;
                    healthz_addr = Some(value.parse::<SocketAddr>().map_err(|_| {
                        StasisdError::Usage(format!(
                            "invalid --healthz-addr '{value}' (expected host:port)"
                        ))
                    })?);
                }
                "--help" | "-h" => {
                    return Err(StasisdError::Usage(help_text()));
                }
                other if other.starts_with('-') => {
                    return Err(StasisdError::Usage(format!("unknown flag: {other}")));
                }
                other => {
                    return Err(StasisdError::Usage(format!(
                        "unexpected argument: {other}"
                    )));
                }
            }
        }

        let config_path = config_path.ok_or_else(|| {
            StasisdError::Usage("missing required --config <path>".to_string())
        })?;
        if !queues.is_empty() {
            tick.queues = queues;
        }

        Ok(Self {
            config_path,
            once,
            strict,
            watch,
            tick_interval,
            reconcile_interval,
            debounce,
            max_ticks,
            run_for,
            healthz_addr,
            tick,
        })
    }
}

fn next_value<I, S>(iter: &mut I, flag: &str) -> Result<String, StasisdError>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    iter.next()
        .map(|v| v.as_ref().to_string())
        .ok_or_else(|| StasisdError::Usage(format!("{flag} requires a value")))
}

fn parse_duration(raw: &str) -> Result<Duration, StasisdError> {
    if let Some(ms) = raw.strip_suffix("ms") {
        let value: u64 = ms
            .parse()
            .map_err(|_| StasisdError::Usage(format!("invalid duration '{raw}'")))?;
        return Ok(Duration::from_millis(value));
    }
    if let Some(secs) = raw.strip_suffix('s') {
        let value: u64 = secs
            .parse()
            .map_err(|_| StasisdError::Usage(format!("invalid duration '{raw}'")))?;
        return Ok(Duration::from_secs(value));
    }
    if let Some(mins) = raw.strip_suffix('m') {
        let value: u64 = mins
            .parse()
            .map_err(|_| StasisdError::Usage(format!("invalid duration '{raw}'")))?;
        return Ok(Duration::from_secs(value.saturating_mul(60)));
    }
    Err(StasisdError::Usage(format!(
        "invalid duration '{raw}' (use 250ms, 1s, 5m)"
    )))
}

fn help_text() -> String {
    "stasisd — declarative Stasis engine\n\n\
     Usage:\n  \
       stasisd --config <path> [--once] [--strict] [--watch|--no-watch]\n\n\
     Flags:\n  \
       -c, --config <path>         Config file or directory\n  \
       --once                      Reconcile+tick once and exit\n  \
       --strict                    Treat config diagnostics as fatal\n  \
       --watch / --no-watch        Enable/disable filesystem watch\n  \
       --tick-interval <dur>       Worker tick interval (default 1s)\n  \
       --reconcile-interval <dur>  Full reconcile interval (default 60s)\n  \
       --debounce <dur>            Watch debounce (default 250ms)\n  \
       --queue <name>              Queue to process (repeatable)\n  \
       --max-ticks <n>             Stop after N ticks (testing)\n  \
       --run-for <dur>             Stop after duration (testing)\n  \
       --healthz-addr <host:port>  Serve /healthz and /readyz\n  \
       -h, --help                  Show help\n"
        .to_string()
}

/// Discover, parse, and validate config sources into desired state.
///
/// Per-file parse/validation failures are quarantined into `diagnostics` unless the
/// caller applies `--strict` (which fails the process when diagnostics are present).
pub fn load_desired_state(path: &Path) -> Result<DesiredState, StasisdError> {
    if !path.exists() {
        return Err(StasisdError::Validation(format!(
            "config path does not exist: {}",
            path.display()
        )));
    }

    let source_paths = discover_sources(path)?;
    let mut desired = DesiredState {
        sources: source_paths.clone(),
        ..DesiredState::default()
    };

    let mut documents = Vec::new();
    for source in &source_paths {
        match load_document(source) {
            Ok(document) => documents.push(document),
            Err(err) => desired.diagnostics.push(err.to_string()),
        }
    }

    desired.documents = documents;
    desired.schedules = desired
        .documents
        .iter()
        .flat_map(|doc| doc.schedules.clone())
        .collect();

    if let Err(errors) = validate_desired_state(&desired) {
        for err in errors {
            desired.diagnostics.push(err.to_string());
        }
        // Drop schedules when cross-file validation fails so reconcile cannot apply a
        // partially invalid snapshot.
        if desired.diagnostics.iter().any(|d| d.contains("duplicate schedule id")) {
            desired.schedules.clear();
        } else {
            // Keep only schedules from documents that still validate in isolation.
            desired.schedules = desired
                .documents
                .iter()
                .filter(|doc| crate::validate::validate_document(doc).is_ok())
                .flat_map(|doc| doc.schedules.clone())
                .collect();
        }
    }

    Ok(desired)
}

fn discover_sources(path: &Path) -> Result<Vec<PathBuf>, StasisdError> {
    if path.is_file() {
        if is_config_file(path) {
            return Ok(vec![path.to_path_buf()]);
        }
        return Err(StasisdError::Validation(format!(
            "unsupported config file extension: {}",
            path.display()
        )));
    }

    if !path.is_dir() {
        return Err(StasisdError::Validation(format!(
            "config path is neither file nor directory: {}",
            path.display()
        )));
    }

    let mut entries = fs::read_dir(path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|entry| entry.is_file() && is_config_file(entry))
        .collect::<Vec<_>>();
    entries.sort();
    Ok(entries)
}

fn load_document(path: &Path) -> Result<StasisdDocument, StasisdError> {
    let bytes = fs::read(path)?;
    let document = parse_config_bytes(path, &bytes)?;
    crate::validate::validate_document(&document)?;
    Ok(document)
}

fn is_config_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("yaml" | "yml" | "toml")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("stasisd-{label}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn parse_requires_config() {
        let err = CliArgs::parse(["--once"]).unwrap_err();
        assert!(matches!(err, StasisdError::Usage(_)));
    }

    #[test]
    fn parse_flags() {
        let args = CliArgs::parse([
            "--config",
            "/tmp/agents.d",
            "--once",
            "--strict",
            "--tick-interval",
            "2s",
            "--queue",
            "agents",
            "--max-ticks",
            "3",
        ])
        .unwrap();
        assert_eq!(args.config_path, PathBuf::from("/tmp/agents.d"));
        assert!(args.once);
        assert!(args.strict);
        assert!(!args.watch);
        assert_eq!(args.tick_interval, Duration::from_secs(2));
        assert_eq!(args.tick.queues, vec!["agents".to_string()]);
        assert_eq!(args.max_ticks, Some(3));
    }

    #[test]
    fn rejects_bad_duration() {
        let err = CliArgs::parse(["--config", "/tmp/x", "--tick-interval", "nope"]).unwrap_err();
        assert!(err.to_string().contains("invalid duration"));
    }

    #[test]
    fn empty_directory_is_valid_desired_state() {
        let dir = temp_dir("empty");
        let desired = load_desired_state(&dir).unwrap();
        assert!(desired.sources.is_empty());
        assert!(desired.schedules.is_empty());
        assert!(desired.diagnostics.is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn loads_valid_toml_and_quarantines_bad_sibling() {
        let dir = temp_dir("mix");
        fs::write(
            dir.join("good.toml"),
            r#"
api_version = "stasisd/v1"
[[schedule]]
id = "good"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 * * * * *"
"#,
        )
        .unwrap();
        fs::write(dir.join("bad.toml"), "api_version = \"stasisd/v0\"\n").unwrap();

        let desired = load_desired_state(&dir).unwrap();
        assert_eq!(desired.schedules.len(), 1);
        assert_eq!(desired.schedules[0].id, "good");
        assert!(desired.diagnostics.iter().any(|d| d.contains("api_version")));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_path_fails() {
        let err = load_desired_state(Path::new("/tmp/stasisd-does-not-exist-hopefully")).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn non_strict_keeps_good_schedules_when_sibling_is_bad() {
        let dir = temp_dir("nonstrict");
        fs::write(
            dir.join("good.toml"),
            r#"
api_version = "stasisd/v1"
[[schedule]]
id = "nightly"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 2 * * * *"
"#,
        )
        .unwrap();
        fs::write(dir.join("bad.yaml"), "not: valid: stasisd\n").unwrap();
        let desired = load_desired_state(&dir).unwrap();
        assert_eq!(desired.schedules.len(), 1);
        assert_eq!(desired.schedules[0].id, "nightly");
        assert!(!desired.diagnostics.is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn parse_healthz_addr() {
        let args = CliArgs::parse([
            "--config",
            "/tmp/agents.d",
            "--healthz-addr",
            "127.0.0.1:0",
        ])
        .unwrap();
        assert_eq!(
            args.healthz_addr.unwrap().to_string(),
            "127.0.0.1:0"
        );
    }

    #[test]
    fn duplicate_ids_clear_schedules() {
        let dir = temp_dir("dup");
        fs::write(
            dir.join("a.toml"),
            r#"
api_version = "stasisd/v1"
[[schedule]]
id = "dup"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 * * * * *"
"#,
        )
        .unwrap();
        fs::write(
            dir.join("b.toml"),
            r#"
api_version = "stasisd/v1"
[[schedule]]
id = "dup"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0 0 * * * * *"
"#,
        )
        .unwrap();
        let desired = load_desired_state(&dir).unwrap();
        assert!(desired.schedules.is_empty());
        assert!(desired.diagnostics.iter().any(|d| d.contains("duplicate")));
        let _ = fs::remove_dir_all(dir);
    }
}
