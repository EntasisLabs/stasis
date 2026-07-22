//! Config discovery + load for `stasisd/v1`.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::StasisdError;
use crate::model::{DesiredState, StasisdDocument};
use crate::parse::parse_config_bytes;
use crate::validate::validate_desired_state;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub config_path: PathBuf,
    pub once: bool,
    pub strict: bool,
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

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            let arg = arg.as_ref();
            match arg {
                "--config" | "-c" => {
                    let value = iter.next().ok_or_else(|| {
                        StasisdError::Usage("--config requires a path".to_string())
                    })?;
                    config_path = Some(PathBuf::from(value.as_ref()));
                }
                "--once" => once = true,
                "--strict" => strict = true,
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

        Ok(Self {
            config_path,
            once,
            strict,
        })
    }
}

fn help_text() -> String {
    "stasisd — declarative Stasis engine\n\n\
     Usage:\n  \
       stasisd --config <path> [--once] [--strict]\n\n\
     Flags:\n  \
       -c, --config <path>  Config file or directory\n  \
       --once               Load/reconcile once and exit\n  \
       --strict             Treat config diagnostics as fatal\n  \
       -h, --help           Show help\n"
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
        let args = CliArgs::parse(["--config", "/tmp/agents.d", "--once", "--strict"]).unwrap();
        assert_eq!(args.config_path, PathBuf::from("/tmp/agents.d"));
        assert!(args.once);
        assert!(args.strict);
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
