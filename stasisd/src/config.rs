//! Config discovery for `stasisd/v1` (Phase 0: discovery only).

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::StasisdError;

pub const API_VERSION: &str = "stasisd/v1";

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DesiredState {
    pub sources: Vec<PathBuf>,
    pub schedules: Vec<()>,
    pub diagnostics: Vec<String>,
}

/// Discover config sources. Phase 0 does not parse YAML/TOML bodies yet.
pub fn load_desired_state(path: &Path) -> Result<DesiredState, StasisdError> {
    if !path.exists() {
        return Err(StasisdError::Validation(format!(
            "config path does not exist: {}",
            path.display()
        )));
    }

    let mut desired = DesiredState::default();

    if path.is_file() {
        if is_config_file(path) {
            desired.sources.push(path.to_path_buf());
        } else {
            desired.diagnostics.push(format!(
                "ignoring unsupported config file extension: {}",
                path.display()
            ));
        }
        return Ok(desired);
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

    desired.sources = entries;
    // Empty directories are valid desired state (zero schedules).
    Ok(desired)
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
    use std::fs;
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
    fn discovers_yaml_and_toml_files() {
        let dir = temp_dir("discover");
        fs::write(dir.join("a.toml"), "api_version = \"stasisd/v1\"\n").unwrap();
        fs::write(dir.join("b.yaml"), "api_version: stasisd/v1\n").unwrap();
        fs::write(dir.join("notes.txt"), "ignore me\n").unwrap();

        let desired = load_desired_state(&dir).unwrap();
        assert_eq!(desired.sources.len(), 2);
        assert!(desired.sources.iter().any(|p| p.ends_with("a.toml")));
        assert!(desired.sources.iter().any(|p| p.ends_with("b.yaml")));

        let _ = fs::remove_dir_all(dir);
    }
}
