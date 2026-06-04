use std::path::{Path, PathBuf};

use crate::application::config::secrets::{
    ChainedSecretsSource, FileSecretsSource, OsEnvSource, default_secrets_dir, install_resolver,
    resolve,
};

const DEFAULT_DOTENV_FILE: &str = ".env";

/// Bootstrap or lookup failure for environment configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvError {
    message: String,
}

impl EnvError {
    pub fn missing(key: &str) -> Self {
        Self {
            message: format!("missing required environment variable: {key}"),
        }
    }

    pub fn bootstrap(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for EnvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for EnvError {}

/// Controls optional `.env` and secrets-directory loading during bootstrap.
#[derive(Debug, Clone, Default)]
pub struct EnvBootstrapOptions {
    pub dotenv_path: Option<PathBuf>,
    pub secrets_dir: Option<PathBuf>,
    pub skip_dotenv: bool,
    pub skip_secrets_dir: bool,
}

/// Summary of what bootstrap loaded. Never includes secret values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvBootstrapReport {
    pub dotenv_loaded: bool,
    pub dotenv_path: Option<PathBuf>,
    pub secrets_dir_loaded: bool,
    pub secrets_dir: Option<PathBuf>,
    pub secrets_keys_loaded: usize,
}

/// Loads `.env` (when present) and optional file secrets, then installs the global resolver.
///
/// Resolution order for [`non_empty`] / [`required`]:
/// 1. Process environment (explicit exports and injected runtime secrets)
/// 2. Files under `STASIS_SECRETS_DIR` (or `options.secrets_dir`)
///
/// Dotenv values are merged into the process environment without overriding existing keys.
pub fn bootstrap() -> Result<EnvBootstrapReport, EnvError> {
    bootstrap_with(EnvBootstrapOptions::default())
}

pub fn bootstrap_with(options: EnvBootstrapOptions) -> Result<EnvBootstrapReport, EnvError> {
    let mut report = EnvBootstrapReport::default();

    if !options.skip_dotenv {
        let dotenv_path = options
            .dotenv_path
            .clone()
            .or_else(|| non_empty("STASIS_ENV_FILE").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DOTENV_FILE));

        match dotenvy::from_path(&dotenv_path) {
            Ok(()) => {
                report.dotenv_loaded = true;
                report.dotenv_path = Some(dotenv_path);
            }
            Err(err) if err.not_found() => {}
            Err(err) => {
                return Err(EnvError::bootstrap(format!(
                    "failed to load dotenv file {}: {err}",
                    dotenv_path.display()
                )));
            }
        }
    }

    let secrets_dir = if options.skip_secrets_dir {
        None
    } else {
        options.secrets_dir.or_else(default_secrets_dir)
    };

    let file_source = secrets_dir
        .as_ref()
        .map(FileSecretsSource::from_dir)
        .unwrap_or_default();
    if let Some(dir) = secrets_dir {
        report.secrets_dir_loaded = !file_source.is_empty() || dir.is_dir();
        report.secrets_dir = Some(dir);
        report.secrets_keys_loaded = file_source.len();
    }

    let resolver = ChainedSecretsSource::new()
        .with_source(OsEnvSource)
        .with_source(file_source);
    install_resolver(resolver);

    Ok(report)
}

/// Returns a trimmed non-empty value for `key`, or `None`.
pub fn non_empty(key: &str) -> Option<String> {
    resolve(key)
}

/// Returns the resolved value or `default`.
pub fn with_default(key: &str, default: &str) -> String {
    non_empty(key).unwrap_or_else(|| default.to_string())
}

/// Returns the first non-empty value from `keys`.
pub fn first_non_empty(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| non_empty(key))
}

/// Returns the resolved value or an [`EnvError`] that names the missing key only.
pub fn required(key: &str) -> Result<String, EnvError> {
    non_empty(key).ok_or_else(|| EnvError::missing(key))
}

/// Parses common truthy/falsey env strings (`1`, `true`, `yes`, `on`).
pub fn truthy(key: &str) -> bool {
    non_empty(key)
        .map(|value| value.to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

/// Loads a dotenv file without installing the global resolver.
pub fn load_dotenv_from(path: impl AsRef<Path>) -> Result<(), EnvError> {
    dotenvy::from_path(path.as_ref()).map_err(|err| {
        EnvError::bootstrap(format!(
            "failed to load dotenv file {}: {err}",
            path.as_ref().display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock should be available")
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{nanos}"))
    }

    #[test]
    fn bootstrap_loads_dotenv_without_overriding_existing_env() {
        let _guard = test_lock();
        let dir = temp_dir("stasis-dotenv");
        fs::create_dir_all(&dir).expect("temp dir should be created");
        fs::write(
            dir.join(".env"),
            "STASIS_BOOTSTRAP_TEST=from-dotenv\nSTASIS_BOOTSTRAP_EXISTING=from-dotenv\n",
        )
        .expect(".env should be written");

        unsafe {
            std::env::set_var("STASIS_BOOTSTRAP_EXISTING", "from-os");
        }
        let report = bootstrap_with(EnvBootstrapOptions {
            dotenv_path: Some(dir.join(".env")),
            skip_secrets_dir: true,
            ..Default::default()
        })
        .expect("bootstrap should succeed");

        assert!(report.dotenv_loaded);
        assert_eq!(non_empty("STASIS_BOOTSTRAP_TEST"), Some("from-dotenv".to_string()));
        assert_eq!(
            non_empty("STASIS_BOOTSTRAP_EXISTING"),
            Some("from-os".to_string())
        );

        unsafe {
            std::env::remove_var("STASIS_BOOTSTRAP_TEST");
            std::env::remove_var("STASIS_BOOTSTRAP_EXISTING");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn required_reports_missing_key_without_value() {
        let _guard = test_lock();
        let key = "STASIS_REQUIRED_MISSING_TEST";
        unsafe {
            std::env::remove_var(key);
        }

        let err = required(key).expect_err("missing key should fail");
        assert_eq!(err.to_string(), format!("missing required environment variable: {key}"));
    }

    #[test]
    fn truthy_parses_common_values() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("STASIS_TRUTHY_TEST", "YeS");
        }
        assert!(truthy("STASIS_TRUTHY_TEST"));
        unsafe {
            std::env::remove_var("STASIS_TRUTHY_TEST");
        }
    }
}
