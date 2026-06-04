use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Lookup surface for configuration and secret values.
pub trait SecretsSource: Send + Sync {
    fn lookup(&self, key: &str) -> Option<String>;
}

/// Reads trimmed non-empty values from the process environment.
#[derive(Debug, Default, Clone, Copy)]
pub struct OsEnvSource;

impl SecretsSource for OsEnvSource {
    fn lookup(&self, key: &str) -> Option<String> {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }
}

/// Reads one secret file per key from a directory (Vault Agent / ESO file sinks).
#[derive(Debug, Clone, Default)]
pub struct FileSecretsSource {
    secrets: HashMap<String, String>,
}

impl FileSecretsSource {
    pub fn from_dir(dir: impl AsRef<Path>) -> Self {
        Self {
            secrets: load_secret_files(dir.as_ref()),
        }
    }

    pub fn len(&self) -> usize {
        self.secrets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }
}

impl SecretsSource for FileSecretsSource {
    fn lookup(&self, key: &str) -> Option<String> {
        self.secrets.get(key).cloned()
    }
}

/// Resolves a key against multiple sources in order.
pub struct ChainedSecretsSource {
    sources: Vec<Box<dyn SecretsSource>>,
}

impl Default for ChainedSecretsSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainedSecretsSource {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn with_source(mut self, source: impl SecretsSource + 'static) -> Self {
        self.sources.push(Box::new(source));
        self
    }
}

impl SecretsSource for ChainedSecretsSource {
    fn lookup(&self, key: &str) -> Option<String> {
        self.sources
            .iter()
            .find_map(|source| source.lookup(key))
    }
}

static SECRETS_RESOLVER: OnceLock<ChainedSecretsSource> = OnceLock::new();

pub(crate) fn install_resolver(resolver: ChainedSecretsSource) {
    let _ = SECRETS_RESOLVER.set(resolver);
}

pub(crate) fn resolve(key: &str) -> Option<String> {
    SECRETS_RESOLVER
        .get()
        .and_then(|resolver| resolver.lookup(key))
        .or_else(|| OsEnvSource.lookup(key))
}

fn load_secret_files(dir: &Path) -> HashMap<String, String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return HashMap::new(),
    };

    let mut secrets = HashMap::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.starts_with('.') {
            continue;
        }

        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let value = raw.trim().to_string();
        if value.is_empty() {
            continue;
        }

        secrets.insert(file_name.to_string(), value);
    }

    secrets
}

pub fn default_secrets_dir() -> Option<PathBuf> {
    OsEnvSource
        .lookup("STASIS_SECRETS_DIR")
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_secrets_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("stasis-secrets-{nanos}"))
    }

    #[test]
    fn file_secrets_source_reads_trimmed_files() {
        let dir = temp_secrets_dir();
        fs::create_dir_all(&dir).expect("temp secrets dir should be created");
        fs::write(dir.join("STASIS_TEST_SECRET"), "  secret-value  \n")
            .expect("secret file should be written");

        let source = FileSecretsSource::from_dir(&dir);
        assert_eq!(
            source.lookup("STASIS_TEST_SECRET"),
            Some("secret-value".to_string())
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn chained_source_uses_first_match() {
        let dir = temp_secrets_dir();
        fs::create_dir_all(&dir).expect("temp secrets dir should be created");
        fs::write(dir.join("STASIS_CHAINED"), "from-file")
            .expect("secret file should be written");

        let chain = ChainedSecretsSource::new()
            .with_source(OsEnvSource)
            .with_source(FileSecretsSource::from_dir(&dir));

        unsafe {
            std::env::set_var("STASIS_CHAINED", "from-env");
        }
        assert_eq!(chain.lookup("STASIS_CHAINED"), Some("from-env".to_string()));
        unsafe {
            std::env::remove_var("STASIS_CHAINED");
        }

        assert_eq!(chain.lookup("STASIS_CHAINED"), Some("from-file".to_string()));

        let _ = fs::remove_dir_all(&dir);
    }
}
