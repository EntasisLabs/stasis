use stasis::application::composition::surreal_backend_config::resolve_surreal_auth_from_env;
use stasis::prelude::{RuntimeBackend, SurrealAuth};

use crate::error::StasisdError;

pub fn resolve_stasisd_runtime_backend_from_env() -> Result<RuntimeBackend, StasisdError> {
    let backend = std::env::var("STASIS_STASISD_RUNTIME_BACKEND")
        .unwrap_or_else(|_| "in-memory".to_string())
        .to_ascii_lowercase();

    match backend.as_str() {
        "in-memory" | "in_memory" | "memory" => Ok(RuntimeBackend::InMemory),
        "surreal-mem" | "surreal_mem" => {
            let mut backend = RuntimeBackend::surreal_mem(
                env_or("STASIS_STASISD_SURREAL_NAMESPACE", "stasis"),
                env_or("STASIS_STASISD_SURREAL_DATABASE", "stasisd"),
            );
            if let Some(auth) = optional_auth()? {
                backend = backend.with_surreal_auth(auth);
            }
            Ok(backend)
        }
        "surreal-ws" | "surreal_ws" => {
            let endpoint = std::env::var("STASIS_STASISD_SURREAL_ENDPOINT").map_err(|_| {
                StasisdError::Validation(
                    "STASIS_STASISD_SURREAL_ENDPOINT is required for surreal-ws".into(),
                )
            })?;
            let mut backend = RuntimeBackend::surreal_ws(
                endpoint,
                env_or("STASIS_STASISD_SURREAL_NAMESPACE", "stasis"),
                env_or("STASIS_STASISD_SURREAL_DATABASE", "stasisd"),
            );
            if let Some(auth) = optional_auth()? {
                backend = backend.with_surreal_auth(auth);
            }
            Ok(backend)
        }
        "surreal-kv" | "surreal_kv" => {
            let path = env_or("STASIS_STASISD_SURREAL_KV_PATH", "./stasisd.surrealkv");
            let mut backend = RuntimeBackend::surreal_kv(
                path,
                env_or("STASIS_STASISD_SURREAL_NAMESPACE", "stasis"),
                env_or("STASIS_STASISD_SURREAL_DATABASE", "stasisd"),
            );
            if let Some(auth) = optional_auth()? {
                backend = backend.with_surreal_auth(auth);
            }
            Ok(backend)
        }
        other => Err(StasisdError::Validation(format!(
            "unsupported STASIS_STASISD_RUNTIME_BACKEND '{other}'"
        ))),
    }
}

fn optional_auth() -> Result<Option<SurrealAuth>, StasisdError> {
    let username = std::env::var("STASIS_STASISD_SURREAL_USERNAME").ok();
    let password = std::env::var("STASIS_STASISD_SURREAL_PASSWORD").ok();
    match (username, password) {
        (Some(username), Some(password)) => Ok(Some(SurrealAuth { username, password })),
        (None, None) => Ok(resolve_surreal_auth_from_env(
            "STASIS_STASISD_SURREAL_USERNAME",
            "STASIS_STASISD_SURREAL_PASSWORD",
            Some("STASIS_DASHBOARD_SURREAL_USERNAME"),
            Some("STASIS_DASHBOARD_SURREAL_PASSWORD"),
        )),
        _ => Err(StasisdError::Validation(
            "both STASIS_STASISD_SURREAL_USERNAME and PASSWORD must be set together".into(),
        )),
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_in_memory() {
        let _guard = EnvLock;
        // SAFETY: test-only env mutation serialized by EnvLock.
        unsafe {
            std::env::remove_var("STASIS_STASISD_RUNTIME_BACKEND");
        }
        let backend = resolve_stasisd_runtime_backend_from_env().unwrap();
        assert!(matches!(backend, RuntimeBackend::InMemory));
    }

    #[test]
    fn rejects_unknown_backend() {
        let _guard = EnvLock;
        // SAFETY: test-only env mutation serialized by EnvLock.
        unsafe {
            std::env::set_var("STASIS_STASISD_RUNTIME_BACKEND", "nope");
        }
        let err = resolve_stasisd_runtime_backend_from_env().unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    struct EnvLock;
    impl Drop for EnvLock {
        fn drop(&mut self) {
            // SAFETY: test-only cleanup.
            unsafe {
                std::env::remove_var("STASIS_STASISD_RUNTIME_BACKEND");
            }
        }
    }
}
