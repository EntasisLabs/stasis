/// Root-level SurrealDB credentials used during runtime connection bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurrealAuth {
    pub username: String,
    pub password: String,
}

impl SurrealAuth {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

pub fn resolve_surreal_namespace_from_env(
    primary_var: &str,
    fallback_var: Option<&str>,
    default: &str,
) -> String {
    crate::application::config::env::first_non_empty(&match fallback_var {
        Some(fallback) => vec![primary_var, fallback],
        None => vec![primary_var],
    })
    .unwrap_or_else(|| default.to_string())
}

pub fn resolve_surreal_database_from_env(
    primary_var: &str,
    fallback_var: Option<&str>,
    default: &str,
) -> String {
    resolve_surreal_namespace_from_env(primary_var, fallback_var, default)
}

pub fn resolve_surreal_auth_from_env(
    username_var: &str,
    password_var: &str,
    fallback_username_var: Option<&str>,
    fallback_password_var: Option<&str>,
) -> Option<SurrealAuth> {
    let username = crate::application::config::env::first_non_empty(&match fallback_username_var {
        Some(fallback) => vec![username_var, fallback],
        None => vec![username_var],
    })?;
    let password = crate::application::config::env::first_non_empty(&match fallback_password_var {
        Some(fallback) => vec![password_var, fallback],
        None => vec![password_var],
    })?;
    Some(SurrealAuth { username, password })
}
