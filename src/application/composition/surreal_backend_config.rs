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
    read_non_empty_env(primary_var)
        .or_else(|| fallback_var.and_then(read_non_empty_env))
        .unwrap_or_else(|| default.to_string())
}

pub fn resolve_surreal_database_from_env(
    primary_var: &str,
    fallback_var: Option<&str>,
    default: &str,
) -> String {
    read_non_empty_env(primary_var)
        .or_else(|| fallback_var.and_then(read_non_empty_env))
        .unwrap_or_else(|| default.to_string())
}

pub fn resolve_surreal_auth_from_env(
    username_var: &str,
    password_var: &str,
    fallback_username_var: Option<&str>,
    fallback_password_var: Option<&str>,
) -> Option<SurrealAuth> {
    let username = read_non_empty_env(username_var)
        .or_else(|| fallback_username_var.and_then(read_non_empty_env))?;
    let password = read_non_empty_env(password_var)
        .or_else(|| fallback_password_var.and_then(read_non_empty_env))?;
    Some(SurrealAuth { username, password })
}

fn read_non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
