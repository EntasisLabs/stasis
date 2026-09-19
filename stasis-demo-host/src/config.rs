use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, bail};

const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_PROXY_BIND: &str = "127.0.0.1:8787";
const DEFAULT_MINT_BIND: &str = "0.0.0.0:8788";
const DEFAULT_MODEL: &str = "gpt-4o-mini";
const DEFAULT_DAILY_COMPLETIONS: u64 = 5;
const DEFAULT_MINT_TTL_SECS: u64 = 30;
const DEFAULT_MINT_MAX_SESSIONS: u32 = 1;
const DEFAULT_MINT_PER_IP: u32 = 10;
const DEFAULT_MINT_PER_SUBJECT: u32 = 5;
const DEFAULT_MINT_WINDOW_SECS: u64 = 600;
const DEFAULT_BOOTSTRAP_ORIGIN: &str = "https://urspace.online";

/// Process configuration loaded from the environment.
#[derive(Clone)]
pub struct Config {
    pub openai_api_key: String,
    pub openai_base_url: String,
    pub proxy_bind: SocketAddr,
    pub mint_bind: SocketAddr,
    pub daily_completions: Option<u64>,
    pub daily_tokens: Option<u64>,
    pub allowed_models: Vec<String>,
    pub mint_ttl: Duration,
    pub mint_max_sessions: u32,
    pub mint_per_ip: u32,
    pub mint_per_subject: u32,
    pub mint_window: Duration,
    pub trust_proxy: bool,
    pub cors_origins: Vec<String>,
    pub urspace_bootstrap_origin: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let openai_api_key = required_env("OPENAI_API_KEY")?;
        if openai_api_key.chars().any(|c| c.is_whitespace()) {
            bail!("OPENAI_API_KEY must not contain whitespace");
        }

        let openai_base_url = env_or("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
            .trim_end_matches('/')
            .to_string();
        let proxy_bind = parse_bind("PROXY_BIND", DEFAULT_PROXY_BIND)?;
        let mint_bind = parse_bind("MINT_BIND", DEFAULT_MINT_BIND)?;

        Ok(Self {
            openai_api_key,
            openai_base_url,
            proxy_bind,
            mint_bind,
            daily_completions: optional_limit("DAILY_COMPLETIONS", DEFAULT_DAILY_COMPLETIONS)?,
            daily_tokens: optional_limit("DAILY_TOKENS", 0)?,
            allowed_models: parse_csv(env_or("ALLOWED_MODELS", DEFAULT_MODEL)),
            mint_ttl: Duration::from_secs(env_u64("MINT_TTL_SECS", DEFAULT_MINT_TTL_SECS)?),
            mint_max_sessions: env_u32("MINT_MAX_SESSIONS", DEFAULT_MINT_MAX_SESSIONS)?.max(1),
            mint_per_ip: env_u32("MINT_PER_IP", DEFAULT_MINT_PER_IP)?.max(1),
            mint_per_subject: env_u32("MINT_PER_SUBJECT", DEFAULT_MINT_PER_SUBJECT)?.max(1),
            mint_window: Duration::from_secs(env_u64(
                "MINT_WINDOW_SECS",
                DEFAULT_MINT_WINDOW_SECS,
            )?),
            trust_proxy: env_bool("TRUST_PROXY", false),
            cors_origins: parse_csv(env_or("CORS_ORIGIN", "*")),
            urspace_bootstrap_origin: env_or("URSPACE_BOOTSTRAP_ORIGIN", DEFAULT_BOOTSTRAP_ORIGIN),
        })
    }
}

fn required_env(key: &str) -> anyhow::Result<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{key} is required"))
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_bind(key: &str, default: &str) -> anyhow::Result<SocketAddr> {
    env_or(key, default)
        .parse()
        .with_context(|| format!("invalid {key} socket address"))
}

fn env_u64(key: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse()
            .with_context(|| format!("invalid {key}")),
        _ => Ok(default),
    }
}

fn env_u32(key: &str, default: u32) -> anyhow::Result<u32> {
    Ok(u32::try_from(env_u64(key, u64::from(default))?)?)
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

/// `0` (or empty override of an optional-only key) means unlimited.
fn optional_limit(key: &str, default_when_unset: u64) -> anyhow::Result<Option<u64>> {
    let raw = match env::var(key) {
        Ok(value) if !value.trim().is_empty() => value,
        _ if default_when_unset == 0 => return Ok(None),
        _ => return Ok(Some(default_when_unset)),
    };
    let parsed: u64 = raw
        .trim()
        .parse()
        .with_context(|| format!("invalid {key}"))?;
    Ok((parsed > 0).then_some(parsed))
}

fn parse_csv(raw: String) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_csv;

    #[test]
    fn csv_splits_and_trims() {
        assert_eq!(
            parse_csv("gpt-4o-mini, gpt-4o".into()),
            vec!["gpt-4o-mini", "gpt-4o"]
        );
    }
}
