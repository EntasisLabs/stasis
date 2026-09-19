//! `stasis-demo-host` — loopback OpenAI proxy shared via urspace, plus public mint.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use stasis_demo_host::config::Config;
use stasis_demo_host::mint::{MintState, TokenMinter};
use stasis_demo_host::proxy::ProxyState;
use stasis_demo_host::quota::{QuotaLimits, QuotaStore};
use stasis_demo_host::rate_limit::SlidingWindowLimiter;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use urspace::{MintOptions, ShareOptions, Site};

struct SiteMinter {
    site: Site,
}

impl TokenMinter for SiteMinter {
    fn mint(&self, subject: [u8; 32], ttl: Duration, max_sessions: u32) -> anyhow::Result<String> {
        self.site
            .mint_with(subject, MintOptions { ttl, max_sessions })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("stasis_demo_host=info")),
        )
        .init();

    let config = Config::from_env()?;
    run(config).await
}

async fn run(config: Config) -> Result<()> {
    let quota = QuotaStore::new(QuotaLimits {
        daily_completions: config.daily_completions,
        daily_tokens: config.daily_tokens,
    });
    let proxy_state = ProxyState::new(
        config.openai_api_key.clone(),
        config.openai_base_url.clone(),
        config.allowed_models.clone(),
        quota,
    )?;

    let proxy_listener = TcpListener::bind(config.proxy_bind)
        .await
        .with_context(|| format!("bind proxy on {}", config.proxy_bind))?;
    let proxy_addr = proxy_listener.local_addr()?;
    tracing::info!(%proxy_addr, "proxy listening (loopback; shared via urspace only)");

    let proxy_app = stasis_demo_host::proxy::router(proxy_state);
    tokio::spawn(async move {
        if let Err(err) = axum::serve(proxy_listener, proxy_app).await {
            tracing::error!(error = %err, "proxy server exited");
        }
    });

    let share = ShareOptions {
        bootstrap_origin: config.urspace_bootstrap_origin.clone(),
        ..ShareOptions::default()
    };
    let origin = loopback_origin(proxy_addr)?;
    let site = Site::serve_with(&origin, share)
        .await
        .context("start urspace Site (production relay)")?;
    tracing::info!(site_id = %site.site_id(), "urspace site online");

    let minter = Arc::new(SiteMinter { site });
    let mint_state = MintState {
        minter: minter.clone(),
        per_ip: Arc::new(SlidingWindowLimiter::new(
            config.mint_per_ip,
            config.mint_window,
        )),
        per_subject: Arc::new(SlidingWindowLimiter::new(
            config.mint_per_subject,
            config.mint_window,
        )),
        mint_ttl: config.mint_ttl,
        max_sessions: config.mint_max_sessions,
        trust_proxy: config.trust_proxy,
    };

    let mint_listener = TcpListener::bind(config.mint_bind)
        .await
        .with_context(|| format!("bind mint on {}", config.mint_bind))?;
    let mint_addr = mint_listener.local_addr()?;
    tracing::info!(%mint_addr, "mint listener (POST /bootstrap, GET /health)");

    let mint_app = stasis_demo_host::mint::router(mint_state, &config.cors_origins)
        .into_make_service_with_connect_info::<SocketAddr>();
    let mint_server = axum::serve(mint_listener, mint_app);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal");
        }
        result = mint_server => {
            result.context("mint server")?;
        }
    }

    if let Ok(minter) = Arc::try_unwrap(minter) {
        minter.site.shutdown().await.ok();
    }
    Ok(())
}

fn loopback_origin(addr: SocketAddr) -> Result<String> {
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_loopback() => ip.to_string(),
        IpAddr::V6(ip) if ip.is_loopback() => format!("[{ip}]"),
        other => anyhow::bail!("PROXY_BIND must be loopback (127.0.0.1 / ::1), got {other}"),
    };
    Ok(format!("http://{host}:{}", addr.port()))
}
