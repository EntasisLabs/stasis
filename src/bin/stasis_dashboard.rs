use std::net::SocketAddr;

use stasis::application::config::env::{bootstrap, non_empty, truthy};
use stasis::dashboard::{
    build_dashboard_query_service, router, DashboardBootstrapOptions, DashboardState,
};

#[tokio::main]
async fn main() {
    if let Err(err) = bootstrap() {
        eprintln!("stasis env bootstrap warning: {err}");
    }

    let seed_demo_data = truthy("STASIS_DASHBOARD_DEMO_SEED");

    let service = build_dashboard_query_service(DashboardBootstrapOptions {
        seed_demo: seed_demo_data,
    })
    .await
    .expect("build dashboard runtime and query service");

    let mut state = DashboardState::new(service);
    if seed_demo_data {
        state = state.with_demo_seed_mode(true);
    }
    if let Some(token) = non_empty("STASIS_DASHBOARD_ACTION_AUTH_BEARER") {
        state = state.with_action_auth_bearer_token(token);
    }
    if let Some(required_role) = non_empty("STASIS_DASHBOARD_ACTION_REQUIRED_ROLE") {
        state = state.with_action_required_role(required_role);
    }
    if let Some(role_claim_header) = non_empty("STASIS_DASHBOARD_ACTION_ROLE_CLAIM_HEADER") {
        state = state.with_action_role_claim_header(role_claim_header);
    }
    let app = router(state);

    let addr: SocketAddr = non_empty("STASIS_DASHBOARD_ADDR")
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(|| {
            "127.0.0.1:3007"
                .parse()
                .expect("valid dashboard bind address")
        });

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind dashboard listener");

    println!("stasis dashboard listening on http://{}", addr);
    if seed_demo_data {
        println!("dashboard demo seed mode enabled via STASIS_DASHBOARD_DEMO_SEED");
    }

    axum::serve(listener, app)
        .await
        .expect("run dashboard server");
}
