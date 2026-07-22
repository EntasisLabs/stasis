//! Task-local MCP recursion depth for boundary re-entry (ADR-0007 Phase 3).

use std::future::Future;

tokio::task_local! {
    static MCP_REMAINING_DEPTH: u8;
}

/// Current remaining depth inside an MCP export/provider scope, if any.
pub fn current_remaining_depth() -> Option<u8> {
    MCP_REMAINING_DEPTH.try_with(|depth| *depth).ok()
}

/// Run `fut` with `remaining_depth` visible to nested MCP boundary crossings.
pub async fn scope_remaining_depth<F, T>(remaining_depth: u8, fut: F) -> T
where
    F: Future<Output = T>,
{
    MCP_REMAINING_DEPTH.scope(remaining_depth, fut).await
}
