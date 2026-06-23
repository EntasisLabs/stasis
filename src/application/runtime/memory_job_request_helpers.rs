use chrono::{DateTime, Utc};

use crate::ports::outbound::memory::memory_models::MemoryScope;

pub fn memory_scope_from_fields(
    tenant_id: Option<String>,
    session_ids: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
    from_utc: Option<DateTime<Utc>>,
    to_utc: Option<DateTime<Utc>>,
) -> MemoryScope {
    MemoryScope {
        tenant_id,
        session_ids,
        tiers,
        from_utc,
        to_utc,
    }
}
