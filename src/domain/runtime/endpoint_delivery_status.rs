use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointDeliveryStatus {
    pub endpoint_id: String,
    pub success_count: u64,
    pub failure_count: u64,
    pub last_event_id: Option<String>,
    pub last_error: Option<String>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl EndpointDeliveryStatus {
    pub fn new(endpoint_id: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            endpoint_id: endpoint_id.into(),
            success_count: 0,
            failure_count: 0,
            last_event_id: None,
            last_error: None,
            last_success_at: None,
            last_failure_at: None,
            updated_at: now,
        }
    }
}
