use chrono::{DateTime, Utc};

use crate::domain::runtime::job::BackoffPolicy;
use crate::domain::runtime::placement::PlacementConstraints;

pub const INBOUND_TRIGGER_SCHEMA_VERSION_V1: u32 = 1;

/// Transport that delivered an inbound job trigger. Listeners own the socket or consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboundProtocol {
    HttpWebhook,
    Tcp,
    Kafka,
    RabbitMq,
}

impl InboundProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HttpWebhook => "http_webhook",
            Self::Tcp => "tcp",
            Self::Kafka => "kafka",
            Self::RabbitMq => "rabbitmq",
        }
    }
}

/// Canonical request that starts one durable job. The same document is used for every protocol.
#[derive(Clone, Debug, PartialEq)]
pub struct InboundJobTrigger {
    pub protocol: InboundProtocol,
    pub idempotency_key: String,
    pub job_type: String,
    pub payload_ref: String,
    pub queue: String,
    pub priority: i32,
    pub max_attempts: u32,
    pub backoff: BackoffPolicy,
    pub correlation_id: Option<String>,
    pub placement: PlacementConstraints,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboundDisposition {
    Accepted,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundAccept {
    pub disposition: InboundDisposition,
    pub job_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundTriggerReceipt {
    pub idempotency_key: String,
    pub protocol: String,
    pub job_id: String,
    pub job_type: String,
    pub accepted_at: DateTime<Utc>,
}
