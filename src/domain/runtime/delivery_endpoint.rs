use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryProtocol {
    HttpWebhook,
    Tcp,
    Kafka,
    RabbitMq,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryEndpoint {
    pub endpoint_id: String,
    pub name: String,
    pub protocol: DeliveryProtocol,
    pub target: String,
    pub metadata: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewDeliveryEndpoint {
    pub endpoint_id: String,
    pub name: String,
    pub protocol: DeliveryProtocol,
    pub target: String,
    pub metadata: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl NewDeliveryEndpoint {
    pub fn into_record(self) -> DeliveryEndpoint {
        DeliveryEndpoint {
            endpoint_id: self.endpoint_id,
            name: self.name,
            protocol: self.protocol,
            target: self.target,
            metadata: self.metadata,
            enabled: true,
            created_at: self.created_at,
            updated_at: self.created_at,
        }
    }
}