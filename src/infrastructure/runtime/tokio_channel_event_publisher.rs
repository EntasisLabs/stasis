use async_trait::async_trait;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::outbox::OutboxEvent;
use crate::ports::outbound::runtime::event_publisher::EventPublisher;

#[derive(Clone)]
pub struct TokioChannelEventPublisher {
    tx: UnboundedSender<OutboxEvent>,
}

impl TokioChannelEventPublisher {
    pub fn new(tx: UnboundedSender<OutboxEvent>) -> Self {
        Self { tx }
    }

    pub fn channel() -> (Self, UnboundedReceiver<OutboxEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self::new(tx), rx)
    }
}

#[async_trait]
impl EventPublisher for TokioChannelEventPublisher {
    async fn publish(&self, event: &OutboxEvent) -> Result<()> {
        self.tx
            .send(event.clone())
            .map_err(|e| StasisError::PortFailure(format!("publish to tokio channel bus: {e}")))
    }
}
