use std::sync::Arc;

use crate::ports::outbound::ai_chat_client::AiChatClient;

pub trait ChatClientMiddleware: Send + Sync {
    fn wrap(&self, inner: Arc<dyn AiChatClient>) -> Arc<dyn AiChatClient>;
}
