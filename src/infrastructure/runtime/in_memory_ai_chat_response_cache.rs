use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use genai::chat::ChatResponse;

use crate::ports::outbound::ai_chat_response_cache::AiChatResponseCache;

#[derive(Clone, Default)]
pub struct InMemoryAiChatResponseCache {
    state: Arc<RwLock<HashMap<String, ChatResponse>>>,
}

impl AiChatResponseCache for InMemoryAiChatResponseCache {
    fn get(&self, key: &str) -> Option<ChatResponse> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.get(key).cloned())
    }

    fn set(&self, key: &str, response: ChatResponse) {
        if let Ok(mut state) = self.state.write() {
            state.insert(key.to_string(), response);
        }
    }
}
