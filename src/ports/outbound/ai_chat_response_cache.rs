use genai::chat::ChatResponse;

pub trait AiChatResponseCache: Send + Sync {
    fn get(&self, key: &str) -> Option<ChatResponse>;
    fn set(&self, key: &str, response: ChatResponse);
}
