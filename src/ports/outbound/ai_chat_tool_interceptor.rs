#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AiToolCallEnvelope {
    pub request_fingerprint: String,
    pub tool_call_count: usize,
    pub tool_names: Vec<String>,
}

pub trait AiChatToolInterceptor: Send + Sync {
    fn on_tool_calls(&self, envelope: AiToolCallEnvelope);
}
