#[derive(Clone, Debug)]
pub struct RegisterAgentRequest {
    pub id: String,
    pub name: String,
    pub system_prompt: String,
}

#[derive(Clone, Debug)]
pub struct InvokeAgentRequest {
    pub agent_id: String,
    pub user_prompt: String,
}

#[derive(Clone, Debug)]
pub struct InvokeAgentResponse {
    pub agent_id: String,
    pub completion: String,
}
