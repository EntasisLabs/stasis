use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct AgentRegistered {
    pub agent_id: String,
    pub at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct AgentInvoked {
    pub agent_id: String,
    pub at: DateTime<Utc>,
    pub prompt_size: usize,
}
