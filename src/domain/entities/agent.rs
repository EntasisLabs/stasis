use chrono::{DateTime, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::value_objects::agent_id::AgentId;

#[derive(Clone, Debug)]
pub struct Agent {
    pub id: AgentId,
    pub name: String,
    pub system_prompt: String,
    pub created_at: DateTime<Utc>,
}

impl Agent {
    pub fn new(id: String, name: String, system_prompt: String) -> Result<Self> {
        if name.trim().is_empty() {
            return Err(StasisError::InvalidName(name));
        }

        if system_prompt.trim().is_empty() {
            return Err(StasisError::InvalidSystemPrompt);
        }

        Ok(Self {
            id: AgentId::new(id)?,
            name,
            system_prompt,
            created_at: Utc::now(),
        })
    }
}
