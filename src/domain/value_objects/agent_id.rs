use crate::domain::errors::{Result, StasisError};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct AgentId(String);

impl AgentId {
    pub fn new(value: String) -> Result<Self> {
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(StasisError::InvalidAgentId(value));
        }

        if !trimmed
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(StasisError::InvalidAgentId(value));
        }

        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<AgentId> for String {
    fn from(value: AgentId) -> Self {
        value.0
    }
}
