use std::error::Error;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, StasisError>;

#[derive(Debug)]
pub enum StasisError {
    InvalidAgentId(String),
    InvalidName(String),
    InvalidSystemPrompt,
    AgentAlreadyExists(String),
    AgentNotFound(String),
    PortFailure(String),
}

impl Display for StasisError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAgentId(id) => write!(f, "invalid agent id: {id}"),
            Self::InvalidName(name) => write!(f, "invalid agent name: {name}"),
            Self::InvalidSystemPrompt => write!(f, "system prompt must not be empty"),
            Self::AgentAlreadyExists(id) => write!(f, "agent already exists: {id}"),
            Self::AgentNotFound(id) => write!(f, "agent not found: {id}"),
            Self::PortFailure(message) => write!(f, "port failure: {message}"),
        }
    }
}

impl Error for StasisError {}
