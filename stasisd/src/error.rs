use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug)]
pub enum StasisdError {
    Usage(String),
    Validation(String),
    Io(io::Error),
    #[allow(dead_code)]
    Runtime(String),
}

impl Display for StasisdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(f, "{message}"),
            Self::Validation(message) => write!(f, "{message}"),
            Self::Io(err) => write!(f, "{err}"),
            Self::Runtime(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for StasisdError {}

impl From<io::Error> for StasisdError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
