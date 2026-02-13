use thiserror::Error;

#[derive(Debug, Error)]
pub enum FfqError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("planning error: {0}")]
    Planning(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, FfqError>;
