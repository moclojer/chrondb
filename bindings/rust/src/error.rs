use std::fmt;

/// Errors returned by ChronDB operations.
#[derive(Debug)]
pub enum ChronDBError {
    /// Failed to setup/download native library
    SetupFailed(String),
    /// Failed to create GraalVM isolate
    IsolateCreationFailed,
    /// Failed to open database
    OpenFailed(String),
    /// Failed to close database
    CloseFailed,
    /// Document not found
    NotFound,
    /// Operation failed with an error message
    OperationFailed(String),
    /// JSON serialization/deserialization error
    JsonError(String),
}

impl fmt::Display for ChronDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChronDBError::SetupFailed(msg) => write!(f, "library setup failed: {}", msg),
            ChronDBError::IsolateCreationFailed => write!(f, "failed to create GraalVM isolate"),
            ChronDBError::OpenFailed(msg) => write!(f, "failed to open database: {}", msg),
            ChronDBError::CloseFailed => write!(f, "failed to close database"),
            ChronDBError::NotFound => write!(f, "document not found"),
            ChronDBError::OperationFailed(msg) => write!(f, "operation failed: {}", msg),
            ChronDBError::JsonError(msg) => write!(f, "JSON error: {}", msg),
        }
    }
}

impl std::error::Error for ChronDBError {}

impl From<serde_json::Error> for ChronDBError {
    fn from(e: serde_json::Error) -> Self {
        ChronDBError::JsonError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ChronDBError>;
