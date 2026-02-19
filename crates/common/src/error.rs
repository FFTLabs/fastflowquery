use thiserror::Error;

/// Canonical FFQ error taxonomy used across crates.
///
/// Classification guidance:
/// - [`FfqError::Planning`]: query shape/name/type issues discovered before execution
/// - [`FfqError::Execution`]: runtime operator evaluation, decode/encode, or data-shape failures
/// - [`FfqError::InvalidConfig`]: catalog/config/environment/path contract violations
/// - [`FfqError::Unsupported`]: syntactically valid but intentionally unimplemented behavior
/// - [`FfqError::Io`]: raw filesystem/network IO failures from std APIs
#[derive(Debug, Error)]
pub enum FfqError {
    /// Invalid or inconsistent configuration/catalog state.
    ///
    /// Examples:
    /// - missing required table location (`uri`/`paths`)
    /// - schema inference disabled for schema-less parquet table
    /// - invalid policy/env/CLI option values
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Query planning/analyzer/optimizer failures.
    ///
    /// Examples:
    /// - unknown table/column
    /// - type mismatch in expressions or join keys
    /// - invalid LIMIT/TOP-K values
    #[error("planning error: {0}")]
    Planning(String),

    /// Runtime execution failures after planning succeeded.
    ///
    /// Examples:
    /// - expression evaluation/type mismatch at runtime
    /// - parquet/shuffle decode failures
    /// - spill/merge state shape mismatches
    #[error("execution error: {0}")]
    Execution(String),

    /// Transparent std IO failures.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Valid request for a feature/shape not implemented in current version.
    ///
    /// Examples:
    /// - SQL constructs outside supported subset
    /// - provider/feature-flag-gated functionality unavailable at build/runtime
    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Standard FFQ result alias.
pub type Result<T> = std::result::Result<T, FfqError>;
