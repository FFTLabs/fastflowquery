//! Shared configuration, error types, IDs, and observability primitives for FFQ crates.
//!
//! Architecture role:
//! - defines engine/runtime configuration passed across layers
//! - provides common [`FfqError`] / [`Result`] contracts
//! - hosts metrics and optional exporter utilities
//!
//! Key modules:
//! - [`config`]
//! - [`error`]
//! - [`ids`]
//! - [`metrics`]
//! - `metrics_exporter` (feature-gated)
//!
//! Feature flags:
//! - `profiling`: enables the metrics HTTP exporter helpers.

pub mod config;
pub mod error;
pub mod ids;
pub mod metrics;
#[cfg(feature = "profiling")]
pub mod metrics_exporter;

pub use config::{EngineConfig, SchemaDriftPolicy, SchemaInferencePolicy};
pub use error::{FfqError, Result};
pub use ids::*;
pub use metrics::MetricsRegistry;
#[cfg(feature = "profiling")]
pub use metrics_exporter::run_metrics_exporter;
