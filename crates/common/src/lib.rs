#![deny(missing_docs)]

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

/// Shared engine/runtime configuration types.
pub mod config;
/// Shared error taxonomy.
pub mod error;
/// Strongly-typed identifier wrappers.
pub mod ids;
/// Metrics registry and Prometheus rendering helpers.
pub mod metrics;
#[cfg(feature = "profiling")]
/// Optional HTTP metrics exporter.
pub mod metrics_exporter;

pub use config::{CteReusePolicy, EngineConfig, SchemaDriftPolicy, SchemaInferencePolicy};
pub use error::{FfqError, Result};
pub use ids::*;
pub use metrics::MetricsRegistry;
#[cfg(feature = "profiling")]
pub use metrics_exporter::run_metrics_exporter;
