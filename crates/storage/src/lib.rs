#![warn(missing_docs)]

//! Storage providers, catalog model, and table metadata APIs.
//!
//! Architecture role:
//! - table definition/catalog load-save contracts
//! - provider abstraction for scan/stat estimation
//! - parquet and optional external/vector backends
//!
//! Key modules:
//! - [`catalog`]
//! - [`provider`]
//! - [`parquet_provider`]
//! - [`stats`]
//! - [`vector_index`]
//! - `object_store_provider` (feature-gated)
//! - `qdrant_provider` (feature-gated)
//!
//! Feature flags:
//! - `s3`: enables object-store provider implementation
//! - `qdrant`: enables qdrant-backed vector index provider.

/// Table/catalog model and persistence.
pub mod catalog;
/// Parquet-backed storage provider and schema inference helpers.
pub mod parquet_provider;
/// Provider traits and scan/stats abstractions.
pub mod provider;
/// Table statistics model.
pub mod stats;
/// Vector index provider abstraction.
pub mod vector_index;

#[cfg(feature = "s3")]
/// Experimental object-store storage provider.
pub mod object_store_provider;

#[cfg(feature = "qdrant")]
/// Qdrant-backed vector index provider.
pub mod qdrant_provider;

pub use catalog::*;
pub use provider::*;
pub use stats::TableStats;
pub use vector_index::*;
