#![deny(missing_docs)]

//! End-user client surface for FFQ.
//!
//! Architecture role:
//! - exposes [`Engine`] and [`DataFrame`] APIs
//! - wires planner, catalog/session state, and runtime execution
//! - provides REPL/benchmark/fixture helper modules used in tests and tooling
//!
//! Key modules:
//! - [`engine`]
//! - [`embedding`]
//! - [`dataframe`]
//! - [`expr`]
//! - [`repl`]
//! - [`tpch_tbl`]
//! - [`bench_queries`]
//! - [`bench_fixtures`]
//!
//! Feature flags:
//! - `distributed`: enables coordinator-backed runtime path
//! - `vector` / `qdrant` / `profiling`: enable optional vector and observability paths.

mod physical_registry;
mod planner_facade;
mod runtime;
mod session;

/// Benchmark fixture generation and loading helpers.
pub mod bench_fixtures;
/// Canonical benchmark SQL query definitions and loaders.
pub mod bench_queries;
/// DataFrame API and write/query execution helpers.
pub mod dataframe;
/// Embedding provider API and built-in providers/plugins.
pub mod embedding;
/// Engine/session entrypoints and table registration APIs.
pub mod engine;
/// Expression builder helpers for DataFrame plans.
pub mod expr;
#[cfg(feature = "ffi")]
mod ffi;
#[cfg(feature = "python")]
mod python;
/// Interactive SQL REPL implementation.
pub mod repl;
/// TPC-H `.tbl` fixture conversion and validation helpers.
pub mod tpch_tbl;

#[cfg(feature = "vector")]
pub use dataframe::VectorKnnOverrides;
pub use dataframe::{DataFrame, WriteMode};
#[cfg(feature = "embedding-http")]
pub use embedding::HttpEmbeddingProvider;
pub use embedding::{EmbeddingProvider, SampleEmbeddingProvider};
pub use engine::Engine;
pub use expr::*;
pub use ffq_execution::ScalarUdf;
pub use physical_registry::PhysicalOperatorFactory;
