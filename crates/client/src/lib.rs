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

mod planner_facade;
mod runtime;
mod session;

/// Benchmark fixture generation and loading helpers.
pub mod bench_fixtures;
/// Canonical benchmark SQL query definitions and loaders.
pub mod bench_queries;
/// DataFrame API and write/query execution helpers.
pub mod dataframe;
/// Engine/session entrypoints and table registration APIs.
pub mod engine;
/// Expression builder helpers for DataFrame plans.
pub mod expr;
/// Interactive SQL REPL implementation.
pub mod repl;
/// TPC-H `.tbl` fixture conversion and validation helpers.
pub mod tpch_tbl;
#[cfg(feature = "ffi")]
mod ffi;

pub use dataframe::{DataFrame, WriteMode};
pub use engine::Engine;
pub use expr::*;
