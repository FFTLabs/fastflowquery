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

pub mod bench_fixtures;
pub mod bench_queries;
pub mod dataframe;
pub mod engine;
pub mod expr;
pub mod repl;
pub mod tpch_tbl;

pub use dataframe::{DataFrame, WriteMode};
pub use engine::Engine;
pub use expr::*;
