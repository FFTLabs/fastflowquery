#![warn(missing_docs)]

//! Logical/physical planning stack for FFQ SQL and DataFrame execution.
//!
//! Architecture role:
//! - SQL frontend translation into logical plans
//! - analysis (name/type resolution) and optimizer rewrites
//! - physical plan model and lowering
//!
//! Key modules:
//! - [`sql_frontend`]
//! - [`analyzer`]
//! - [`optimizer`]
//! - [`physical_plan`]
//! - [`physical_planner`]
//! - [`explain`]
//!
//! Feature flags:
//! - vector and qdrant-related rewrites are conditionally compiled via crate features.

/// Semantic analyzer and schema resolution.
pub mod analyzer;
/// Logical plan explain/pretty formatting helpers.
pub mod explain;
/// Logical plan and expression model.
pub mod logical_plan;
/// Rule-based logical optimizer.
pub mod optimizer;
/// Physical operator plan model.
pub mod physical_plan;
/// Logical-to-physical lowering.
pub mod physical_planner;
/// SQL frontend translation into logical plans.
pub mod sql_frontend;

pub use analyzer::*;
pub use explain::*;
pub use logical_plan::*;
pub use optimizer::*;
pub use physical_plan::*;
pub use physical_planner::*;
pub use sql_frontend::*;
