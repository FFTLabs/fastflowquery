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

pub mod analyzer;
pub mod explain;
pub mod logical_plan;
pub mod optimizer;
pub mod physical_plan;
pub mod physical_planner;
pub mod sql_frontend;

pub use analyzer::*;
pub use explain::*;
pub use logical_plan::*;
pub use optimizer::*;
pub use physical_plan::*;
pub use physical_planner::*;
pub use sql_frontend::*;
