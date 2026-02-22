#![deny(missing_docs)]

//! Execution-layer primitives used by runtimes and physical operators.
//!
//! Architecture role:
//! - task context and execution node contracts
//! - expression compilation/evaluation
//! - batch stream abstractions and channels
//!
//! Key modules:
//! - [`context`]
//! - [`exec_node`]
//! - [`expressions`]
//! - [`physical_registry`]
//! - [`stream`]
//!
//! Feature flags:
//! - no crate-level flags; vector expression support is coordinated with planner/runtime features.

pub mod context;
pub mod exec_node;
pub mod expressions;
/// Custom physical operator registry contracts and global registration helpers.
pub mod physical_registry;
pub mod stream;
pub mod udf;

// Re-export only what you want at the crate root (no globs).
pub use context::{SharedTaskContext, TaskContext};
pub use exec_node::ExecNode;
pub use expressions::{PhysicalExpr, compile_expr};
pub use physical_registry::{
    PhysicalOperatorFactory, PhysicalOperatorRegistry, deregister_global_physical_operator_factory,
    global_physical_operator_registry, register_global_physical_operator_factory,
};
pub use stream::{
    BatchSender, RecordBatchStream, SendableRecordBatchStream, StreamAdapter,
    bounded_batch_channel, empty_stream,
};
pub use udf::{ScalarUdf, deregister_scalar_udf, get_scalar_udf, register_scalar_udf};
