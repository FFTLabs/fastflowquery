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
//! - [`stream`]
//!
//! Feature flags:
//! - no crate-level flags; vector expression support is coordinated with planner/runtime features.

pub mod context;
pub mod exec_node;
pub mod expressions;
pub mod stream;

// Re-export only what you want at the crate root (no globs).
pub use context::{SharedTaskContext, TaskContext};
pub use exec_node::ExecNode;
pub use expressions::{PhysicalExpr, compile_expr};
pub use stream::{
    BatchSender, RecordBatchStream, SendableRecordBatchStream, StreamAdapter,
    bounded_batch_channel, empty_stream,
};
