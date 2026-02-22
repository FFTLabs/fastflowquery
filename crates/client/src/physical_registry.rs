//! Client-level re-exports for custom physical operator extension hooks.
//!
//! The underlying registry and factory contract are defined in `ffq-execution`
//! so both embedded and distributed runtimes can use the same types.

pub use ffq_execution::{
    PhysicalOperatorFactory, PhysicalOperatorRegistry, global_physical_operator_registry,
};
