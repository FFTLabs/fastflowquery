#![deny(missing_docs)]

//! Shuffle file layout and read/write utilities.
//!
//! Architecture role:
//! - defines deterministic shuffle path contracts
//! - writes partitioned Arrow IPC data and index metadata
//! - reads shuffle partitions for downstream stages
//!
//! Key modules:
//! - [`layout`]
//! - [`writer`]
//! - [`reader`]
//!
//! Feature flags:
//! - none.

/// Deterministic path and index metadata contracts for shuffle files.
pub mod layout;
/// Shuffle readers for partition/index payloads.
pub mod reader;
/// Shuffle writers for partition/index payloads and TTL cleanup.
pub mod writer;

pub use layout::*;
pub use reader::ShuffleReader;
pub use writer::ShuffleWriter;
