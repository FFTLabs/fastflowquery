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

pub mod layout;
pub mod reader;
pub mod writer;

pub use layout::*;
pub use reader::ShuffleReader;
pub use writer::ShuffleWriter;
