//! Append-only log storage with segment files.

mod log;
mod segment;

pub use log::{PartitionLog, PartitionLogConfig};
pub use segment::Segment;
