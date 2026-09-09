//! Write sharding — deterministic scatter over hot partitions.
pub mod shard;
pub use shard::{scatter_pks, shard_key};
