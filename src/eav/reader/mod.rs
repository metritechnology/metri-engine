//! EAV readers — `pull` (entity state) and `query` (physical plans).
pub mod pull;
pub mod query;
pub use pull::{EavReader, EntityMap, HistoryEntry};
