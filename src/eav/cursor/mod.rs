//! Pagination cursors — opaque, stable tokens across pages.
pub mod composite;
pub use composite::{fingerprint_query, CompositeCursor, IndexCursorState};
