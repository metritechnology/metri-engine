//! Hierarchies — materialized paths with O(1) subtree queries.
pub mod path;
pub use path::{build_hierarchy_path, subtree_query_pk};
