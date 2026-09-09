//! Attribute registry — descriptors, O(1) lookup and schema migration.
pub mod descriptor;
pub mod migration;
pub mod registry;

pub use descriptor::{AttrStatus, AttributeDescriptor, Cardinality, UniqueStrategy, ValueType};
pub use migration::{diff_registries, BackfillStrategy, RegistryMigration};
pub use registry::AttributeRegistry;
