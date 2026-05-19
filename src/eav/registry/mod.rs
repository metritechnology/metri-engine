pub mod descriptor;
pub mod registry;
pub mod migration;

pub use descriptor::{AttributeDescriptor, Cardinality, UniqueStrategy, AttrStatus, ValueType};
pub use registry::AttributeRegistry;
pub use migration::{diff_registries, BackfillStrategy, RegistryMigration};
