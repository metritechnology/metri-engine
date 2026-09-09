//! Zero-downtime schema evolution — registry diff and backfill.
//!
//! eav/registry/migration.rs
//! Schema evolution sin downtime — diff de registries y estrategia de backfill.
//! Blueprint: Metri EAV §XIII.3

use super::descriptor::AttributeDescriptor;
use super::registry::AttributeRegistry;

#[derive(Debug, Clone)]
pub enum BackfillStrategy {
    Async,
    Sync,
}

/// Resultado del diff entre la versión en DynamoDB y la compilada en el binario.
#[derive(Debug, Default)]
pub struct RegistryMigration {
    pub added: Vec<AttributeDescriptor>,
    pub deprecated: Vec<u16>,
    pub renamed: Vec<(u16, String)>,
}

/// Calcula la diferencia entre dos registries.
/// [Blueprint: §XIII.3 — 3-Phase Schema Evolution Protocol]
pub fn diff_registries(
    stored: &AttributeRegistry,
    compiled: &AttributeRegistry,
) -> RegistryMigration {
    let mut migration = RegistryMigration::default();

    // Detectar atributos nuevos en el binario compilado que no existen en el stored
    for id in 0u16..=255 {
        let in_compiled = compiled.get_by_id(id);
        let in_stored = stored.get_by_id(id);
        match (in_stored, in_compiled) {
            (None, Some(new_attr)) => {
                // Attr nuevo — añadir con BACKFILL_PENDING
                migration.added.push(new_attr.clone());
            }
            (Some(old), None) => {
                // Attr eliminado — marcar deprecated
                migration.deprecated.push(old.id);
            }
            (Some(old), Some(new)) if old.name != new.name => {
                // Renombrado — alias transition
                migration.renamed.push((old.id, new.name.clone()));
            }
            _ => {}
        }
    }

    migration
}
