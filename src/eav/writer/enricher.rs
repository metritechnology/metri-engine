// eav/writer/enricher.rs
// Auto-generación de atributos de sistema en el Write Path.
// Blueprint: Metri EAV §MÓDULO 3 writer/enricher.rs

use ulid::Ulid;
use crate::eav::types::datom::{Datom, DatomValue};

/// Enriquece la lista de datoms con atributos de sistema generados automáticamente.
/// Se llama ANTES de build_write_items para garantizar que todos los datoms estén completos.
///
/// Atributos de sistema generados:
///   - entity/ulid: el ULID de la entidad (obligatorio)
///   - meta/created_at: epoch ms del momento de escritura (solo en Create)
///   - meta/updated_at: epoch ms de la última modificación
///   - entity/type: tipo de la entidad (ej. "work_order")
///   - tenant/id: tenant_id (inyectado siempre, no puede venir del cliente)
pub fn enrich_datoms(
    datoms:      &mut Vec<Datom>,
    entity_id:   &str,
    entity_type: &str,
    tenant_id:   &str,
    tx_id:       u64,
    is_create:   bool,
) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    // entity/ulid — identidad de la entidad
    datoms.push(Datom::assert(tenant_id, entity_id, "entity/ulid", 0x0000, DatomValue::Str(entity_id.to_string()), tx_id));
    // entity/type — tipo de la entidad
    datoms.push(Datom::assert(tenant_id, entity_id, "entity/type", 0x0001, DatomValue::Str(entity_type.to_string()), tx_id));
    // tenant/id — aislamiento multitenant (SIEMPRE sobreescrito por el servidor)
    datoms.push(Datom::assert(tenant_id, entity_id, "tenant/id", 0x0002, DatomValue::Str(tenant_id.to_string()), tx_id));

    if is_create {
        // meta/created_at — epoch ms de creación
        datoms.push(Datom::assert(tenant_id, entity_id, "meta/created_at", 0x0003, DatomValue::Instant(now_ms), tx_id));
    }

    // meta/updated_at — siempre actualizado
    datoms.push(Datom::assert(tenant_id, entity_id, "meta/updated_at", 0x0004, DatomValue::Instant(now_ms), tx_id));
}

/// Genera un nuevo ULID para una entidad recién creada.
pub fn generate_entity_id() -> String {
    Ulid::new().to_string()
}

/// Genera un TX ID monotónico basado en el timestamp del ULID.
pub fn generate_tx_id() -> u64 {
    Ulid::new().timestamp_ms()
}
