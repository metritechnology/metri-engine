//! Invalidación de las cachés de lectura (`EAV_CACHE` y `AEVT_SCAN_CACHE`) tras confirmar una transacción.
//!
//! Las cachés viven en el lector (`EAV_CACHE` por entidad, `AEVT_SCAN_CACHE` por
//! `tenant + tipo`); el escritor conoce qué entidades mutaron (incluida la entidad
//! madre y las entidades proyectadas de saga) y coordina su invalidación en un
//! único punto.
//!
//! La ruta masiva (`bulk`) difiere la invalidación al final del lote mediante
//! `CachePolicy::Deferred` e `invalidate_aevt_scan`.

/// Política que determina cuándo invalida el escritor las cachés de lectura.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CachePolicy {
    /// Invalidar inmediatamente al finalizar la transacción (camino estándar).
    #[default]
    Immediate,
    /// No invalidar por fila; el llamador (`route_bulk`) invalida una sola vez al finalizar el lote completo.
    Deferred,
}

/// Referencia a una entidad escrita dentro de la transacción para invalidar su entrada puntual y el escaneo de su tipo.
pub struct WrittenEntity<'a> {
    /// Identificador único de la entidad mutada.
    pub entity_id: &'a str,
    /// Tipo o modelo de la entidad mutada en el Códice.
    pub entity_type: &'a str,
}

/// Invalida las caches de TODAS las entidades escritas: la fila EAVT cacheada
/// de cada una y el scan AEVT de cada (tenant, entity_type) involucrado.
pub fn invalidate_entity_caches(tenant_id: &str, written: &[WrittenEntity<'_>]) {
    if let Ok(mut cache) = crate::eav::reader::pull::EAV_CACHE.write() {
        for w in written {
            let pk = format!("T#{tenant_id}#E#{}", w.entity_id);
            cache.remove(&pk);
        }
    }

    if let Ok(mut cache) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
        for w in written {
            let key = (tenant_id.to_string(), w.entity_type.to_string());
            cache.remove(&key);
            tracing::debug!(
                "AEVT_SCAN_CACHE INVALIDATED for tenant={}, type={}",
                tenant_id,
                w.entity_type
            );
        }
    }
}

/// Invalida sólo el scan AEVT de un (tenant, entity_type) — la parte que la
/// ruta bulk difiere y ejecuta una sola vez al final del lote.
pub fn invalidate_aevt_scan(tenant_id: &str, entity_type: &str) {
    if let Ok(mut cache) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
        let key = (tenant_id.to_string(), entity_type.to_string());
        cache.remove(&key);
    }
}
