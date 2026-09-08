// eav/writer/cache_policy.rs — Invalidación de las caches de lectura tras el commit.
//
// Las caches viven en el lector (EAV_CACHE por entidad, AEVT_SCAN_CACHE por
// tenant+tipo); el escritor sólo sabe QUÉ entidades cambió. Este módulo hace
// esa traducción en un solo punto — incluidas las entidades proyectadas de
// saga, que antes no se invalidaban y podían quedar invisibles tras un scan
// cacheado de su tipo.
//
// La ruta bulk difiere la invalidación al final del lote (una sola
// invalidación AEVT en vez de una por fila): `CachePolicy::Deferred` aquí y
// `invalidate_aevt_scan` desde el canal.

/// Cuándo invalida el escritor las caches de lectura.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CachePolicy {
    /// Invalidar al terminar la transacción — el camino normal.
    #[default]
    Immediate,
    /// No invalidar aquí: el caller (route_bulk) lo hace UNA vez al final del
    /// lote completo — evita N `RwLock::write()` por fila [OPTIMIZATION: 10x Bulk].
    Deferred,
}

/// Una entidad escrita en la transacción, para invalidar su fila y el scan
/// de su tipo. La madre y cada proyección aportan la suya.
pub struct WrittenEntity<'a> {
    pub entity_id: &'a str,
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
