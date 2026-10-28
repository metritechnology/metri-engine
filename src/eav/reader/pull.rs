//! Ruta de lectura del motor EAV — reconstrucción selectiva (`pull`) e histórica (`pull_as_of`, `history`) sobre `.mbf`.
//!
//! Invariantes de la lectura por partición de entidad:
//! - Toda lectura de la partición `T#<tenant>#E#<entity_id>` devuelve SÓLO los
//!   datoms de esa entidad exacta: el query `EAVT` del storage ya casa la `PK`
//!   por igualdad, y `filter_exact_entity_partition` lo garantiza como
//!   post-filtro defensivo (los ids que son prefijo de otros —`<fileId>` frente
//!   a `<fileId>_chunk_N`— no deben sangrar entre particiones).
//! - `history` honra `track_history: false` del Códice: esas entidades son
//!   infraestructura derivada (p. ej. `document_chunk`) — estado actual sí,
//!   historial no.

use std::collections::HashMap;
use std::sync::Arc;

use crate::mbf::storage::{AttributeValue, Blob, MbfStorageClient};
use tracing::debug;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::{
    datom::DatomValue,
    encoding::{decode_eavt_sk, eavt_sk_as_of},
};

use once_cell::sync::Lazy;
use std::sync::RwLock;

/// Entrada almacenada en la caché en memoria `EAV_CACHE` para una entidad reconstruida.
#[derive(Clone, Debug)]
pub struct CacheEntry {
    /// Indica si el mapa contiene la totalidad de los atributos vigentes de la entidad (`true`) o un subconjunto parcial (`false`).
    pub is_complete: bool,
    /// Mapa de atributos y valores vigentes de la entidad.
    pub map: EntityMap,
    /// Instante de creación de la entrada para acotar la obsolescencia entre instancias mediante `EAV_CACHE_TTL`.
    pub cached_at: std::time::Instant,
}

/// Tiempo de vida máximo de una entrada en la caché de lectura de entidades.
pub const EAV_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Capacidad máxima de entidades ensambladas en la caché en memoria por instancia.
pub const MAX_EAV_CACHE_SIZE: usize = 10_000;

/// Caché global concurrente en memoria de entidades EAV ensambladas, indexada por `T#<tenant_id>#E#<entity_id>`.
pub static EAV_CACHE: Lazy<RwLock<HashMap<String, CacheEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Expulsa una entidad de la caché local de point-lookups.
///
/// Punto de entrada explícito para la invalidación cruzada de cachés (el bus
/// de cedar/cache): las otras capas no tocan este candado directamente.
pub fn evict_cached_entity(tenant_id: &str, entity_id: &str) {
    let pk = format!("T#{}#E#{}", tenant_id, entity_id);
    if let Ok(mut lock) = EAV_CACHE.write() {
        lock.remove(&pk);
        tracing::info!("[CacheInvalidation] Evicted from EAV_CACHE: {}", pk);
    }
}

/// Inserta una entrada en EAV_CACHE asegurando que el número total de entradas no exceda MAX_EAV_CACHE_SIZE.
pub fn insert_eav_cache_entry(
    cache: &mut HashMap<String, CacheEntry>,
    pk: String,
    entry: CacheEntry,
) {
    if cache.len() >= MAX_EAV_CACHE_SIZE && !cache.contains_key(&pk) {
        let to_remove: Vec<String> = cache.keys().take(MAX_EAV_CACHE_SIZE / 5).cloned().collect();
        for k in to_remove {
            cache.remove(&k);
        }
    }
    cache.insert(pk, entry);
}

/// Resultado de un pull — mapa de atributos con sus valores actuales.
pub type EntityMap = HashMap<String, DatomValue>;

/// Estructura compacta para representar un datom decodificado durante el precalentamiento de caché (`warm_cache_for_tenant`).
#[derive(Debug)]
pub struct WarmupItem {
    /// Identificador único de la entidad a la que pertenece el datom.
    pub entity_id: String,
    /// Identificador monotónico de la transacción (`tx_id`).
    pub tx_id: u64,
    /// Operación del datom (`true` = assert, `false` = retract).
    pub op: bool,
    /// Valor tipado del atributo en este datom.
    pub value: DatomValue,
}

/// Lector de entidades del motor EAV sobre el almacenamiento indexado `.mbf`.
#[derive(Clone)]
pub struct EavReader {
    /// Cliente de almacenamiento `.mbf` subyacente.
    pub ddb: Arc<MbfStorageClient>,
    /// Nombre lógico de la tabla o partición `.mbf`.
    pub table: String,
}

impl EavReader {
    /// Crea una nueva instancia de `EavReader` asociada al cliente `.mbf` y tabla indicados.
    pub fn new(ddb: Arc<MbfStorageClient>, table: impl Into<String>) -> Self {
        EavReader {
            ddb,
            table: table.into(),
        }
    }

    /// Carga el estado actual de una entidad (todos los atributos vigentes).
    /// Equivale a `(d/pull db '[*] entity-id)` sobre el índice primario EAVT de `.mbf`.
    pub async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs: Option<&[&str]>, // None = todos los atributos
    ) -> Result<EntityMap, DomainError> {
        let _ = self.ddb.sync_if_needed(&self.table).await;
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        // 1. Check in-memory cache
        if let Ok(cache) = EAV_CACHE.read() {
            if let Some(entry) = cache.get(&pk) {
                let fresh = entry.cached_at.elapsed() <= EAV_CACHE_TTL;

                let is_sufficient = if !fresh {
                    false
                } else if entry.is_complete {
                    true
                } else if let Some(filter) = attrs {
                    filter.iter().all(|&attr| {
                        entry.map.contains_key(attr)
                            || entry
                                .map
                                .keys()
                                .any(|k| k.split('/').next_back() == Some(attr))
                    })
                } else {
                    false
                };

                if is_sufficient {
                    if let Some(filter) = attrs {
                        let filtered: EntityMap = entry
                            .map
                            .iter()
                            .filter(|(k, _)| {
                                let bare = k.split('/').next_back().unwrap_or(k);
                                filter.contains(&k.as_str()) || filter.contains(&bare)
                            })
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        return Ok(filtered);
                    } else {
                        return Ok(entry.map.clone());
                    }
                }
            }
        }

        // Query en índice primario EAVT de .mbf — todos los datoms de la entidad, ordenados por SK
        // SK binario: [attr_id:2B][tx_id:8B][op:1B]
        let key_condition = "#pk = :pk".to_string();
        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

        let raw_items = self
            .ddb
            .query(
                &self.table,
                None,
                &key_condition,
                attr_names.clone(),
                attr_values.clone(),
                true,
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("pull falló: {e:?}")))?;
        let raw_items = filter_exact_entity_partition(raw_items, &pk);

        if raw_items.is_empty() {
            return Ok(HashMap::new());
        }

        // Reconstruir el estado actual completo (None filter) para poblar la cache
        // [BLUEPRINT: §V.1 — "el estado actual es el datom assert más reciente por atributo"]
        let full_entity_map = assemble_current_state(raw_items, None);

        if let Ok(mut cache) = EAV_CACHE.write() {
            insert_eav_cache_entry(
                &mut cache,
                pk.clone(),
                CacheEntry {
                    is_complete: true,
                    map: full_entity_map.clone(),
                    cached_at: std::time::Instant::now(),
                },
            );
        }

        // Retornar filtrado si es necesario
        let result_map = if let Some(filter) = attrs {
            let filtered: EntityMap = full_entity_map
                .iter()
                .filter(|(k, _)| {
                    let bare = k.split('/').next_back().unwrap_or(k);
                    filter.contains(&k.as_str()) || filter.contains(&bare)
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            filtered
        } else {
            full_entity_map
        };

        debug!(
            "[EAV] pull (CACHE MISS/FALLBACK) tenant={tenant_id} entity_id={entity_id} attrs={}",
            result_map.len()
        );

        Ok(result_map)
    }

    /// Reconstruye mediante *time-travel* el estado de la entidad vigente hasta el corte transaccional `as_of_tx`.
    pub async fn pull_as_of(
        &self,
        tenant_id: &str,
        entity_id: &str,
        as_of_tx: u64,
        attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        // Query con SK <= [attr_id][as_of_tx][0x01], ScanIndexForward=false, Limit=1 por atributo
        // Para simplicidad en FASE 1: traemos todos y filtramos por tx_id <= as_of_tx
        // FASE 2 optimizará con queries per-atributo con SK range.
        let key_condition = "#pk = :pk AND #sk <= :sk_max".to_string();
        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();

        // SK máximo: tx_id = as_of_tx, op = true → filtra todo lo posterior al snapshot
        // [BLUEPRINT: "SK ≤ T, max per attr"]
        let sk_max = eavt_sk_as_of(0xFFFF, as_of_tx); // attr_id=0xFFFF → cap universal

        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_names.insert("#sk".to_string(), "SK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));
        attr_values.insert(":sk_max".to_string(), AttributeValue::B(Blob::new(sk_max)));

        let raw_items = self
            .ddb
            .query(
                &self.table,
                None,
                &key_condition,
                attr_names,
                attr_values,
                true,
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("pull_as_of falló: {e:?}")))?;

        let raw_items = filter_exact_entity_partition(raw_items, &pk);
        Ok(assemble_current_state(raw_items, attrs))
    }

    /// Consulta el historial completo de una entidad retornando todos sus datoms (incluyendo retracciones) para auditoría.
    ///
    /// Devuelve vacío cuando el tipo real de la entidad declara
    /// `track_history: false` en el Códice (`document_chunk`, `audit_log`, …):
    /// es infraestructura derivada — su estado actual se lee con `pull`, pero
    /// no expone línea de tiempo. El tipo se resuelve del datom `entity/type`
    /// más reciente de la propia partición, no del tipo que el caller declara:
    /// un id de chunk consultado "como file" también queda vetado.
    pub async fn history(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attr_name: Option<&str>,
    ) -> Result<Vec<HistoryEntry>, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        let key_condition = "#pk = :pk".to_string();
        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

        let raw_items = self
            .ddb
            .query(
                &self.table,
                None,
                &key_condition,
                attr_names,
                attr_values,
                true,
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("history falló: {e:?}")))?;
        let raw_items = filter_exact_entity_partition(raw_items, &pk);

        let entries: Vec<HistoryEntry> = raw_items
            .into_iter()
            .filter_map(|item| {
                let sk = match item.get("SK") {
                    Some(AttributeValue::B(blob)) => blob.as_ref(),
                    _ => return None,
                };
                let (attr_id, tx_id, op) = decode_eavt_sk(sk)?;

                let attr = crate::codice::global()
                    .get_attr_name(attr_id)
                    .unwrap_or("unknown_attr")
                    .to_string();
                let value = extract_datom_value(&item);
                Some(HistoryEntry {
                    attr_name: attr.to_string(),
                    value,
                    tx_id,
                    op,
                })
            })
            .collect();

        // El veto `track_history` se evalúa ANTES del filtro por atributo:
        // necesita el datom `entity/type` aunque el caller pida otro atributo.
        if !history_tracking_enabled(&entries) {
            return Ok(Vec::new());
        }

        if let Some(filter) = attr_name {
            return Ok(entries
                .into_iter()
                .filter(|e| e.attr_name == filter)
                .collect());
        }

        Ok(entries)
    }

    /// Escanea el índice `AEVT` de `.mbf` y precalienta en `EAV_CACHE` las entidades de un tenant en paralelo.
    /// Evita consultas individuales repetitivas durante cargas analíticas o masivas.
    pub async fn warm_cache_for_tenant(
        &self,
        tenant_id: &str,
        entity_type: Option<&str>,
        specific_attrs: Option<&[String]>,
    ) -> Result<(), DomainError> {
        tracing::info!(
            "[EAV Cache Warmup] Iniciando query paralelo en GSI-AEVT para tenant: {}, entity_type: {:?}, specific_attrs: {:?}",
            tenant_id,
            entity_type,
            specific_attrs
        );

        let registry = crate::codice::global();
        let mut target_attrs = std::collections::HashSet::new();

        // Atributos de sistema reservados
        target_attrs.insert("entity/ulid".to_string());
        target_attrs.insert("entity/type".to_string());
        target_attrs.insert("tenant/id".to_string());
        target_attrs.insert("meta/created_at".to_string());
        target_attrs.insert("meta/updated_at".to_string());
        target_attrs.insert("meta/created_by".to_string());
        target_attrs.insert("meta/updated_by".to_string());

        if let Some(attrs_list) = specific_attrs {
            // Calentar únicamente los atributos específicos de negocio requeridos
            for attr in attrs_list {
                target_attrs.insert(attr.clone());
            }
        } else if let Some(entity_name) = entity_type {
            // Calentar únicamente la entidad solicitada para ahorrar memoria drásticamente
            if let Some(attrs) = registry.get_attributes(entity_name) {
                for attr in attrs {
                    target_attrs.insert(attr.name.clone());
                }
            }
        } else {
            // Atributos definidos en todos los modelos del registry
            for entity_name in registry.entity_names() {
                if let Some(attrs) = registry.get_attributes(entity_name) {
                    for attr in attrs {
                        target_attrs.insert(attr.name.clone());
                    }
                }
            }
        }

        let mut join_set: tokio::task::JoinSet<Result<(String, Vec<WarmupItem>), DomainError>> =
            tokio::task::JoinSet::new();
        // Usar semáforo para limitar la concurrencia a máximo 4 queries simultáneos,
        // controlando picos de asignación de red y deserialización del SDK.
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(4));

        for attr_name in target_attrs {
            let client = self.ddb.clone();
            let table = self.table.clone();
            let tenant_id = tenant_id.to_string();
            let sem_permit = sem.clone();

            join_set.spawn(async move {
                let _permit = sem_permit.acquire().await.map_err(|e| {
                    DomainError::infra(
                        ErrorCode::Infra001,
                        format!("Fallo al adquirir permiso del semáforo para {attr_name}: {e:?}"),
                    )
                })?;

                let pk_ap = format!("T#{}#A#{}", tenant_id, attr_name);

                let key_condition = "#ap = :ap".to_string();
                let mut expr_attr_names = HashMap::new();
                let mut expr_attr_values = HashMap::new();
                expr_attr_names.insert("#ap".to_string(), "ap".to_string());
                expr_attr_values.insert(":ap".to_string(), AttributeValue::S(pk_ap));

                let raw_items = client
                    .query(
                        &table,
                        Some("GSI-AEVT"),
                        &key_condition,
                        expr_attr_names,
                        expr_attr_values,
                        true,
                        None,
                    )
                    .await
                    .map_err(|e| {
                        DomainError::infra(
                            ErrorCode::Infra001,
                            format!("GSI-AEVT query falló para {attr_name}: {e:?}"),
                        )
                    })?;

                // Transformar inmediatamente a estructuras ultra livianas para liberar los HashMaps de DynamoDB del heap
                let mut warmup_items = Vec::with_capacity(raw_items.len());
                for item in raw_items {
                    let pk = match item.get("PK") {
                        Some(AttributeValue::S(s)) => s,
                        _ => continue,
                    };

                    let entity_id = match pk.split('#').next_back() {
                        Some(id) => id.to_string(),
                        None => continue,
                    };

                    let sk = match item.get("SK") {
                        Some(AttributeValue::B(blob)) => blob.as_ref(),
                        _ => continue,
                    };
                    let Some((_attr_id, tx_id, op)) = decode_eavt_sk(sk) else {
                        continue;
                    };

                    let value = extract_datom_value(&item).unwrap_or(DatomValue::Null);

                    warmup_items.push(WarmupItem {
                        entity_id,
                        tx_id,
                        op,
                        value,
                    });
                }

                Ok::<(String, Vec<WarmupItem>), DomainError>((attr_name, warmup_items))
            });
        }

        // Mapa incremental: entity_pk -> (attr_name -> (tx_id, value))
        let mut entity_states: HashMap<String, HashMap<String, (u64, DatomValue)>> = HashMap::new();
        let mut total_datoms_count = 0;

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok((attr_name, items))) => {
                    total_datoms_count += items.len();
                    for item in items {
                        let entity_pk = format!("T#{}#E#{}", tenant_id, item.entity_id);
                        let attrs_map = entity_states.entry(entity_pk).or_default();

                        let should_update = match attrs_map.get(&attr_name) {
                            None => true,
                            Some((prev_tx, _)) => item.tx_id > *prev_tx,
                        };

                        if should_update {
                            if item.op {
                                attrs_map.insert(attr_name.clone(), (item.tx_id, item.value));
                            } else {
                                attrs_map.remove(&attr_name);
                            }
                        }
                    }
                }
                Ok(Err(err)) => return Err(err),
                Err(join_err) => {
                    return Err(DomainError::infra(
                        ErrorCode::Infra001,
                        format!(
                            "Error al unir hilo de query paralelo durante warmup: {join_err:?}"
                        ),
                    ));
                }
            }
        }

        tracing::info!(
            "[EAV Cache Warmup] Queries paralelos en GSI-AEVT completados. {} datoms livianos procesados para tenant {}",
            total_datoms_count,
            tenant_id
        );

        if entity_states.is_empty() {
            return Ok(());
        }

        // Ensamblar cada entidad e insertarla en la caché local EAV_CACHE de un solo golpe
        let mut cache_write_count = 0;
        if let Ok(mut cache) = EAV_CACHE.write() {
            for (pk, attrs_map) in entity_states {
                let final_map: EntityMap =
                    attrs_map.into_iter().map(|(k, (_, v))| (k, v)).collect();

                if cache.len() >= MAX_EAV_CACHE_SIZE && !cache.contains_key(&pk) {
                    let to_remove: Vec<String> =
                        cache.keys().take(MAX_EAV_CACHE_SIZE / 5).cloned().collect();
                    for k in to_remove {
                        cache.remove(&k);
                    }
                }

                let entry = cache.entry(pk).or_insert_with(|| CacheEntry {
                    is_complete: false,
                    map: HashMap::new(),
                    cached_at: std::time::Instant::now(),
                });
                entry.map.extend(final_map);
                cache_write_count += 1;
            }
        }

        tracing::info!(
            "[EAV Cache Warmup] Caché local poblada con {} entidades del tenant {}",
            cache_write_count,
            tenant_id
        );

        Ok(())
    }
}

/// Entrada individual del historial cronológico de mutaciones de una entidad.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// Nombre canónico del atributo afectado.
    pub attr_name: String,
    /// Valor tipado del datom (si aplica).
    pub value: Option<DatomValue>,
    /// Identificador monotónico de la transacción (`tx_id`).
    pub tx_id: u64,
    /// Indicador de operación (`true` = assert, `false` = retract).
    pub op: bool,
}

/// Post-filtro defensivo de partición exacta: conserva sólo los items cuya
/// `PK` es exactamente la de la entidad pedida.
///
/// El query `EAVT` del storage `.mbf` ya casa la `PK` por igualdad estricta;
/// este filtro es la garantía de la invariante en la capa de lectura: un
/// entity_id que es prefijo de otro (`<fileId>` frente a `<fileId>_chunk_N`)
/// nunca debe sangrar datoms ajenos aunque el mecanismo de acceso cambie.
fn filter_exact_entity_partition(
    items: Vec<HashMap<String, AttributeValue>>,
    expected_pk: &str,
) -> Vec<HashMap<String, AttributeValue>> {
    items
        .into_iter()
        .filter(|item| matches!(item.get("PK"), Some(AttributeValue::S(pk)) if pk == expected_pk))
        .collect()
}

/// Evalúa el veto `track_history: false` del Códice sobre el historial ya decodificado.
///
/// El tipo real de la entidad se resuelve del datom `entity/type` de mayor
/// `tx_id` de su partición — assert o retract (una entidad borrada conserva su
/// tipo)— y se consulta `CodeRegistry::tracks_history`. Sin datom de tipo
/// (entidad sin historial visible o de otra época) se conserva el
/// comportamiento previo: fail-open.
fn history_tracking_enabled(entries: &[HistoryEntry]) -> bool {
    let entity_type = entries
        .iter()
        .filter(|e| e.attr_name == crate::eav::writer::system_attrs::TYPE_NAME)
        .max_by_key(|e| e.tx_id)
        .and_then(|e| match &e.value {
            Some(DatomValue::Str(t)) if !t.is_empty() => Some(t.as_str()),
            _ => None,
        });
    match entity_type {
        Some(t) => crate::codice::global().tracks_history(t),
        None => true,
    }
}

/// Ensambla el estado actual de una entidad desde los datoms raw de la partición `.mbf`.
/// Toma el último datom con op=true por atributo.
fn assemble_current_state(
    items: Vec<HashMap<String, AttributeValue>>,
    attr_filter: Option<&[&str]>,
) -> EntityMap {
    // Mapa: attr_name → (tx_id, value) — mantenemos el más reciente con op=true
    let mut latest: HashMap<String, (u64, DatomValue)> = HashMap::new();

    for item in items {
        let sk = match item.get("SK") {
            Some(AttributeValue::B(blob)) => blob.as_ref(),
            _ => continue,
        };
        let Some((attr_id, tx_id, op)) = decode_eavt_sk(sk) else {
            continue;
        };

        let attr_name = match crate::codice::global().get_attr_name(attr_id) {
            Some(n) => n.to_string(),
            None => format!("attr_{}", attr_id),
        };
        tracing::debug!(
            "DEBUG PULL ITEM: attr_id={}, attr_name={}, op={}, val={:?}",
            attr_id,
            attr_name,
            op,
            extract_datom_value(&item)
        );

        // Aplicar filtro de atributos si se especificó
        if let Some(filter) = attr_filter {
            let bare_name = attr_name.split('/').next_back().unwrap_or(&attr_name);
            if !filter.contains(&attr_name.as_str()) && !filter.contains(&bare_name) {
                continue;
            }
        }

        // Solo actualizar si este datom es más reciente que el que tenemos
        let should_update = match latest.get(&attr_name) {
            None => true,
            Some((prev_tx, _)) => tx_id > *prev_tx,
        };

        if should_update {
            if op {
                // Assert — registrar el valor
                if let Some(val) = extract_datom_value(&item) {
                    latest.insert(attr_name, (tx_id, val));
                } else {
                    tracing::warn!("[EAV] Invalid datom value for attr {}", attr_name);
                }
            } else {
                // Retract — eliminar del mapa si el retract es más reciente
                latest.remove(&attr_name);
            }
        }
    }

    // Convertir a EntityMap simple
    latest.into_iter().map(|(k, (_, v))| (k, v)).collect()
}

/// Extrae el `DatomValue` del campo `v` de un registro `.mbf` (compartido por reader y writer).
pub(crate) fn extract_datom_value(item: &HashMap<String, AttributeValue>) -> Option<DatomValue> {
    match item.get("v")? {
        AttributeValue::S(s) => Some(DatomValue::Str(s.clone())),
        AttributeValue::N(n) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(DatomValue::Long(i))
            } else if !n.contains('.') && !n.contains('e') && !n.contains('E') {
                if let Ok(big) = n.parse::<i128>() {
                    Some(DatomValue::BigInt(big))
                } else {
                    n.parse::<f64>().ok().map(DatomValue::Double)
                }
            } else {
                n.parse::<f64>().ok().map(DatomValue::Double)
            }
        }
        AttributeValue::Bool(b) => Some(DatomValue::Bool(*b)),
        AttributeValue::B(blob) => Some(DatomValue::Bytes(blob.as_ref().to_vec())),
        AttributeValue::Null(_) => Some(DatomValue::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eav::types::datom::Datom;
    use crate::eav::types::encoding::build_eavt_sk;
    use crate::eav::writer::system_attrs as sys;
    use crate::mbf::storage::partition_for;

    fn init() {
        crate::eav::writer::test_support::init_codice();
    }

    /// Item `EAVT` canónico con la forma que escribe `transact.rs` (PK/SK + `v`).
    fn datom_item(
        pk: &str,
        attr_id: u16,
        tx: u64,
        op: bool,
        value: &str,
    ) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("PK".to_string(), AttributeValue::S(pk.to_string()));
        m.insert(
            "SK".to_string(),
            AttributeValue::B(Blob::new(build_eavt_sk(attr_id, tx, op))),
        );
        m.insert("v".to_string(), AttributeValue::S(value.to_string()));
        m
    }

    fn insert_items(table: &str, pk: &str, items: Vec<HashMap<String, AttributeValue>>) {
        let part = partition_for(table);
        let mut guard = part.write().expect("partition lock");
        for item in items {
            let sk = match item.get("SK") {
                Some(AttributeValue::B(b)) => b.as_ref().to_vec(),
                _ => Vec::new(),
            };
            guard.insert_indexed((pk.to_string(), sk), item);
        }
    }

    fn reader(table: &str) -> EavReader {
        EavReader::new(
            Arc::new(crate::mbf::storage::MbfStorageClient::new_stub()),
            table,
        )
    }

    fn history_entry(attr: &str, value: Option<&str>, tx: u64, op: bool) -> HistoryEntry {
        HistoryEntry {
            attr_name: attr.to_string(),
            value: value.map(|v| DatomValue::Str(v.to_string())),
            tx_id: tx,
            op,
        }
    }

    // ── (a) Partición exacta ────────────────────────────────────────────────

    #[test]
    fn filtro_de_particion_exacta_descarta_ids_con_prefijo_compartido() {
        let file_pk = "T#t1#E#file_1";
        let chunk_pk = "T#t1#E#file_1_chunk_0";
        let items = vec![
            datom_item(chunk_pk, sys::TYPE_ID, 100, true, "document_chunk"),
            datom_item(file_pk, sys::TYPE_ID, 100, true, "file"),
        ];
        let filtered = filter_exact_entity_partition(items, file_pk);
        assert_eq!(
            filtered.len(),
            1,
            "el datom del chunk NO es de la partición del file"
        );
        assert_eq!(
            filtered[0]
                .get("PK")
                .and_then(crate::mbf::storage::av_string),
            Some(file_pk)
        );
    }

    #[tokio::test]
    async fn history_devuelve_solo_los_datoms_de_la_entidad_exacta() {
        init();
        let table = "test-history-exact-partition";
        let file_pk = "T#t1#E#file_1";
        let chunk_pk = "T#t1#E#file_1_chunk_0";
        let title_id = Datom::hash_attr_name("title");

        insert_items(
            table,
            file_pk,
            vec![
                datom_item(file_pk, sys::TYPE_ID, 100, true, "reminder"),
                datom_item(file_pk, title_id, 100, true, "título del padre"),
            ],
        );
        // El id del chunk comparte prefijo exacto con el del padre: sus datoms
        // viven en OTRA partición y no deben aparecer en el historial del padre.
        insert_items(
            table,
            chunk_pk,
            vec![
                datom_item(chunk_pk, sys::TYPE_ID, 100, true, "reminder"),
                datom_item(chunk_pk, title_id, 100, true, "datom ajeno del chunk"),
            ],
        );

        let entries = reader(table)
            .history("t1", "file_1", None)
            .await
            .expect("history ok");
        assert_eq!(entries.len(), 2, "sólo los datoms del padre");
        assert!(entries.iter().all(|e| !matches!(
            &e.value,
            Some(DatomValue::Str(s)) if s == "datom ajeno del chunk"
        )));
    }

    // ── (c) Veto track_history: false ───────────────────────────────────────

    #[test]
    fn veto_track_history_se_resuelve_del_datom_entity_type_mas_reciente() {
        init();
        // document_chunk declara track_history: false en el Códice real.
        let chunk_entries = vec![
            history_entry(sys::TYPE_NAME, Some("document_chunk"), 100, true),
            history_entry("chunk_content", Some("texto"), 100, true),
        ];
        assert!(!history_tracking_enabled(&chunk_entries));

        // Una entidad borrada conserva su tipo (el retract más reciente manda).
        let deleted_chunk = vec![
            history_entry(sys::TYPE_NAME, Some("document_chunk"), 100, true),
            history_entry(sys::TYPE_NAME, Some("document_chunk"), 200, false),
        ];
        assert!(!history_tracking_enabled(&deleted_chunk));

        // reminder rastrea historial.
        let reminder_entries = vec![history_entry(sys::TYPE_NAME, Some("reminder"), 100, true)];
        assert!(history_tracking_enabled(&reminder_entries));

        // Sin datom de tipo: fail-open (comportamiento previo a la flag).
        let sin_tipo = vec![history_entry("title", Some("x"), 100, true)];
        assert!(history_tracking_enabled(&sin_tipo));
        assert!(history_tracking_enabled(&[]));
    }

    #[tokio::test]
    async fn history_devuelve_vacio_para_entidades_con_track_history_false() {
        init();
        let table = "test-history-track-veto";
        let chunk_pk = "T#t1#E#file_9_chunk_0";
        let content_id = Datom::hash_attr_name("chunk_content");

        insert_items(
            table,
            chunk_pk,
            vec![
                datom_item(chunk_pk, sys::TYPE_ID, 100, true, "document_chunk"),
                datom_item(chunk_pk, content_id, 100, true, "contenido del chunk"),
            ],
        );

        let entries = reader(table)
            .history("t1", "file_9_chunk_0", None)
            .await
            .expect("history ok");
        assert!(
            entries.is_empty(),
            "document_chunk es infraestructura: estado actual sí, historial no"
        );

        // El veto se evalúa también con filtro de atributo (no lo esquiva).
        let entries = reader(table)
            .history("t1", "file_9_chunk_0", Some("chunk_content"))
            .await
            .expect("history ok");
        assert!(entries.is_empty());

        // Una entidad de negocio en la misma tabla sí expone su historial.
        let note_pk = "T#t1#E#note_1";
        let title_id = Datom::hash_attr_name("title");
        insert_items(
            table,
            note_pk,
            vec![
                datom_item(note_pk, sys::TYPE_ID, 100, true, "reminder"),
                datom_item(note_pk, title_id, 100, true, "nota viva"),
            ],
        );
        let entries = reader(table)
            .history("t1", "note_1", None)
            .await
            .expect("history ok");
        assert_eq!(entries.len(), 2, "reminder expone su historial completo");
    }
}
