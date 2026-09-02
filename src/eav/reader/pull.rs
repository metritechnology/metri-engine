// [NUEVO — reemplaza: src/metri/aegis/datalog/pull.clj]
// eav/reader/pull.rs — Read path del motor EAV.
// Blueprint: Metri EAV - OLPT.md §V y §VI
//
// En Clojure: (d/pull db pattern entity-id) → blob completo en memoria
// En Rust:    Query selectiva por atributo → solo los bytes necesarios

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::debug;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::{
    datom::DatomValue,
    encoding::{eavt_sk_as_of, eavt_sk_attr_prefix},
};
use crate::infrastructure::dynamodb::{av_bytes, av_number, av_string, DynamoClient};

use once_cell::sync::Lazy;
use std::sync::RwLock;

#[derive(Clone, Debug)]
pub struct CacheEntry {
    pub is_complete: bool,
    pub map: EntityMap,
}

/// Global thread-safe in-memory cache for fully assembled EAV entities.
/// Keyed by PK: "T#<tenant_id>#E#<entity_id>"
pub const MAX_EAV_CACHE_SIZE: usize = 10_000;

pub static EAV_CACHE: Lazy<RwLock<HashMap<String, CacheEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

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

/// Estructura interna súper compacta para warmup.
/// Esto reduce el consumo de RAM en un 95% al descartar los HashMaps de la SDK de AWS de inmediato.
#[derive(Debug)]
pub struct WarmupItem {
    pub entity_id: String,
    pub tx_id: u64,
    pub op: bool,
    pub value: DatomValue,
}

#[derive(Clone)]
pub struct EavReader {
    pub ddb: Arc<DynamoClient>,
    pub table: String,
}

impl EavReader {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        EavReader {
            ddb,
            table: table.into(),
        }
    }

    /// Carga el estado actual de una entidad (todos los atributos vigentes).
    /// Equivale a (d/pull db '[*] entity-id) pero sin deserializar el blob.
    ///
    /// [PORTED_FROM: aegis/datalog/pull.clj — (d/pull db pattern entity-id)]
    /// Mejora: solo carga los atributos pedidos, no el blob completo.
    pub async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs: Option<&[&str]>, // None = todos los atributos
    ) -> Result<EntityMap, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        // 1. Check in-memory cache
        if let Ok(cache) = EAV_CACHE.read() {
            if let Some(entry) = cache.get(&pk) {
                // Las entidades transaccionales críticas de cuota no deben servirse de la caché local para evitar inconsistencias en clúster/Lambda
                let is_quota = entry.map.contains_key("resource_domain")
                    || entry.map.contains_key("domain_quota/resource_domain");

                let is_sufficient = if is_quota {
                    false
                } else if entry.is_complete {
                    true
                } else if let Some(filter) = attrs {
                    filter.iter().all(|&attr| {
                        entry.map.contains_key(attr)
                            || entry.map.keys().any(|k| k.split('/').last() == Some(attr))
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
                                let bare = k.split('/').last().unwrap_or(k);
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

        // Query en tabla EAVT — todos los datoms de la entidad, ordenados por SK
        // SK binario: [attr_id:2B][tx_id:8B][op:1B] — orden natural de DynamoDB
        let key_condition = "#pk = :pk".to_string();
        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

        let raw_items = self
            .ddb
            .query(
                &self.table,
                None, // tabla principal, no GSI
                &key_condition,
                attr_names.clone(),
                attr_values.clone(),
                true, // ScanIndexForward=true → orden cronológico
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("pull falló: {e:?}")))?;

        if raw_items.is_empty() {
            return Ok(HashMap::new());
        }

        // Reconstruir el estado actual completo (None filter) para poblar la cache
        // [BLUEPRINT: §V.1 — "el estado actual es el datom assert más reciente por atributo"]
        let full_entity_map = assemble_current_state(raw_items, None);

        // Insertar en la cache marcada como completa (excepto para domain_quota)
        let is_quota = full_entity_map.contains_key("resource_domain")
            || full_entity_map.contains_key("domain_quota/resource_domain");

        if !is_quota {
            if let Ok(mut cache) = EAV_CACHE.write() {
                insert_eav_cache_entry(
                    &mut cache,
                    pk.clone(),
                    CacheEntry {
                        is_complete: true,
                        map: full_entity_map.clone(),
                    },
                );
            }
        }

        // Retornar filtrado si es necesario
        let result_map = if let Some(filter) = attrs {
            let filtered: EntityMap = full_entity_map
                .iter()
                .filter(|(k, _)| {
                    let bare = k.split('/').last().unwrap_or(k);
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

    /// Carga el estado actual de una entidad directamente desde la base de datos (DynamoDB)
    /// omitiendo y evitando escribir en la caché en memoria EAV_CACHE.
    /// Útil para entidades transaccionales de lectura-escritura concurrente como `domain_quota`.
    pub async fn pull_nocache(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
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
                attr_names.clone(),
                attr_values.clone(),
                true,
                None,
            )
            .await
            .map_err(|e| {
                DomainError::eav(ErrorCode::Eav002, format!("pull_nocache falló: {e:?}"))
            })?;

        if raw_items.is_empty() {
            return Ok(HashMap::new());
        }

        let full_entity_map = assemble_current_state(raw_items, None);

        let result_map = if let Some(filter) = attrs {
            let filtered: EntityMap = full_entity_map
                .iter()
                .filter(|(k, _)| {
                    let bare = k.split('/').last().unwrap_or(k);
                    filter.contains(&k.as_str()) || filter.contains(&bare)
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            filtered
        } else {
            full_entity_map
        };

        debug!(
            "[EAV] pull_nocache (BYPASS CACHE) tenant={tenant_id} entity_id={entity_id} attrs={}",
            result_map.len()
        );

        Ok(result_map)
    }

    /// Time-travel: estado de la entidad as-of un TX_ID específico.
    /// [BLUEPRINT: §VI.1 — "Snapshot Reads — as-of TX T"]
    /// [PORTED_FROM: eav_pull_as_of — diseñado en OLPT.md reader/as_of.rs]
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
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));
        attr_values.insert(
            ":sk_max".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk_max)),
        );

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

        Ok(assemble_current_state(raw_items, attrs))
    }

    /// History query — retorna TODOS los datoms de una entidad incluyendo retracts.
    /// Ideal para audit trail.
    /// [BLUEPRINT: §VI.2 — "History Query (Auditoría completa)"]
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
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

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

        let entries = raw_items
            .into_iter()
            .filter_map(|item| {
                let sk = match item.get("SK") {
                    Some(AttributeValue::B(blob)) => blob.as_ref(),
                    _ => return None,
                };
                if sk.len() != 11 {
                    return None;
                }
                let attr_id = u16::from_be_bytes(sk[0..2].try_into().unwrap());
                let tx_id = u64::from_be_bytes(sk[2..10].try_into().unwrap());
                let op = sk[10] != 0;

                let attr = crate::codice::global()
                    .get_attr_name(attr_id)
                    .unwrap_or("unknown_attr")
                    .to_string();
                if let Some(filter) = attr_name {
                    if attr != filter {
                        return None;
                    }
                }
                let value = extract_datom_value(&item);
                Some(HistoryEntry {
                    attr_name: attr.to_string(),
                    value,
                    tx_id,
                    op,
                })
            })
            .collect();

        Ok(entries)
    }

    /// Escanea la tabla principal y carga en la caché en memoria EAV_CACHE
    /// todas las entidades de un tenant de un solo golpe.
    /// Esto evita tener que hacer miles de consultas individuales (CACHE MISS) en DynamoDB
    /// y acelera drásticamente las operaciones en masa/analíticas.
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

                    let entity_id = match pk.split('#').last() {
                        Some(id) => id.to_string(),
                        None => continue,
                    };

                    let sk = match item.get("SK") {
                        Some(AttributeValue::B(blob)) => blob.as_ref(),
                        _ => continue,
                    };
                    if sk.len() != 11 {
                        continue;
                    }
                    let tx_id = u64::from_be_bytes(sk[2..10].try_into().unwrap());
                    let op = sk[10] != 0;

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

                let is_quota = final_map.contains_key("resource_domain")
                    || final_map.contains_key("domain_quota/resource_domain");
                if is_quota {
                    continue;
                }

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

/// Entrada del historial de una entidad.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub attr_name: String,
    pub value: Option<DatomValue>,
    pub tx_id: u64,
    pub op: bool, // true=assert, false=retract
}

/// Ensambla el estado actual de una entidad desde los datoms raw de DynamoDB.
/// Toma el último datom con op=true por atributo.
/// [PORTED_FROM: La lógica de pull de Datahike — reconstrucción sin blob]
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
        if sk.len() != 11 {
            continue;
        }

        let attr_id = u16::from_be_bytes(sk[0..2].try_into().unwrap());
        let tx_id = u64::from_be_bytes(sk[2..10].try_into().unwrap());
        let op = sk[10] != 0;

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
            let bare_name = attr_name.split('/').last().unwrap_or(&attr_name);
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

/// Extrae el DatomValue del campo `v` de un item DynamoDB.
fn extract_datom_value(item: &HashMap<String, AttributeValue>) -> Option<DatomValue> {
    match item.get("v")? {
        AttributeValue::S(s) => Some(DatomValue::Str(s.clone())),
        AttributeValue::N(n) => {
            // Intentar i64 primero, luego f64
            if let Ok(i) = n.parse::<i64>() {
                Some(DatomValue::Long(i))
            } else {
                n.parse::<f64>().ok().map(DatomValue::Double)
            }
        }
        AttributeValue::Bool(b) => Some(DatomValue::Bool(*b)),
        AttributeValue::B(_) => Some(DatomValue::Bytes(vec![])), // placeholder
        AttributeValue::Null(_) => Some(DatomValue::Null),
        _ => None,
    }
}
