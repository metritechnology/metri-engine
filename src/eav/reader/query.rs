//! Ejecutor físico de planes de consulta sobre los índices EAV del motor `.mbf`.
//!
//! Implementa los 5 planes físicos seleccionados por el `PlanSelector`:
//! - `PointLookup`: acceso directo O(1) sobre el índice primario `EAVT`.
//! - `AvetSingle`: consulta por valor exacto o prefijo sobre `GSI-AVET` con `SK` binario.
//! - `AvetIntersect`: intersección en memoria de múltiples consultas selectivas sobre `GSI-AVET`.
//! - `FtsSearch`: búsqueda híbrida por trigramas, fonética romance y Zona 5B (`MbfHybridSearchIndex`).
//! - `AevtScan`: escaneo segmentado por tipo de entidad sobre `GSI-AEVT`.

use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use tracing::debug;

/// Capacidad máxima de entradas `(tenant_id, entity_type)` almacenadas en `AEVT_SCAN_CACHE`.
pub const MAX_AEVT_SCAN_CACHE_SIZE: usize = 2_000;

/// Vida máxima de una entrada del caché de escaneo por tipo (`AEVT_SCAN_CACHE`).
/// Acota la obsolescencia entre instancias cuando ocurren escrituras concurrentes.
pub const AEVT_SCAN_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(15);

/// Entrada del caché de escaneo `AEVT`: lista de `entity_id` activos del par `(tenant, entity_type)` e instante de lectura.
pub struct CachedAevtScan {
    /// Identificadores de las entidades activas pertenecientes al tipo consultado.
    pub ids: Vec<String>,
    /// Instante en que se materializó la entrada en caché.
    pub cached_at: std::time::Instant,
}

/// Caché global concurrente de escaneos `AEVT` indexada por `(tenant_id, entity_type)`.
pub static AEVT_SCAN_CACHE: Lazy<RwLock<HashMap<(String, String), CachedAevtScan>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Inserta una entrada en AEVT_SCAN_CACHE asegurando que no exceda MAX_AEVT_SCAN_CACHE_SIZE.
pub fn insert_aevt_scan_cache_entry(
    cache: &mut HashMap<(String, String), CachedAevtScan>,
    key: (String, String),
    ids: Vec<String>,
) {
    if cache.len() >= MAX_AEVT_SCAN_CACHE_SIZE && !cache.contains_key(&key) {
        let to_remove: Vec<(String, String)> = cache
            .keys()
            .take(MAX_AEVT_SCAN_CACHE_SIZE / 5)
            .cloned()
            .collect();
        for k in to_remove {
            cache.remove(&k);
        }
    }
    cache.insert(
        key,
        CachedAevtScan {
            ids,
            cached_at: std::time::Instant::now(),
        },
    );
}

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::fts::trigram::generate_trigrams;
use crate::eav::types::datom::DatomValue;
use crate::eav::types::encoding::avet_sk_prefix;
use crate::mbf::storage::{av_string, AttributeValue, Blob, MbfStorageClient};

/// Plan físico nativo de consulta EAV (expresado con nombres de atributo `attr_name`).
/// Se construye a partir de `janus::plan_selector::EavQueryPlan` y se ejecuta contra `MbfStorageClient`.
#[derive(Debug, Clone)]
pub enum NativeQueryPlan {
    /// Búsqueda puntual directa O(1) por identificador de entidad.
    PointLookup {
        /// Identificador único de la entidad buscada.
        entity_id: String,
    },
    /// Consulta en el índice `GSI-AVET` por igualdad exacta de un atributo y valor.
    AvetSingle {
        /// Identificador del tenant propietario.
        tenant_id: String,
        /// Nombre canónico del atributo filtrado.
        attr_name: String,
        /// Valor tipado buscado en el índice `AVET`.
        value: DatomValue,
    },
    /// Intersección de múltiples filtros de igualdad sobre el índice `GSI-AVET`.
    AvetIntersect {
        /// Identificador del tenant propietario.
        tenant_id: String,
        /// Lista de pares `(attr_name, valor)` que deben cumplirse simultáneamente.
        filters: Vec<(String, DatomValue)>,
    },
    /// Búsqueda de texto completo e híbrida (trigramas, fonética y Zona 5B).
    FtsSearch {
        /// Identificador del tenant propietario.
        tenant_id: String,
        /// Término o frase de búsqueda textual.
        term: String,
    },
    /// Escaneo completo de las entidades activas de un tipo en el índice `GSI-AEVT`.
    AevtScan {
        /// Identificador del tenant propietario.
        tenant_id: String,
        /// Nombre del modelo o tipo de entidad a escanear.
        entity_type: String,
    },
}

/// Ejecutor físico de planes `NativeQueryPlan` sobre el motor de almacenamiento `.mbf`.
#[derive(Clone)]
pub struct EavQueryExecutor {
    ddb: Arc<MbfStorageClient>,
    table: String,
}

impl EavQueryExecutor {
    /// Crea un nuevo `EavQueryExecutor` conectado al cliente `.mbf` y tabla especificados.
    pub fn new(ddb: Arc<MbfStorageClient>, table: impl Into<String>) -> Self {
        EavQueryExecutor {
            ddb,
            table: table.into(),
        }
    }

    // ── API nativa (janus/router.rs a través de execute_native_plan) ─────────

    /// Ejecuta un NativeQueryPlan y devuelve entity_ids.
    pub async fn execute_native_plan(
        &self,
        plan: &NativeQueryPlan,
    ) -> Result<Vec<String>, DomainError> {
        tracing::debug!("execute_native_plan called with plan: {:?}", plan);
        match plan {
            NativeQueryPlan::PointLookup { entity_id } => Ok(vec![entity_id.clone()]),

            NativeQueryPlan::AvetSingle {
                tenant_id,
                attr_name,
                value,
            } => self.execute_avet_single(tenant_id, attr_name, value).await,

            NativeQueryPlan::AvetIntersect { tenant_id, filters } => {
                self.execute_avet_intersection(tenant_id, filters).await
            }

            NativeQueryPlan::FtsSearch { tenant_id, term } => {
                self.execute_fts_search(tenant_id, term).await
            }

            NativeQueryPlan::AevtScan {
                tenant_id,
                entity_type,
            } => {
                // Pasamos limit=0 para que la paginación automática obtenga TODOS los ids.
                // El limit real de paginación se aplica en el executor (offset/page_limit).
                self.execute_aevt_scan_by_type(tenant_id, entity_type, 0)
                    .await
            }
        }
    }

    // ── Implementaciones de índice ────────────────────────────────────────────

    /// GSI-AVET: PK = "T#<tenant>#AV#<attr_name>", SK begins_with <value_prefix>
    /// Retorna los entity_ids almacenados en `eid` del datom encontrado.
    async fn execute_avet_single(
        &self,
        tenant: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        let mut ids = self
            .execute_avet_single_with_type_tolerance(tenant, attr_name, value)
            .await?;

        let master_tenant = &crate::domain::config::engine_config().master_tenant_id;
        if ids.is_empty() && tenant == master_tenant {
            // Si la consulta parte del tenant maestro, buscar de forma agnóstica en los tenants registrados
            let tenants = self
                .execute_aevt_scan_by_type(master_tenant, "tenant", 0)
                .await
                .unwrap_or_default();

            for t in tenants {
                if t == *master_tenant {
                    continue;
                }
                if let Ok(tenant_ids) = self
                    .execute_avet_single_with_type_tolerance(&t, attr_name, value)
                    .await
                {
                    if !tenant_ids.is_empty() {
                        ids = tenant_ids;
                        break;
                    }
                }
            }
        }

        Ok(ids)
    }

    async fn execute_avet_single_with_type_tolerance(
        &self,
        tenant: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        let mut ids = self
            .execute_avet_single_for_tenant(tenant, attr_name, value)
            .await?;

        // Tolerancia de tipo textual: el mismo valor escrito como Str (tag
        // 0x04) o Uuid (0x05) comparte forma — el AVET prefix no. Los filtros
        // del gRPC llegan como Str aunque el attr se almacene como Uuid.
        if ids.is_empty() {
            let alt = match value {
                DatomValue::Str(s) => Some(DatomValue::Uuid(s.clone())),
                DatomValue::Uuid(s) => Some(DatomValue::Str(s.clone())),
                _ => None,
            };
            if let Some(alt) = alt {
                ids = self
                    .execute_avet_single_for_tenant(tenant, attr_name, &alt)
                    .await?;
            }
        }

        Ok(ids)
    }

    async fn execute_avet_single_for_tenant(
        &self,
        tenant: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        let pk = format!("T#{tenant}#AV#{attr_name}");
        let sk_prefix = avet_sk_prefix(value);

        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "vp".to_string());
        attr_names.insert("#sk".to_string(), "vs".to_string());

        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));
        attr_values.insert(
            ":sk_prefix".to_string(),
            AttributeValue::B(Blob::new(sk_prefix)),
        );

        let raw = self
            .ddb
            .query(
                &self.table,
                Some("GSI-AVET"),
                "#pk = :pk AND begins_with(#sk, :sk_prefix)",
                attr_names,
                attr_values,
                true,
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("AVET single err: {e:?}")))?;

        let expected_v = datom_value_to_string(value);
        Ok(dedup_latest_asserted_by_value(raw, &expected_v))
    }

    /// GSI-AVET intersección: ejecuta N queries en secuencia y hace HashSet intersection.
    /// Blueprint: §I.2 Paso 5 — AvetIntersection "paralelo + intersección HashSet"
    async fn execute_avet_intersection(
        &self,
        tenant: &str,
        filters: &[(String, DatomValue)],
    ) -> Result<Vec<String>, DomainError> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        let mut common_ids: Option<HashSet<String>> = None;

        for (attr_name, value) in filters {
            let ids = self.execute_avet_single(tenant, attr_name, value).await?;
            let id_set: HashSet<String> = ids.into_iter().collect();

            match common_ids {
                None => {
                    common_ids = Some(id_set);
                }
                Some(ref mut current) => {
                    current.retain(|id| id_set.contains(id));
                    if current.is_empty() {
                        // Short-circuit: ningún elemento puede satisfacer todos los filtros
                        return Ok(vec![]);
                    }
                }
            }
        }

        Ok(common_ids.unwrap_or_default().into_iter().collect())
    }

    /// GSI-FTS + MBF Zone 5B Hybrid Search:
    /// Consulta cada trigram del término de búsqueda, el índice fonético bilingüe
    /// Romance/Anglo y el índice híbrido Zone 5B en memoria (`fts_search_entity_ids`).
    async fn execute_fts_search(
        &self,
        tenant: &str,
        term: &str,
    ) -> Result<Vec<String>, DomainError> {
        let clean_term = term.trim();
        let trigrams = generate_trigrams(clean_term);
        if trigrams.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            "[EAV-FTS] Buscando '{}' → {} trigrams",
            term,
            trigrams.len()
        );

        // Mapa entity_id → conteo de trigrams que lo contienen
        let mut score_map: HashMap<String, usize> = HashMap::new();

        for trigram in &trigrams {
            let pk = format!("T#{tenant}#FTS#{trigram}");

            let mut attr_names = HashMap::new();
            let mut attr_values = HashMap::new();
            attr_names.insert("#pk".to_string(), "PK".to_string());
            attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

            let raw = self
                .ddb
                .query(
                    &self.table,
                    None,
                    "#pk = :pk",
                    attr_names,
                    attr_values,
                    true,
                    None,
                )
                .await;

            match raw {
                Ok(items) => {
                    tracing::debug!("[EAV-FTS] Trigram '{}' -> {} items", trigram, items.len());
                    for item in items {
                        if let Some(AttributeValue::B(blob)) = item.get("SK") {
                            let bytes = blob.as_ref();
                            if bytes.len() > 2 {
                                let eid_bytes = &bytes[2..]; // skip 2 bytes attr_id
                                if let Ok(eid) = String::from_utf8(eid_bytes.to_vec()) {
                                    *score_map.entry(eid).or_insert(0) += 1;
                                } else {
                                    tracing::warn!("[EAV-FTS] Error decodificando UTF-8 de SK");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("[EAV-FTS] Error en trigram '{}': {:?}", trigram, e);
                }
            }
        }

        // Threshold: al menos 50% de los trigrams deben coincidir (fuzzy tolerance)
        let threshold = (trigrams.len() / 2).max(1);

        // Integración MBF Zone 5B: Consulta fonética bilingüe Romance/Anglo
        for word in clean_term.split(|c: char| !c.is_alphanumeric()) {
            if word.len() >= 3 {
                let phon =
                    crate::mbf::zones::hybrid_search::MbfHybridSearchIndex::encode_romance_phonetic(
                        word,
                    );
                if !phon.is_empty() {
                    let phon_pk = format!("T#{tenant}#FTS#PHON#{phon}");
                    let mut attr_names = HashMap::new();
                    let mut attr_values = HashMap::new();
                    attr_names.insert("#pk".to_string(), "PK".to_string());
                    attr_values.insert(":pk".to_string(), AttributeValue::S(phon_pk));
                    if let Ok(items) = self
                        .ddb
                        .query(
                            &self.table,
                            None,
                            "#pk = :pk",
                            attr_names,
                            attr_values,
                            true,
                            None,
                        )
                        .await
                    {
                        for item in items {
                            if let Some(AttributeValue::B(blob)) = item.get("SK") {
                                let bytes = blob.as_ref();
                                if bytes.len() > 2 {
                                    if let Ok(eid) = String::from_utf8(bytes[2..].to_vec()) {
                                        *score_map.entry(eid).or_insert(0) += threshold;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Integración con el buscador en memoria Zone 5B (BM25 + Fuzzy + Phonetic RRF)
        let memory_hits = crate::eav::fts::searcher::fts_search_entity_ids(clean_term, tenant)
            .await
            .unwrap_or_default();
        let mem_len = memory_hits.len();
        for (rank, hit) in memory_hits.into_iter().enumerate() {
            let boost = threshold + mem_len.saturating_sub(rank);
            *score_map.entry(hit.entity_id).or_insert(0) += boost;
        }

        let mut results: Vec<(String, usize)> = score_map
            .into_iter()
            .filter(|(_, score)| *score >= threshold)
            .collect();

        // Ordenar por score descendente (mejor coincidencia primero)
        results.sort_by(|a, b| b.1.cmp(&a.1));
        Ok(results.into_iter().map(|(id, _)| id).collect())
    }

    /// GSI-AEVT: PK = "T#<tenant>#A#<attr_name>" — Scan por tipo de entidad.
    /// Retorna todos los entity_ids que tienen el atributo `entity_type = X`.
    async fn execute_aevt_scan_by_type(
        &self,
        tenant: &str,
        entity_type: &str,
        limit: u64,
    ) -> Result<Vec<String>, DomainError> {
        let _ = self.ddb.sync_if_needed(&self.table).await;
        if limit == 0 {
            if let Ok(cache) = AEVT_SCAN_CACHE.read() {
                let cache_key = (tenant.to_string(), entity_type.to_string());
                if let Some(entry) = cache.get(&cache_key) {
                    // Caducada se trata como miss: re-escanear (y repoblar).
                    // Ésta es la corrección del read-your-write multi-instancia.
                    if entry.cached_at.elapsed() < AEVT_SCAN_CACHE_TTL && !entry.ids.is_empty() {
                        tracing::debug!(
                            "AEVT_SCAN_CACHE HIT for tenant={}, type={}. Count={}",
                            tenant,
                            entity_type,
                            entry.ids.len()
                        );
                        return Ok(entry.ids.clone());
                    }
                }
            }
            tracing::debug!(
                "AEVT_SCAN_CACHE MISS for tenant={}, type={}",
                tenant,
                entity_type
            );
        }

        // En el esquema EAV el atributo entity/type almacena el tipo de entidad.
        // PK del GSI-AEVT: T#<tenant>#A#entity/type#<entity_type> (segmentado por tipo para optimizar al máximo)
        let pk = format!(
            "T#{tenant}#A#{}#{entity_type}",
            crate::eav::writer::system_attrs::TYPE_NAME
        );

        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "ap".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

        // limit=0 significa "traer todos" — se pagina automáticamente vía last_evaluated_key
        let ddb_limit = if limit == 0 { None } else { Some(limit as i32) };

        tracing::debug!(
            "execute_aevt_scan_by_type: starting ddb.query for pk={}",
            pk
        );
        let raw = self
            .ddb
            .query(
                &self.table,
                Some("GSI-AEVT"),
                "#pk = :pk",
                attr_names,
                attr_values,
                true,
                ddb_limit,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("AEVT scan err: {e:?}")))?;
        tracing::debug!("execute_aevt_scan_by_type: {} items from ddb", raw.len());

        // Filtrar y deduplicar en memoria agrupando por entity_id para verificar el último estado (op/assert)
        let mut entity_map: HashMap<String, (bool, String)> = HashMap::new();
        let mut entity_order: Vec<String> = Vec::new();

        for item in raw {
            let pk_str = match item.get("PK").and_then(|v| av_string(v)) {
                Some(pk) => pk,
                None => continue,
            };
            let entity_id = match pk_str.split('#').nth(3) {
                Some(id) => id.to_string(),
                None => continue,
            };

            let op = item
                .get("SK")
                .and_then(|v| match v {
                    AttributeValue::B(blob) => {
                        let bytes = blob.as_ref();
                        if bytes.len() == 11 {
                            Some(bytes[10] != 0)
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .unwrap_or(true);

            let val = item
                .get("v")
                .and_then(|v| av_string(v))
                .unwrap_or("")
                .to_string();

            if !entity_map.contains_key(&entity_id) {
                entity_order.push(entity_id.clone());
            }
            entity_map.insert(entity_id, (op, val));
        }

        let mut unique_active_ids = Vec::new();
        for eid in entity_order {
            if let Some((op, val)) = entity_map.get(&eid) {
                if *op && val == entity_type {
                    unique_active_ids.push(eid);
                }
            }
        }

        let ids = unique_active_ids;

        if limit == 0 && !ids.is_empty() {
            if let Ok(mut cache) = AEVT_SCAN_CACHE.write() {
                let cache_key = (tenant.to_string(), entity_type.to_string());
                insert_aevt_scan_cache_entry(&mut cache, cache_key, ids.clone());
                tracing::debug!(
                    "AEVT_SCAN_CACHE POPULATED for tenant={}, type={}. Count={}",
                    tenant,
                    entity_type,
                    ids.len()
                );
            }
        }

        Ok(ids)
    }
}

/// Convierte un DatomValue a su representación String para comparación en memoria.
fn datom_value_to_string(value: &DatomValue) -> String {
    match value {
        DatomValue::Str(s) => s.clone(),
        // Uuid/Ref/Instant viven en el índice con la misma forma que Str/Long:
        // sin estos brazos, TODO filtro eq sobre esos tipos devolvía cero filas
        // aunque el item existiera (la reproducción del loop con la query por
        // parent_entity_ref lo dejó ver: la reproyección no encontraba a sus hijos).
        DatomValue::Uuid(s) => s.clone(),
        DatomValue::Ref(r) => r.to_string(),
        DatomValue::Instant(n) => n.to_string(),
        DatomValue::Long(n) => n.to_string(),
        DatomValue::Double(f) => {
            if f.fract() == 0.0 {
                format!("{}", *f as i64)
            } else {
                f.to_string()
            }
        }
        DatomValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Post-filtro de GSI-AVET: valor exacto + deduplicación por entidad.
///
/// El índice guarda una fila por datom — asserts y retracts comparten
/// `(vp, vs)` pero viven en items distintos (PK/SK de la tabla base)—, así
/// que una entidad cuyo atributo fue re-escrito aparece una vez por datom.
/// Devuelve cada entidad UNA vez, sólo si su datom de mayor `tx` bajo ese
/// valor es un assert (`op=1`) con el valor exacto esperado: un retract
/// posterior (el atributo cambió o la fila se borró) la excluye.
///
/// Los items llegan ordenados por `vs` (valor + entity_id), NO por tx: el
/// "más reciente" se decide comparando el tx decodificado del SK base, no
/// por orden de llegada.
fn dedup_latest_asserted_by_value(
    raw: Vec<HashMap<String, AttributeValue>>,
    expected_v: &str,
) -> Vec<String> {
    use crate::eav::types::encoding::decode_eavt_sk;

    // eid → (tx, op, value_matches): la versión de mayor tx gana.
    let mut latest: HashMap<String, (u64, bool, bool)> = HashMap::new();
    let mut order: Vec<String> = Vec::new();

    for item in raw {
        let value_matches = match item.get("v") {
            Some(AttributeValue::S(s)) => s == expected_v,
            Some(AttributeValue::N(n)) => n == expected_v,
            // Los booleanos viajan como BOOL nativo (transact.rs): sin este
            // brazo, TODO filtro eq booleano devolvía cero filas aunque el
            // índice tuviera el item — el post-filter los descartaba.
            Some(AttributeValue::Bool(b)) => expected_v == if *b { "true" } else { "false" },
            _ => false,
        };
        let pk_str = match item.get("PK").and_then(av_string) {
            Some(pk) => pk,
            None => continue,
        };
        // PK is T#<tenant>#E#<eid>
        let eid = match pk_str.split('#').nth(3) {
            Some(id) => id.to_string(),
            None => continue,
        };
        // SK base = [attr][tx][op]; sin decodificación canónica no hay versión.
        let (tx, op) = match item.get("SK").and_then(|v| match v {
            AttributeValue::B(blob) => decode_eavt_sk(blob.as_ref()),
            _ => None,
        }) {
            Some(decoded) => (decoded.1, decoded.2),
            None => continue,
        };

        // Empate de tx: RET (op=0) y SET (op=1) del mismo update comparten tx;
        // en el orden canónico del SK el assert va después — a igual tx, gana
        // el op mayor.
        match latest.get(&eid) {
            Some((prev_tx, prev_op, _)) if (*prev_tx, *prev_op) >= (tx, op) => {}
            _ => {
                if !latest.contains_key(&eid) {
                    order.push(eid.clone());
                }
                latest.insert(eid, (tx, op, value_matches));
            }
        }
    }

    order
        .into_iter()
        .filter(|eid| {
            latest
                .get(eid)
                .map(|(_, op, value_matches)| *op && *value_matches)
                .unwrap_or(false)
        })
        .collect()
}

#[cfg(test)]
mod avet_dedup_tests {
    use super::*;

    fn item(eid: &str, tx: u64, op: bool, v: &str) -> HashMap<String, AttributeValue> {
        // SK binario canónico: [attr=1][tx][op]
        let mut sk = Vec::with_capacity(11);
        sk.extend_from_slice(&1u16.to_be_bytes());
        sk.extend_from_slice(&tx.to_be_bytes());
        sk.push(if op { 1 } else { 0 });
        let mut m = HashMap::new();
        m.insert("PK".to_string(), AttributeValue::S(format!("T#t#E#{eid}")));
        m.insert("SK".to_string(), AttributeValue::B(Blob::new(sk)));
        m.insert("v".to_string(), AttributeValue::S(v.to_string()));
        m
    }

    #[test]
    fn reescrituras_del_mismo_valor_devuelven_la_entidad_una_vez() {
        // El bug real: 1 create + 3 updates (RET+SET) = 7 items del índice
        // para UNA entidad. La query debe devolver 1 id.
        let raw = vec![
            item("E1", 100, true, "WO1"),
            item("E1", 200, false, "WO1"),
            item("E1", 200, true, "WO1"),
            item("E1", 300, false, "WO1"),
            item("E1", 300, true, "WO1"),
            item("E1", 400, false, "WO1"),
            item("E1", 400, true, "WO1"),
        ];
        let ids = dedup_latest_asserted_by_value(raw, "WO1");
        assert_eq!(ids, vec!["E1".to_string()]);
    }

    #[test]
    fn datoms_desordenados_por_tx_se_resuelven_por_version() {
        // GSI-AVET ordena por vs (valor+entity), no por tx: el más reciente
        // puede llegar primero.
        let raw = vec![
            item("E1", 400, true, "WO1"),
            item("E1", 100, true, "WO1"),
            item("E1", 300, false, "WO1"),
        ];
        let ids = dedup_latest_asserted_by_value(raw, "WO1");
        assert_eq!(ids, vec!["E1".to_string()]);
    }

    #[test]
    fn retract_final_excluye_la_entidad() {
        // El atributo cambió de valor o la fila se borró: el último datom
        // bajo este valor es un retract — la entidad ya no matchea.
        let raw = vec![
            item("E1", 100, true, "WO1"),
            item("E1", 200, false, "WO1"),
            item("E2", 150, true, "WO1"),
            item("E2", 150, true, "WO1"), // duplicado exacto por colisión de prefix
        ];
        let ids = dedup_latest_asserted_by_value(raw, "WO1");
        assert_eq!(ids, vec!["E2".to_string()]);
    }

    #[test]
    fn booleanos_como_bool_nativo_matchean_el_valor_exacto() {
        // transact.rs escribe booleanos como AttributeValue::Bool: sin el
        // brazo Bool del post-filtro, eq(is_primary, true) devolvía cero.
        let mut a = item("E1", 100, true, "placeholder");
        a.insert("v".to_string(), AttributeValue::Bool(true));
        let mut b = item("E2", 100, true, "placeholder");
        b.insert("v".to_string(), AttributeValue::Bool(false));
        let ids = dedup_latest_asserted_by_value(vec![a, b], "true");
        assert_eq!(ids, vec!["E1".to_string()]);
    }

    #[test]
    fn valor_truncado_que_no_coincide_queda_fuera_en_la_ultima_version() {
        // begins_with truncado a 31 bytes: colisión de prefijo resuelta por
        // el post-filtro de valor exacto SOBRE la versión ganadora.
        let raw = vec![
            item("E1", 100, true, "WO1"),
            item("E1", 200, true, "WO1-cola-que-colisiona"),
        ];
        let ids = dedup_latest_asserted_by_value(raw, "WO1");
        assert!(ids.is_empty());
    }

    #[test]
    fn uuid_y_referencia_matchean_el_valor_exacto() {
        // parent_entity_ref es uuid: sin su brazo en datom_value_to_string,
        // la query AVET por la madre devolvía cero hijos (bug de la reproyección).
        let raw = vec![
            item("E1", 100, true, "01M365N0KHB9A4SK0ZRKEP6WAT"),
            item("E2", 100, true, "01M365N0KHB9A4SK0ZRKEP6WA9"),
        ];
        let ids = dedup_latest_asserted_by_value(
            raw,
            &datom_value_to_string(&DatomValue::Uuid("01M365N0KHB9A4SK0ZRKEP6WAT".to_string())),
        );
        assert_eq!(ids, vec!["E1".to_string()]);
    }
}
