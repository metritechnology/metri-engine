//! Physical executor of query plans on DynamoDB.
//!
//! Ejecutor físico de planes de consulta en DynamoDB.
//! Implementa los 5 planes del PlanSelector con los índices EAV reales.
//!
//! Planes soportados:
//! PointLookup        → tabla EAVT (1 Get, O(1))
//! AvetSingleFilter   → GSI-AVET  (1 Query, valor exacto con SK binario)
//! AvetIntersection   → GSI-AVET  (N Queries paralelas + intersección HashSet)
//! FtsSearch          → GSI-FTS   (K Query trigrams + Levenshtein threshold)
//! AevtScan           → GSI-AEVT  (Scan por tipo de entidad)

use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use tracing::debug;

pub const MAX_AEVT_SCAN_CACHE_SIZE: usize = 2_000;

pub static AEVT_SCAN_CACHE: Lazy<RwLock<HashMap<(String, String), Vec<String>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Inserta una entrada en AEVT_SCAN_CACHE asegurando que no exceda MAX_AEVT_SCAN_CACHE_SIZE.
pub fn insert_aevt_scan_cache_entry(
    cache: &mut HashMap<(String, String), Vec<String>>,
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
    cache.insert(key, ids);
}

use aws_sdk_dynamodb::types::AttributeValue;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::fts::trigram::generate_trigrams;
use crate::eav::types::datom::DatomValue;
use crate::eav::types::encoding::build_avet_sk;
use crate::infrastructure::dynamodb::{av_string, DynamoClient};

/// Estrategia de índice legacy — mantenida para compatibilidad con aegis/oltp/compiler.rs.
#[derive(Debug, Clone)]
pub enum IndexStrategy {
    /// Obtiene un entity_id directamente desde EAVT
    PointLookup(String),
    /// Escaneo por Atributo-Entidad usando GSI-AEVT
    AevtScan { attr_id: u16 },
    /// Búsqueda por valor exacto usando GSI-AVET
    AvetLookup { attr_id: u16, value: DatomValue },
    /// Múltiples filtros que requieren intersección paralela de AVET
    ParallelAvetIntersection(Vec<(u16, DatomValue)>),
}

/// Plan físico de ejecución de consulta para EAV.
#[derive(Debug, Clone)]
pub struct QueryExecutionPlan {
    pub tenant_id: String,
    pub entity_type: String,
    pub strategy: IndexStrategy,
    pub pull_pattern: Vec<String>,
    pub limit: u64,
}

/// Plan nativo del PlanSelector (usa attr_name en lugar de attr_id numérico).
/// Se genera desde `janus::plan_selector::EavQueryPlan` y ejecuta contra DynamoDB.
#[derive(Debug, Clone)]
pub enum NativeQueryPlan {
    PointLookup {
        entity_id: String,
    },
    AvetSingle {
        tenant_id: String,
        attr_name: String,
        value: DatomValue,
    },
    AvetIntersect {
        tenant_id: String,
        filters: Vec<(String, DatomValue)>,
    },
    FtsSearch {
        tenant_id: String,
        term: String,
    },
    AevtScan {
        tenant_id: String,
        entity_type: String,
    },
}

#[derive(Clone)]
pub struct EavQueryExecutor {
    ddb: Arc<DynamoClient>,
    table: String,
}

impl EavQueryExecutor {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        EavQueryExecutor {
            ddb,
            table: table.into(),
        }
    }

    // ── API principal: plan legacy (aegis/oltp/compiler) ────────────────────

    /// Ejecuta el plan de consulta legacy y devuelve los IDs de entidades.
    pub async fn execute_plan(
        &self,
        plan: &QueryExecutionPlan,
    ) -> Result<Vec<String>, DomainError> {
        debug!("[EAV] Executing legacy plan: {:?}", plan.strategy);

        let entity_ids = match &plan.strategy {
            IndexStrategy::PointLookup(ulid) => {
                vec![ulid.clone()]
            }
            IndexStrategy::AevtScan { .. } => {
                // Usar AevtScan nativo con entity_type del plan
                self.execute_aevt_scan_by_type(&plan.tenant_id, &plan.entity_type, plan.limit)
                    .await?
            }
            IndexStrategy::AvetLookup { attr_id: _, value } => {
                // Usar el attr_name del pull_pattern como heurística
                let attr_name = plan.pull_pattern.first().cloned().unwrap_or_default();
                self.execute_avet_single(&plan.tenant_id, &attr_name, value)
                    .await?
            }
            IndexStrategy::ParallelAvetIntersection(filters) => {
                // Convertir attr_id numérico a attr_name (best-effort)
                let named: Vec<(String, DatomValue)> = filters
                    .iter()
                    .enumerate()
                    .map(|(i, (_, v))| (format!("attr_{i}"), v.clone()))
                    .collect();
                self.execute_avet_intersection(&plan.tenant_id, &named)
                    .await?
            }
        };

        Ok(entity_ids.into_iter().take(plan.limit as usize).collect())
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
                // Pasamos limit=None para que la paginación automática obtenga TODOS los ids.
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
        let is_global =
            attr_name == "username" || attr_name == "email" || attr_name == "primary_phone";
        let target_tenant = if is_global { "GLOBAL" } else { tenant };

        let mut ids = self
            .execute_avet_single_for_tenant(target_tenant, attr_name, value)
            .await?;

        if ids.is_empty()
            && tenant == "system"
            && !is_global
            && (attr_name == "username" || attr_name == "email")
        {
            // Buscar en todos los otros tenants registrados
            let tenants = match self.execute_aevt_scan_by_type("system", "tenant", 0).await {
                Ok(t) => t,
                Err(_) => vec![],
            };

            for t in tenants {
                if t == "system" {
                    continue;
                }
                if let Ok(tenant_ids) = self
                    .execute_avet_single_for_tenant(&t, attr_name, value)
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

    async fn execute_avet_single_for_tenant(
        &self,
        tenant: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        let pk = format!("T#{tenant}#AV#{attr_name}");

        // Construir el SK prefix para el valor buscado
        let sk_prefix = build_avet_sk(value, ""); // sin entity_id = solo value prefix
                                                  // Truncar al prefix sin el entity_id (últimos bytes son entity_id)
                                                  // Para begins_with necesitamos solo [type_tag][value_bytes]
        let sk_prefix_truncated = if sk_prefix.len() > 1 {
            sk_prefix[..sk_prefix.len().saturating_sub(0)].to_vec()
        } else {
            sk_prefix
        };

        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "vp".to_string());
        attr_names.insert("#sk".to_string(), "vs".to_string()); // Mapear Sort Key indexada

        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));
        attr_values.insert(
            ":sk_prefix".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk_prefix_truncated)),
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

        // Post-filter en memoria por valor exacto (seguridad contra colisiones de truncamiento)
        // y DEDUPLICACIÓN por entidad con el datom más reciente: GSI-AVET guarda
        // UNA fila por datom (assert y retract incluidos), así que una entidad
        // cuyo atributo fue re-escrito N veces aparece N veces en el índice —
        // sin esto, la query devolvía la fila duplicada (bug «la OT lista el
        // mismo procedimiento N veces»). Mismo convenio que el scan AEVT:
        // sólo cuenta la versión de mayor tx, y sólo si es un assert.
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

        // FASE 1: secuencial — FASE 2 usa tokio::join! para paralelismo real
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

    /// GSI-FTS: Trigram intersection — Consulta cada trigram del término de búsqueda
    /// y retorna los entity_ids que aparecen en K de los N trigrams (threshold ≥ 50%).
    /// Blueprint: §I.2 Paso 5 — FtsSearch "trigram + post-filter DL"
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
            attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

            let raw = self
                .ddb
                .query(
                    &self.table,
                    None, // tabla principal — FTS usa la misma tabla
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
                        if let Some(aws_sdk_dynamodb::types::AttributeValue::B(blob)) =
                            item.get("SK")
                        {
                            let bytes = blob.as_ref();
                            if bytes.len() > 2 {
                                let eid_bytes = &bytes[2..]; // skip 2 bytes attr_id
                                if let Ok(eid) = String::from_utf8(eid_bytes.to_vec()) {
                                    *score_map.entry(eid).or_insert(0) += 1;
                                } else {
                                    tracing::warn!("[EAV-FTS] Error decodificando UTF-8 de SK");
                                }
                            } else {
                                tracing::warn!("[EAV-FTS] SK demasiado corto para contener attr_id y entity_id");
                            }
                        } else {
                            tracing::warn!(
                                "[EAV-FTS] SK no es de tipo Binary B para item en FTS: {:?}",
                                item.get("SK")
                            );
                        }
                    }
                }
                Err(e) => {
                    // Log y continuar — un trigram fallido no aborta la búsqueda
                    tracing::error!("[EAV-FTS] Error en trigram '{}': {:?}", trigram, e);
                }
            }
        }

        // Threshold: al menos 50% de los trigrams deben coincidir (fuzzy tolerance)
        let threshold = (trigrams.len() / 2).max(1);
        tracing::debug!(
            "[EAV-FTS] Trigrams generados: {}, Threshold: {}",
            trigrams.len(),
            threshold
        );
        let mut results: Vec<(String, usize)> = score_map
            .into_iter()
            .filter(|(_, score)| *score >= threshold)
            .collect();

        tracing::debug!("[EAV-FTS] Results tras threshold: {}", results.len());

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
        if limit == 0 {
            if let Ok(cache) = AEVT_SCAN_CACHE.read() {
                let cache_key = (tenant.to_string(), entity_type.to_string());
                if let Some(cached_ids) = cache.get(&cache_key) {
                    if !cached_ids.is_empty() {
                        tracing::debug!(
                            "AEVT_SCAN_CACHE HIT for tenant={}, type={}. Count={}",
                            tenant,
                            entity_type,
                            cached_ids.len()
                        );
                        return Ok(cached_ids.clone());
                    }
                }
            }
            tracing::debug!(
                "AEVT_SCAN_CACHE MISS for tenant={}, type={}",
                tenant,
                entity_type
            );
        }

        // En el esquema EAV el atributo entity_type almacena el tipo de entidad.
        // PK del GSI-AEVT: T#<tenant>#A#entity_type (segmentado por tipo para optimizar al máximo)
        let pk = format!("T#{tenant}#A#entity_type#{entity_type}");

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
        m.insert(
            "SK".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk)),
        );
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
}
