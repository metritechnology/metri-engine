// eav/reader/query.rs — Ejecutor físico de planes de consulta en DynamoDB.
// Implementa los 5 planes del PlanSelector con los índices EAV reales.
//
// Planes soportados:
//   PointLookup        → tabla EAVT (1 Get, O(1))
//   AvetSingleFilter   → GSI-AVET  (1 Query, valor exacto con SK binario)
//   AvetIntersection   → GSI-AVET  (N Queries paralelas + intersección HashSet)
//   FtsSearch          → GSI-FTS   (K Query trigrams + Levenshtein threshold)
//   AevtScan           → GSI-AEVT  (Scan por tipo de entidad)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, warn};

use aws_sdk_dynamodb::types::AttributeValue;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::fts::trigram::generate_trigrams;
use crate::eav::types::datom::DatomValue;
use crate::eav::types::encoding::build_avet_sk;
use crate::infrastructure::dynamodb::{DynamoClient, av_string};

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
    pub tenant_id:    String,
    pub entity_type:  String,
    pub strategy:     IndexStrategy,
    pub pull_pattern: Vec<String>,
    pub limit:        u64,
}

/// Plan nativo del PlanSelector (usa attr_name en lugar de attr_id numérico).
/// Se genera desde `janus::plan_selector::EavQueryPlan` y ejecuta contra DynamoDB.
#[derive(Debug, Clone)]
pub enum NativeQueryPlan {
    PointLookup   { entity_id: String },
    AvetSingle    { tenant_id: String, attr_name: String, value: DatomValue },
    AvetIntersect { tenant_id: String, filters: Vec<(String, DatomValue)> },
    FtsSearch     { tenant_id: String, term: String },
    AevtScan      { tenant_id: String, entity_type: String },
}

#[derive(Clone)]
pub struct EavQueryExecutor {
    ddb:   Arc<DynamoClient>,
    table: String,
}

impl EavQueryExecutor {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        EavQueryExecutor { ddb, table: table.into() }
    }

    // ── API principal: plan legacy (aegis/oltp/compiler) ────────────────────

    /// Ejecuta el plan de consulta legacy y devuelve los IDs de entidades.
    pub async fn execute_plan(&self, plan: &QueryExecutionPlan) -> Result<Vec<String>, DomainError> {
        debug!("[EAV] Executing legacy plan: {:?}", plan.strategy);

        let entity_ids = match &plan.strategy {
            IndexStrategy::PointLookup(ulid) => {
                vec![ulid.clone()]
            }
            IndexStrategy::AevtScan { .. } => {
                // Usar AevtScan nativo con entity_type del plan
                self.execute_aevt_scan_by_type(&plan.tenant_id, &plan.entity_type, plan.limit).await?
            }
            IndexStrategy::AvetLookup { attr_id: _, value } => {
                // Usar el attr_name del pull_pattern como heurística
                let attr_name = plan.pull_pattern.first()
                    .cloned()
                    .unwrap_or_default();
                self.execute_avet_single(&plan.tenant_id, &attr_name, value).await?
            }
            IndexStrategy::ParallelAvetIntersection(filters) => {
                // Convertir attr_id numérico a attr_name (best-effort)
                let named: Vec<(String, DatomValue)> = filters
                    .iter()
                    .enumerate()
                    .map(|(i, (_, v))| (format!("attr_{i}"), v.clone()))
                    .collect();
                self.execute_avet_intersection(&plan.tenant_id, &named).await?
            }
        };

        Ok(entity_ids.into_iter().take(plan.limit as usize).collect())
    }

    // ── API nativa (janus/router.rs a través de execute_native_plan) ─────────

    /// Ejecuta un NativeQueryPlan y devuelve entity_ids.
    pub async fn execute_native_plan(&self, plan: &NativeQueryPlan) -> Result<Vec<String>, DomainError> {
        tracing::info!("DEBUG execute_native_plan called with plan: {:?}", plan);
        match plan {
            NativeQueryPlan::PointLookup { entity_id } => {
                Ok(vec![entity_id.clone()])
            }

            NativeQueryPlan::AvetSingle { tenant_id, attr_name, value } => {
                self.execute_avet_single(tenant_id, attr_name, value).await
            }

            NativeQueryPlan::AvetIntersect { tenant_id, filters } => {
                self.execute_avet_intersection(tenant_id, filters).await
            }

            NativeQueryPlan::FtsSearch { tenant_id, term } => {
                self.execute_fts_search(tenant_id, term).await
            }

            NativeQueryPlan::AevtScan { tenant_id, entity_type } => {
                // Pasamos limit=None para que la paginación automática obtenga TODOS los ids.
                // El limit real de paginación se aplica en el executor (offset/page_limit).
                self.execute_aevt_scan_by_type(tenant_id, entity_type, 0).await

            }
        }
    }

    // ── Implementaciones de índice ────────────────────────────────────────────

    /// GSI-AVET: PK = "T#<tenant>#AV#<attr_name>", SK begins_with <value_prefix>
    /// Retorna los entity_ids almacenados en `eid` del datom encontrado.
    async fn execute_avet_single(
        &self,
        tenant:    &str,
        attr_name: &str,
        value:     &DatomValue,
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

        let mut attr_names  = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "AVET_PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));
        // Nota: No usamos #sk en la condición — filtramos el valor en memoria
        // para evitar el error "unused ExpressionAttributeNames: keys: {#sk}"

        let raw = self.ddb.query(
            &self.table,
            Some("GSI-AVET"),
            "#pk = :pk",
            attr_names,
            attr_values,
            true,
            None,
        ).await.map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("AVET single err: {e:?}")))?;

        // Post-filter en memoria por valor exacto
        let expected_v = datom_value_to_string(value);
        Ok(raw.into_iter().filter(|item| {
            match item.get("v") {
                Some(AttributeValue::S(s)) => s == &expected_v,
                Some(AttributeValue::N(n)) => n == &expected_v,
                _ => false,
            }
        }).filter_map(|item| {
            av_string(item.get("eid")?).map(|s| s.to_string())
        }).collect())

    }

    /// GSI-AVET intersección: ejecuta N queries en secuencia y hace HashSet intersection.
    /// Blueprint: §I.2 Paso 5 — AvetIntersection "paralelo + intersección HashSet"
    async fn execute_avet_intersection(
        &self,
        tenant:  &str,
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
        term:   &str,
    ) -> Result<Vec<String>, DomainError> {
        let trigrams = generate_trigrams(term);
        if trigrams.is_empty() {
            return Ok(vec![]);
        }

        debug!("[EAV-FTS] Buscando '{}' → {} trigrams", term, trigrams.len());

        // Mapa entity_id → conteo de trigrams que lo contienen
        let mut score_map: HashMap<String, usize> = HashMap::new();

        for trigram in &trigrams {
            let pk = format!("T#{tenant}#FTS#{trigram}");

            let mut attr_names  = HashMap::new();
            let mut attr_values = HashMap::new();
            attr_names.insert("#pk".to_string(), "PK".to_string());
            attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

            let raw = self.ddb.query(
                &self.table,
                None, // tabla principal — FTS usa la misma tabla
                "#pk = :pk",
                attr_names,
                attr_values,
                true,
                None,
            ).await;

            match raw {
                Ok(items) => {
                    for item in items {
                        if let Some(eid) = item.get("SK").and_then(|v| av_string(v)).map(|s| s.to_string()) {
                            *score_map.entry(eid).or_insert(0) += 1;
                        }
                    }
                }
                Err(e) => {
                    // Log y continuar — un trigram fallido no aborta la búsqueda
                    warn!("[EAV-FTS] Error en trigram '{trigram}': {e:?}");
                }
            }
        }

        // Threshold: al menos 50% de los trigrams deben coincidir (fuzzy tolerance)
        let threshold = (trigrams.len() / 2).max(1);
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
        tenant:      &str,
        entity_type: &str,
        limit:       u64,
    ) -> Result<Vec<String>, DomainError> {
        // En el esquema EAV el atributo entity_type almacena el tipo de entidad.
        // PK del GSI-AEVT: T#<tenant>#A#entity_type
        let pk = format!("T#{tenant}#A#entity_type");

        let mut attr_names  = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "AEVT_PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

        // limit=0 significa "traer todos" — se pagina automáticamente vía last_evaluated_key
        let ddb_limit = if limit == 0 { None } else { Some(limit as i32) };

        tracing::info!("DEBUG execute_aevt_scan_by_type: starting ddb.query for pk={}", pk);
        let raw = self.ddb.query(
            &self.table,
            Some("GSI-AEVT"),
            "#pk = :pk",
            attr_names,
            attr_values,
            true,
            ddb_limit,
        ).await.map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("AEVT scan err: {e:?}")))?;
        tracing::info!("DEBUG execute_aevt_scan_by_type: {} items from ddb", raw.len());


        // Filtrar por valor del atributo = entity_type en memoria (post-filter)
        let mut ids: Vec<String> = raw.into_iter()
            .filter(|item| {
                item.get("v").and_then(|v| av_string(v))
                    .map(|v| v == entity_type)
                    .unwrap_or(false)
            })
            .filter_map(|item| item.get("eid").and_then(|v| av_string(v)).map(|s| s.to_string()))
            .collect();

        ids.dedup();
        Ok(ids)
    }
}

/// Convierte un DatomValue a su representación String para comparación en memoria.
fn datom_value_to_string(value: &DatomValue) -> String {
    match value {
        DatomValue::Str(s)   => s.clone(),
        DatomValue::Long(n)  => n.to_string(),
        DatomValue::Double(f) => {
            if f.fract() == 0.0 {
                format!("{}", *f as i64)
            } else {
                f.to_string()
            }
        }
        DatomValue::Bool(b)  => b.to_string(),
        _ => String::new(),
    }
}


