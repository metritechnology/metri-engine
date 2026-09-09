//! Step 5 — optimal EAV index selection.
//!
//! Paso 5: Selección del índice EAV óptimo
//!
//! # Origin
//! Blueprint: JANUS - Rust.md §I.2 Paso 5 — Plan Selection
//!
//! Analiza el AST IR compilado y selecciona el QueryPlan más eficiente:
//! Prioridad: PointLookup > AvetSingleFilter > AvetIntersection > FtsSearch > AevtScan

use serde_json::Value;
use tracing::debug;

use crate::eav::types::datom::DatomValue;

/// Plan de ejecución seleccionado por el Plan Selector.
/// Equivale al enum QueryPlan del blueprint §I.2 Paso 5.
#[derive(Debug, Clone)]
pub enum EavQueryPlan {
    /// GET por ULID explícito → Main Table EAVT (1 Query, mínimo costo)
    PointLookup { entity_id: String },

    /// WHERE attr = value (1 filtro indexado) → GSI-AVET
    AvetSingleFilter {
        attr_name: String,
        value: DatomValue,
    },

    /// WHERE attr1 = v1 AND attr2 = v2 → 2+ AVET en paralelo + intersección HashSet
    AvetIntersection { filters: Vec<(String, DatomValue)> },

    /// Búsqueda fuzzy FTS → GSI-FTS trigram + EAVT reconstituir
    FtsSearch { term: String },

    /// Scan por tipo de entidad (sin filtros de valor) → GSI-AEVT
    /// shard: None = sin sharding, Some(N) = shard total
    AevtScan {
        entity_type: String,
        shard_total: Option<u8>,
    },

    /// Reverse reference lookup → GSI-VAET
    VaetLookup {
        ref_entity_id: String,
        attr_name: Option<String>,
    },

    /// Time-travel as-of un TX_ID → EAVT con SK ≤ T
    AsOfSnapshot { entity_id: String, as_of_tx: u64 },
}

/// Selecciona el plan de ejecución EAV más eficiente a partir del AST IR.
///
/// Orden de prioridad (del menos al más costoso):
///   1. PointLookup   — 1 GET, O(1), el más barato posible
///   2. AvetSingle    — 1 Query GSI, O(log N)
///   3. AvetIntersect — N Queries paralelas + HashSet, O(min(|A|,|B|,...))
///   4. FtsSearch     — K Query trigrams + post-filter DL, O(k + matching)
///   5. AevtScan      — Scan completo por tipo, O(total_entities_of_type)
pub fn select_plan(ast_ir: &Value) -> EavQueryPlan {
    let entity_type = ast_ir
        .get("entity")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // ── P1: ¿Hay un ULID explícito en el WHERE? (PointLookup)
    if let Some(ulid) = extract_ulid_filter(ast_ir) {
        debug!("[PlanSelector] → PointLookup entity_id={ulid}");
        return EavQueryPlan::PointLookup { entity_id: ulid };
    }

    // ── P2: ¿Hay time-travel as_of_tx > 0?
    if let Some(as_of) = ast_ir
        .get("as_of_tx")
        .and_then(|v| v.as_u64())
        .filter(|&t| t > 0)
    {
        let entity_id = extract_ulid_filter(ast_ir).unwrap_or_default();
        debug!("[PlanSelector] → AsOfSnapshot entity_id={entity_id} as_of_tx={as_of}");
        return EavQueryPlan::AsOfSnapshot {
            entity_id,
            as_of_tx: as_of,
        };
    }

    // ── P3: ¿Hay búsqueda FTS?
    if let Some(term) = ast_ir.get("search").and_then(|v| v.as_str()) {
        debug!("[PlanSelector] → FtsSearch term={term}");
        return EavQueryPlan::FtsSearch {
            term: term.to_string(),
        };
    }

    // ── P4: ¿Hay filtros de valor exacto indexables? (AVET)
    let indexed_filters = extract_indexed_filters(ast_ir);
    match indexed_filters.len() {
        0 => {
            // ── P5: Sin filtros → AevtScan (más genérico y costoso)
            debug!("[PlanSelector] → AevtScan entity_type={entity_type}");
            EavQueryPlan::AevtScan {
                entity_type,
                shard_total: None,
            }
        }
        1 => {
            if let Some((attr, val)) = indexed_filters.into_iter().next() {
                debug!("[PlanSelector] → AvetSingleFilter attr={attr}");
                EavQueryPlan::AvetSingleFilter {
                    attr_name: attr,
                    value: val,
                }
            } else {
                debug!("[PlanSelector] → AevtScan entity_type={entity_type}");
                EavQueryPlan::AevtScan {
                    entity_type,
                    shard_total: None,
                }
            }
        }
        _ => {
            debug!(
                "[PlanSelector] → AvetIntersection filters={}",
                indexed_filters.len()
            );
            EavQueryPlan::AvetIntersection {
                filters: indexed_filters,
            }
        }
    }
}

/// Extrae el ULID explícito de un filtro WHERE entity/ulid = "..."
fn extract_ulid_filter(ast_ir: &Value) -> Option<String> {
    let where_clause = ast_ir.get("where")?;
    search_ulid_in_node(where_clause)
}

fn search_ulid_in_node(node: &Value) -> Option<String> {
    if let Some(arr) = node.as_array() {
        if arr.len() >= 3 {
            let op = arr[0].as_str().unwrap_or("");
            let field = arr[1].as_str().unwrap_or("");
            if op == "=" && (field == "entity/ulid" || field == "entity/id") {
                return arr[2].as_str().map(|s| s.to_string());
            }
            // Recursión para AND/OR
            if op == "and" || op == "or" {
                for child in arr.iter().skip(1) {
                    if let Some(ulid) = search_ulid_in_node(child) {
                        return Some(ulid);
                    }
                }
            }
        }
    }
    None
}

/// Extrae filtros de igualdad sobre atributos indexados en el WHERE.
/// Solo considera filtros EQ (=) — los rangos y FTS usan otras estrategias.
fn extract_indexed_filters(ast_ir: &Value) -> Vec<(String, DatomValue)> {
    let Some(where_clause) = ast_ir.get("where") else {
        return vec![];
    };

    let mut filters = Vec::new();
    collect_eq_filters(where_clause, &mut filters);

    // Excluir los filtros de sistema que no son indexables en AVET
    filters
        .retain(|(attr, _)| attr != "tenant/id" && attr != "entity/type" && attr != "entity/ulid");

    filters
}

fn collect_eq_filters(node: &Value, out: &mut Vec<(String, DatomValue)>) {
    let Some(arr) = node.as_array() else { return };
    if arr.is_empty() {
        return;
    }

    let op = arr[0].as_str().unwrap_or("");
    match op {
        "=" if arr.len() >= 3 => {
            let field = arr[1].as_str().unwrap_or("").to_string();
            let val = json_to_datum_value(&arr[2]);
            out.push((field, val));
        }
        "and" | "or" => {
            for child in arr.iter().skip(1) {
                collect_eq_filters(child, out);
            }
        }
        _ => {}
    }
}

fn json_to_datum_value(val: &Value) -> DatomValue {
    match val {
        Value::String(s) => DatomValue::Str(s.clone()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DatomValue::Long(i)
            } else if let Some(f) = n.as_f64() {
                DatomValue::Double(f)
            } else {
                DatomValue::Null
            }
        }
        Value::Bool(b) => DatomValue::Bool(*b),
        Value::Null => DatomValue::Null,
        _ => DatomValue::Null,
    }
}

use crate::janus::fbs;

/// Selecciona el plan de ejecución EAV usando AnalyticsRequestT nativo
pub fn select_plan_fbs(ast_ir: &fbs::AnalyticsRequestT) -> EavQueryPlan {
    let entity_type = ast_ir.entity.as_deref().unwrap_or("unknown").to_string();

    // ── P1: ¿Hay un ULID explícito en los filtros? (PointLookup)
    if let Some(ulid) = extract_ulid_fbs(ast_ir) {
        debug!("[PlanSelector] → PointLookup entity_id={ulid}");
        return EavQueryPlan::PointLookup { entity_id: ulid };
    }

    // ── P2: ¿Hay time-travel as_of_tx > 0? (Mapeado desde end_ts si existe)
    if let Some(time_frame) = &ast_ir.time_frame {
        if time_frame.end_ts > 0
            && time_frame.type_ == fbs::TimeFrameContext_TimeFilterType::CUSTOM_RANGE
        {
            if let Some(entity_id) = extract_ulid_fbs(ast_ir) {
                let as_of = time_frame.end_ts as u64;
                debug!("[PlanSelector] → AsOfSnapshot entity_id={entity_id} as_of_tx={as_of}");
                return EavQueryPlan::AsOfSnapshot {
                    entity_id,
                    as_of_tx: as_of,
                };
            }
        }
    }

    // ── P3: ¿Hay búsqueda FTS?
    // FTS Index: usamos la capacidad máxima técnica del eav con Trigrams persistidos
    if let Some(term) = &ast_ir.search {
        if !term.is_empty() {
            debug!("[PlanSelector] → FtsSearch term={term}");
            return EavQueryPlan::FtsSearch {
                term: term.to_string(),
            };
        }
    }

    // ── P4: ¿Hay filtros indexables (AVET)?
    let indexed_filters = extract_indexed_filters_fbs(ast_ir);
    match indexed_filters.len() {
        0 => {
            debug!("[PlanSelector] → AevtScan entity_type={entity_type}");
            EavQueryPlan::AevtScan {
                entity_type,
                shard_total: None,
            }
        }
        1 => {
            if let Some((attr, val)) = indexed_filters.into_iter().next() {
                debug!("[PlanSelector] → AvetSingleFilter attr={attr}");
                EavQueryPlan::AvetSingleFilter {
                    attr_name: attr,
                    value: val,
                }
            } else {
                debug!("[PlanSelector] → AevtScan entity_type={entity_type}");
                EavQueryPlan::AevtScan {
                    entity_type,
                    shard_total: None,
                }
            }
        }
        _ => {
            debug!(
                "[PlanSelector] → AvetIntersection filters={}",
                indexed_filters.len()
            );
            EavQueryPlan::AvetIntersection {
                filters: indexed_filters,
            }
        }
    }
}

fn extract_ulid_fbs(ast_ir: &fbs::AnalyticsRequestT) -> Option<String> {
    if let Some(filters) = &ast_ir.filters {
        for f in filters {
            if let Some(crit) = &f.criteria {
                if crit.op_ref.0 == fbs::FilterOperator::EQ.0
                    && crit.field.as_deref() == Some("entity/ulid")
                {
                    return crit.value.as_ref().and_then(|v| v.string_val.clone());
                }
            }
        }
    }
    None
}

fn extract_indexed_filters_fbs(ast_ir: &fbs::AnalyticsRequestT) -> Vec<(String, DatomValue)> {
    let _entity_type = ast_ir.entity.as_deref().unwrap_or("unknown");
    let mut results = Vec::new();

    // 1. Hierarchy Indexing: si hay current_node_id explícito (!= "__none__"), aprovechar el índice AVET.
    // FIX CRÍTICO: el AVET_PK en DynamoDB almacena el attr_name SIN namespace (ej: "parent_location_id"),
    // por lo que NO debemos agregar el namespace de la entidad aquí.
    // El write path en transact.rs usa el attr_name tal como viene del payload (bare name).
    if let Some(h) = &ast_ir.hierarchy {
        if let Some(node_id) = &h.current_node_id {
            if node_id != "__none__" && !node_id.is_empty() {
                let pf = h
                    .parent_field
                    .clone()
                    .unwrap_or_else(|| "parent_id".to_string());
                // Usar solo la parte después del último '/' (bare name) para coincidir con AVET_PK
                let bare_attr = pf.split('/').next_back().unwrap_or(&pf).to_string();
                results.push((bare_attr, DatomValue::Str(node_id.to_string())));
            }
        }
    }

    if let Some(filters) = &ast_ir.filters {
        for f in filters {
            if let Some(crit) = &f.criteria {
                if crit.op_ref.0 == fbs::FilterOperator::EQ.0 {
                    let raw_attr = crit.field.clone().unwrap_or_default();
                    if raw_attr.starts_with("entity/")
                        || raw_attr == "tenant_id"
                        || raw_attr == "entity_type"
                        || raw_attr.contains('.')
                    {
                        continue; // Skip intrinsic and dot-path fields for AVET indexing
                    }
                    // FIX CRÍTICO: el AVET_PK en DynamoDB almacena el attr_name SIN namespace (bare name)
                    // tal como viene en el write path de transact.rs, por lo que usamos el bare name.
                    let attr = raw_attr
                        .split('/')
                        .next_back()
                        .unwrap_or(&raw_attr)
                        .to_string();

                    let val = if let Some(v) = &crit.value {
                        if let Some(s) = &v.string_val {
                            if let Ok(n) = s.parse::<i64>() {
                                DatomValue::Long(n)
                            } else if let Ok(b) = s.parse::<bool>() {
                                DatomValue::Bool(b)
                            } else {
                                DatomValue::Str(s.clone())
                            }
                        } else if v.number_val != 0.0 {
                            DatomValue::Long(v.number_val as i64)
                        } else {
                            DatomValue::Null
                        }
                    } else {
                        DatomValue::Null
                    };

                    if !matches!(val, DatomValue::Null) {
                        results.push((attr, val));
                    }
                }
            }
        }
    }
    results
}
