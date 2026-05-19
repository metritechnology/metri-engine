// aegis/oltp/executor.rs — Ejecutor que coordina EAV y Aegis.
// Orquesta: compile → execute_native_plan → pull → post_process.
//
// Flujo moderno (post-refactoring):
//   1. compile_native_plan()         → NativeQueryPlan (PlanSelector-driven)
//   2. executor.execute_native_plan() → Vec<entity_id>
//   3. pull_reader.pull()             → EntityMap por cada entity_id
//   4. apply_sort_and_limit()         → Vec<JSON row> ordenadas

use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{info, debug, warn};

use crate::domain::errors::DomainError;
use crate::aegis::oltp::compiler::{compile_oltp_query, compile_native_plan};
use crate::aegis::temporal_bridge::{resolve_fbs_time_frame, no_time_range};
use crate::eav::reader::query::{EavQueryExecutor, NativeQueryPlan};
use crate::eav::reader::pull::EavReader;
use crate::eav::types::datom::DatomValue;

#[derive(Clone)]
pub struct OltpExecutor {
    query_executor: EavQueryExecutor,
    pull_reader:    EavReader,
}

impl OltpExecutor {
    pub fn new(query_executor: EavQueryExecutor, pull_reader: EavReader) -> Self {
        Self { query_executor, pull_reader }
    }

    /// Flujo principal moderno: PlanSelector → execute_native_plan → pull → sort.
    pub async fn run_oltp_query(&self, tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError> {
        info!("[Aegis OLTP] Iniciando query para tenant: {}", tenant_id);

        let entity_type = ast_ir.get("entity").and_then(|v| v.as_str()).unwrap_or("unknown");
        let limit       = ast_ir.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

        // ── AsOfSnapshot: path especial ───────────────────────────────────────
        if let Some(as_of_tx) = ast_ir.get("as_of_tx").and_then(|v| v.as_u64()).filter(|&t| t > 0) {
            if let Some(entity_id) = extract_ulid_from_ast(ast_ir) {
                debug!("[Aegis OLTP] AsOfSnapshot entity_id={entity_id} as_of_tx={as_of_tx}");
                let select  = extract_select_attrs(ast_ir);
                let sel_ref: Vec<&str> = select.iter().map(|s| s.as_str()).collect();
                let map = self.pull_reader.pull_as_of(
                    tenant_id, &entity_id, as_of_tx,
                    if sel_ref.is_empty() { None } else { Some(&sel_ref) },
                ).await?;
                let row = entity_map_to_json(map);
                return Ok(json!([row]));
            }
        }

        // ── Plan normal vía PlanSelector ──────────────────────────────────────
        let native_plan = compile_native_plan(ast_ir, tenant_id);
        debug!("[Aegis OLTP] Plan: {:?}", native_plan);

        let mut entity_ids = self.query_executor
            .execute_native_plan(&native_plan)
            .await?;

        // Aplicar limit al resultado del executor
        entity_ids.truncate(limit);
        debug!("[Aegis OLTP] {} entity_ids obtenidos", entity_ids.len());

        // ── Pull hidratación ──────────────────────────────────────────────────
        let select  = extract_select_attrs(ast_ir);
        let sel_ref: Vec<&str> = select.iter().map(|s| s.as_str()).collect();
        let sel_opt = if sel_ref.is_empty() || sel_ref[0] == "*" { None } else { Some(sel_ref.as_slice()) };

        // FASE 1: pull secuencial — FASE 2 usa BatchGetItem en paralelo
        let mut rows = Vec::with_capacity(entity_ids.len());
        for entity_id in &entity_ids {
            match self.pull_reader.pull(tenant_id, entity_id, sel_opt).await {
                Ok(mut map) if !map.is_empty() => {
                    map.insert("id".to_string(), crate::eav::types::datom::DatomValue::Str(entity_id.clone()));
                    rows.push(entity_map_to_json(map));
                }
                Ok(_) => {
                    warn!("[Aegis OLTP] pull vacío para entity_id={entity_id}");
                }
                Err(e) => {
                    warn!("[Aegis OLTP] pull falló para entity_id={entity_id}: {e:?}");
                }
            }
        }

        // ── Ordering en memoria ───────────────────────────────────────────────
        if let Some(order_by) = ast_ir.get("order_by").and_then(|v| v.as_array()) {
            apply_sort_in_memory(&mut rows, order_by);
        }

        Ok(Value::Array(rows))
    }

    pub async fn run_oltp_query_fbs(&self, tenant_id: &str, ast_ir: &crate::janus::fbs::AnalyticsRequestT) -> Result<Value, DomainError> {
        info!("[Aegis OLTP FBS] Iniciando query para tenant: {}", tenant_id);

        let entity_type = ast_ir.entity.as_deref().unwrap_or("unknown");
        let limit_raw = ast_ir.limit as usize;
        let effective_limit = if limit_raw == 0 { 100 } else { limit_raw };

        // ── Resolver TimeFrame via temporal canónico ───────────────────────────
        let time_range = ast_ir.time_frame
            .as_ref()
            .and_then(|tf| resolve_fbs_time_frame(tf))
            .unwrap_or_else(no_time_range);

        debug!(
            "[Aegis OLTP FBS] TimeRange resuelto → start_ts={:?} end_ts={:?}",
            time_range.start_ts, time_range.end_ts
        );

        // ── AsOfSnapshot: path especial ───────────────────────────────────────
        let is_custom_range = ast_ir.time_frame
            .as_ref()
            .map(|tf| tf.type_.0 == 1 /* CUSTOM_RANGE */)
            .unwrap_or(false);

        if is_custom_range {
            if let (Some(end_ts), Some(entity_id)) = (
                time_range.end_ts,
                extract_ulid_fbs_executor(ast_ir)
            ) {
                let as_of_tx = end_ts as u64;
                debug!("[Aegis OLTP FBS] AsOfSnapshot entity_id={entity_id} as_of_tx={as_of_tx}");
                let select = extract_select_attrs_fbs(ast_ir);
                let sel_ref: Vec<&str> = select.iter().map(|s| s.as_str()).collect();
                let mut map = self.pull_reader.pull_as_of(
                    tenant_id, &entity_id, as_of_tx,
                    if sel_ref.is_empty() { None } else { Some(&sel_ref) },
                ).await?;
                map.insert("id".to_string(), crate::eav::types::datom::DatomValue::Str(entity_id.clone()));
                let row = entity_map_to_json(map);
                return Ok(json!([row]));
            }
        }

        // ── Plan normal vía PlanSelector FBS ──────────────────────────────────
        let native_plan = crate::aegis::oltp::compiler::compile_native_plan_fbs(ast_ir, tenant_id);
        debug!("[Aegis OLTP FBS] Plan: {:?}", native_plan);

        // ── Cursor-based pagination: decodificar cursor ───────────────────────
        let cursor_str = ast_ir.cursor.as_deref();
        let (offset, page_limit) = crate::aegis::pagination::decode_cursor(cursor_str, effective_limit);

        // Para AevtScan: obtenemos TODOS los entity_ids (para total real).
        let all_entity_ids = self.query_executor
            .execute_native_plan(&native_plan)
            .await?;

        let total_count = all_entity_ids.len();
        info!("[Aegis OLTP FBS] {} entity_ids totales (offset={} limit={})", total_count, offset, page_limit);

        // ── Pull hidratación con Streaming Filtering ──────────────────────────
        let output_cast = ast_ir.output_cast.0;
        let is_analytical = matches!(output_cast, 1 /* KPI */ | 4 /* PIE */ | 5 /* BUBBLE */);

        // Para KPI/PIE/BUBBLE: sel_opt=None → traer todos los atributos
        // Para TABLE: filtrar por atributos específicos (sin namespace)
        let sel_opt: Option<&[&str]> = if is_analytical {
            None  // traer todo — aggregation necesita todos los campos
        } else {
            let select = extract_select_attrs_fbs(ast_ir);
            // Strip namespaces: "asset/area_value" → "area_value" para match DynamoDB
            let _stripped: Vec<String> = select.iter()
                .map(|s| s.split('/').last().unwrap_or(s).to_string())
                .collect();
            None // Por ahora, sin filtro de atributos en pull para mayor compatibilidad
        };

        // Extraer filtros de negocio
        let infrastructure_fields = ["tenant_id"];
        let business_filters: Vec<_> = ast_ir.filters.as_ref()
            .map(|filters| filters.iter().filter(|f| {
                if let Some(crit) = &f.criteria {
                    let field = crit.field.as_deref().unwrap_or("");
                    !infrastructure_fields.contains(&field)
                } else {
                    true
                }
            }).collect())
            .unwrap_or_default();

        let target_matches = offset + page_limit;
        let max_hydrations = std::cmp::max(target_matches * 5, 250); // Cap de seguridad
        let mut hydrated_count = 0;
        let mut matching_rows = Vec::new();

        // ── Resolve node_id para hierarchy filter ────────────────────────────
        // [PORTED_FROM: build-hierarchy-parts en hierarchy.clj]
        // El HierarchyContext usa ULID como current_node_id. Los rows almacenan
        // el parent como ULID también (EAV string). Comparación directa ULID vs ULID.
        let hierarchy_parent_field_bare: Option<String> = ast_ir.hierarchy.as_ref()
            .and_then(|h| h.parent_field.as_deref())
            .map(|pf| pf.split('/').last().unwrap_or(pf).to_string());

        for entity_id in all_entity_ids {
            if matching_rows.len() >= target_matches || hydrated_count >= max_hydrations {
                break;
            }

            match self.pull_reader.pull(tenant_id, &entity_id, sel_opt).await {
                Ok(mut map) if !map.is_empty() => {
                    hydrated_count += 1;
                    map.insert("id".to_string(), crate::eav::types::datom::DatomValue::Str(entity_id.clone()));
                    let row = entity_map_to_json(map);

                    // ── Filtros de negocio in-memory ──────────────────────────────────────
                    let passes_business = business_filters.is_empty() || business_filters.iter().all(|f| {
                        crate::aegis::oltp::aggregation::eval_filter_node(&row, f)
                    });

                    // ── Filtro jerárquico ─────────────────────────────────────────────────
                    // [PORTED_FROM: passes_hierarchy en executor.clj]
                    // Compara parent_field del row contra current_node_id (ULID).
                    // Prueba tanto el campo con namespace como el bare (sin namespace).
                    let mut passes_hierarchy = true;
                    if let Some(h) = &ast_ir.hierarchy {
                        if let Some(pf) = &h.parent_field {
                            let bare_pf = hierarchy_parent_field_bare.as_deref().unwrap_or(pf);
                            // Probar con namespace y sin namespace
                            let val = row.get(pf.as_str()).or_else(|| row.get(bare_pf));
                            let node_id = h.current_node_id.as_deref().unwrap_or("");

                            if node_id == "__none__" || node_id.is_empty() {
                                // Nodos raíz: no deben tener parent_field
                                // [PORTED_FROM: (nil? output-cast) root check]
                                passes_hierarchy = val.is_none()
                                    || val.unwrap().is_null()
                                    || val.unwrap().as_str() == Some("")
                                    || val.unwrap().as_str() == Some("null");
                            } else {
                                // Hijos: parent_field debe ser == current_node_id (ULID directo)
                                // [PORTED_FROM: (= cid val) en hierarchy.clj]
                                passes_hierarchy = val
                                    .and_then(|v| v.as_str())
                                    .map(|s| s == node_id)
                                    .unwrap_or(false);
                                debug!(
                                    "[Aegis OLTP FBS] hierarchy: node_id={} field={} val={:?} passes={}",
                                    node_id, bare_pf, val, passes_hierarchy
                                );
                            }
                        }
                    }

                    // ── Filtro de búsqueda (in-memory Omnisearch) ───────────────────────────
                    let mut passes_search = true;
                    if let Some(term) = &ast_ir.search {
                        if !term.is_empty() {
                            passes_search = false;
                            let entity_name = ast_ir.entity.as_deref().unwrap_or("");
                            
                            // 1. Intentar buscar en fts_fields configurados
                            let mut checked = false;
                            if let Some(model) = crate::codice::registry::global().get_model(entity_name) {
                                if !model.fts_fields.is_empty() {
                                    for f in &model.fts_fields {
                                        // Try bare name and namespaced name
                                        let bare_f = f.split('/').last().unwrap_or(f);
                                        let val_opt = row.get(f).or_else(|| row.get(bare_f));
                                        
                                        if let Some(val) = val_opt.and_then(|v| v.as_str()) {
                                            if crate::aegis::oltp::fuzzy::fuzzy_match(val, term) {
                                                passes_search = true;
                                                checked = true;
                                                break;
                                            }
                                        }
                                    }
                                    if !passes_search { checked = true; }
                                }
                            }
                            
                            // 2. Si no hay fts_fields o no se encontró modelo, OmniSearch global sobre TODOS los valores String
                            if !checked {
                                if let Some(obj) = row.as_object() {
                                    for (_k, v) in obj {
                                        if let Some(val_str) = v.as_str() {
                                            if crate::aegis::oltp::fuzzy::fuzzy_match(val_str, term) {
                                                passes_search = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if passes_business && passes_hierarchy && passes_search {
                        matching_rows.push(row);
                    }
                }
                Ok(_) => { warn!("[Aegis OLTP FBS] pull vacío para entity_id={entity_id}"); }
                Err(e) => { warn!("[Aegis OLTP FBS] pull falló para entity_id={entity_id}: {e:?}"); }
            }
        }

        info!("[Aegis OLTP FBS] {} rows hidratados, {} pasaron filtros (cap={})", hydrated_count, matching_rows.len(), max_hydrations);

        // ── inject_has_children (Modo 2 EAV string) ──────────────────────────────
        // [PORTED_FROM: inject-has-children-from-rows en hierarchy.clj]
        // Debe ejecutarse ANTES de paginar, sobre el conjunto completo de rows
        // que pasaron el filtro jerárquico (en el nivel actual).
        // Para el nivel raíz: pasa todos los rows; para hijos: el subconjunto filtrado.
        let mut matching_rows = matching_rows;
        if let Some(h) = &ast_ir.hierarchy {
            if h.inject_has_children {
                if let Some(pf) = &h.parent_field {
                    // Para calcular has_children correcto, necesitamos saber qué entidades
                    // del nivel SIGUIENTE referencian a los rows actuales.
                    // Solución: inject_has_children_from_rows computa sobre el conjunto
                    // de rows ya hidratados. Para el nivel raíz necesitamos ALL rows.
                    let bare_pf = pf.split('/').last().unwrap_or(pf);
                    crate::aegis::oltp::hierarchy::inject_has_children_from_rows(
                        &mut matching_rows, bare_pf
                    );
                }
            }
        }

        // ── Normalize timestamps: ms → seconds ───────────────────────────────────
        // [PORTED_FROM: normalize-timestamps-in-rows en executor.clj]
        // Solo TABLE/TREE/CSV_EXPORT. TIMESERIES normaliza internamente.
        if !matches!(output_cast, 2 /* TIMESERIES */) {
            normalize_timestamps_in_rows(&mut matching_rows);
        }

        // Aplicar el offset (paginación en memoria)
        let rows: Vec<Value> = matching_rows.into_iter().skip(offset).collect();

        info!("[Aegis OLTP FBS] {} rows después de filtros y paginación", rows.len());


        // ── OutputCast aggregation con apply_metrics_fbs ──────────────────────
        // Para KPI/PIE/TIMESERIES: agregar sobre los rows filtrados
        // Para TABLE: devolver rows paginados directamente
        let output_cast = ast_ir.output_cast.0;

        let rows = if let Some(metrics) = &ast_ir.metrics {
            if !metrics.is_empty() {
                match output_cast {
                    1 /* KPI */ => {
                        // KPI: agregar todos los rows hidratados → 1 fila de métricas
                        let agg_result = crate::aegis::oltp::aggregation::apply_metrics_fbs(
                            &rows, metrics
                        );
                        vec![agg_result]
                    }
                    4 /* PIE */ | 5 /* BUBBLE */ => {
                        // PIE: group-by dimensiones + métricas por grupo
                        if let Some(dims) = &ast_ir.dimensions {
                            let dim_keys: Vec<String> = dims.iter()
                                .filter_map(|d| d.attribute.clone())
                                .collect();
                            if !dim_keys.is_empty() {
                                let mut groups: std::collections::HashMap<String, Vec<Value>> =
                                    std::collections::HashMap::new();
                                for row in &rows {
                                    let key = dim_keys.iter()
                                        .filter_map(|k| row.get(k).and_then(|v| v.as_str()))
                                        .collect::<Vec<_>>().join("|");
                                    groups.entry(key).or_default().push(row.clone());
                                }
                                let mut result: Vec<Value> = groups.into_iter().map(|(_, group_rows)| {
                                    let mut agg = crate::aegis::oltp::aggregation::apply_metrics_fbs(&group_rows, metrics);
                                    // Inyectar valores de dimensiones
                                    if let (Some(first), Some(obj)) = (group_rows.first(), agg.as_object_mut()) {
                                        for dk in &dim_keys {
                                            if let Some(v) = first.get(dk) {
                                                obj.insert(dk.clone(), v.clone());
                                            }
                                        }
                                    }
                                    agg
                                }).collect();
                                result.sort_by(|a, b| {
                                    let ka = a.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    let kb = b.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    kb.partial_cmp(&ka).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                result
                            } else {
                                vec![crate::aegis::oltp::aggregation::apply_metrics_fbs(&rows, metrics)]
                            }
                        } else {
                            vec![crate::aegis::oltp::aggregation::apply_metrics_fbs(&rows, metrics)]
                        }
                    }
                    2 /* TIMESERIES */ => {
                        // TIMESERIES: group-by (interval-bucket, dims) → apply-metrics
                        // [PORTED_FROM: apply-output-cast :TIMESERIES en executor.clj]
                        if let Some(dims) = &ast_ir.dimensions {
                            apply_timeseries_bucketing(&rows, metrics, dims)
                        } else {
                            rows
                        }
                    }
                    _ /* TABLE / CSV_EXPORT */ => rows,
                }
            } else {
                // Sin métricas → TABLE directo
                rows
            }
        } else {
            rows
        };

        // ── Ordering en memoria (multi-key, cortocircuito correcto) ──────────────
        // [PORTED_FROM: sort-oltp-result en sort.clj]
        let mut rows = rows;
        if let Some(sort_defs) = &ast_ir.sort {
            if !sort_defs.is_empty() {
                apply_sort_fbs(&mut rows, sort_defs);
            }
        }

        // ── Construir paginación real ─────────────────────────────────────────
        let pagination = crate::aegis::pagination::build_pagination(offset, page_limit, total_count);

        Ok(json!({
            "data":       rows,
            "total":      total_count,
            "pagination": pagination,
        }))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────


/// Extrae el ULID explícito de un nodo WHERE `["=", "entity/ulid", "<id>"]`
fn extract_ulid_from_ast(ast_ir: &Value) -> Option<String> {
    let where_node = ast_ir.get("where")?;
    search_ulid_in_node(where_node)
}

fn search_ulid_in_node(node: &Value) -> Option<String> {
    let arr = node.as_array()?;
    if arr.len() >= 3 {
        let op    = arr[0].as_str().unwrap_or("");
        let field = arr[1].as_str().unwrap_or("");
        if op == "=" && (field == "entity/ulid" || field == "entity/id") {
            return arr[2].as_str().map(|s| s.to_string());
        }
        if op == "and" || op == "or" {
            for child in arr.iter().skip(1) {
                if let Some(id) = search_ulid_in_node(child) {
                    return Some(id);
                }
            }
        }
    }
    None
}

fn extract_ulid_fbs_executor(ast_ir: &crate::janus::fbs::AnalyticsRequestT) -> Option<String> {
    if let Some(filters) = &ast_ir.filters {
        for f in filters {
            if let Some(crit) = &f.criteria {
                if crit.op_ref.0 == crate::janus::fbs::FilterOperator::EQ.0 && 
                   crit.field.as_deref() == Some("entity/ulid") {
                    return crit.value.as_ref().and_then(|v| v.string_val.clone());
                }
            }
        }
    }
    None
}

/// Extrae la lista de atributos del `select` del AST IR.
fn extract_select_attrs(ast_ir: &Value) -> Vec<String> {
    ast_ir.get("select")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default()
}

fn extract_select_attrs_fbs(ast_ir: &crate::janus::fbs::AnalyticsRequestT) -> Vec<String> {
    let mut attrs = Vec::new();
    if let Some(metrics) = &ast_ir.metrics {
        for m in metrics {
            if let Some(attr) = &m.attribute {
                attrs.push(attr.clone());
            }
            if let Some(sec) = &m.secondary_attribute {
                attrs.push(sec.clone());
            }
        }
    }
    if let Some(dims) = &ast_ir.dimensions {
        for d in dims {
            if let Some(attr) = &d.attribute {
                attrs.push(attr.clone());
            }
        }
    }
    attrs
}

/// Convierte un EntityMap (HashMap<String, DatomValue>) a un JSON Object plano.
fn entity_map_to_json(map: std::collections::HashMap<String, DatomValue>) -> Value {
    let mut row = serde_json::Map::new();
    for (k, v) in map {
        let json_val = match v {
            DatomValue::Str(s)         => json!(s),
            DatomValue::Uuid(s)        => json!(s),
            DatomValue::Long(n)        => json!(n),
            DatomValue::Instant(n)     => json!(n),
            DatomValue::Double(d)      => json!(d),
            DatomValue::Bool(b)        => json!(b),
            DatomValue::BigInt(n)      => json!(n),
            DatomValue::Ref(r)         => json!(r),
            DatomValue::Array(arr)     => json!(arr),
            DatomValue::Geo { lat, lon } => json!({"lat": lat, "lon": lon}),
            DatomValue::Null           => Value::Null,
            DatomValue::Bytes(_)       => json!("_binary_"),
        };
        row.insert(k, json_val);
    }
    Value::Object(row)
}

/// Ordena rows en memoria según la especificación `order_by` del AST IR.
/// Blueprint: §I.2 Paso 6 — "post-procesamiento: Sorting"
fn apply_sort_in_memory(rows: &mut Vec<Value>, order_by: &[Value]) {
    if order_by.is_empty() { return; }

    rows.sort_by(|a, b| {
        for spec in order_by {
            let field = spec.get("attribute").or(spec.get("field"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let dir = spec.get("direction").and_then(|v| v.as_str()).unwrap_or("ASC");

            let val_a = a.get(field).cloned().unwrap_or(Value::Null);
            let val_b = b.get(field).cloned().unwrap_or(Value::Null);

            let ord = compare_json_values(&val_a, &val_b);
            let ord = if dir.to_uppercase() == "DESC" { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => {
            let fa = na.as_f64().unwrap_or(0.0);
            let fb = nb.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(sa), Value::String(sb)) => sa.cmp(sb),
        _ => std::cmp::Ordering::Equal,
    }
}

// ── New helpers ported from Clojure ──────────────────────────────────────────

/// Normaliza timestamps ms→segundos en los rows.
///
/// [PORTED_FROM: normalize-timestamps-in-rows en executor.clj]
/// Regla: si el número > 1e11 → está en ms → dividir entre 1000.
/// Solo afecta TABLE/TREE/CSV_EXPORT — TIMESERIES normaliza internamente.
fn normalize_timestamps_in_rows(rows: &mut Vec<Value>) {
    const TS_FIELDS: &[&str] = &[
        "created_at", "updated_at", "deleted_at", "ingested_at",
        "timestamp", "meta/created_at",
    ];
    for row in rows.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            for field in TS_FIELDS {
                if let Some(Value::Number(n)) = obj.get(*field) {
                    if let Some(v) = n.as_f64() {
                        if v > 1e11 {
                            let secs = (v / 1000.0) as i64;
                            obj.insert((*field).to_string(), json!(secs));
                        }
                    }
                }
            }
        }
    }
}

/// Sort multi-key con cortocircuito correcto.
///
/// [PORTED_FROM: sort-oltp-result en sort.clj]
/// La versión anterior tenía un bug: `return` dentro del `for` no hace
/// cortocircuito en `sort_by` — el closure debe retornar el `Ordering` correcto.
fn apply_sort_fbs(rows: &mut Vec<Value>, sort_defs: &[crate::janus::fbs::SortDefinitionT]) {
    if sort_defs.is_empty() { return; }
    rows.sort_by(|a, b| {
        for spec in sort_defs {
            let field = spec.field.as_deref().unwrap_or("");
            let va = a.get(field).cloned().unwrap_or(Value::Null);
            let vb = b.get(field).cloned().unwrap_or(Value::Null);
            let ord = compare_json_values(&va, &vb);
            let ord = if spec.descending { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Trunca epoch-segundos al inicio del intervalo dado (UTC).
///
/// [PORTED_FROM: truncate-to-interval en executor.clj]
fn truncate_to_interval(epoch_secs: i64, interval: &str) -> i64 {
    match interval {
        "minute"  => (epoch_secs / 60) * 60,
        "hour"    => (epoch_secs / 3600) * 3600,
        "day"     => (epoch_secs / 86400) * 86400,
        "week"    => {
            // Truncar al lunes anterior (día 1 de la semana ISO)
            // epoch_secs=0 es jueves 1970-01-01; lunes -3 días
            let day_of_week = ((epoch_secs / 86400).rem_euclid(7) + 4).rem_euclid(7); // 0=lun
            let start_of_week = epoch_secs - day_of_week * 86400;
            (start_of_week / 86400) * 86400
        }
        "month"   => {
            // Primer día del mes UTC
            // Usar arithesis simple: días desde epoch, convertir a año/mes
            let days = epoch_secs / 86400;
            // Algoritmo civil_from_days (Richards)
            let z = days + 719468;
            let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
            let doe = z - era * 146097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let _doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let _mp  = (5 * _doy + 2) / 153;
            let _y   = yoe + era * 400 + if _mp >= 10 { 1 } else { 0 };
            let _m   = if _mp < 10 { _mp + 3 } else { _mp - 9 };
            // Primer día del mes → epoch
            let first_day = days_from_civil(_y, _m, 1);
            first_day * 86400
        }
        _         => epoch_secs, // quarter/year: sin truncado (fallback)
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let yy = if m <= 2 { y - 1 } else { y };
    let era = (if yy >= 0 { yy } else { yy - 399 }) / 400;
    let yoe = yy - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Bucketing TIMESERIES completo.
///
/// [PORTED_FROM: apply-output-cast :TIMESERIES en executor.clj]
/// Pipeline:
///   1. Detectar time_dim con interval
///   2. Normalizar ts ms→s
///   3. Truncar al bucket
///   4. Agrupar (bucket, other_dims)
///   5. apply_metrics_fbs por grupo
///   6. Ordenar por bucket ASC
fn apply_timeseries_bucketing(
    rows: &[Value],
    metrics: &[crate::janus::fbs::MetricDefinitionT],
    dimensions: &[crate::janus::fbs::DimensionDefinitionT],
) -> Vec<Value> {
    use std::collections::BTreeMap;

    // Encontrar la time_dim (la que tiene interval)
    let time_dim = dimensions.iter().find(|d| {
        d.interval.as_deref().map(|i| !i.is_empty()).unwrap_or(false)
    });
    let interval = time_dim.and_then(|d| d.interval.as_deref()).unwrap_or("day");
    let bucket_attr = time_dim.and_then(|d| d.attribute.as_deref()).unwrap_or("bucket");

    // Otras dims (sin interval)
    let other_dims: Vec<&str> = dimensions.iter()
        .filter(|d| d.interval.as_deref().map(|i| i.is_empty()).unwrap_or(true))
        .filter_map(|d| d.attribute.as_deref())
        .collect();

    // Agrupar por (bucket, other_dims): BTreeMap preserva orden bucket ASC
    let ts_candidates = &["created_at", "meta/created_at", "timestamp",
                          "updated_at", "ingested_at"];
    let mut groups: BTreeMap<(i64, Vec<String>), Vec<Value>> = BTreeMap::new();

    for row in rows {
        // Resolver campo timestamp
        let ts_raw = ts_candidates.iter()
            .find_map(|f| row.get(*f).and_then(|v| v.as_f64()))
            .unwrap_or(0.0);
        let ts_secs = if ts_raw > 1e11 { (ts_raw / 1000.0) as i64 } else { ts_raw as i64 };
        let bucket = truncate_to_interval(ts_secs, interval);

        let dim_vals: Vec<String> = other_dims.iter()
            .map(|k| row.get(*k).and_then(|v| v.as_str()).unwrap_or("").to_string())
            .collect();

        groups.entry((bucket, dim_vals)).or_default().push(row.clone());
    }

    // Agregar por grupo + inyectar bucket y dims
    groups.into_iter().map(|((bucket_val, dim_vals), group_rows)| {
        let mut agg = crate::aegis::oltp::aggregation::apply_metrics_fbs(&group_rows, metrics);
        if let Some(obj) = agg.as_object_mut() {
            obj.insert(bucket_attr.to_string(), json!(bucket_val));
            // Inyectar otras dimensiones del primer row
            for (k, v) in other_dims.iter().zip(&dim_vals) {
                obj.insert((*k).to_string(), json!(v));
            }
        }
        agg
    }).collect()
}
