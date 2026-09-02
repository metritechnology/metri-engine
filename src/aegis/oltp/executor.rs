// aegis/oltp/executor.rs — Ejecutor que coordina EAV y Aegis.
// Orquesta: compile → execute_native_plan → hydrate (con cache + streaming filters) → sort → output cast.

use serde_json::{json, Value};
use tracing::{debug, info, warn};

use crate::aegis::oltp::compiler::compile_native_plan;
use crate::aegis::oltp::hydrator::{entity_map_to_json, OltpEntityHydrator};
use crate::aegis::temporal_bridge::{no_time_range, resolve_fbs_time_frame};
use crate::domain::errors::DomainError;
use crate::eav::reader::pull::EavReader;
use crate::eav::reader::query::EavQueryExecutor;

/// Retoque de última hora sobre las filas ya hidratadas, antes de devolverlas.
///
/// Existe para las entidades cuyo valor de verdad NO vive en el log de datoms.
/// El caso que lo motiva es `domain_quota.current_usage`: el consumo real lo
/// lleva un contador atómico fuera del log —ver `quota/ledger.rs`—, así que la
/// alternativa a superponerlo en lectura era escribir una transacción EAV
/// completa en cada débito solo para mantener una copia legible al día. Se
/// pagaba en el camino caliente y, aun así, la copia iba por detrás.
///
/// No devuelve error a propósito: un retoque que falla deja el valor
/// almacenado, que es viejo pero no falso. Tumbar la consulta de un usuario
/// porque un contador no responde sería peor que enseñarle una cifra atrasada.
#[async_trait::async_trait]
pub trait RowOverlay: Send + Sync {
    /// Entidad cuyas filas retoca.
    fn entity(&self) -> &str;
    async fn apply(&self, tenant_id: &str, rows: &mut [Value]);
}

#[derive(Clone)]
pub struct OltpExecutor {
    query_executor: EavQueryExecutor,
    pull_reader: EavReader,
    overlays: Vec<std::sync::Arc<dyn RowOverlay>>,
}

impl OltpExecutor {
    pub fn new(query_executor: EavQueryExecutor, pull_reader: EavReader) -> Self {
        Self {
            query_executor,
            pull_reader,
            overlays: Vec::new(),
        }
    }

    /// Registra un retoque de lectura. Se compone en el arranque.
    pub fn with_overlay(mut self, overlay: std::sync::Arc<dyn RowOverlay>) -> Self {
        self.overlays.push(overlay);
        self
    }

    /// Aplica los retoques que correspondan a esta entidad.
    async fn apply_overlays(&self, tenant_id: &str, entity_type: &str, rows: &mut [Value]) {
        if rows.is_empty() {
            return;
        }
        for overlay in &self.overlays {
            if overlay.entity() == entity_type {
                overlay.apply(tenant_id, rows).await;
            }
        }
    }

    /// Ejecutor de consultas EAV. Lo necesita ListEntities para resolver un
    /// AvetIntersect sin pasar por el compilador analitico.
    pub fn query_executor(&self) -> &EavQueryExecutor {
        &self.query_executor
    }

    pub fn pull_reader(&self) -> &EavReader {
        &self.pull_reader
    }

    fn resolve_row_relations<'a>(
        &'a self,
        tenant_id: &'a str,
        entity_type: &'a str,
        row: &'a mut serde_json::Value,
        select_tree: &'a serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DomainError>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut select_obj = match select_tree.as_object() {
                Some(obj) => obj,
                None => return Ok(()),
            };

            // Unpack gRPC's recursive wrapping under the "fields" key if present
            if let Some(inner) = select_obj.get("fields").and_then(|v| v.as_object()) {
                select_obj = inner;
            }

            let row_obj = match row.as_object_mut() {
                Some(obj) => obj,
                None => return Ok(()),
            };

            let registry = crate::codice::global();
            let model = match registry.get_model(entity_type) {
                Some(m) => m,
                None => return Ok(()),
            };

            for (field_name, sub_tree_val) in select_obj {
                // If it's a nested selection (an object), it means we want to resolve a relationship
                if let Some(_sub_tree_obj) = sub_tree_val.as_object() {
                    // Check if this field_name is a reference attribute in our model
                    let attr_opt = model.attributes.iter().find(|a| {
                        a.name == *field_name
                            || a.name.split('/').next_back() == Some(field_name.as_str())
                    });

                    if let Some(attr) = attr_opt {
                        if let Some(ref_entity_type) = &attr.entity_ref {
                            // Find all keys matching in the row (bare and namespaced)
                            let mut keys_to_replace = Vec::new();
                            if row_obj.contains_key(field_name) {
                                keys_to_replace.push(field_name.clone());
                            }
                            let namespaced = format!("{}/{field_name}", entity_type);
                            if row_obj.contains_key(&namespaced) {
                                keys_to_replace.push(namespaced);
                            }

                            if !keys_to_replace.is_empty() {
                                let first_key = &keys_to_replace[0];
                                if let Some(ref_id) =
                                    row_obj.get(first_key).and_then(|v| v.as_str())
                                {
                                    if !ref_id.is_empty() {
                                        // Pull the referenced entity
                                        match self.pull_reader.pull(tenant_id, ref_id, None).await {
                                            Ok(mut map) if !map.is_empty() => {
                                                map.insert(
                                                    "id".to_string(),
                                                    crate::eav::types::datom::DatomValue::Str(
                                                        ref_id.to_string(),
                                                    ),
                                                );
                                                let mut hydrated = entity_map_to_json(map);
                                                // Recurse to resolve nested relationships in the target entity
                                                self.resolve_row_relations(
                                                    tenant_id,
                                                    ref_entity_type,
                                                    &mut hydrated,
                                                    sub_tree_val,
                                                )
                                                .await?;

                                                // Replace in the row
                                                for fkey in keys_to_replace {
                                                    row_obj.insert(fkey.clone(), hydrated.clone());
                                                    if fkey.ends_with("_id") {
                                                        let alias = fkey
                                                            .trim_end_matches("_id")
                                                            .to_string();
                                                        row_obj.insert(alias, hydrated.clone());
                                                    }
                                                }
                                            }
                                            _ => {
                                                // If pull failed or is empty, fallback to basic object with id
                                                let dummy = serde_json::json!({ "id": ref_id });
                                                for fkey in keys_to_replace {
                                                    row_obj.insert(fkey.clone(), dummy.clone());
                                                    if fkey.ends_with("_id") {
                                                        let alias = fkey
                                                            .trim_end_matches("_id")
                                                            .to_string();
                                                        row_obj.insert(alias, dummy.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else if let Some(ref_ids) =
                                    row_obj.get(first_key).and_then(|v| v.as_array())
                                {
                                    let mut resolved_arr = Vec::new();
                                    for ref_val in ref_ids {
                                        if let Some(ref_id) = ref_val.as_str() {
                                            if !ref_id.is_empty() {
                                                match self
                                                    .pull_reader
                                                    .pull(tenant_id, ref_id, None)
                                                    .await
                                                {
                                                    Ok(mut map) if !map.is_empty() => {
                                                        map.insert("id".to_string(), crate::eav::types::datom::DatomValue::Str(ref_id.to_string()));
                                                        let mut hydrated = entity_map_to_json(map);
                                                        self.resolve_row_relations(
                                                            tenant_id,
                                                            ref_entity_type,
                                                            &mut hydrated,
                                                            sub_tree_val,
                                                        )
                                                        .await?;
                                                        resolved_arr.push(hydrated);
                                                    }
                                                    _ => {
                                                        resolved_arr.push(
                                                            serde_json::json!({ "id": ref_id }),
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    for fkey in keys_to_replace {
                                        row_obj.insert(
                                            fkey.clone(),
                                            Value::Array(resolved_arr.clone()),
                                        );
                                        if fkey.ends_with("_id") {
                                            let alias = fkey.trim_end_matches("_id").to_string();
                                            row_obj
                                                .insert(alias, Value::Array(resolved_arr.clone()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }

    /// Flujo principal moderno: PlanSelector → execute_native_plan → pull → sort.
    pub async fn run_oltp_query(
        &self,
        tenant_id: &str,
        ast_ir: &Value,
    ) -> Result<Value, DomainError> {
        let entity_type = ast_ir
            .get("entity")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let master_tenant_id =
            std::env::var("METRI_MASTER_TENANT_ID").unwrap_or_else(|_| "system".to_string());
        let target_tenant = if entity_type == "tenant" {
            &master_tenant_id
        } else {
            tenant_id
        };

        info!(
            "[Aegis OLTP] Iniciando query para tenant: {}",
            target_tenant
        );

        let limit = ast_ir.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

        // ── EavReader::history: path especial para Time-Travel ────────────────
        if let Some(stree) = ast_ir
            .get("select_tree")
            .or_else(|| ast_ir.get("select-tree"))
        {
            if stree
                .get("_history")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                if let Some(entity_id) = stree.get("id").and_then(|v| v.as_str()) {
                    debug!("[Aegis OLTP] History timeline query for entity_id={entity_id}");
                    let entries = self
                        .pull_reader
                        .history(target_tenant, entity_id, None)
                        .await?;
                    let rows: Vec<Value> = entries.into_iter().map(|entry| {
                        json!({
                            "attr_name": entry.attr_name,
                            "value": match entry.value {
                                Some(crate::eav::types::datom::DatomValue::Str(s)) => json!(s),
                                Some(crate::eav::types::datom::DatomValue::Bool(b)) => json!(b),
                                Some(crate::eav::types::datom::DatomValue::Long(l)) => json!(l),
                                Some(crate::eav::types::datom::DatomValue::Double(d)) => json!(d),
                                Some(crate::eav::types::datom::DatomValue::Uuid(u)) => json!(u),
                                Some(crate::eav::types::datom::DatomValue::Array(a)) => json!(a),
                                Some(crate::eav::types::datom::DatomValue::Ref(r)) => json!(r.to_string()),
                                Some(crate::eav::types::datom::DatomValue::Instant(i)) => json!(i),
                                _ => Value::Null,
                            },
                            "tx_id": entry.tx_id,
                            "op": entry.op,
                            "user_id": Value::Null,
                            "timestamp": entry.tx_id,
                        })
                    }).collect();

                    return Ok(json!({
                        "data": rows,
                        "total": rows.len(),
                        "pagination": Value::Null,
                    }));
                }
            }
        }

        // ── AsOfSnapshot: path especial ───────────────────────────────────────
        if let Some(as_of_tx) = ast_ir
            .get("as_of_tx")
            .and_then(|v| v.as_u64())
            .filter(|&t| t > 0)
        {
            if let Some(entity_id) = extract_ulid_from_ast(ast_ir) {
                debug!("[Aegis OLTP] AsOfSnapshot entity_id={entity_id} as_of_tx={as_of_tx}");
                let select = extract_select_attrs(ast_ir);
                let sel_ref: Vec<&str> = select.iter().map(|s| s.as_str()).collect();
                let map = self
                    .pull_reader
                    .pull_as_of(
                        target_tenant,
                        &entity_id,
                        as_of_tx,
                        if sel_ref.is_empty() {
                            None
                        } else {
                            Some(&sel_ref)
                        },
                    )
                    .await?;
                let mut rows = vec![entity_map_to_json(map)];
                self.apply_overlays(target_tenant, entity_type, &mut rows)
                    .await;
                return Ok(Value::Array(rows));
            }
        }

        // ── Plan normal vía PlanSelector ──────────────────────────────────────
        let native_plan = compile_native_plan(ast_ir, target_tenant);
        debug!("[Aegis OLTP] Plan: {:?}", native_plan);

        let mut entity_ids = self
            .query_executor
            .execute_native_plan(&native_plan)
            .await?;

        // Aplicar limit al resultado del executor
        entity_ids.truncate(limit);
        debug!("[Aegis OLTP] {} entity_ids obtenidos", entity_ids.len());

        // ── Pull hidratación ──────────────────────────────────────────────────
        let select = extract_select_attrs(ast_ir);
        let sel_ref: Vec<&str> = select.iter().map(|s| s.as_str()).collect();

        let select_extended;
        let sel_ref_ext;
        let sel_opt = if sel_ref.is_empty() || sel_ref[0] == "*" {
            None
        } else {
            let mut ext = select.clone();
            ext.push("entity/type".to_string());
            ext.push("entity_type".to_string());
            select_extended = ext;
            sel_ref_ext = select_extended
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>();
            Some(sel_ref_ext.as_slice())
        };

        // FASE 1: pull secuencial
        let mut rows = Vec::with_capacity(entity_ids.len());
        for entity_id in &entity_ids {
            match self
                .pull_reader
                .pull(target_tenant, entity_id, sel_opt)
                .await
            {
                Ok(mut map) if !map.is_empty() => {
                    map.insert(
                        "id".to_string(),
                        crate::eav::types::datom::DatomValue::Str(entity_id.clone()),
                    );
                    let row = entity_map_to_json(map);
                    let row_entity_type = row
                        .get("entity/type")
                        .or_else(|| row.get("entity_type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if entity_type.is_empty() || row_entity_type == entity_type {
                        let mut final_row = row;
                        if !sel_ref.is_empty() && sel_ref[0] != "*" {
                            if !select.iter().any(|s| s == "entity/type") {
                                if let Some(obj) = final_row.as_object_mut() {
                                    obj.remove("entity/type");
                                }
                            }
                            if !select.iter().any(|s| s == "entity_type") {
                                if let Some(obj) = final_row.as_object_mut() {
                                    obj.remove("entity_type");
                                }
                            }
                        }
                        rows.push(final_row);
                    }
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

        // ── Resolver relaciones según select_tree ──────────────────────────────
        if let Some(stree) = ast_ir
            .get("select_tree")
            .or_else(|| ast_ir.get("select-tree"))
        {
            for row in &mut rows {
                self.resolve_row_relations(target_tenant, entity_type, row, stree)
                    .await?;
            }
        }

        self.apply_overlays(target_tenant, entity_type, &mut rows)
            .await;

        Ok(Value::Array(rows))
    }

    pub async fn run_oltp_query_fbs(
        &self,
        tenant_id: &str,
        ast_ir: &crate::janus::fbs::AnalyticsRequestT,
    ) -> Result<Value, DomainError> {
        let entity_type = ast_ir.entity.as_deref().unwrap_or("unknown");
        let master_tenant_id =
            std::env::var("METRI_MASTER_TENANT_ID").unwrap_or_else(|_| "system".to_string());
        let target_tenant = if entity_type == "tenant" {
            &master_tenant_id
        } else {
            tenant_id
        };

        let mut ast_ir_mut = ast_ir.clone();

        // Enriquecer select_tree dinámicamente analizando las dimensiones que contienen label_template
        let mut dynamic_select = if let Some(stree_str) = &ast_ir_mut.select_tree {
            serde_json::from_str::<serde_json::Value>(stree_str)
                .unwrap_or_else(|_| serde_json::json!({}))
        } else {
            serde_json::json!({})
        };

        let mut has_dynamic_relations = false;
        if let Some(dims) = &ast_ir_mut.dimensions {
            let registry = crate::codice::global();
            if let Some(model) = registry.get_model(entity_type) {
                for d in dims {
                    if let Some(lt) = &d.label_template {
                        for field in crate::aegis::label_template::extract_fields(lt) {
                            let base_ref = field.split('.').next().unwrap_or(&field);
                            // Buscar un atributo tipo reference en el modelo
                            let ref_attr_opt = model.attributes.iter().find(|a| {
                                a.attr_type == crate::codice::registry::AttrType::Reference
                                    && (a.name == *base_ref
                                        || a.name.split('/').next_back() == Some(base_ref)
                                        || a.entity_ref.as_deref() == Some(base_ref))
                            });
                            if let Some(ref_attr) = ref_attr_opt {
                                let attr_name = ref_attr
                                    .name
                                    .split('/')
                                    .next_back()
                                    .unwrap_or(&ref_attr.name)
                                    .to_string();
                                if let Some(obj) = dynamic_select.as_object_mut() {
                                    let fields_obj = if let Some(inner) =
                                        obj.get_mut("fields").and_then(|v| v.as_object_mut())
                                    {
                                        inner
                                    } else {
                                        obj
                                    };
                                    fields_obj
                                        .entry(attr_name)
                                        .or_insert_with(|| serde_json::json!({}));
                                    has_dynamic_relations = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        if has_dynamic_relations {
            ast_ir_mut.select_tree = Some(dynamic_select.to_string());
        }
        if let Some(filters) = &mut ast_ir_mut.filters {
            let registry = crate::codice::global();
            for filter in filters {
                self.resolve_dotpath_filters(filter, entity_type, target_tenant, registry)
                    .await?;
            }
        }
        let ast_ir = &ast_ir_mut;

        info!(
            "[Aegis OLTP FBS] Iniciando query para tenant: {}",
            target_tenant
        );
        println!("[DEBUG FBS] filters={:?}", ast_ir.filters);

        // ── Determinar si es una query analítica/agregación ───────────────────
        let output_cast = ast_ir.output_cast.0;
        let is_analytical = matches!(
            output_cast,
            1 /* KPI */ | 2 /* TIMESERIES */ | 4 /* PIE */ | 5 /* BUBBLE */
        ) || (ast_ir.metrics.as_ref().map(|m| !m.is_empty()).unwrap_or(false) && output_cast != 3 /* TABLE */ && output_cast != 6/* CSV_EXPORT */);

        let limit_raw = ast_ir.limit as usize;
        let effective_limit = if limit_raw == 0 {
            if is_analytical {
                100_000
            } else {
                100
            }
        } else {
            limit_raw
        };

        // ── Resolver TimeFrame via temporal canónico ───────────────────────────
        let time_range = ast_ir
            .time_frame
            .as_ref()
            .and_then(|tf| resolve_fbs_time_frame(tf))
            .unwrap_or_else(no_time_range);

        debug!(
            "[Aegis OLTP FBS] TimeRange resuelto → start_ts={:?} end_ts={:?}",
            time_range.start_ts, time_range.end_ts
        );

        // ── EavReader::history: path especial para Time-Travel ────────────────
        if let Some(stree) = ast_ir
            .select_tree
            .as_ref()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
        {
            if stree
                .get("_history")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                if let Some(entity_id) = stree.get("id").and_then(|v| v.as_str()) {
                    debug!("[Aegis OLTP FBS] History timeline query for entity_id={entity_id}");
                    let entries = self
                        .pull_reader
                        .history(target_tenant, entity_id, None)
                        .await?;
                    let rows: Vec<Value> = entries.into_iter().map(|entry| {
                        json!({
                            "attr_name": entry.attr_name,
                            "value": match entry.value {
                                Some(crate::eav::types::datom::DatomValue::Str(s)) => json!(s),
                                Some(crate::eav::types::datom::DatomValue::Bool(b)) => json!(b),
                                Some(crate::eav::types::datom::DatomValue::Long(l)) => json!(l),
                                Some(crate::eav::types::datom::DatomValue::Double(d)) => json!(d),
                                Some(crate::eav::types::datom::DatomValue::Uuid(u)) => json!(u),
                                Some(crate::eav::types::datom::DatomValue::Array(a)) => json!(a),
                                Some(crate::eav::types::datom::DatomValue::Ref(r)) => json!(r.to_string()),
                                Some(crate::eav::types::datom::DatomValue::Instant(i)) => json!(i),
                                _ => Value::Null,
                            },
                            "tx_id": entry.tx_id,
                            "op": entry.op,
                            "user_id": Value::Null,
                            "timestamp": entry.tx_id,
                        })
                    }).collect();

                    return Ok(json!({
                        "data": rows,
                        "total": rows.len(),
                        "pagination": Value::Null,
                    }));
                }
            }
        }

        // ── AsOfSnapshot: path especial ───────────────────────────────────────
        let is_custom_range = ast_ir
            .time_frame
            .as_ref()
            .map(|tf| tf.type_.0 == 1 /* CUSTOM_RANGE */)
            .unwrap_or(false);

        if is_custom_range {
            if let (Some(end_ts), Some(entity_id)) =
                (time_range.end_ts, extract_ulid_fbs_executor(ast_ir))
            {
                let as_of_tx = end_ts as u64;
                debug!("[Aegis OLTP FBS] AsOfSnapshot entity_id={entity_id} as_of_tx={as_of_tx}");
                let select = extract_select_attrs_fbs(ast_ir);
                let sel_ref: Vec<&str> = select.iter().map(|s| s.as_str()).collect();
                let mut map = self
                    .pull_reader
                    .pull_as_of(
                        target_tenant,
                        &entity_id,
                        as_of_tx,
                        if sel_ref.is_empty() {
                            None
                        } else {
                            Some(&sel_ref)
                        },
                    )
                    .await?;
                map.insert(
                    "id".to_string(),
                    crate::eav::types::datom::DatomValue::Str(entity_id.clone()),
                );
                let mut rows = vec![entity_map_to_json(map)];
                self.apply_overlays(target_tenant, entity_type, &mut rows)
                    .await;
                return Ok(Value::Array(rows));
            }
        }

        // ── Plan normal vía PlanSelector FBS ──────────────────────────────────
        let native_plan =
            crate::aegis::oltp::compiler::compile_native_plan_fbs(ast_ir, target_tenant);
        debug!("[Aegis OLTP FBS] Plan: {:?}", native_plan);

        // ── Cursor-based pagination: decodificar cursor ───────────────────────
        let cursor_str = ast_ir.cursor.as_deref();
        let (offset, page_limit) =
            crate::aegis::pagination::decode_cursor(cursor_str, effective_limit);

        // Para AevtScan: obtenemos TODOS los entity_ids (para total real).
        let all_entity_ids = self
            .query_executor
            .execute_native_plan(&native_plan)
            .await?;

        let mut total_count = all_entity_ids.len();
        info!(
            "[Aegis OLTP FBS] {} entity_ids totales (offset={} limit={})",
            total_count, offset, page_limit
        );

        // ── Extracción y resolución de atributos requeridos ──────────────────
        let mut raw_attrs = std::collections::HashSet::new();

        // Extraer de metrics
        if let Some(metrics) = &ast_ir.metrics {
            for m in metrics {
                if let Some(attr) = &m.attribute {
                    raw_attrs.insert(attr.clone());
                }
                if let Some(sec) = &m.secondary_attribute {
                    raw_attrs.insert(sec.clone());
                }
            }
        }

        // Extraer de dimensions
        if let Some(dims) = &ast_ir.dimensions {
            for d in dims {
                if let Some(attr) = &d.attribute {
                    raw_attrs.insert(attr.clone());
                }
            }
        }

        // Extraer de filters
        if let Some(filters) = &ast_ir.filters {
            for f in filters {
                extract_attrs_from_filter_node(f, &mut raw_attrs);
            }
        }

        // Extraer de sort
        if let Some(sort_defs) = &ast_ir.sort {
            for spec in sort_defs {
                if let Some(field) = &spec.field {
                    raw_attrs.insert(field.clone());
                }
            }
        }

        // Extraer de hierarchy
        if let Some(h) = &ast_ir.hierarchy {
            if let Some(pf) = &h.parent_field {
                raw_attrs.insert(pf.clone());
            }
        }

        // Extraer de select_tree
        if let Some(stree_str) = &ast_ir.select_tree {
            if let Ok(stree) = serde_json::from_str::<Value>(stree_str) {
                if let Some(obj) = stree.as_object() {
                    let mut select_obj = obj;
                    if let Some(inner) = obj.get("fields").and_then(|v| v.as_object()) {
                        select_obj = inner;
                    }
                    for key in select_obj.keys() {
                        raw_attrs.insert(key.clone());
                    }
                }
            }
        }

        // Mapear/Resolver nombres con namespace usando el global CodeRegistry
        let registry = crate::codice::global();
        let mut resolved_attrs = std::collections::HashSet::new();
        let infrastructure_fields = [
            "tenant_id",
            "entity_type",
            "entity/type",
            "entity/ulid",
            "id",
        ];

        for raw_attr in raw_attrs {
            if infrastructure_fields.contains(&raw_attr.as_str()) {
                continue;
            }
            let mapped_attr = match raw_attr.as_str() {
                "created_at" | "createdAt" => "meta/created_at".to_string(),
                "updated_at" | "updatedAt" => "meta/updated_at".to_string(),
                other => other.to_string(),
            };

            let mut resolved = false;
            if let Some(attrs) = registry.get_attributes(entity_type) {
                for attr in attrs {
                    if attr.name == mapped_attr {
                        resolved_attrs.insert(attr.name.clone());
                        resolved = true;
                        break;
                    }
                    if let Some(last_part) = attr.name.split('/').next_back() {
                        if last_part == mapped_attr {
                            resolved_attrs.insert(attr.name.clone());
                            resolved = true;
                            break;
                        }
                    }
                }
            }
            if !resolved {
                resolved_attrs.insert(mapped_attr);
            }
        }

        // ── Calentamiento de Caché Incremental y Concurrencia de Hydration ─────
        let hydrator = OltpEntityHydrator::new(self.pull_reader.clone());
        hydrator
            .warm_cache_incremental(
                target_tenant,
                entity_type,
                &all_entity_ids,
                &resolved_attrs,
                is_analytical,
            )
            .await;

        // Siempre None: la agregación necesita todos los campos y el pull sin
        // filtro selectivo es la vía compatible. La rama selectiva para lecturas
        // analíticas quedó como intención futura, no como código.
        let sel_opt: Option<&[&str]> = None;

        // Extraer filtros de negocio
        let business_filters: Vec<_> = ast_ir
            .filters
            .as_ref()
            .map(|filters| {
                filters
                    .iter()
                    .filter(|f| {
                        if let Some(crit) = &f.criteria {
                            let field = crit.field.as_deref().unwrap_or("");
                            !infrastructure_fields.contains(&field)
                        } else {
                            true
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let has_time_range = time_range.start_ts.is_some() || time_range.end_ts.is_some();
        let has_filters = !business_filters.is_empty()
            || ast_ir.hierarchy.is_some()
            || ast_ir
                .search
                .as_ref()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
            || has_time_range;

        let target_matches = offset + page_limit;
        let has_sort = ast_ir.sort.as_ref().map(|s| !s.is_empty()).unwrap_or(false);
        let needs_large_scan = has_filters || has_sort;

        let mut sliced_entity_ids = all_entity_ids.clone();
        let mut is_pre_sliced = false;

        if !is_analytical
            && !needs_large_scan
            && (output_cast == 3 /* TABLE */ || output_cast == 0 /* UNSPECIFIED */ || output_cast == 6/* CSV_EXPORT */)
        {
            let start = std::cmp::min(offset, all_entity_ids.len());
            let end = std::cmp::min(offset + page_limit, all_entity_ids.len());
            sliced_entity_ids = all_entity_ids[start..end].to_vec();
            is_pre_sliced = true;
            tracing::debug!(
                "Pre-sliced entity_ids before pull to range {}..{} (count={})",
                start,
                end,
                sliced_entity_ids.len()
            );
        }

        let max_hydrations = if is_analytical {
            100_000
        } else if needs_large_scan {
            std::cmp::max(target_matches * 5, 10_000)
        } else {
            std::cmp::max(target_matches * 5, 250)
        };

        // ── Resolve parent location bare name for hierarchy ───────────────────
        let hierarchy_parent_field_bare: Option<String> = ast_ir
            .hierarchy
            .as_ref()
            .and_then(|h| h.parent_field.as_deref())
            .map(|pf| pf.split('/').next_back().unwrap_or(pf).to_string());

        let should_break_early = !has_sort && !has_filters && !is_analytical;
        let target_break = if is_pre_sliced {
            page_limit
        } else {
            target_matches
        };

        // Copias requeridas para moverlas a la clausura del filtro en streaming
        let hierarchy_cloned = ast_ir.hierarchy.clone();
        let search_cloned = ast_ir.search.clone();
        let entity_cloned = ast_ir.entity.clone();
        let hierarchy_parent_field_bare_cloned = hierarchy_parent_field_bare.clone();

        let candidates_to_hydrate = if is_pre_sliced {
            &sliced_entity_ids
        } else {
            &all_entity_ids
        };

        // Orquestar fetch y filtro en streaming
        let mut matching_rows = hydrator
            .hydrate_entities(
                target_tenant,
                candidates_to_hydrate,
                &resolved_attrs,
                is_analytical,
                sel_opt,
                max_hydrations,
                target_break,
                should_break_early,
                move |row| {
                    // ── Strict Entity Type Filter ────────────────────
                    let row_entity_type = row
                        .get("entity/type")
                        .or_else(|| row.get("entity_type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let target_entity = entity_cloned.as_deref().unwrap_or("");
                    if !target_entity.is_empty() && row_entity_type != target_entity {
                        return false;
                    }

                    // ── Business Filters (in-memory) ─────────────────
                    let passes_business = business_filters.is_empty()
                        || business_filters
                            .iter()
                            .all(|f| crate::aegis::oltp::filter::eval_filter_node(row, f));

                    // ── Hierarchical Filter ──────────────────────────
                    let mut passes_hierarchy = true;
                    if let Some(h) = &hierarchy_cloned {
                        if let Some(pf) = &h.parent_field {
                            let bare_pf =
                                hierarchy_parent_field_bare_cloned.as_deref().unwrap_or(pf);
                            let val = row.get(pf.as_str()).or_else(|| row.get(bare_pf));
                            let node_id = h.current_node_id.as_deref().unwrap_or("");
                            let has_search = search_cloned
                                .as_ref()
                                .map(|s| !s.is_empty())
                                .unwrap_or(false);
                            if has_search && (node_id == "__none__" || node_id.is_empty()) {
                                passes_hierarchy = true;
                            } else if node_id == "__none__" || node_id.is_empty() {
                                passes_hierarchy = match val {
                                    None => true,
                                    Some(v) => {
                                        v.is_null()
                                            || v.as_str() == Some("")
                                            || v.as_str() == Some("null")
                                    }
                                };
                            } else {
                                passes_hierarchy = val
                                    .and_then(|v| v.as_str())
                                    .map(|s| s == node_id)
                                    .unwrap_or(false);
                            }
                        }
                    }

                    // ── Omni-Search Fuzzy Filter ─────────────────────
                    let mut passes_search = true;
                    if let Some(term) = &search_cloned {
                        if !term.is_empty() {
                            passes_search = false;
                            let entity_name = entity_cloned.as_deref().unwrap_or("");

                            let mut checked = false;
                            if let Some(model) =
                                crate::codice::registry::global().get_model(entity_name)
                            {
                                if !model.fts_fields.is_empty() {
                                    for f in &model.fts_fields {
                                        let bare_f = f.split('/').next_back().unwrap_or(f);
                                        let val_opt = row.get(f).or_else(|| row.get(bare_f));

                                        if let Some(val) = val_opt.and_then(|v| v.as_str()) {
                                            if crate::aegis::oltp::fuzzy::fuzzy_match(val, term) {
                                                passes_search = true;
                                                checked = true;
                                                break;
                                            }
                                        }
                                    }
                                    if !passes_search {
                                        checked = true;
                                    }
                                }
                            }

                            if !checked {
                                if let Some(obj) = row.as_object() {
                                    for (_k, v) in obj {
                                        if let Some(val_str) = v.as_str() {
                                            if crate::aegis::oltp::fuzzy::fuzzy_match(val_str, term)
                                            {
                                                passes_search = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    passes_business && passes_hierarchy && passes_search
                },
            )
            .await?;

        // ── inject_has_children (Modo 2 EAV string) - Deshabilitado en favor del lookup real de base de datos

        // ── Normalize timestamps: ms → seconds ───────────────────────────────────
        if !matches!(output_cast, 2 /* TIMESERIES */) {
            normalize_timestamps_in_rows(&mut matching_rows);
        }

        // Conservar copia de todas las filas que pasaron filtros para comparaciones analíticas
        let all_rows_for_comparison = matching_rows.clone();

        // ── In-Memory TimeFrame Filtering (Primary Window) ───────────────────
        if has_time_range {
            const TS_CANDIDATES: &[&str] = &[
                "created_at",
                "meta/created_at",
                "timestamp",
                "updated_at",
                "ingested_at",
            ];
            matching_rows.retain(|row| {
                let ts_raw = TS_CANDIDATES
                    .iter()
                    .find_map(|f| row.get(*f).and_then(|v| v.as_f64()))
                    .unwrap_or(0.0);
                let ts_secs = if ts_raw > 1e11 {
                    (ts_raw / 1000.0) as i64
                } else {
                    ts_raw as i64
                };

                let passes_start = time_range
                    .start_ts
                    .map(|start| ts_secs >= start)
                    .unwrap_or(true);
                let passes_end = time_range.end_ts.map(|end| ts_secs <= end).unwrap_or(true);
                passes_start && passes_end
            });
        }

        if !is_pre_sliced {
            total_count = matching_rows.len();
        }

        // ── Ordering en memoria ──────────────────────────────────────────────
        if let Some(sort_defs) = &ast_ir.sort {
            if !sort_defs.is_empty() {
                apply_sort_fbs(&mut matching_rows, sort_defs);
            }
        }

        // Aplicar el offset y limit (paginación en memoria)
        let rows: Vec<Value> = if output_cast == 3 /* TABLE */ || output_cast == 6 /* CSV_EXPORT */ || output_cast == 0
        /* UNSPECIFIED */
        {
            if is_pre_sliced {
                matching_rows.into_iter().take(page_limit).collect()
            } else {
                matching_rows
                    .into_iter()
                    .skip(offset)
                    .take(page_limit)
                    .collect()
            }
        } else {
            matching_rows
        };

        info!(
            "[Aegis OLTP FBS] {} rows después de filtros y paginación",
            rows.len()
        );

        // ── OutputCast aggregation con apply_output_cast_fbs ──────────────────
        let mut rows = crate::aegis::oltp::caster::apply_output_cast_fbs(
            &rows,
            &all_rows_for_comparison,
            ast_ir,
            &time_range,
        );

        // ── Resolver relaciones según select_tree ──────────────────────────────
        if let Some(stree_str) = &ast_ir.select_tree {
            if let Ok(stree) = serde_json::from_str::<Value>(stree_str) {
                for row in &mut rows {
                    self.resolve_row_relations(target_tenant, entity_type, row, &stree)
                        .await?;
                }
            }
        }

        // ── inject_has_children real de base de datos ────────────────────────────
        if let Some(h) = &ast_ir.hierarchy {
            if h.inject_has_children {
                if let Some(pf) = &h.parent_field {
                    let registry = crate::codice::global();
                    let mut resolved_parent_field = pf.clone();
                    if let Some(attrs) = registry.get_attributes(entity_type) {
                        for attr in attrs {
                            if &attr.name == pf {
                                resolved_parent_field = attr.name.clone();
                                break;
                            }
                            if let Some(last_part) = attr.name.split('/').next_back() {
                                if last_part == pf {
                                    resolved_parent_field = attr.name.clone();
                                    break;
                                }
                            }
                        }
                    }

                    let mut join_set = tokio::task::JoinSet::new();
                    for row in &rows {
                        let id = row
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let field_clone = resolved_parent_field.clone();
                        let tenant_clone = target_tenant.to_string();
                        let executor = self.query_executor.clone();

                        join_set.spawn(async move {
                            if id.is_empty() {
                                return (id, false);
                            }
                            // GSI-AVET lookup for children via execute_native_plan
                            let plan = crate::eav::reader::query::NativeQueryPlan::AvetSingle {
                                tenant_id: tenant_clone,
                                attr_name: field_clone,
                                value: crate::eav::types::datom::DatomValue::Str(id.clone()),
                            };
                            match executor.execute_native_plan(&plan).await {
                                Ok(child_ids) => {
                                    let ids: Vec<String> = child_ids;
                                    (id, !ids.is_empty())
                                }
                                Err(_) => (id, false),
                            }
                        });
                    }

                    let mut has_children_map: std::collections::HashMap<String, bool> =
                        std::collections::HashMap::new();
                    while let Some(res) = join_set.join_next().await {
                        if let Ok((id, has_kids)) = res {
                            has_children_map.insert(id, has_kids);
                        }
                    }

                    for row in &mut rows {
                        if let Some(obj) = row.as_object_mut() {
                            let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("");
                            let has_kids = has_children_map.get(id).copied().unwrap_or(false);
                            obj.insert("has_children".to_string(), json!(has_kids));
                        }
                    }
                }
            }
        }

        self.apply_overlays(target_tenant, entity_type, &mut rows)
            .await;

        // ── Construir paginación real ─────────────────────────────────────────
        let pagination =
            crate::aegis::pagination::build_pagination(offset, page_limit, total_count);

        Ok(json!({
            "data":       rows,
            "total":      total_count,
            "pagination": pagination,
        }))
    }

    fn resolve_dotpath_filters<'a>(
        &'a self,
        node: &'a mut crate::janus::fbs::FilterNodeT,
        entity_type: &'a str,
        tenant_id: &'a str,
        registry: &'a crate::codice::CodeRegistry,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DomainError>> + Send + 'a>>
    {
        Box::pin(async move {
            if let Some(group) = &mut node.group {
                if let Some(nodes) = &mut group.nodes {
                    for child in nodes {
                        self.resolve_dotpath_filters(child, entity_type, tenant_id, registry)
                            .await?;
                    }
                }
            } else if let Some(crit) = &mut node.criteria {
                if let Some(field) = &crit.field {
                    if field.contains('.') {
                        let parts: Vec<&str> = field.split('.').collect();
                        if parts.len() == 2 {
                            let ref_attr_name = parts[0];
                            let target_attr_name = parts[1];

                            let mut ref_entity_type = None;
                            if let Some(attrs) = registry.get_attributes(entity_type) {
                                for attr in attrs {
                                    let bare_name =
                                        attr.name.split('/').next_back().unwrap_or(&attr.name);
                                    if bare_name == ref_attr_name {
                                        if let Some(ref_entity) = &attr.entity_ref {
                                            ref_entity_type = Some(ref_entity.clone());
                                        }
                                        break;
                                    }
                                }
                            }

                            if let Some(ref_entity_type) = ref_entity_type {
                                debug!("[resolve_dotpath_filters] Resolving {ref_attr_name}.{target_attr_name} for entity {entity_type} (referenced: {ref_entity_type})");
                                let mut datom_vals = Vec::new();
                                if let Some(v) = &crit.value {
                                    if let Some(list) = &v.list_val {
                                        if let Some(vals) = &list.values {
                                            for val_str in vals {
                                                let dv = if let Ok(n) = val_str.parse::<i64>() {
                                                    crate::eav::types::datom::DatomValue::Long(n)
                                                } else if let Ok(b) = val_str.parse::<bool>() {
                                                    crate::eav::types::datom::DatomValue::Bool(b)
                                                } else {
                                                    crate::eav::types::datom::DatomValue::Str(
                                                        val_str.clone(),
                                                    )
                                                };
                                                datom_vals.push(dv);
                                            }
                                        }
                                    } else if let Some(s) = &v.string_val {
                                        let dv = if let Ok(n) = s.parse::<i64>() {
                                            crate::eav::types::datom::DatomValue::Long(n)
                                        } else if let Ok(b) = s.parse::<bool>() {
                                            crate::eav::types::datom::DatomValue::Bool(b)
                                        } else {
                                            crate::eav::types::datom::DatomValue::Str(s.clone())
                                        };
                                        datom_vals.push(dv);
                                    } else if v.number_val != 0.0 {
                                        datom_vals.push(
                                            crate::eav::types::datom::DatomValue::Long(
                                                v.number_val as i64,
                                            ),
                                        );
                                    }
                                }

                                let mut matching_ids_set = std::collections::HashSet::new();
                                for dv in datom_vals {
                                    let plan =
                                        crate::eav::reader::query::NativeQueryPlan::AvetSingle {
                                            tenant_id: tenant_id.to_string(),
                                            attr_name: target_attr_name.to_string(),
                                            value: dv,
                                        };
                                    if let Ok(ids) =
                                        self.query_executor.execute_native_plan(&plan).await
                                    {
                                        for id in ids {
                                            matching_ids_set.insert(id);
                                        }
                                    }
                                }
                                let matching_ids: Vec<String> =
                                    matching_ids_set.into_iter().collect();

                                crit.field = Some(ref_attr_name.to_string());

                                if matching_ids.is_empty() {
                                    crit.op_ref = crate::janus::fbs::FilterOperator::EQ;
                                    crit.value = Some(Box::new(crate::janus::fbs::FilterValueT {
                                        string_val: Some("__non_existent_ulid__".to_string()),
                                        ..Default::default()
                                    }));
                                } else if matching_ids.len() == 1 {
                                    crit.op_ref = crate::janus::fbs::FilterOperator::EQ;
                                    crit.value = Some(Box::new(crate::janus::fbs::FilterValueT {
                                        string_val: Some(matching_ids[0].clone()),
                                        ..Default::default()
                                    }));
                                } else {
                                    crit.op_ref = crate::janus::fbs::FilterOperator::IN;
                                    crit.value = Some(Box::new(crate::janus::fbs::FilterValueT {
                                        list_val: Some(Box::new(crate::janus::fbs::StringListT {
                                            values: Some(matching_ids),
                                        })),
                                        ..Default::default()
                                    }));
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        })
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
        let op = arr[0].as_str().unwrap_or("");
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
                if crit.op_ref.0 == crate::janus::fbs::FilterOperator::EQ.0
                    && crit.field.as_deref() == Some("entity/ulid")
                {
                    return crit.value.as_ref().and_then(|v| v.string_val.clone());
                }
            }
        }
    }
    None
}

/// Extrae la lista de atributos del `select` del AST IR.
fn extract_select_attrs(ast_ir: &Value) -> Vec<String> {
    ast_ir
        .get("select")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
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

/// Ordena rows en memoria según la especificación `order_by` del AST IR.
fn apply_sort_in_memory(rows: &mut [Value], order_by: &[Value]) {
    if order_by.is_empty() {
        return;
    }

    rows.sort_by(|a, b| {
        for spec in order_by {
            let field = spec
                .get("attribute")
                .or(spec.get("field"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let dir = spec
                .get("direction")
                .and_then(|v| v.as_str())
                .unwrap_or("ASC");

            let val_a = a.get(field).cloned().unwrap_or(Value::Null);
            let val_b = b.get(field).cloned().unwrap_or(Value::Null);

            let ord = compare_json_values(&val_a, &val_b);
            let ord = if dir.to_uppercase() == "DESC" {
                ord.reverse()
            } else {
                ord
            };
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

/// Normaliza timestamps ms→segundos en los rows.
fn normalize_timestamps_in_rows(rows: &mut [Value]) {
    const TS_FIELDS: &[&str] = &[
        "created_at",
        "updated_at",
        "deleted_at",
        "ingested_at",
        "timestamp",
        "meta/created_at",
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
fn apply_sort_fbs(rows: &mut [Value], sort_defs: &[crate::janus::fbs::SortDefinitionT]) {
    if sort_defs.is_empty() {
        return;
    }
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

fn extract_attrs_from_filter_node(
    node: &crate::janus::fbs::FilterNodeT,
    attrs: &mut std::collections::HashSet<String>,
) {
    if let Some(crit) = &node.criteria {
        if let Some(field) = &crit.field {
            attrs.insert(field.clone());
        }
    }
    if let Some(group) = &node.group {
        if let Some(nodes) = &group.nodes {
            for child in nodes {
                extract_attrs_from_filter_node(child, attrs);
            }
        }
    }
}
