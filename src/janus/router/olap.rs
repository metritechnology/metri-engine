use crate::domain::protocols::IQueryEngine;
use crate::janus::ast_compiler::compile_ast_internal;
use crate::janus::fbs::AnalyticsRequestT;
use crate::janus::normalizer::normalize_chunk;
use crate::janus::router::post_processor;
use crate::janus::router::translator::analytics_request_to_json;
use crate::janus::router::{CedarCtx, QueryChunk};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn execute_olap_query(
    query_key: &str,
    query_map: &AnalyticsRequestT,
    cedar_ctx: &CedarCtx,
    schema: &Value,
    athena_engine: Option<&Arc<dyn IQueryEngine>>,
    explain_plan: bool,
    start_time: std::time::Instant,
) -> Vec<QueryChunk> {
    let entity_type = query_map.entity.as_deref().unwrap_or("unknown");
    let query_desc_json = analytics_request_to_json(query_map);

    let ast_ir = match compile_ast_internal(&query_desc_json, cedar_ctx, schema) {
        Ok(ast) => ast,
        Err(e) => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_400",
                    "reason":            e.to_string(),
                    "query_key":         query_key,
                    "execution_time_ms": elapsed_ms,
                })),
                success: false,
            }];
        }
    };

    if explain_plan {
        let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
        let ast_str = format!("{:#?}", ast_ir);
        let body = json!({
            "query_key":         query_key,
            "entity_type":       entity_type,
            "tenant_id":         cedar_ctx.tenant_id,
            "data":              [[ast_str]],
            "columns":           [
                {"key": "ast_ir", "type": "string", "label": "AST IR"}
            ],
            "channel":           "explain",
            "total":             1,
            "output_cast":       "TABLE",
            "execution_time_ms": elapsed_ms,
        });
        return vec![QueryChunk {
            query_key: query_key.to_string(),
            body: normalize_chunk(&body),
            success: true,
        }];
    }

    // Security gate check (must contain tenant isolation)
    if !crate::aegis::sql::compiler::ast_contains_tenant(&ast_ir) {
        let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
        return vec![QueryChunk {
            query_key: query_key.to_string(),
            body: normalize_chunk(&json!({
                "code":              "JANUS_400",
                "reason":            format!("Missing tenant isolation for tenant: {}", cedar_ctx.tenant_id),
                "query_key":         query_key,
                "execution_time_ms": elapsed_ms,
            })),
            success: false,
        }];
    }

    let time_range = query_map
        .time_frame
        .as_ref()
        .and_then(|tf| crate::aegis::temporal_bridge::resolve_fbs_time_frame(tf))
        .unwrap_or_else(crate::aegis::temporal_bridge::no_time_range);

    let ts_col = crate::aegis::sql::registry::get_hybrid_config(entity_type)
        .map(|config| config.timestamp_column.clone());
    let db_name = std::env::var("GLUE_DATABASE_NAME").unwrap_or_else(|_| "metri_olap".to_string());

    let compiled_sql = match crate::aegis::sql::compiler::compile_athena_sql_with_time_frame(
        &ast_ir,
        &db_name,
        &cedar_ctx.tenant_id,
        &time_range,
        ts_col.as_deref(),
    ) {
        Ok(res) => res,
        Err(e) => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_500",
                    "reason":            format!("SQL compilation failed: {}", e),
                    "query_key":         query_key,
                    "execution_time_ms": elapsed_ms,
                })),
                success: false,
            }];
        }
    };

    let athena = match athena_engine {
        Some(ae) => ae,
        None => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_500",
                    "reason":            "Athena query engine no está inicializado",
                    "query_key":         query_key,
                    "execution_time_ms": elapsed_ms,
                })),
                success: false,
            }];
        }
    };

    let exec_id = match athena.start_query(&compiled_sql.sql, &db_name).await {
        Ok(id) => id,
        Err(e) => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_500",
                    "reason":            format!("Error al iniciar query en Athena: {}", e),
                    "query_key":         query_key,
                    "execution_time_ms": elapsed_ms,
                })),
                success: false,
            }];
        }
    };

    let query_results = match athena.get_query_results(&exec_id).await {
        Ok(res) => res,
        Err(e) => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_500",
                    "reason":            format!("Error al obtener resultados de Athena: {}", e),
                    "query_key":         query_key,
                    "execution_time_ms": elapsed_ms,
                })),
                success: false,
            }];
        }
    };

    let mut raw_rows = Vec::new();
    for row in query_results.rows {
        let mut map = row.into_iter().collect::<serde_json::Map<String, Value>>();
        map.remove("_tenant");
        map.remove("_entity");
        map.remove("_partition_path");
        raw_rows.push(Value::Object(map));
    }

    // Post-procesar comparaciones CTE para KPIs y series temporales analíticas
    let has_comparisons = query_map
        .comparisons
        .as_ref()
        .map(|c| !c.is_empty())
        .unwrap_or(false);
    if has_comparisons {
        for row_val in &mut raw_rows {
            if let Some(row_obj) = row_val.as_object_mut() {
                let keys: Vec<String> = row_obj.keys().cloned().collect();
                for key in keys {
                    if key.starts_with("current_") {
                        let base_alias = &key["current_".len()..];
                        if let Some(val) = row_obj.get(&key).cloned() {
                            row_obj.entry(base_alias.to_string()).or_insert(val);
                        }
                    }
                    if key.starts_with("prev_0_") {
                        if let Some(val) = row_obj.get(&key).cloned() {
                            row_obj.entry("previous_value".to_string()).or_insert(val);
                        }
                    }
                }
            }
        }
    }

    let total_count = raw_rows.len() as u64;
    let limit_raw = query_map.limit as usize;
    let fallback_limit = if limit_raw == 0 { 1000 } else { limit_raw };
    let (offset, limit) =
        crate::aegis::pagination::decode_cursor(query_map.cursor.as_deref(), fallback_limit);
    let mut processed_rows = crate::aegis::pagination::paginate_rows(raw_rows, offset, limit);

    // Apply label templates and hierarchy from post_processor
    let dimensions = query_map.dimensions.as_deref().unwrap_or(&[]);
    let metrics = query_map.metrics.as_deref().unwrap_or(&[]);
    let output_cast_i = query_map.output_cast.0;

    post_processor::apply_label_templates(
        &mut processed_rows,
        dimensions,
        output_cast_i,
        &query_map.viz,
    );
    post_processor::inject_hierarchy_children(&mut processed_rows, query_map.hierarchy.as_deref());

    let columns = post_processor::derive_columns(&processed_rows, dimensions, metrics);
    let query_spec = post_processor::build_query_spec(entity_type, dimensions, metrics);

    let output_cast_str = match query_map.output_cast.0 {
        1 => "KPI",
        2 => "TIMESERIES",
        3 => "TABLE",
        4 => "PIE",
        5 => "BUBBLE",
        6 => "CSV_EXPORT",
        _ => "TABLE",
    };

    let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
    let mut body = json!({
        "query_key":         query_key,
        "entity_type":       entity_type,
        "tenant_id":         cedar_ctx.tenant_id,
        "data":              processed_rows,
        "columns":           columns,
        "channel":           "olap",
        "total":             total_count,
        "output_cast":       output_cast_str,
        "query_spec":        query_spec,
        "execution_time_ms": elapsed_ms,
        "query_id":          exec_id,
    });

    if let Some(obj) = body.as_object_mut() {
        if let Some(ref viz) = query_map.viz {
            obj.insert("viz".to_string(), json!(viz));
        }

        let pagination_meta =
            crate::aegis::pagination::build_pagination(offset, limit, total_count as usize);
        if !pagination_meta.is_null() {
            obj.insert("pagination".to_string(), pagination_meta);
        }
    }

    vec![QueryChunk {
        query_key: query_key.to_string(),
        body: normalize_chunk(&body),
        success: true,
    }]
}
