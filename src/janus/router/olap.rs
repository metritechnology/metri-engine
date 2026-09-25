//! OLAP channel execution — compile to FBS and query Athena.
use crate::domain::protocols::{CacheEntry, IQueryEngine, QueryResults};
use crate::janus::ast_compiler::compile_ast_internal;
use crate::janus::cache::{self, CacheCandidate, CacheChannel, LookupOutcome, QueryCacheFrontend};
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
    cache: &QueryCacheFrontend,
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

    // ── Cache-aside (D1/D6): alrededor de start_query + polling. El SQL ya
    // contiene tenant + ABAC (gate ast_contains_tenant de arriba), así que
    // hashearlo captura toda la dependencia de seguridad. El payload guarda
    // el exec_id original para trazabilidad del body.query_id.
    let cache_key = cache::keys::olap_key(
        &compiled_sql.sql,
        &db_name,
        &time_range,
        &cedar_ctx.tenant_id,
    );
    let candidate = CacheCandidate {
        channel: CacheChannel::Olap,
        tenant_id: &cedar_ctx.tenant_id,
        entity: entity_type,
        explain_plan,
        overlay_entity: false, // overlays son retoques OLTP
    };
    let cacheable = !matches!(cache.evaluate(&candidate), LookupOutcome::Bypass { .. });

    let mut hit_remaining_secs: Option<i64> = None;
    let mut cached_entry: Option<CacheEntry> = None;
    if cacheable {
        if let LookupOutcome::Hit { entry } = cache
            .lookup(&cache_key, &cedar_ctx.tenant_id, CacheChannel::Olap)
            .await
        {
            hit_remaining_secs = Some(cache.remaining_secs(entry.expires_at));
            cached_entry = Some(entry);
        }
    }

    let executed: Result<(String, QueryResults), String> = if let Some(entry) = cached_entry {
        match results_from_payload(&entry.payload) {
            Some(pair) => Ok(pair),
            // Entrada ilegible (formato inesperado): ejecutar como miss.
            None => execute_athena(athena, &compiled_sql.sql, &db_name).await,
        }
    } else {
        let executed = execute_athena(athena, &compiled_sql.sql, &db_name).await;
        if let Ok((ref exec_id, ref results)) = executed {
            if cacheable {
                let payload = json!({
                    "exec_id": exec_id,
                    "columns": results.columns,
                    "rows":    results.rows,
                });
                cache
                    .store(
                        &cache_key,
                        &cedar_ctx.tenant_id,
                        CacheChannel::Olap,
                        &payload,
                    )
                    .await;
            }
        }
        executed
    };

    let (exec_id, query_results) = match executed {
        Ok(pair) => pair,
        Err(reason) => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_500",
                    "reason":            reason,
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

    // Transporte de caché para el normalizer (§8.1): solo en modo ddb.
    if cache.reports_metadata() {
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "cache".to_string(),
                json!({
                    "hit":            hit_remaining_secs.is_some(),
                    "remaining_secs": hit_remaining_secs.unwrap_or(0),
                    "channel":       "olap",
                }),
            );
        }
    }

    vec![QueryChunk {
        query_key: query_key.to_string(),
        body: normalize_chunk(&body),
        success: true,
    }]
}

/// Ejecuta la query en Athena (start + polling de resultados). Factorizado
/// del cuerpo principal para que el camino cacheado y el ejecutado compartan
/// exactamente el mismo manejo de errores.
async fn execute_athena(
    athena: &Arc<dyn IQueryEngine>,
    sql: &str,
    db_name: &str,
) -> Result<(String, QueryResults), String> {
    let exec_id = athena
        .start_query(sql, db_name)
        .await
        .map_err(|e| format!("Error al iniciar query en Athena: {e}"))?;

    let results = athena
        .get_query_results(&exec_id)
        .await
        .map_err(|e| format!("Error al obtener resultados de Athena: {e}"))?;

    Ok((exec_id, results))
}

/// Reconstruye `(exec_id, QueryResults)` desde el payload cacheado.
/// `None` si el formato no es el esperado — el llamador ejecuta como miss.
fn results_from_payload(payload: &Value) -> Option<(String, QueryResults)> {
    let exec_id = payload.get("exec_id")?.as_str()?.to_string();
    let columns = payload
        .get("columns")?
        .as_array()?
        .iter()
        .map(|c| c.as_str().map(str::to_string))
        .collect::<Option<Vec<String>>>()?;
    let rows = payload
        .get("rows")?
        .as_array()?
        .iter()
        .map(|r| {
            r.as_object().map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<std::collections::HashMap<String, Value>>()
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some((exec_id, QueryResults { columns, rows }))
}
