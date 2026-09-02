use serde_json::{json, Value};
use tracing::{error, info};
use crate::janus::fbs::AnalyticsRequestT;
use crate::janus::router::{CedarCtx, QueryChunk};
use crate::aegis::oltp::executor::OltpExecutor;
use crate::janus::normalizer::normalize_chunk;
use crate::janus::router::post_processor;
use crate::janus::ast_compiler::compile_ast_fbs;
use crate::janus::plan_selector::select_plan_fbs;
use crate::janus::aggregator::apply_output_cast_fbs;

pub async fn execute_oltp_query(
    query_key: &str,
    query_map: &AnalyticsRequestT,
    cedar_ctx: &CedarCtx,
    schema: &Value,
    executor:  &OltpExecutor,
    explain_plan: bool,
    start_time: std::time::Instant,
) -> Vec<QueryChunk> {
    let entity_type = query_map.entity.as_deref().unwrap_or("unknown");

    let ast_ir = match compile_ast_fbs(query_map, cedar_ctx, schema) {
        Ok(ast) => ast,
        Err(e) => {
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body:      normalize_chunk(&json!({
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
            body:      normalize_chunk(&body),
            success:   true,
        }];
    }

    let plan = select_plan_fbs(&ast_ir);
    info!(
        query_key = %query_key,
        entity    = %entity_type,
        plan      = ?plan,
        "[Janus] Plan seleccionado"
    );

    let executor_result = match executor.run_oltp_query_fbs(&cedar_ctx.tenant_id, &ast_ir).await {
        Ok(v)  => v,
        Err(e) => {
            error!("[Janus] Error EAV Query: {:?}", e);
            let elapsed_ms = std::cmp::max(start_time.elapsed().as_millis() as i64, 1);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":              "JANUS_500",
                    "reason":            e.to_string(),
                    "query_key":         query_key,
                    "execution_time_ms": elapsed_ms,
                })),
                success: false,
            }];
        }
    };

    let (raw_rows, total_count, pagination_meta) = if let Some(data_arr) = executor_result.get("data").and_then(|v| v.as_array()) {
        let total = executor_result.get("total").and_then(|v| v.as_u64()).unwrap_or(data_arr.len() as u64);
        let pag   = executor_result.get("pagination").cloned().unwrap_or(Value::Null);
        (data_arr.clone(), total, pag)
    } else if let Some(arr) = executor_result.as_array() {
        (arr.clone(), arr.len() as u64, Value::Null)
    } else {
        (vec![executor_result.clone()], 1u64, Value::Null)
    };

    let output_cast_i = ast_ir.output_cast.0;
    let mut processed_rows = match output_cast_i {
        1 /* KPI */ | 2 /* TIMESERIES */ | 4 /* PIE */ | 5 /* BUBBLE */ => {
            raw_rows
        }
        _ => {
            apply_output_cast_fbs(raw_rows, &ast_ir)
        }
    };

    // Apply label templates and hierarchy from post_processor
    let dimensions = ast_ir.dimensions.as_deref().unwrap_or(&[]);
    let metrics = ast_ir.metrics.as_deref().unwrap_or(&[]);

    post_processor::apply_label_templates(&mut processed_rows, dimensions, output_cast_i, &ast_ir.viz);
    post_processor::inject_hierarchy_children(&mut processed_rows, ast_ir.hierarchy.as_deref());
    post_processor::redact_sensitive_attributes(&mut processed_rows, entity_type, cedar_ctx);

    if let Some(first_row) = processed_rows.first() {
        if let Some(obj) = first_row.as_object() {
            tracing::debug!("PROCESSED ROW KEYS: {:?}", obj.keys().collect::<Vec<&String>>());
        }
    }

    let columns = post_processor::derive_columns(&processed_rows, dimensions, metrics);
    let query_spec = post_processor::build_query_spec(entity_type, dimensions, metrics);

    let output_cast_str = match output_cast_i {
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
        "channel":           "oltp",
        "total":             total_count,
        "output_cast":       output_cast_str,
        "query_spec":        query_spec,
        "execution_time_ms": elapsed_ms,
    });

    if let Some(ref viz) = ast_ir.viz {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("viz".to_string(), json!(viz));
        }
    }

    if let Some(ref h) = ast_ir.hierarchy {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("hierarchy".to_string(), json!({
                "parent_field":     h.parent_field,
                "current_node_id":  h.current_node_id,
                "inject_has_children": h.inject_has_children,
            }));
        }
    }

    if !pagination_meta.is_null() {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("pagination".to_string(), pagination_meta);
        }
    }

    // Inyectar ChartDecoration hints para el normalizer
    {
        let dims = ast_ir.dimensions.as_deref().unwrap_or(&[]);
        let label_template = dims.iter()
            .find_map(|d| d.label_template.as_deref().filter(|s| !s.is_empty()))
            .unwrap_or("")
            .to_string();

        if !label_template.is_empty() {
            if let Some(obj) = body.as_object_mut() {
                let existing = obj.get("decoration").cloned().unwrap_or(json!({}));
                let mut dec_map = match existing {
                    Value::Object(m) => m,
                    _ => serde_json::Map::new(),
                };
                dec_map.entry("label_template").or_insert(json!(label_template));
                obj.insert("decoration".to_string(), Value::Object(dec_map));
            }
        }
    }

    vec![QueryChunk {
        query_key: query_key.to_string(),
        body:      normalize_chunk(&body),
        success:   true,
    }]
}
