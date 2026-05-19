// [PORTED_FROM: src/metri/janus/core.clj]
// janus/router.rs — Read Path del Metri Engine (JanusCerebro / Query Pipeline).
// En Clojure: ig/init-key :janus/query — pipeline de consultas analíticas.
//
// Responsabilidad: SOLO Read Path.
// El Write Path vive en src/janus_router/router.rs (JanusRouter).
//
// Pipeline Read Path (8 pasos):
//   1. Gate Zero-Trust (validate_tenant)
//   2-3. Cedar authorize (stub FASE 4)
//   4. Compilar AST IR (compile_ast_internal)
//   5. Selección del plan de ejecución (select_plan)
//   6. Ejecución EAV (OltpExecutor)
//   7. OutputCast aggregation (apply_output_cast)
//   8. Normalización (normalize_chunk)

use serde_json::{json, Value};
use tracing::{error, info, debug};
use std::collections::HashMap;
use crate::janus::fbs::AnalyticsRequestT;

use crate::codice::global as codice_global;
use crate::domain::errors::DomainError;
use crate::iop::core::IopContext;
use crate::janus::normalizer::normalize_chunk;
use crate::janus::validator;
use crate::janus::ast_compiler::compile_ast_internal;
use crate::janus::plan_selector::{select_plan, EavQueryPlan};
use crate::janus::aggregator::apply_output_cast;
use crate::aegis::oltp::executor::OltpExecutor;

// ── Cedar Context ─────────────────────────────────────────────────────────────

/// Contexto Cedar enriquecido para el Read Path.
/// [PORTED_FROM: (build-stub-cedar-ctx ctx) en janus/core.clj]
#[derive(Debug, Clone)]
pub struct CedarCtx {
    pub tenant_id:          String,
    pub user_id:            String,
    pub roles:              Vec<String>,
    pub is_super_master:    bool,
    pub cross_tenant_scope: String,
}

impl CedarCtx {
    /// Construye el Cedar context desde un IopContext.
    pub fn from_iop_ctx(ctx: &IopContext) -> Self {
        CedarCtx {
            tenant_id:          ctx.tenant_id.clone(),
            user_id:            ctx.user_id.clone(),
            roles:              ctx.roles.clone(),
            is_super_master:    false,
            cross_tenant_scope: "NONE".to_string(),
        }
    }
}

// ── QueryChunk ────────────────────────────────────────────────────────────────

/// Resultado de un sub-query — decorado con query_key.
/// [PORTED_FROM: chunk decorado con :query-key en process-single-query]
#[derive(Debug)]
pub struct QueryChunk {
    pub query_key: String,
    pub body:      Value,
    pub success:   bool,
}

// ── run_query_pipeline ────────────────────────────────────────────────────────

/// Ejecuta el pipeline Read Path completo (8 pasos) en paralelo sobre cada sub-query.
/// Retorna Vec<QueryChunk> — nunca lanza, los errores se encapsulan en chunks.
///
/// [PORTED_FROM: (run-query-pipeline ctx cedar-authorizer ast-compiler aegis-engine)]
pub async fn run_query_pipeline(
    tenant_id: &str,
    queries:   &HashMap<String, AnalyticsRequestT>,
    cedar_ctx: &CedarCtx,
    executor:  &OltpExecutor,
) -> Vec<QueryChunk> {
    // Paso 1: Gate Zero-Trust
    if let Err(e) = validator::validate_tenant(tenant_id) {
        error!("[Janus] Zero-Trust gate rechazó | tenant: {tenant_id}");
        return vec![QueryChunk {
            query_key: "__pipeline__".to_string(),
            body:      json!({"code": "JANUS_400", "reason": e.to_string()}),
            success:   false,
        }];
    }

    // Pasos 2-3: Cedar (stub FASE 4 — pass-through con cedar_ctx ya construido)

    // Pasos 4-8: Procesar cada sub-query en paralelo
    // [PORTED_FROM: (a/go (doseq chs ...) ...)]
    let mut handles = Vec::new();

    for (query_key, query_map) in queries {
        let qk         = query_key.clone();
        let qm         = query_map.clone();
        let cedar      = cedar_ctx.clone();
        let exec_clone = executor.clone();

        handles.push(tokio::spawn(async move {
            // Paso 1.5: Validación Semántica del Contrato FlatBuffers
            if let Err(e) = validator::validate_analytics_request_fbs(&qm) {
                error!("[Janus] Validación FBS falló en query_key '{}': {}", qk, e);
                return vec![QueryChunk {
                    query_key: qk,
                    body:      json!({"code": "JANUS_400", "reason": e.to_string()}),
                    success:   false,
                }];
            }

            process_single_query(&qk, &qm, &cedar, &exec_clone).await
        }));
    }

    let mut chunks = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(chunk_vec) => chunks.extend(chunk_vec),
            Err(e) => {
                error!("[Janus] Sub-query panicked: {e}");
                chunks.push(QueryChunk {
                    query_key: "unknown".to_string(),
                    body:      json!({"code": "JANUS_500", "reason": "sub-query panicked"}),
                    success:   false,
                });
            }
        }
    }

    chunks
}

// ── process_single_query ──────────────────────────────────────────────────────

/// Procesa un sub-query individual (Pasos 4–8).
/// [PORTED_FROM: (process-single-query query-key query-map cedar-ctx executor)]
async fn process_single_query(
    query_key: &str,
    query_map: &AnalyticsRequestT,
    cedar_ctx: &CedarCtx,
    executor:  &OltpExecutor,
) -> Vec<QueryChunk> {
    let entity_type = query_map.entity.as_deref().unwrap_or("unknown");

    // Paso 4: Compilar AST IR
    let registry = codice_global();
    let schema = registry.get_model(entity_type)
        .and_then(|m| serde_json::to_value(m).ok())
        .unwrap_or(json!({}));

    let ast_ir = match crate::janus::ast_compiler::compile_ast_fbs(query_map, cedar_ctx, &schema) {
        Ok(ast) => ast,
        Err(e) => {
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body:      normalize_chunk(&json!({
                    "code":      "JANUS_400",
                    "reason":    e.to_string(),
                    "query_key": query_key,
                })),
                success: false,
            }];
        }
    };

    // Paso 5: Selección del plan de ejecución (log para observabilidad)
    let plan = crate::janus::plan_selector::select_plan_fbs(&ast_ir);
    info!(
        query_key = %query_key,
        entity    = %entity_type,
        plan      = ?plan,
        "[Janus] Plan seleccionado"
    );

    // Paso 6+7: Ejecución EAV + OutputCast
    // El executor devuelve {data: [...], total: N, pagination: {...}}
    let executor_result = match executor.run_oltp_query_fbs(&cedar_ctx.tenant_id, &ast_ir).await {
        Ok(v)  => v,
        Err(e) => {
            error!("[Janus] Error EAV Query: {:?}", e);
            return vec![QueryChunk {
                query_key: query_key.to_string(),
                body: normalize_chunk(&json!({
                    "code":      "JANUS_500",
                    "reason":    e.to_string(),
                    "query_key": query_key,
                })),
                success: false,
            }];
        }
    };

    // Extraer el envelope: el executor puede devolver {data, total, pagination} o Array legacy
    let (raw_rows, total_count, pagination_meta) = if let Some(data_arr) = executor_result.get("data").and_then(|v| v.as_array()) {
        let total = executor_result.get("total").and_then(|v| v.as_u64()).unwrap_or(data_arr.len() as u64);
        let pag   = executor_result.get("pagination").cloned().unwrap_or(serde_json::Value::Null);
        (data_arr.clone(), total, pag)
    } else if let Some(arr) = executor_result.as_array() {
        (arr.clone(), arr.len() as u64, serde_json::Value::Null)
    } else {
        (vec![executor_result.clone()], 1u64, serde_json::Value::Null)
    };

    // OutputCast: para KPI/PIE el aggregation ya ocurrió en el executor.
    // Para TIMESERIES y TABLE aplicamos el aggregator de janus para compatibilidad.
    let output_cast_i = ast_ir.output_cast.0;
    let mut processed_rows = match output_cast_i {
        1 /* KPI */ | 4 /* PIE */ | 5 /* BUBBLE */ => {
            // Ya agregado en executor, pasar directo
            raw_rows
        }
        _ => {
            // TABLE / TIMESERIES: aplicar aggregator janus para bucketing
            crate::janus::aggregator::apply_output_cast_fbs(raw_rows, &ast_ir)
        }
    };

    if let Some(h) = &ast_ir.hierarchy {
        if h.inject_has_children {
            if let Some(parent_field) = &h.parent_field {
                for row in processed_rows.iter_mut() {
                    if let Some(obj) = row.as_object_mut() {
                        // LAZY-LOAD FIX: We must assume all nodes have children to render the UI expander.
                        // When the UI fetches children and receives [], it will automatically remove the expander.
                        // We cannot compute has_children perfectly in memory because we only have the current page of rows!
                        obj.insert("has_children".to_string(), json!(true));
                    }
                }
            } else {
                // No parent_field configured — conservatively inject false (no expanders)
                for row in processed_rows.iter_mut() {
                    if let Some(obj) = row.as_object_mut() {
                        obj.entry("has_children").or_insert(json!(false));
                    }
                }
            }
        }
    }

    // Paso 8: columnas derivadas
    let dim_attrs: Vec<String> = ast_ir.dimensions.as_deref().unwrap_or(&[]).iter()
        .filter_map(|d| d.attribute.clone()).collect();
    let metric_aliases: Vec<String> = ast_ir.metrics.as_deref().unwrap_or(&[]).iter()
        .map(|m| {
            m.name.clone().unwrap_or_else(|| {
                let agg = match m.aggregation.0 {
                    1 => "count", 2 => "sum", 3 => "avg", 4 => "min", 5 => "max", _ => "agg",
                };
                format!("{}_{}", agg, m.attribute.as_deref().unwrap_or("total"))
            })
        }).collect();

    let columns = crate::aegis::oltp::aggregation::derive_columns(&processed_rows, &dim_attrs, &metric_aliases);

    let output_cast_str = match output_cast_i {
        1 => "KPI",
        2 => "TIMESERIES",
        3 => "TABLE",
        4 => "PIE",
        5 => "BUBBLE",
        6 => "CSV_EXPORT",
        _ => "TABLE",
    };

    let mut body = json!({
        "query_key":   query_key,
        "entity_type": entity_type,
        "tenant_id":   cedar_ctx.tenant_id,
        "data":        processed_rows,
        "columns":     columns,
        "channel":     "oltp",
        "total":       total_count,
        "output_cast": output_cast_str,
    });

    if let Some(viz) = &ast_ir.viz {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("viz".to_string(), json!(viz));
        }
    }

    if let Some(h) = &ast_ir.hierarchy {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("hierarchy".to_string(), json!({
                "parent_field":     h.parent_field,
                "current_node_id":  h.current_node_id,
                "inject_has_children": h.inject_has_children,
            }));
        }
    }

    // Inyectar paginación real si está disponible
    if !pagination_meta.is_null() {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("pagination".to_string(), pagination_meta);
        }
    }

    vec![QueryChunk {
        query_key: query_key.to_string(),
        body:      normalize_chunk(&body),
        success:   true,
    }]
}

