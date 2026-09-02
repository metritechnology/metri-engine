pub mod olap;
pub mod oltp;
pub mod post_processor;
pub mod translator;

use crate::aegis::oltp::executor::OltpExecutor;
use crate::codice::global as codice_global;
use crate::domain::protocols::IQueryEngine;
use crate::iop::core::IopContext;
use crate::janus::fbs::AnalyticsRequestT;
use crate::janus::normalizer::normalize_chunk;
use crate::janus::validator;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

// ── Cedar Context ─────────────────────────────────────────────────────────────

/// Contexto Cedar enriquecido para el Read Path.
#[derive(Debug, Clone)]
pub struct CedarCtx {
    pub tenant_id: String,
    pub user_id: String,
    pub roles: Vec<String>,
    pub is_super_master: bool,
    pub cross_tenant_scope: String,
    pub domain_boundaries: serde_json::Value,
}

impl CedarCtx {
    /// Construye el Cedar context desde un IopContext.
    pub fn from_iop_ctx(ctx: &IopContext) -> Self {
        CedarCtx {
            tenant_id: ctx.tenant_id.clone(),
            user_id: ctx.user_id.clone(),
            roles: ctx.roles.clone(),
            is_super_master: false,
            cross_tenant_scope: "NONE".to_string(),
            domain_boundaries: ctx.domain_boundaries.clone(),
        }
    }
}

// ── QueryChunk ────────────────────────────────────────────────────────────────

/// Resultado de un sub-query — decorado con query_key.
#[derive(Debug, Clone)]
pub struct QueryChunk {
    pub query_key: String,
    pub body: Value,
    pub success: bool,
}

// ── run_query_pipeline ────────────────────────────────────────────────────────

/// Ejecuta el pipeline Read Path completo (8 pasos) en paralelo sobre cada sub-query.
/// Retorna Vec<QueryChunk> — nunca lanza, los errores se encapsulan en chunks.
pub async fn run_query_pipeline(
    tenant_id: &str,
    queries: &HashMap<String, AnalyticsRequestT>,
    cedar_ctx: &CedarCtx,
    executor: &OltpExecutor,
    athena_engine: Option<&Arc<dyn IQueryEngine>>,
    explain_plan: bool,
) -> Vec<QueryChunk> {
    // Paso 1: Gate Zero-Trust
    if let Err(e) = validator::validate_tenant(tenant_id) {
        error!("[Janus] Zero-Trust gate rechazó | tenant: {tenant_id}");
        return vec![QueryChunk {
            query_key: "__pipeline__".to_string(),
            body: json!({"code": "JANUS_400", "reason": e.to_string()}),
            success: false,
        }];
    }

    // Pasos 4-8: Procesar cada sub-query en paralelo
    let mut handles = Vec::new();

    for (query_key, query_map) in queries {
        let qk = query_key.clone();
        let qm = query_map.clone();
        let cedar = cedar_ctx.clone();
        let exec_clone = executor.clone();
        let athena_clone = athena_engine.cloned();

        handles.push(tokio::spawn(async move {
            // Paso 1.5: Validación Semántica del Contrato FlatBuffers
            if let Err(e) = validator::validate_analytics_request_fbs(&qm) {
                error!("[Janus] Validación FBS falló en query_key '{}': {}", qk, e);
                return vec![QueryChunk {
                    query_key: qk,
                    body: json!({"code": "JANUS_400", "reason": e.to_string()}),
                    success: false,
                }];
            }

            process_single_query(
                &qk,
                &qm,
                &cedar,
                &exec_clone,
                athena_clone.as_ref(),
                explain_plan,
            )
            .await
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
                    body: json!({"code": "JANUS_500", "reason": "sub-query panicked"}),
                    success: false,
                });
            }
        }
    }

    chunks
}

// ── process_single_query ──────────────────────────────────────────────────────

/// Procesa un sub-query individual (Pasos 4–8).
pub async fn process_single_query(
    query_key: &str,
    query_map: &AnalyticsRequestT,
    cedar_ctx: &CedarCtx,
    executor: &OltpExecutor,
    athena_engine: Option<&Arc<dyn IQueryEngine>>,
    explain_plan: bool,
) -> Vec<QueryChunk> {
    let start_time = std::time::Instant::now();
    let entity_type = query_map.entity.as_deref().unwrap_or("unknown");

    let registry = codice_global();
    let schema = registry
        .get_model(entity_type)
        .and_then(|m| serde_json::to_value(m).ok())
        .unwrap_or(json!({}));

    let is_olap = schema.get("engine").and_then(|v| v.as_str()) == Some("olap");

    if is_olap {
        olap::execute_olap_query(
            query_key,
            query_map,
            cedar_ctx,
            &schema,
            athena_engine,
            explain_plan,
            start_time,
        )
        .await
    } else {
        oltp::execute_oltp_query(
            query_key,
            query_map,
            cedar_ctx,
            &schema,
            executor,
            explain_plan,
            start_time,
        )
        .await
    }
}
