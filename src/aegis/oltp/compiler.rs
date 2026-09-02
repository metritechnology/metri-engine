// aegis/oltp/compiler.rs — Compilador OLTP: AST IR → plan físico EAV.
//
// Conecta la salida del PlanSelector (janus::plan_selector::EavQueryPlan)
// con el ejecutor físico (eav::reader::query::EavQueryExecutor).
//
// Cadena completa:
//   AST IR (serde_json::Value)
//     → select_plan()         [plan_selector.rs]
//     → compile_oltp_query()  [este archivo]
//     → QueryExecutionPlan    [eav/reader/query.rs]
//     → execute_plan()

use serde_json::Value;
use tracing::debug;

use crate::aegis::temporal_bridge::resolve_fbs_time_frame;
use crate::domain::errors::DomainError;
use crate::eav::reader::query::{IndexStrategy, NativeQueryPlan, QueryExecutionPlan};
use crate::eav::types::datom::DatomValue;
use crate::janus::plan_selector::{select_plan, EavQueryPlan};
use crate::temporal::adapters::{datalog_clause_to_parts, to_datalog_clauses};
use crate::temporal::core::TimeRange;

// ── Helpers internos ──────────────────────────────────────────────────────────

/// Infiere el subconjunto mínimo de atributos a extraer (pull pattern).
fn infer_required_fields(ast_ir: &Value, ts_field: &str) -> Vec<String> {
    let mut fields = vec![
        ts_field.to_string(),
        "entity/ulid".to_string(),
        "entity/type".to_string(),
        "tenant/id".to_string(),
    ];

    if let Some(metrics) = ast_ir.get("metrics").and_then(|v| v.as_array()) {
        for m in metrics {
            if let Some(f) = m
                .get("attribute")
                .or(m.get("field"))
                .and_then(|v| v.as_str())
            {
                fields.push(f.to_string());
            }
            if let Some(f) = m
                .get("secondary_attribute")
                .or(m.get("secondary_field"))
                .and_then(|v| v.as_str())
            {
                fields.push(f.to_string());
            }
        }
    }

    if let Some(dims) = ast_ir.get("dimensions").and_then(|v| v.as_array()) {
        for d in dims {
            if let Some(f) = d
                .get("attribute")
                .or(d.get("field"))
                .and_then(|v| v.as_str())
            {
                fields.push(f.to_string());
            }
        }
    }

    if let Some(order) = ast_ir.get("order_by").and_then(|v| v.as_array()) {
        for o in order {
            if let Some(f) = o
                .get("attribute")
                .or(o.get("field"))
                .and_then(|v| v.as_str())
            {
                fields.push(f.to_string());
            }
        }
    }

    fields.sort();
    fields.dedup();
    fields
}

/// Recupera el campo de tipo 'epoch' desde el esquema para usar como timestamp de serie temporal.
pub(crate) fn resolve_ts_field(entity_type: &str, schema: Option<&Value>) -> String {
    if let Some(attrs) = schema
        .and_then(|s| s.get("attributes"))
        .and_then(|v| v.as_array())
    {
        for attr in attrs {
            if attr.get("type").and_then(|v| v.as_str()) == Some("epoch") {
                if let Some(name) = attr.get("name").and_then(|v| v.as_str()) {
                    return format!("{entity_type}/{name}");
                }
            }
        }
    }
    "meta/created_at".to_string()
}

// ── API Pública ───────────────────────────────────────────────────────────────

/// Compila el AST IR → NativeQueryPlan usando el PlanSelector real.
/// Este es el punto de integración entre Janus y el ejecutor EAV.
///
/// Flujo:
///   1. `select_plan(ast_ir)` → elige el índice óptimo
///   2. Mapea EavQueryPlan → NativeQueryPlan con tenant_id del contexto
///   3. El router llama a `executor.execute_native_plan(&native_plan)`
pub fn compile_native_plan(ast_ir: &Value, tenant_id: &str) -> NativeQueryPlan {
    let plan = select_plan(ast_ir);
    debug!("[Aegis Compiler] Plan seleccionado: {:?}", plan);

    match plan {
        EavQueryPlan::PointLookup { entity_id } => NativeQueryPlan::PointLookup { entity_id },

        EavQueryPlan::AvetSingleFilter { attr_name, value } => NativeQueryPlan::AvetSingle {
            tenant_id: tenant_id.to_string(),
            attr_name,
            value,
        },

        EavQueryPlan::AvetIntersection { filters } => NativeQueryPlan::AvetIntersect {
            tenant_id: tenant_id.to_string(),
            filters,
        },

        EavQueryPlan::FtsSearch { term } => NativeQueryPlan::FtsSearch {
            tenant_id: tenant_id.to_string(),
            term,
        },

        EavQueryPlan::AevtScan { entity_type, .. } => NativeQueryPlan::AevtScan {
            tenant_id: tenant_id.to_string(),
            entity_type,
        },

        EavQueryPlan::VaetLookup { ref_entity_id, .. } => {
            // FASE 4: VAET lookup de grafo — por ahora fallback a PointLookup
            NativeQueryPlan::PointLookup {
                entity_id: ref_entity_id,
            }
        }

        EavQueryPlan::AsOfSnapshot { entity_id, .. } => {
            // AsOf usa el pull_as_of reader — el router lo gestiona directamente
            NativeQueryPlan::PointLookup { entity_id }
        }
    }
}

/// Compila AST IR → QueryExecutionPlan legacy (para compatibilidad con aegis/oltp/executor.rs).
pub fn compile_oltp_query(
    ast_ir: &Value,
    tenant_id: &str,
) -> Result<QueryExecutionPlan, DomainError> {
    let entity_type = ast_ir
        .get("entity")
        .and_then(|v| v.as_str())
        .unwrap_or("events");
    let output_cast = ast_ir
        .get("output_cast")
        .and_then(|v| v.as_str())
        .unwrap_or("TABLE");

    let ts_field = resolve_ts_field(entity_type, ast_ir.get("schema"));

    let is_analytical = ["KPI", "PIE", "TIMESERIES", "BUBBLE"].contains(&output_cast)
        || ast_ir
            .get("metrics")
            .and_then(|v| v.as_array())
            .map(|arr| !arr.is_empty())
            .unwrap_or(false);

    let pull_pattern = if is_analytical {
        infer_required_fields(ast_ir, &ts_field)
    } else {
        if let Some(select) = ast_ir.get("select").and_then(|v| v.as_array()) {
            select
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            vec!["*".to_string()]
        }
    };

    let limit = ast_ir.get("limit").and_then(|v| v.as_u64()).unwrap_or(100);

    // Usar el PlanSelector para determinar la estrategia real
    let native_plan = compile_native_plan(ast_ir, tenant_id);
    let strategy = match native_plan {
        NativeQueryPlan::PointLookup { ref entity_id } => {
            IndexStrategy::PointLookup(entity_id.clone())
        }
        _ => {
            // Para los planes nativos, usamos AevtScan como fallback en el executor legacy
            // El router moderno usa execute_native_plan() directamente
            IndexStrategy::AevtScan { attr_id: 0 }
        }
    };

    debug!("[Aegis Compiler] QueryExecutionPlan generado para {entity_type}");

    Ok(QueryExecutionPlan {
        tenant_id: tenant_id.to_string(),
        entity_type: entity_type.to_string(),
        strategy,
        pull_pattern,
        limit,
    })
}

use crate::janus::fbs;

/// Compila AST IR FBS → NativeQueryPlan, resolviendo el TimeFrame via temporal canónico.
///
/// Si el `ast_ir` incluye un `time_frame` válido, el rango temporal resuelto
/// se loggea y está disponible como retorno opcional para el executor.
///
/// El NativeQueryPlan actual no lleva `time_range` como campo; el executor lo
/// usa desde `temporal_bridge::resolve_fbs_time_frame` directamente.
/// Esta función centraliza la selección del índice — la ventana temporal
/// se aplica como post-filtro en memoria en el executor OLTP.
pub fn compile_native_plan_fbs(
    ast_ir: &fbs::AnalyticsRequestT,
    tenant_id: &str,
) -> NativeQueryPlan {
    let abstract_plan = crate::janus::plan_selector::select_plan_fbs(ast_ir);

    // Resolver y loggear el TimeRange para trazabilidad
    if let Some(tf) = ast_ir.time_frame.as_ref() {
        if let Some(range) = resolve_fbs_time_frame(tf) {
            debug!(
                "[Aegis OLTP Compiler] TimeRange → start={:?} end={:?}",
                range.start_ts, range.end_ts
            );
        }
    }

    match abstract_plan {
        crate::janus::plan_selector::EavQueryPlan::PointLookup { entity_id } => {
            NativeQueryPlan::PointLookup { entity_id }
        }
        crate::janus::plan_selector::EavQueryPlan::AvetSingleFilter { attr_name, value } => {
            NativeQueryPlan::AvetSingle {
                tenant_id: tenant_id.to_string(),
                attr_name,
                value,
            }
        }
        crate::janus::plan_selector::EavQueryPlan::AvetIntersection { filters } => {
            NativeQueryPlan::AvetIntersect {
                tenant_id: tenant_id.to_string(),
                filters,
            }
        }
        crate::janus::plan_selector::EavQueryPlan::AevtScan {
            entity_type,
            shard_total: _,
        } => NativeQueryPlan::AevtScan {
            tenant_id: tenant_id.to_string(),
            entity_type,
        },
        crate::janus::plan_selector::EavQueryPlan::FtsSearch { term } => {
            NativeQueryPlan::FtsSearch {
                tenant_id: tenant_id.to_string(),
                term,
            }
        }
        crate::janus::plan_selector::EavQueryPlan::VaetLookup {
            ref_entity_id,
            attr_name,
        } => NativeQueryPlan::AvetSingle {
            tenant_id: tenant_id.to_string(),
            attr_name: attr_name.unwrap_or_default(),
            value: DatomValue::Str(ref_entity_id),
        },
        crate::janus::plan_selector::EavQueryPlan::AsOfSnapshot { .. } => {
            unreachable!("AsOfSnapshot se maneja en OltpExecutor directamente")
        }
    }
}

/// Genera las cláusulas Datalog temporales para el path OLTP a partir de un
/// `TimeRange` ya resuelto. Retorna Vec<String> listo para concat en la query.
///
/// Ejemplo de uso en el executor:
/// ```rust,ignore
/// let clauses = build_temporal_datalog_clauses(&time_range, "_created_at", 1);
/// // clauses = ["[?e :_created_at ?ts1]", "[>= ?ts1 1735689600000]", ...]
/// ```
pub fn build_temporal_datalog_clauses(
    time_range: &TimeRange,
    ts_field: &str,
    counter: usize,
) -> Vec<String> {
    match to_datalog_clauses(time_range, ts_field, counter) {
        Some(clause) => datalog_clause_to_parts(&clause),
        None => vec![],
    }
}
