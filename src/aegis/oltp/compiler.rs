//! OLTP compiler — AST IR to physical EAV plan.
//!
//! Compilador OLTP: AST IR → plan físico EAV (`NativeQueryPlan`).
//!
//! Conecta la salida del PlanSelector (`janus::plan_selector::EavQueryPlan`)
//! con el ejecutor físico (`eav::reader::query::EavQueryExecutor`).

use serde_json::Value;
use tracing::debug;

use crate::aegis::temporal_bridge::resolve_fbs_time_frame;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::query::NativeQueryPlan;
use crate::eav::types::datom::DatomValue;
use crate::janus::fbs;
use crate::janus::plan_selector::{select_plan, EavQueryPlan};

// ── Helpers internos ──────────────────────────────────────────────────────────

/// Recupera el campo de tipo 'epoch' desde el esquema para usar como timestamp de serie temporal.
pub fn resolve_ts_field(entity_type: &str, schema: Option<&Value>) -> String {
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

        EavQueryPlan::VaetLookup { ref_entity_id, .. } => NativeQueryPlan::PointLookup {
            entity_id: ref_entity_id,
        },

        EavQueryPlan::AsOfSnapshot { entity_id, .. } => {
            // AsOf usa el pull_as_of reader — el router lo gestiona directamente
            NativeQueryPlan::PointLookup { entity_id }
        }
    }
}

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
) -> Result<NativeQueryPlan, DomainError> {
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
            Ok(NativeQueryPlan::PointLookup { entity_id })
        }
        crate::janus::plan_selector::EavQueryPlan::AvetSingleFilter { attr_name, value } => {
            Ok(NativeQueryPlan::AvetSingle {
                tenant_id: tenant_id.to_string(),
                attr_name,
                value,
            })
        }
        crate::janus::plan_selector::EavQueryPlan::AvetIntersection { filters } => {
            Ok(NativeQueryPlan::AvetIntersect {
                tenant_id: tenant_id.to_string(),
                filters,
            })
        }
        crate::janus::plan_selector::EavQueryPlan::AevtScan {
            entity_type,
            shard_total: _,
        } => Ok(NativeQueryPlan::AevtScan {
            tenant_id: tenant_id.to_string(),
            entity_type,
        }),
        crate::janus::plan_selector::EavQueryPlan::FtsSearch { term } => {
            Ok(NativeQueryPlan::FtsSearch {
                tenant_id: tenant_id.to_string(),
                term,
            })
        }
        crate::janus::plan_selector::EavQueryPlan::VaetLookup {
            ref_entity_id,
            attr_name,
        } => Ok(NativeQueryPlan::AvetSingle {
            tenant_id: tenant_id.to_string(),
            attr_name: attr_name.unwrap_or_default(),
            value: DatomValue::Str(ref_entity_id),
        }),
        // AsOfSnapshot se maneja en OltpExecutor (fetch_as_of_snapshot) antes
        // de llegar aquí. Este brazo es defensivo ante un cambio del plan
        // selector: error de compilación, nunca pánico (R3).
        crate::janus::plan_selector::EavQueryPlan::AsOfSnapshot { .. } => Err(DomainError::aegis(
            ErrorCode::Aeg001,
            "AsOfSnapshot debe manejarse vía fetch_as_of_snapshot — contrato del plan selector",
        )),
    }
}
