// [PORTED_FROM: src/metri/janus/validator.clj]
// janus/validator.rs — Validador de contratos del AST IR.
// En Clojure: Malli Registry cargado desde resources/schema/janus-ast-ir.edn.
// En Rust: validación estructural con serde_json + reglas semánticas.
//
// "Defensa Inquebrantable": cualquier payload que viole el contrato
// es rechazado antes de llegar al motor EAV.

use serde_json::Value;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::janus::fbs::{AnalyticsRequestT, OutputCastType};

/// Valida que el tenant_id esté presente y no sea el placeholder "unknown-tenant".
/// [PORTED_FROM: (validate-tenant! tenant-id)]
pub fn validate_tenant(tenant_id: &str) -> Result<(), DomainError> {
    if tenant_id.is_empty() || tenant_id == "unknown-tenant" {
        return Err(DomainError::janus(
            ErrorCode::Janus400,
            format!("tenant_id vacío — Zero-Trust gate rechazó la request | tenant: '{tenant_id}'"),
        ));
    }
    Ok(())
}

/// Valida que el entity_type sea un string no vacío.
/// [PORTED_FROM: (m/validate :metri.spec/query-request ctx)]
pub fn validate_entity_type(entity_type: &str) -> Result<(), DomainError> {
    if entity_type.is_empty() {
        return Err(DomainError::janus(
            ErrorCode::JanusVal001,
            "entity_type vacío — Defensa Inquebrantable bloqueó la mutación",
        ));
    }
    Ok(())
}

/// Valida el contrato completo de una query request.
/// [PORTED_FROM: (validate! :metri.spec/query-request ctx)]
pub fn validate_query_request(payload: &Value) -> Result<(), DomainError> {
    let obj = payload.as_object().ok_or_else(|| {
        DomainError::janus(ErrorCode::JanusVal001, "payload no es un objeto JSON")
    })?;

    // tenant_id obligatorio
    let tenant_id = obj.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("");
    validate_tenant(tenant_id)?;

    // queries: mapa no vacío
    let queries = obj.get("queries");
    if queries
        .map(|q| {
            q.is_null()
                || (q.is_object() && q.as_object().map(|obj| obj.is_empty()).unwrap_or(true))
        })
        .unwrap_or(true)
    {
        return Err(DomainError::janus(
            ErrorCode::JanusVal001,
            "queries vacío — la request no tiene sub-queries definidas",
        ));
    }

    Ok(())
}

/// Valida un AnalyticsRequestT ya transmutado, asegurando integridad semántica.
/// Identifica errores de consistencia en el AST antes de la compilación.
pub fn validate_analytics_request_fbs(req: &AnalyticsRequestT) -> Result<(), DomainError> {
    validate_tenant(req.tenant_id.as_deref().unwrap_or(""))?;

    if req.entity.as_deref().unwrap_or("").is_empty() {
        return Err(DomainError::janus(
            ErrorCode::JanusVal001,
            "entity vacío — se requiere una entidad base (ej. 'work_order')",
        ));
    }

    let metrics_empty = req.metrics.as_ref().map(|v| v.is_empty()).unwrap_or(true);

    // Validar OutputCast vs Métricas
    if req.output_cast == OutputCastType::KPI && metrics_empty {
        return Err(DomainError::janus(
            ErrorCode::JanusVal001,
            "KPI output_cast requiere al menos 1 métrica definida",
        ));
    }

    // Validar que las métricas no tengan atributos vacíos
    if let Some(metrics) = &req.metrics {
        for m in metrics {
            if m.attribute.as_deref().unwrap_or("").is_empty() {
                return Err(DomainError::janus(
                    ErrorCode::JanusVal001,
                    "Métrica con 'attribute' vacío detectada",
                ));
            }
        }
    }

    // Validar dimensiones
    if let Some(dimensions) = &req.dimensions {
        for d in dimensions {
            if d.attribute.as_deref().unwrap_or("").is_empty() {
                return Err(DomainError::janus(
                    ErrorCode::JanusVal001,
                    "Dimensión con 'attribute' vacío detectada",
                ));
            }
        }
    }

    Ok(())
}

/// Valida el contrato de una transacción IOP (Create / Update / Delete).
/// [PORTED_FROM: (validate! :metri.spec/transaction-request ctx)]
pub fn validate_transaction_request(payload: &Value) -> Result<(), DomainError> {
    let obj = payload.as_object().ok_or_else(|| {
        DomainError::janus(
            ErrorCode::JanusVal001,
            "payload de transacción no es un objeto JSON",
        )
    })?;

    // tenant_id obligatorio
    let tenant_id = obj
        .get("tenant_id")
        .or_else(|| obj.get("tenant-id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    validate_tenant(tenant_id)?;

    // entity_type obligatorio
    let entity_type = obj
        .get("entity_type")
        .or_else(|| obj.get("entity-type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    validate_entity_type(entity_type)?;

    // operation obligatoria
    let operation = obj.get("operation").and_then(|v| v.as_str()).unwrap_or("");
    if operation.is_empty() {
        return Err(DomainError::janus(
            ErrorCode::JanusVal001,
            "operation vacía — se requiere CREATE | UPDATE | DELETE",
        ));
    }

    Ok(())
}
