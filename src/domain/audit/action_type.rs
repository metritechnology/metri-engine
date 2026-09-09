//! Pure (request, result) to ActionType derivation for the audit log.
//!
//! derive-action-type (fn pura).
//!
//! # Origin
//! En el stack anterior: (defn derive-action-type [request result] ...)
//!
//! Mapea (request, result) → ActionType para el audit_log OLAP.
//! Dominio puro: sin I/O, sin efectos secundarios.

/// Tipo de acción para el audit_log OLAP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionType {
    /// Escritura exitosa (Create / Update / Delete / BulkIngest)
    Write,
    /// Acceso denegado (Cedar → ABAC_401 | ABAC_403)
    AccessDenied,
    /// Quota agotada (QuotaGuard → QTA_001)
    QuotaExhausted,
    /// Error de escritura (Janus → JNS_VAL_001 | JNS_001)
    WriteError,
    /// Tipo desconocido — fallback seguro
    Unknown,
}

impl ActionType {
    /// Serialización para el stream Kinesis (columna `action_type`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionType::Write => "WRITE",
            ActionType::AccessDenied => "ACCESS_DENIED",
            ActionType::QuotaExhausted => "QUOTA_EXHAUSTED",
            ActionType::WriteError => "WRITE_ERROR",
            ActionType::Unknown => "UNKNOWN",
        }
    }
}

/// Derivación del ActionType desde (request, result).
/// Función pura — reemplaza (defn derive-action-type [request result]).
///
/// Lógica:
///   - Ok(...)           → Write
///   - Err stage=cedar   → AccessDenied
///   - Err stage=quota   → QuotaExhausted
///   - Err stage=janus   → WriteError
///   - Err otros         → Unknown
///
pub fn derive_action_type(succeeded: bool, error_stage: Option<&str>) -> ActionType {
    if succeeded {
        return ActionType::Write;
    }

    match error_stage {
        Some("cedar") | Some("auth") => ActionType::AccessDenied,
        Some("quota") => ActionType::QuotaExhausted,
        Some("janus") => ActionType::WriteError,
        _ => ActionType::Unknown,
    }
}

#[cfg(test)]
#[path = "../tests/action_type_tests.rs"]
mod tests;
