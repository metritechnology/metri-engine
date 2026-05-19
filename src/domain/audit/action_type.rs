// [PORTED_FROM: src/metri/domain/audit/action_type.clj]
// domain/audit/action_type.rs — derive-action-type (fn pura).
// En Clojure: (defn derive-action-type [request result] ...)
//
// Mapea (request, result) → ActionType para el audit_log OLAP.
// Dominio puro: sin I/O, sin efectos secundarios.

use serde_json::Value;

/// Tipo de acción para el audit_log OLAP.
/// [PORTED_FROM: (defn derive-action-type [request result])]
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
            ActionType::Write          => "WRITE",
            ActionType::AccessDenied   => "ACCESS_DENIED",
            ActionType::QuotaExhausted => "QUOTA_EXHAUSTED",
            ActionType::WriteError     => "WRITE_ERROR",
            ActionType::Unknown        => "UNKNOWN",
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
/// [PORTED_FROM: (defn derive-action-type [request result])]
pub fn derive_action_type(succeeded: bool, error_stage: Option<&str>) -> ActionType {
    if succeeded {
        return ActionType::Write;
    }

    match error_stage {
        Some("cedar") | Some("auth")  => ActionType::AccessDenied,
        Some("quota")                  => ActionType::QuotaExhausted,
        Some("janus")                  => ActionType::WriteError,
        _                              => ActionType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_is_write() {
        assert_eq!(derive_action_type(true, None), ActionType::Write);
    }

    #[test]
    fn cedar_deny_is_access_denied() {
        assert_eq!(derive_action_type(false, Some("cedar")), ActionType::AccessDenied);
    }

    #[test]
    fn quota_is_quota_exhausted() {
        assert_eq!(derive_action_type(false, Some("quota")), ActionType::QuotaExhausted);
    }

    #[test]
    fn janus_is_write_error() {
        assert_eq!(derive_action_type(false, Some("janus")), ActionType::WriteError);
    }

    #[test]
    fn unknown_stage_is_unknown() {
        assert_eq!(derive_action_type(false, Some("infra")), ActionType::Unknown);
        assert_eq!(derive_action_type(false, None), ActionType::Unknown);
    }
}
