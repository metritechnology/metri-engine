// grpc/error_status.rs — Traducción única `DomainError` → `tonic::Status` (R6).
//
// Este `From` es el ÚNICO sitio del crate que convierte un error de dominio a
// estado gRPC: consulta `ErrorCatalog` con el `canonical_code` y toma de allí
// `grpc_status`. Los handlers devuelven `?` hasta el borde y aquí se traduce.
// Prohibido el mapeo manual disperso (`Status::invalid_argument(...)`) para
// errores que ya son `DomainError` — PLAN_PATRON_RESULT.md §2, regla R6.

use crate::domain::error_catalog;
use crate::domain::errors::DomainError;
use tonic::{Code, Status};

/// Mapea el `grpc_status` del catálogo TOML a `tonic::Code`.
fn tonic_code(grpc_status: &str) -> Code {
    match grpc_status {
        "OK" => Code::Ok,
        "INVALID_ARGUMENT" => Code::InvalidArgument,
        "NOT_FOUND" => Code::NotFound,
        "ALREADY_EXISTS" => Code::AlreadyExists,
        "RESOURCE_EXHAUSTED" => Code::ResourceExhausted,
        "FAILED_PRECONDITION" => Code::FailedPrecondition,
        "ABORTED" => Code::Aborted,
        "OUT_OF_RANGE" => Code::OutOfRange,
        "UNIMPLEMENTED" => Code::Unimplemented,
        "PERMISSION_DENIED" => Code::PermissionDenied,
        "UNAUTHENTICATED" => Code::Unauthenticated,
        "DEADLINE_EXCEEDED" => Code::DeadlineExceeded,
        "UNAVAILABLE" => Code::Unavailable,
        "DATA_LOSS" => Code::DataLoss,
        // Código desconocido en el catálogo: fallar como INTERNAL, nunca paniquear.
        _ => Code::Internal,
    }
}

impl From<DomainError> for Status {
    fn from(err: DomainError) -> Self {
        let grpc_status = error_catalog::try_global()
            .and_then(|catalog| catalog.get(err.code.canonical_code()))
            .map(|entry| entry.grpc_status.clone())
            .unwrap_or_else(|| "INTERNAL".to_string());
        let payload = serde_json::json!({
            "canonical_code": err.code.canonical_code(),
            "stage":          err.stage,
            "detail":         err.detail,
            "retryable":      err.retryable,
        });
        Status::new(tonic_code(&grpc_status), payload.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::errors::ErrorCode;
    use std::sync::Once;

    static CATALOG_ONCE: Once = Once::new();

    fn ensure_catalog() {
        CATALOG_ONCE.call_once(|| {
            let catalog = crate::domain::ErrorCatalog::load("config/errors/error_catalog.toml")
                .expect("catálogo de test");
            crate::domain::init_error_catalog(catalog);
        });
    }

    /// I6 — TODAS las variantes se traducen según el catálogo, sin excepciones
    /// ni drift hacia INTERNAL.
    #[test]
    fn status_map_usa_el_catalogo_para_todas_las_variantes() {
        ensure_catalog();
        let catalog = crate::domain::error_catalog::global();
        for code in ErrorCode::ALL {
            let entry = catalog
                .get(code.canonical_code())
                .unwrap_or_else(|| panic!("C2: falta {} en el catálogo", code.canonical_code()));
            let expected = tonic_code(&entry.grpc_status);
            let status = Status::from(DomainError::new(code.clone(), "guard"));
            assert_eq!(status.code(), expected, "{code:?} → {}", entry.grpc_status);
        }
    }

    /// Anclas no circulares: mapeos conocidos hardcodeados.
    #[test]
    fn mapeos_conocidos() {
        ensure_catalog();
        assert_eq!(
            Status::from(DomainError::new(ErrorCode::Quota001, "x")).code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            Status::from(DomainError::new(ErrorCode::Auth401, "x")).code(),
            Code::Unauthenticated
        );
        assert_eq!(
            Status::from(DomainError::new(ErrorCode::Aeg003, "x")).code(),
            Code::DeadlineExceeded
        );
        assert_eq!(
            Status::from(DomainError::new(ErrorCode::JnsConflict001, "x")).code(),
            Code::Aborted
        );
        assert_eq!(
            Status::from(DomainError::new(ErrorCode::Eav003, "x")).code(),
            Code::Aborted
        );
    }
}
