// [PORTED_FROM: src/metri/iop/error_response.clj]
// iop/error_response.rs — DTO forense para errores de dominio → gRPC.
// En Clojure: sanitize-context + build-error-dto + OTel trace_id.
// En Rust: mismo contrato con otel::tracer::current_trace_id().

use serde_json::{json, Value};

use crate::domain::errors::DomainError;
use crate::codice::registry::EntityModel;
use crate::otel::tracer::current_trace_id;

/// Sanitiza el contexto del error eliminando campos sensibles del schema.
/// [PORTED_FROM: (sanitize-context context schema)]
/// Lee el flag :sensitive directamente del Códice — semántica exacta al Clojure.
fn sanitize_context(context: Value, model: Option<&EntityModel>) -> Value {
    let Some(model) = model else { return context; };
    let Some(obj) = context.as_object() else { return context; };

    // Campos marcados como sensitive:true en el Códice JSON.
    // [PORTED_FROM: (->> (:attributes schema) (filter :sensitive) (map (comp keyword :name)) set)]
    let sensitive_keys: std::collections::HashSet<&str> = model
        .attributes
        .iter()
        .filter(|a| a.sensitive)
        .map(|a| a.name.as_str())
        .collect();

    let sanitized = obj
        .iter()
        .map(|(k, v)| {
            if sensitive_keys.contains(k.as_str()) {
                (k.clone(), Value::String("[REDACTED]".to_string()))
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect::<serde_json::Map<_, _>>();

    Value::Object(sanitized)
}

/// Construye el DTO forense estructurado para una respuesta de error.
/// Extrae trace_id y span_id del contexto OTel activo.
///
/// [PORTED_FROM: (build-error-dto error-map ctx schema)]
pub fn build_error_dto(
    error:    &DomainError,
    tenant_id: &str,
    user_id:  &str,
    context:  Option<Value>,
    model:    Option<&EntityModel>,
) -> Value {
    let trace_id     = current_trace_id();
    let correlation  = format!("REQ-{}", &trace_id[..8.min(trace_id.len())]);
    let error_code   = format!("{:?}", error.code);
    let description  = error.to_string();
    let stage        = error.stage.clone();
    let retryable    = error.retryable;
    let timestamp    = chrono::Utc::now().timestamp_millis();

    let sanitized_ctx = context
        .map(|c| sanitize_context(c, model))
        .unwrap_or(Value::Null);

    // [PORTED_FROM: {:status "error" :error {...}}]
    json!({
        "status": "error",
        "error": {
            "code":           error_code,
            "description":    description,
            "trace_id":       trace_id,
            "span_id":        "0000000000000000",  // FASE 2: extraer de OTel
            "correlation_id": correlation,
            "tenant_id":      tenant_id,
            "user_id":        user_id,
            "timestamp":      timestamp,
            "stage":          stage,
            "retryable":      retryable,
            "context":        sanitized_ctx,
        }
    })
}
