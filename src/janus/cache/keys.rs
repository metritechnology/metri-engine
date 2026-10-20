//! Constructor de claves de caché (`keys`) — resumen SHA-256 canónico de la consulta compilada.
//!
//! La clave se calcula como el SHA-256 del JSON canónico del **AST ya compilado**
//! (con `tenant_id` y cláusulas ABAC inyectadas por `ast_compiler`) más la ventana temporal
//! resuelta y el fingerprint del esquema del Códice. Así, la dependencia del resultado
//! respecto a `(tenant, usuario, scope)` queda capturada por construcción: dos usuarios
//! con alcances distintos producen ASTs distintos y claves distintas.
//!
//! `serde_json` utiliza `BTreeMap` para `Map` (feature `preserve_order` desactivada),
//! por lo que la serialización ordena las claves de forma determinista.

use sha2::{Digest, Sha256};

use crate::janus::fbs::AnalyticsRequestT;
use crate::janus::router::translator::analytics_request_to_json;
use crate::temporal::core::TimeRange;

/// Versión del namespace de claves. Bump manual cuando cambie la semántica
/// de canonicalización — invalida todo el namespace sin tocar la tabla.
pub const CACHE_KEY_VERSION: &str = "qc1";

/// Prefijo del PK de las entradas de caché en la tabla DynamoDB.
pub const ENTRY_KEY_PREFIX: &str = "QC";

fn digest(canonical: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

fn window_json(window: &TimeRange) -> serde_json::Value {
    serde_json::json!([window.start_ts, window.end_ts])
}

/// Clave para el canal OLTP: AST compilado post-ABAC + ventana resuelta +
/// fingerprint de schema del Códice + tenant.
pub fn oltp_key(
    ast: &AnalyticsRequestT,
    window: &TimeRange,
    schema_fingerprint: &str,
    tenant_id: &str,
) -> String {
    let canonical = serde_json::json!({
        "ns":     CACHE_KEY_VERSION,
        "ch":     "oltp",
        "tenant": tenant_id,
        "ast":    analytics_request_to_json(ast),
        "win":    window_json(window),
        "schema": schema_fingerprint,
    });
    format!(
        "{ENTRY_KEY_PREFIX}#{CACHE_KEY_VERSION}#{}",
        digest(&canonical)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(start: i64, end: i64) -> TimeRange {
        TimeRange {
            start_ts: Some(start),
            end_ts: Some(end),
        }
    }

    #[test]
    fn key_is_deterministic() {
        let ast = AnalyticsRequestT::default();
        let a = oltp_key(&ast, &window(1, 2), "fp", "t1");
        let b = oltp_key(&ast, &window(1, 2), "fp", "t1");
        assert_eq!(a, b);
        assert!(a.starts_with("QC#qc1#"));
    }

    #[test]
    fn key_changes_with_tenant() {
        let ast = AnalyticsRequestT::default();
        assert_ne!(
            oltp_key(&ast, &window(1, 2), "fp", "t1"),
            oltp_key(&ast, &window(1, 2), "fp", "t2")
        );
    }

    #[test]
    fn key_changes_with_window() {
        let ast = AnalyticsRequestT::default();
        assert_ne!(
            oltp_key(&ast, &window(1, 2), "fp", "t1"),
            oltp_key(&ast, &window(2, 3), "fp", "t1")
        );
    }

    #[test]
    fn key_changes_with_schema_fingerprint() {
        let ast = AnalyticsRequestT::default();
        assert_ne!(
            oltp_key(&ast, &window(1, 2), "fp1", "t1"),
            oltp_key(&ast, &window(1, 2), "fp2", "t1")
        );
    }
}
