// janus/partition.rs — Estrategias de particionamiento S3/Hive para OLAP.
// Dominio puro — sin I/O, sin infra. Evaluación de paths en memoria.
//
// Soporta las estrategias de particionamiento Hive:
//   "YYYY-MM-DD/HH" → "year=2024/month=01/day=15/hour=09"
//   "YYYY-MM-DD"    → "year=2024/month=01/day=15"
//   "YYYY-MM"       → "year=2024/month=01"
//   "YYYY"          → "year=2024"
//   + sustitución dinámica de {atributo} → atributo=valor (sanitizado)

use chrono::{DateTime, TimeZone, Utc};

/// Pre-evalúa la parte estática de la estrategia (fechas UTC) una sola vez por batch.
///
pub fn pre_evaluate_date_strategy(strategy: Option<&str>, timestamp_ms: i64) -> String {
    let base = strategy.unwrap_or("YYYY-MM-DD");

    let dt: DateTime<Utc> = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(Utc::now);

    let yyyy = format!("{:04}", dt.format("%Y"));
    let mm = format!("{:02}", dt.format("%m"));
    let dd = format!("{:02}", dt.format("%d"));
    let hh = format!("{:02}", dt.format("%H"));

    // Reemplazos en orden — de más específico a menos específico.
    base.replace(
        "YYYY-MM-DD/HH",
        &format!("year={yyyy}/month={mm}/day={dd}/hour={hh}"),
    )
    .replace("YYYY-MM-DD", &format!("year={yyyy}/month={mm}/day={dd}"))
    .replace("YYYY-MM", &format!("year={yyyy}/month={mm}"))
    .replace("YYYY", &format!("year={yyyy}"))
}

/// Evalúa solo los atributos dinámicos del registro sobre la estrategia pre-calculada.
///
/// Sanitización estricta: previene Path Traversal (../) e inyección S3.
/// Solo permite [a-zA-Z0-9\-_] — el resto se reemplaza con '_'.
///
/// Placeholder `{atributo}` en la estrategia de particionado — patrón literal.
#[allow(clippy::expect_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
static PATH_PLACEHOLDER_RE: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| {
        regex::Regex::new(r"\{([^}]+)\}").expect("patrón literal de regex válida")
    });

pub fn build_dynamic_path(date_strategy: &str, record: &serde_json::Value) -> String {
    // Regex: {atributo} → atributo=valor_sanitizado
    let result = PATH_PLACEHOLDER_RE.replace_all(date_strategy, |caps: &regex::Captures| {
        let key = &caps[1];
        let raw = record
            .get(key)
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    v.to_string()
                }
            })
            .unwrap_or_else(|| "UNKNOWN".to_string());

        // Sanitización: solo [a-zA-Z0-9\-_]
        let safe: String = raw
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        format!("{key}={safe}")
    });

    result.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn date_strategy_daily() {
        // 2024-01-15 09:30 UTC — epoch ms
        let ts = 1705310200000_i64;
        let path = pre_evaluate_date_strategy(Some("YYYY-MM-DD"), ts);
        assert_eq!(path, "year=2024/month=01/day=15");
    }

    #[test]
    fn date_strategy_hourly() {
        let ts = 1705310200000_i64;
        let path = pre_evaluate_date_strategy(Some("YYYY-MM-DD/HH"), ts);
        assert!(
            path.starts_with("year=2024/month=01/day=15/hour="),
            "{path}"
        );
    }

    #[test]
    fn dynamic_path_sanitizes_unsafe_chars() {
        let record = json!({"tenant": "acme/../evil", "status": "active"});
        let path = build_dynamic_path("data/{tenant}/{status}", &record);
        // Los '/' y '.' en los VALORES deben convertirse en '_'
        // (los '/' del template estático son separadores intencionales de Hive)
        assert!(!path.contains(".."), "Path traversal no permitido: {path}");
        // Verificar que los valores sanitizados no contienen '/' (solo los separadores estáticos pueden)
        let tenant_segment = path
            .split('/')
            .find(|s| s.starts_with("tenant="))
            .unwrap_or("");
        assert!(
            !tenant_segment.contains('/'),
            "Slash en valor de tenant no permitido: {tenant_segment}"
        );
        assert!(
            path.contains("tenant=acme____evil"),
            "El valor debe estar sanitizado: {path}"
        );
    }

    #[test]
    fn dynamic_path_unknown_key() {
        let record = json!({"tenant": "acme"});
        let path = build_dynamic_path("{tenant}/{missing}", &record);
        assert!(path.contains("missing=UNKNOWN"), "{path}");
    }
}
