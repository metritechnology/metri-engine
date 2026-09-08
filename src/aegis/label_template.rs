// aegis/label_template.rs — Interpolación de label templates Mustache-style.
// SRP: resolución pura de templates — sin I/O, sin estado.

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;

/// Regex que captura {{campo}} en cualquier posición del template.
#[allow(clippy::unwrap_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
static PLACEHOLDER_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{\{([^}]+)\}\}").unwrap());

/// Busca un campo en el row tolerando diferencias de tipos (string/keyword) o estructura.
fn coerce_key<'a>(row: &'a Value, field_str: &str) -> Option<&'a Value> {
    if let Some(obj) = row.as_object() {
        if let Some(v) = obj.get(field_str) {
            return Some(v);
        }
        // Intentar sin namespace si lo hubiera, ej. `entity/asset_name` -> `asset_name`
        let base_field = field_str.split('/').next_back().unwrap_or(field_str);
        if let Some(v) = obj.get(base_field) {
            return Some(v);
        }
        for (k, v) in obj {
            if k == field_str || k.ends_with(&format!("/{field_str}")) {
                return Some(v);
            }
        }
    }
    None
}

/// Formatea un valor para display en un label.
/// - null      -> ""
/// - entero    -> "42"
/// - decimal   -> "45.23" (2 decimales max)
/// - string    -> valor directo
///   Resuelve un camino (por ejemplo "location.name") navegando en objetos JSON.
fn resolve_path<'a>(mut current: &'a Value, path: &str) -> Option<&'a Value> {
    // Si la clave entera coincide directamente en el valor actual, úsala directamente.
    if let Some(v) = coerce_key(current, path) {
        return Some(v);
    }

    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() > 1 {
        for part in parts {
            current = coerce_key(current, part)?;
        }
        Some(current)
    } else {
        None
    }
}

/// Formatea un valor para display en un label.
/// - null      -> ""
/// - entero    -> "42"
/// - decimal   -> "45.23" (2 decimales max)
/// - string    -> valor directo
pub(crate) fn format_value(v: Option<&Value>) -> String {
    match v {
        Some(Value::Null) | None => "".to_string(),
        Some(Value::Number(n)) => {
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 {
                    format!("{f}")
                } else {
                    format!("{f:.2}")
                }
            } else {
                n.to_string()
            }
        }
        Some(Value::String(s)) => s.clone(),
        Some(val) => val.to_string(),
    }
}

/// Interpola un template string contra un data row.
/// Template: '{{asset_name}} - {{area_value}} KW'
/// Row:      {"asset_name": "Pump A", "area_value": 45.2}
/// Retorna:  'Pump A - 45.2 KW'
pub fn interpolate(template: &str, row: &Value) -> Option<String> {
    if template.trim().is_empty() {
        return None;
    }

    let result = PLACEHOLDER_PATTERN.replace_all(template, |caps: &regex::Captures| {
        let field_str = caps[1].trim();
        let val = resolve_path(row, field_str).or_else(|| coerce_key(row, field_str));
        format_value(val)
    });

    Some(result.into_owned())
}

/// Aplica `interpolate` a todos los rows de un vector de JSON values.
/// Añade la key `_label` a cada row con el label resuelto.
#[allow(dead_code)]
pub fn interpolate_rows(rows: &mut [Value], template: &str) {
    if template.trim().is_empty() {
        return;
    }

    for row in rows.iter_mut() {
        if let Some(lbl) = interpolate(template, row) {
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_label".to_string(), Value::String(lbl));
            }
        }
    }
}

/// Extrae los nombres de campo referenciados en un template.
#[allow(dead_code)]
pub fn extract_fields(template: &str) -> Vec<String> {
    if template.trim().is_empty() {
        return vec![];
    }

    PLACEHOLDER_PATTERN
        .captures_iter(template)
        .map(|caps| caps[1].trim().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_dotted_path_interpolation() {
        let row = json!({
            "asset/name": "Pump Alpha",
            "location_id": {
                "id": "loc-123",
                "name": "Warehouse A"
            },
            "status": "ACTIVE"
        });

        // Test normal keys
        assert_eq!(
            interpolate("{{asset/name}}", &row),
            Some("Pump Alpha".to_string())
        );
        assert_eq!(interpolate("{{status}}", &row), Some("ACTIVE".to_string()));

        // Test dotted path keys (canonical)
        assert_eq!(
            interpolate("{{location_id.name}}", &row),
            Some("Warehouse A".to_string())
        );
        assert_eq!(
            interpolate("{{location_id.id}}", &row),
            Some("loc-123".to_string())
        );

        // Test dotted path keys (aliased/fallback via coerce_key)
        assert_eq!(
            interpolate("{{location_id.name}} - {{status}}", &row),
            Some("Warehouse A - ACTIVE".to_string())
        );
    }
}
