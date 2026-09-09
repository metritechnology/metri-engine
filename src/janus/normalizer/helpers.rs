//! Shared normalizer helpers — safe conversions and derived labels.
use serde_json::{json, Value};
use uuid::Uuid;

/// Convierte cualquier valor numérico a f64 de forma segura.
pub fn safe_double(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        Value::Null => 0.0,
        _ => 0.0,
    }
}

/// Genera un nuevo query_id UUID.
pub fn new_query_id() -> String {
    Uuid::new_v4().to_string()
}

/// Garantiza que :status esté presente en cualquier body.
///
/// Un body no-objeto (escalar/array) no admite la clave `:status`: se respeta
/// su forma y se retorna sin cambios — nunca pánico (R2).
pub fn ensure_status(body: &mut Value, success: bool) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    if !obj.contains_key("status") {
        if success {
            obj.insert("status".to_string(), json!({"success": true}));
        } else {
            let error_code = obj
                .get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("INTERNAL_ERROR");
            let error_message = obj
                .get("reason")
                .and_then(|v| v.as_str())
                .or_else(|| obj.get("detail").and_then(|v| v.as_str()))
                .unwrap_or("Error interno");
            obj.insert(
                "status".to_string(),
                json!({
                    "success":       false,
                    "error_code":    error_code,
                    "error_message": error_message,
                }),
            );
        }
    }
}

/// Infiere el tipo de visualización desde output_cast o viz_hint.
pub fn infer_viz_type(output_cast: Option<&str>, viz_hint: Option<&str>) -> &'static str {
    if let Some(hint) = viz_hint {
        return hint_to_static(hint);
    }
    match output_cast {
        Some("KPI") => "indicator",
        Some("PIE") => "pie",
        Some("TIMESERIES") => "line",
        Some("BUBBLE") => "scatter",
        Some("TABLE") => "table",
        Some("CSV_EXPORT") => "table",
        _ => "table",
    }
}

pub fn hint_to_static(hint: &str) -> &'static str {
    match hint {
        "indicator" | "kpi" | "gauge" => "indicator",
        "pie" | "donut" => "pie",
        "line" => "line",
        "bar" => "bar",
        "area" => "area",
        "scatter" => "scatter",
        "timeseries" => "line",
        "tree" => "tree",
        _ => "table",
    }
}

pub fn capitalize_key(key: &str) -> String {
    key.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn derive_semantic_label(key: &str, label_template: Option<&str>) -> String {
    if let Some(template) = label_template {
        if !template.is_empty() {
            if let Some(pos) = template.find("{{") {
                let prefix = &template[..pos];
                let trimmed = prefix.trim().trim_end_matches(':').trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            } else {
                return template.to_string();
            }
        }
    }
    capitalize_key(key)
}
