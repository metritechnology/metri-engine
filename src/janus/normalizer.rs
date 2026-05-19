// [PORTED_FROM: src/metri/janus/normalizer.clj]
// janus/normalizer.rs — Janus Output Contract Guarantor.
// Cubre los 7 tipos de respuesta del contrato metres.proto.
// SRP: único módulo responsable del contrato de salida.
// OCP: nuevo tipo = nuevo match arm, sin modificar el resto.

use serde_json::{json, Value};
use tracing::error;
use uuid::Uuid;

/// Tipo de respuesta — dispatch equivalente al defmulti de Clojure.
/// [PORTED_FROM: (defmulti normalize-response (fn [[_tag body]] ...))]
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseType {
    Query,
    Discovery,
    Explore,
    Transaction,
    Bulk,
    MatchRules,
    MatchRulesBatch,
    Default,
}

impl ResponseType {
    /// Infiere el tipo desde las claves del body.
    /// [PORTED_FROM: (cond (contains? body :data) :query-response ...)]
    pub fn infer(body: &Value) -> Self {
        let obj = match body.as_object() {
            Some(o) => o,
            None    => return ResponseType::Default,
        };
        if obj.contains_key("data") || obj.contains_key("viz-ext") || obj.contains_key("query_key") {
            ResponseType::Query
        } else if obj.contains_key("schemas") {
            ResponseType::Discovery
        } else if obj.contains_key("values") {
            ResponseType::Explore
        } else if obj.contains_key("entity_id") || obj.contains_key("entity-id") {
            ResponseType::Transaction
        } else if obj.contains_key("ingested_count") || obj.contains_key("outbox_count") {
            ResponseType::Bulk
        } else if obj.contains_key("responses") {
            ResponseType::MatchRulesBatch
        } else if obj.contains_key("matched_rules") {
            ResponseType::MatchRules
        } else {
            ResponseType::Default
        }
    }
}

// ── Helpers compartidos ──────────────────────────────────────────────────────

/// Convierte cualquier valor numérico a f64 de forma segura.
/// [PORTED_FROM: (safe-double v)]
fn safe_double(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        Value::Null      => 0.0,
        _                => 0.0,
    }
}

/// Genera un nuevo query_id UUID.
/// [PORTED_FROM: (new-query-id)]
fn new_query_id() -> String {
    Uuid::new_v4().to_string()
}

// ── Status garantizado ───────────────────────────────────────────────────────

/// Garantiza que :status esté presente en cualquier body.
/// [PORTED_FROM: (ensure-status body tag)]
pub fn ensure_status(body: &mut Value, success: bool) {
    let obj = body.as_object_mut().expect("body debe ser un objeto JSON");
    if !obj.contains_key("status") {
        if success {
            obj.insert("status".to_string(), json!({"success": true}));
        } else {
            let error_code   = obj.get("code").and_then(|v| v.as_str()).unwrap_or("INTERNAL_ERROR");
            let error_message = obj.get("reason").and_then(|v| v.as_str())
                .or_else(|| obj.get("detail").and_then(|v| v.as_str()))
                .unwrap_or("Error interno");
            obj.insert("status".to_string(), json!({
                "success":       false,
                "error_code":    error_code,
                "error_message": error_message,
            }));
        }
    }
}

// ── QueryResponse helpers ────────────────────────────────────────────────────

/// Garantiza :metadata en un chunk de QueryResponse.
/// [PORTED_FROM: (ensure-metadata chunk)]
fn ensure_metadata(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    if !obj.contains_key("metadata") {
        let channel   = obj.get("channel").and_then(|v| v.as_str()).unwrap_or("unknown");
        let engine    = match channel { "olap" => "olap", "oltp" => "oltp", _ => "unknown" };
        let total     = obj.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        obj.insert("metadata".to_string(), json!({
            "engine":             engine,
            "query_id":           new_query_id(),
            "total_count":        total,
            "total_queries":      1,
            "parallelism_factor": 1.0,
            "cache_hits":         0,
        }));
    }
}

/// Garantiza :pagination básica.
/// [PORTED_FROM: (ensure-pagination chunk)]
fn ensure_pagination(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    if !obj.contains_key("pagination") {
        let total = obj.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        obj.insert("pagination".to_string(), json!({
            "page_size":    0,
            "has_next":     false,
            "has_previous": false,
            "links": [
                {"rel": "first",   "href": "/query", "method": "POST"},
                {"rel": "last",    "href": "/query", "method": "POST"},
            ]
        }));
    }
}

/// Inffiere el tipo de visualización desde output_cast o viz_hint.
/// [PORTED_FROM: (infer-viz-type output-cast viz-hint)]
fn infer_viz_type(output_cast: Option<&str>, viz_hint: Option<&str>) -> &'static str {
    if let Some(hint) = viz_hint { return hint_to_static(hint); }
    match output_cast {
        Some("KPI")        => "indicator",
        Some("PIE")        => "pie",
        Some("TIMESERIES") => "line",
        Some("BUBBLE")     => "scatter",
        Some("TABLE")      => "table",
        Some("CSV_EXPORT") => "table",
        _                  => "table",
    }
}

fn hint_to_static(hint: &str) -> &'static str {
    match hint {
        "indicator" | "kpi" | "gauge" => "indicator",
        "pie" | "donut"               => "pie",
        "line"                        => "line",
        "bar"                         => "bar",
        "scatter"                     => "scatter",
        "timeseries"                  => "line",
        "tree"                        => "tree",
        _                             => "table",
    }
}

/// Garantiza :viz_ext en el chunk con el tipo de visualización correcto.
/// [PORTED_FROM: (ensure-viz-meta chunk)]
fn ensure_viz_meta(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    if obj.contains_key("viz_ext") { return; }

    let output_cast = obj.get("output_cast").and_then(|v| v.as_str()).map(str::to_string);
    let viz_hint    = obj.get("viz").and_then(|v| v.as_str()).map(str::to_string);
    let viz_type    = infer_viz_type(output_cast.as_deref(), viz_hint.as_deref());

    let rows    = obj.get("data").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let columns = obj.get("columns").and_then(|v| v.as_array()).cloned().unwrap_or_default();

    let payload: Value = match viz_type {
        "indicator" | "kpi" | "gauge" => {
            // [PORTED_FROM: :signal {:value (safe-double v)}]
            let val = rows.first()
                .and_then(|r| r.as_array())
                .and_then(|r| r.first())
                .or_else(|| rows.first().and_then(|r| r.as_object()).and_then(|m| m.values().next()))
                .map(|v| safe_double(v))
                .unwrap_or(0.0);
            json!({"signal": {"value": val}})
        }
        "pie" | "donut" => {
            // [PORTED_FROM: :breakdown {:signals {...}}]
            // Key = valor real de la primera columna dimensión (no "slice_N" sintético)
            let signals: serde_json::Map<String, Value> = rows.iter().enumerate().map(|(i, row)| {
                let key = if let Some(arr) = row.as_array() {
                    arr.first()
                        .and_then(|v| v.as_str().map(str::to_string)
                            .or_else(|| if v.is_null() { None } else { Some(v.to_string()) }))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| format!("slice_{i}"))
                } else if let Some(obj) = row.as_object() {
                    obj.values().next()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_else(|| format!("slice_{i}"))
                } else {
                    format!("slice_{i}")
                };
                let val = if let Some(arr) = row.as_array() {
                    arr.get(1).map(|v| safe_double(v)).unwrap_or(0.0)
                } else { 0.0 };
                (key, json!({"value": val}))
            }).collect();
            json!({"breakdown": {"signals": signals}})
        }
        "line" | "bar" | "area" | "scatter" | "timeseries" => {
            // [PORTED_FROM: :chart {:x-dimension col1 :y-dimensions [col2...]}]
            let col_names: Vec<_> = columns.iter()
                .filter_map(|c| c.get("key").and_then(|v| v.as_str()).map(str::to_string))
                .collect();
            let x_dim  = col_names.first().cloned().unwrap_or_default();
            let y_dims  = col_names.into_iter().skip(1).collect::<Vec<_>>();
            let fill_gaps = viz_type == "timeseries" || output_cast.as_deref() == Some("TIMESERIES");
            let mut chart_meta = json!({
                "x_dimension":  x_dim,
                "y_dimensions": y_dims,
                "show_tooltip": true,
                "show_legend":  true,
            });
            if fill_gaps {
                chart_meta["fill_gaps"] = json!(true);
            }
            json!({"chart": chart_meta})
        }
        "tree" => {
            let col_names: Vec<String> = columns.iter()
                .filter_map(|c| c.get("key").and_then(|v| v.as_str()).map(str::to_string))
                .collect();

            // 1. parent_id_key: prefer hierarchy.parent_field from router body,
            //    fall back to column name convention (parent_*_id), then "parent_id".
            let parent_key = obj.get("hierarchy")
                .and_then(|h| h.get("parent_field"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    col_names.iter()
                        .find(|c| c.starts_with("parent_") && c.ends_with("_id"))
                        .cloned()
                        .unwrap_or_else(|| "parent_id".to_string())
                });

            // 2. label_key: pick the first of these candidates that exists in columns.
            let label_key = ["name", "title", "label", "description"].iter()
                .find(|k| col_names.contains(&k.to_string()))
                .map(|s| s.to_string())
                .unwrap_or_else(|| col_names.first().cloned().unwrap_or_else(|| "name".to_string()));

            // 3. icon_key: only set if the column actually exists in the result set.
            let icon_key = if col_names.contains(&"icon".to_string()) {
                "icon".to_string()
            } else {
                String::new()
            };

            let mut tree_meta = json!({
                "id_key": "id",
                "parent_id_key": parent_key,
                "label_key": label_key,
                "has_children_key": "has_children",
            });
            if !icon_key.is_empty() {
                tree_meta["icon_key"] = json!(icon_key);
            }
            json!({"tree": tree_meta})
        }
        _ => {
            // Table
            // [PORTED_FROM: :table (build-table-meta columns)]
            let cols: Vec<Value> = columns.iter().map(|c| {
                let key = c.get("key").and_then(|v| v.as_str()).unwrap_or("");
                json!({"key": key, "label": key, "sortable": true, "type": "string"})
            }).collect();
            json!({"table": {"columns": cols, "row_actions": [], "global_links": []}})
        }
    };

    obj.insert("viz_ext".to_string(), json!({"type": viz_type, "payload": payload}));
}

/// Enriquece la inteligencia TIME_SHIFT o BENCHMARK.
/// [PORTED_FROM: (enrich-viz-intelligence chunk)]
fn enrich_viz_intelligence(body: &mut Value) {
    let signal_val = body
        .pointer("/viz_ext/payload/signal/value")
        .and_then(|v| v.as_f64());
    let prev_val = body
        .pointer("/viz_ext/payload/signal/previous_value")
        .and_then(|v| v.as_f64());
    let already_has = body
        .pointer("/viz_ext/payload/signal/intelligence")
        .is_some();

    if already_has { return; }

    if let (Some(curr), Some(prev)) = (signal_val, prev_val) {
        let delta = curr - prev;
        let pct   = if prev == 0.0 {
            if curr > 0.0 { 100.0 } else if curr < 0.0 { -100.0 } else { 0.0 }
        } else {
            100.0 * delta / prev.abs()
        };
        let dir = if delta > 0.001 { "up" } else if delta < -0.001 { "down" } else { "neutral" };
        let label = format!("{}{:.1}%", if pct > 0.0 { "+" } else { "" }, pct);

        if let Some(obj) = body.pointer_mut("/viz_ext/payload/signal") {
            if let Some(m) = obj.as_object_mut() {
                m.insert("intelligence".to_string(), json!({
                    "direction":      dir,
                    "percentage":     pct,
                    "delta_abs":      delta,
                    "previous_value": prev,
                    "label":          label,
                }));
            }
        }
    }
}

// ── Discovery helpers ────────────────────────────────────────────────────────

fn ensure_discovery_defaults(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    // [PORTED_FROM: (update body :schemas #(or % []))] — null OR missing → []
    let schemas = obj.get("schemas").cloned().unwrap_or(Value::Null);
    if schemas.is_null() || (schemas.is_array() && schemas.as_array().map(|a| a.is_empty()).unwrap_or(false) && !obj.contains_key("schemas")) {
        obj.insert("schemas".to_string(), json!([]));
    } else if schemas.is_null() {
        obj.insert("schemas".to_string(), json!([]));
    }
    if obj.get("has_next").map(|v| v.is_null()).unwrap_or(true) {
        obj.insert("has_next".to_string(), json!(false));
    }
    if obj.get("next_cursor").map(|v| v.is_null()).unwrap_or(true) {
        obj.insert("next_cursor".to_string(), json!(""));
    }
}

fn ensure_explore_defaults(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    obj.entry("values").or_insert(json!([]));
}

fn ensure_transaction_defaults(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    // entity_id siempre string
    let eid = obj.get("entity_id")
        .or_else(|| obj.get("entity-id"))
        .or_else(|| obj.get("result").and_then(|r| r.get("entity_id")))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    obj.insert("entity_id".to_string(), json!(eid));
}

fn ensure_bulk_defaults(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    obj.entry("ingested_count").or_insert(json!(0));
    obj.entry("outbox_count").or_insert(json!(0));
}

fn ensure_match_defaults(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    obj.entry("matched_rules").or_insert(json!([]));
}

fn ensure_match_batch_defaults(body: &mut Value) {
    let obj = body.as_object_mut().unwrap();
    obj.entry("responses").or_insert(json!([]));
}

// ── API Pública ──────────────────────────────────────────────────────────────

/// Normaliza una respuesta al 100% de cobertura del contrato.
/// Dispatch por ResponseType — equivalente al defmulti de Clojure.
/// [PORTED_FROM: (normalize-response [[_tag body]] dispatch)]
pub fn normalize_response(body: &Value, response_type: ResponseType) -> Value {
    let mut body = body.clone();

    // Garantizar que body sea un objeto
    if !body.is_object() {
        body = json!({"raw": body});
    }

    let success = true; // la inferencia de ok/error viene del caller

    match response_type {
        ResponseType::Query => {
            ensure_status(&mut body, success);
            ensure_metadata(&mut body);
            ensure_pagination(&mut body);
            ensure_viz_meta(&mut body);
            enrich_viz_intelligence(&mut body);
        }
        ResponseType::Discovery => {
            ensure_status(&mut body, success);
            ensure_discovery_defaults(&mut body);
        }
        ResponseType::Explore => {
            ensure_status(&mut body, success);
            ensure_explore_defaults(&mut body);
        }
        ResponseType::Transaction => {
            ensure_status(&mut body, success);
            ensure_transaction_defaults(&mut body);
        }
        ResponseType::Bulk => {
            ensure_status(&mut body, success);
            ensure_bulk_defaults(&mut body);
        }
        ResponseType::MatchRules => {
            ensure_status(&mut body, success);
            ensure_match_defaults(&mut body);
        }
        ResponseType::MatchRulesBatch => {
            ensure_status(&mut body, success);
            ensure_match_batch_defaults(&mut body);
        }
        ResponseType::Default => {
            ensure_status(&mut body, success);
        }
    }

    body
}

/// Alias para normalizar chunks de QueryResponse (streaming, Paso 7).
/// [PORTED_FROM: (normalize-chunk chunk)]
pub fn normalize_chunk(body: &Value) -> Value {
    let mut body = body.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.insert("response_type".to_string(), json!("query_response"));
    }
    normalize_response(&body, ResponseType::Query)
}

/// Alias para normalizar respuestas unarias (Discovery, Explore, Match).
/// [PORTED_FROM: (normalize-unary chunk)]
pub fn normalize_unary(body: &Value) -> Value {
    let response_type = ResponseType::infer(body);
    normalize_response(body, response_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_status_adds_success_true() {
        let mut body = json!({"data": []});
        ensure_status(&mut body, true);
        assert_eq!(body["status"]["success"], json!(true));
    }

    #[test]
    fn ensure_status_adds_error_fields() {
        let mut body = json!({"code": "EAV_001", "reason": "not found"});
        ensure_status(&mut body, false);
        assert_eq!(body["status"]["success"], json!(false));
        assert_eq!(body["status"]["error_code"], json!("EAV_001"));
    }

    #[test]
    fn response_type_infers_query() {
        let body = json!({"data": [], "query_key": "q1"});
        assert_eq!(ResponseType::infer(&body), ResponseType::Query);
    }

    #[test]
    fn response_type_infers_discovery() {
        let body = json!({"schemas": []});
        assert_eq!(ResponseType::infer(&body), ResponseType::Discovery);
    }

    #[test]
    fn normalize_discovery_has_defaults() {
        let body = json!({"schemas": null});
        let result = normalize_response(&body, ResponseType::Discovery);
        assert_eq!(result["schemas"], json!([]));
        assert_eq!(result["has_next"], json!(false));
    }
}
