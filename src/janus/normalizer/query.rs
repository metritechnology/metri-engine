use crate::janus::normalizer::helpers::{
    derive_semantic_label, infer_viz_type, new_query_id, safe_double,
};
use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

pub struct QueryNormalizer;

impl NormalizerStrategy for QueryNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        ensure_metadata(body);
        ensure_pagination(body);
        ensure_hateoas_links(body);
        ensure_viz_meta(body);
        enrich_viz_intelligence(body);
    }
}

/// Garantiza :metadata en un chunk de QueryResponse.
/// [PORTED_FROM: (ensure-metadata chunk)]
fn ensure_metadata(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let root_exec_time = obj.get("execution_time_ms").and_then(|v| v.as_i64());

    if !obj.contains_key("metadata") {
        let channel = obj
            .get("channel")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let engine = match channel {
            "olap" => "olap",
            "oltp" => "oltp",
            _ => "unknown",
        };
        let total = obj.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        obj.insert(
            "metadata".to_string(),
            json!({
                "engine":             engine,
                "query_id":           new_query_id(),
                "total_count":        total,
                "total_queries":      1,
                "parallelism_factor": 1.0,
                "cache_hits":         0,
                "execution_time_ms":  root_exec_time.unwrap_or(0),
            }),
        );
    } else {
        if let Some(metadata_obj) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            if !metadata_obj.contains_key("execution_time_ms") {
                metadata_obj.insert(
                    "execution_time_ms".to_string(),
                    json!(root_exec_time.unwrap_or(0)),
                );
            } else if let Some(exec_time) = root_exec_time {
                let current_val = metadata_obj
                    .get("execution_time_ms")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                if current_val == 0 && exec_time > 0 {
                    metadata_obj.insert("execution_time_ms".to_string(), json!(exec_time));
                }
            }
        }
    }
}

/// Garantiza :pagination básica.
/// [PORTED_FROM: (ensure-pagination chunk)]
fn ensure_pagination(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    if !obj.contains_key("pagination") {
        let _total = obj.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        obj.insert(
            "pagination".to_string(),
            json!({
                "page_size":    0,
                "has_next":     false,
                "has_previous": false,
                "links": [
                    {"rel": "first",   "href": "/query", "method": "POST"},
                    {"rel": "last",    "href": "/query", "method": "POST"},
                ]
            }),
        );
    }
}

/// Garantiza :links de HATEOAS a nivel raíz.
/// [PORTED_FROM: (ensure-hateoas-links chunk)]
fn ensure_hateoas_links(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let tenant_id = obj
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let base_href = format!("/query?tenant_id={}", tenant_id);

    if !obj.contains_key("links") {
        obj.insert("links".to_string(), json!([]));
    }

    let Some(links_arr) = obj.get_mut("links").and_then(|v| v.as_array_mut()) else {
        return;
    };
    let rels: std::collections::HashSet<String> = links_arr
        .iter()
        .filter_map(|lnk| {
            lnk.get("rel")
                .and_then(|r| r.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    if !rels.contains("self") {
        links_arr.push(json!({
            "rel": "self",
            "href": base_href,
            "method": "POST"
        }));
    }
    if !rels.contains("refresh") {
        links_arr.push(json!({
            "rel": "refresh",
            "href": base_href,
            "method": "POST"
        }));
    }
}

/// Garantiza :viz_ext en el chunk con el tipo de visualización correcto.
/// [PORTED_FROM: (ensure-viz-meta chunk)]
fn ensure_viz_meta(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    if obj.contains_key("viz_ext") {
        return;
    }

    let output_cast = obj
        .get("output_cast")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let mut viz_hint = obj.get("viz").and_then(|v| v.as_str()).map(str::to_string);

    // Parse JSON viz hint if present
    let mut json_override = None;
    if let Some(ref hint) = viz_hint {
        if hint.trim().starts_with('{') {
            if let Ok(parsed) = serde_json::from_str::<Value>(hint) {
                if let Some(parsed_obj) = parsed.as_object() {
                    if let Some(t) = parsed_obj.get("type").and_then(|v| v.as_str()) {
                        viz_hint = Some(t.to_string());
                    }
                    json_override = Some(parsed_obj.clone());
                }
            }
        }
    }

    let viz_type = infer_viz_type(output_cast.as_deref(), viz_hint.as_deref());

    let rows = obj
        .get("data")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let columns = obj
        .get("columns")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let payload: Value = match viz_type {
        "indicator" | "kpi" | "gauge" => {
            let mut history: Vec<f64> = vec![];
            let mut val = 0.0;

            // Resolve primary metric key following SOLID and robust metadata checks
            let mut primary_metric_key: Option<String> = None;
            if let Some(qs) = obj.get("query_spec") {
                if let Some(metrics) = qs.get("metrics").and_then(|m| m.as_array()) {
                    if let Some(first_metric) = metrics.first() {
                        if let Some(name) = first_metric.get("name").and_then(|n| n.as_str()) {
                            primary_metric_key = Some(name.to_string());
                        } else if let Some(attr) =
                            first_metric.get("attribute").and_then(|a| a.as_str())
                        {
                            let fn_str = first_metric
                                .get("fn")
                                .and_then(|f| f.as_str())
                                .unwrap_or("agg");
                            primary_metric_key = Some(format!("{}_{}", fn_str, attr));
                        }
                    }
                }
            }

            if primary_metric_key.is_none() {
                primary_metric_key = columns
                    .iter()
                    .find(|c| {
                        c.get("is_measure")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .and_then(|c| c.get("key").and_then(|k| k.as_str()))
                    .map(|s| s.to_string());
            }

            for row in &rows {
                let current_val = if let Some(ref key) = primary_metric_key {
                    row.get(key)
                        .or_else(|| row.get(format!("current_{}", key)))
                        .map(safe_double)
                } else {
                    None
                };

                let current_val = current_val.unwrap_or_else(|| {
                    row.as_array()
                        .and_then(|r| r.get(1).or(r.first())) // if [time, value], pick value; else pick first
                        .or_else(|| {
                            row.as_object().and_then(|m| {
                                // try to pick a numeric value
                                m.values()
                                    .find(|v| v.is_number())
                                    .or_else(|| m.values().next())
                            })
                        })
                        .map(safe_double)
                        .unwrap_or(0.0)
                });

                history.push(current_val);
                val = current_val; // Last value is the current value
            }

            if history.len() == 1 {
                history.clear(); // No need for history if it's a single value
            }

            let mut signal_obj = serde_json::Map::new();
            signal_obj.insert("value".to_string(), json!(val));

            if !history.is_empty() {
                signal_obj.insert("history".to_string(), json!(history));
            }

            if let Some(first_col) = columns.first().and_then(|c| c.as_object()) {
                if let Some(unit) = first_col.get("unit") {
                    signal_obj.insert("unit".to_string(), unit.clone());
                }
                if let Some(entity_ref) = first_col.get("entity_ref") {
                    signal_obj.insert("entity_ref".to_string(), entity_ref.clone());
                }
            }

            // Also check for thresholds at the root or within metadata
            if let Some(thresholds) = obj.get("thresholds") {
                signal_obj.insert("thresholds".to_string(), thresholds.clone());
            }

            json!({"signal": signal_obj})
        }
        "pie" | "donut" => {
            // [PORTED_FROM: :breakdown {:signals {...}}]
            // Key = valor real de la primera columna dimensión (no "slice_N" sintético)
            // Los rows del executor PIE son Object: {"area": "Mecánica", "count": 12}
            let dim_key = columns
                .iter()
                .find(|c| {
                    c.get("is_dimension")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
                .and_then(|c| c.get("key").and_then(|k| k.as_str()))
                .map(|s| s.to_string());

            let metric_key = columns
                .iter()
                .find(|c| {
                    c.get("is_measure")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
                .and_then(|c| c.get("key").and_then(|k| k.as_str()))
                .map(|s| s.to_string());

            let signals: serde_json::Map<String, Value> = rows
                .iter()
                .enumerate()
                .map(|(i, row)| {
                    let key = if let Some(arr) = row.as_array() {
                        // Array legacy: [dimension, value]
                        arr.first()
                            .and_then(|v| {
                                v.as_str().map(str::to_string).or_else(|| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        Some(v.to_string())
                                    }
                                })
                            })
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| format!("slice_{i}"))
                    } else if let Some(obj) = row.as_object() {
                        // Object format: {"dim_field": "label", "metric_field": 42}
                        let k = dim_key
                            .as_ref()
                            .and_then(|dk| obj.get(dk))
                            .or_else(|| obj.iter().find(|(_, v)| v.is_string()).map(|(_, v)| v))
                            .or_else(|| obj.values().next());

                        k.and_then(|v| {
                            v.as_str().map(str::to_string).or_else(|| {
                                if v.is_null() {
                                    None
                                } else {
                                    Some(v.to_string())
                                }
                            })
                        })
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| format!("slice_{i}"))
                    } else {
                        format!("slice_{i}")
                    };

                    let val = if let Some(arr) = row.as_array() {
                        // Array: [dimension, value]
                        arr.get(1).map(safe_double).unwrap_or(0.0)
                    } else if let Some(obj) = row.as_object() {
                        // Object: buscar el primer campo numérico (la métrica)
                        metric_key
                            .as_ref()
                            .and_then(|mk| obj.get(mk))
                            .or_else(|| obj.values().find(|v| v.is_number()))
                            .map(safe_double)
                            .unwrap_or(0.0)
                    } else {
                        0.0
                    };

                    (key, json!({"value": val}))
                })
                .collect();
            json!({"breakdown": {"signals": signals}})
        }
        "line" | "bar" | "area" | "scatter" | "timeseries" => {
            // [PORTED_FROM: (build-chart-decoration chunk)]
            // Construye ChartDecoration con los 10 campos del contrato proto §VizMeta.
            // Fuente de verdad de encoding: columnas derivadas por el executor.
            let col_names: Vec<_> = columns
                .iter()
                .filter_map(|c| c.get("key").and_then(|v| v.as_str()).map(str::to_string))
                .collect();
            let x_dim = col_names.first().cloned().unwrap_or_default();
            let y_dims = col_names.into_iter().skip(1).collect::<Vec<_>>();

            // Leer overrides de `decoration` (inyectado por router desde ast_ir o widget layout)
            // [PORTED_FROM: (merge default-decoration (:decoration chunk))]
            let mut dec = obj.get("decoration").cloned().unwrap_or(Value::Null);
            if let Some(ref override_obj) = json_override {
                let mut dec_map = match dec {
                    Value::Object(m) => m,
                    _ => serde_json::Map::new(),
                };
                for (k, v) in override_obj {
                    if k != "type" {
                        dec_map.insert(k.clone(), v.clone());
                    }
                }
                dec = Value::Object(dec_map);
            }

            // fill_gaps: el override explícito del JSON hint tiene prioridad absoluta.
            // Si el hint JSON contiene "fill_gaps" explícito (true o false), ese valor se respeta.
            // Solo si no hay override explícito se aplica la heurística automática de timeseries.
            // [PORTED_FROM: ANO-005: fill-gaps directive]
            let fill_gaps = if let Some(ref override_obj) = json_override {
                if let Some(explicit) = override_obj.get("fill_gaps").and_then(|v| v.as_bool()) {
                    // Valor explícito en JSON hint — tiene prioridad máxima
                    explicit
                } else {
                    // Sin override explícito: aplicar heurística automática
                    viz_hint.as_deref() == Some("timeseries")
                        || output_cast.as_deref() == Some("TIMESERIES")
                        || dec
                            .get("fill_gaps")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                }
            } else {
                // Sin JSON hint: heurística automática normal
                viz_hint.as_deref() == Some("timeseries")
                    || output_cast.as_deref() == Some("TIMESERIES")
                    || dec
                        .get("fill_gaps")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
            };

            let mut chart_meta = json!({
                // §1 — ECharts Dataset Encoding Strategy
                "x_dimension":  x_dim,
                "y_dimensions": y_dims,

                // §2 — ECharts Component Toggles (defaults seguros)
                "color_scheme": dec.get("color_scheme").and_then(|v| v.as_str()).unwrap_or(""),
                "show_legend":  dec.get("show_legend").and_then(|v| v.as_bool()).unwrap_or(true),
                "show_tooltip": dec.get("show_tooltip").and_then(|v| v.as_bool()).unwrap_or(true),
                "title":        dec.get("title").and_then(|v| v.as_str()).unwrap_or(""),

                // §3 — Specific Visual Hooks
                "stacked":      dec.get("stacked").and_then(|v| v.as_bool()).unwrap_or(false),
                "smooth":       dec.get("smooth").and_then(|v| v.as_bool()).unwrap_or(false),

                // §4 — Mustache label template (Resolved at render-time por ECharts formatter)
                // [PORTED_FROM: label-template/interpolate-rows en label_template.clj]
                "label_template": dec.get("label_template").and_then(|v| v.as_str()).unwrap_or(""),
                "x_axis_label_template": dec.get("x_axis_label_template").and_then(|v| v.as_str()).unwrap_or(""),
                "y_axis_label_template": dec.get("y_axis_label_template").and_then(|v| v.as_str()).unwrap_or(""),
                "horizontal": dec.get("horizontal").and_then(|v| v.as_bool()).unwrap_or(false),
            });

            // fill_gaps solo se agrega si está activo para no contaminar otros tipos
            if fill_gaps {
                chart_meta["fill_gaps"] = json!(true);
            }

            json!({"chart": chart_meta})
        }
        "tree" => {
            let col_names: Vec<String> = columns
                .iter()
                .filter_map(|c| c.get("key").and_then(|v| v.as_str()).map(str::to_string))
                .collect();

            // 1. parent_id_key: prefer hierarchy.parent_field from router body,
            //    fall back to column name convention (parent_*_id), then "parent_id".
            let parent_key = obj
                .get("hierarchy")
                .and_then(|h| h.get("parent_field"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    col_names
                        .iter()
                        .find(|c| c.starts_with("parent_") && c.ends_with("_id"))
                        .cloned()
                        .unwrap_or_else(|| "parent_id".to_string())
                });

            // 2. label_key: pick the first of these candidates that exists in columns.
            let label_key = ["name", "title", "label", "description"]
                .iter()
                .find(|k| col_names.contains(&k.to_string()))
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    col_names
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "name".to_string())
                });

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
            // Table / Pivot
            let query_spec = obj.get("query_spec");
            let entity_ref = query_spec
                .and_then(|qs| qs.get("entity"))
                .and_then(|e| e.as_str())
                .unwrap_or_else(|| {
                    obj.get("entity_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                });

            let mut specified_keys = Vec::new();
            let mut has_spec = false;
            if let Some(qs) = query_spec {
                if let Some(dims) = qs.get("dimensions").and_then(|d| d.as_array()) {
                    for d in dims {
                        if let Some(attr) = d.get("attribute").and_then(|a| a.as_str()) {
                            specified_keys.push((attr.to_string(), false)); // (key, is_measure)
                            has_spec = true;
                        }
                    }
                }
                if let Some(metrics) = qs.get("metrics").and_then(|m| m.as_array()) {
                    for m in metrics {
                        if let Some(name) = m.get("name").and_then(|n| n.as_str()) {
                            specified_keys.push((name.to_string(), true));
                            has_spec = true;
                        } else {
                            let attr = m
                                .get("attribute")
                                .and_then(|a| a.as_str())
                                .unwrap_or("total");
                            let fn_str = m.get("fn").and_then(|f| f.as_str()).unwrap_or("agg");
                            specified_keys.push((format!("{}_{}", fn_str, attr), true));
                            has_spec = true;
                        }
                    }
                }
            }

            let cols: Vec<Value> = if has_spec {
                specified_keys
                    .iter()
                    .map(|(key, is_m)| {
                        let raw_col = columns
                            .iter()
                            .find(|c| c.get("key").and_then(|v| v.as_str()) == Some(key));
                        let col_type = raw_col
                            .and_then(|c| c.get("type").and_then(|v| v.as_str()))
                            .unwrap_or(if *is_m { "number" } else { "string" });
                        let is_measure = *is_m
                            || raw_col
                                .and_then(|c| c.get("is_measure").and_then(|v| v.as_bool()))
                                .unwrap_or(false);
                        let metadata = raw_col
                            .and_then(|c| c.get("metadata").cloned())
                            .unwrap_or_else(|| json!({}));

                        // Find custom label template from dimensions in query_spec
                        let mut label_template = None;
                        if let Some(qs) = query_spec {
                            if let Some(dims) = qs.get("dimensions").and_then(|d| d.as_array()) {
                                for d in dims {
                                    if d.get("attribute").and_then(|a| a.as_str()) == Some(key) {
                                        label_template =
                                            d.get("label_template").and_then(|t| t.as_str());
                                        break;
                                    }
                                }
                            }
                        }

                        // If label_template is not found, check if it's a metric name
                        let mut metric_name = None;
                        if let Some(qs) = query_spec {
                            if let Some(metrics) = qs.get("metrics").and_then(|m| m.as_array()) {
                                for m in metrics {
                                    if m.get("name").and_then(|n| n.as_str()) == Some(key) {
                                        metric_name = Some(key);
                                        break;
                                    }
                                }
                            }
                        }

                        // Determine label
                        let label = if let Some(mn) = metric_name {
                            mn.to_string()
                        } else {
                            derive_semantic_label(key, label_template)
                        };

                        // Alignment: metrics/numerical RIGHT, categorical/temporal LEFT
                        let align = if col_type == "number" || is_measure {
                            "RIGHT"
                        } else {
                            "LEFT"
                        };

                        json!({
                            "key": key,
                            "label": label,
                            "type": col_type,
                            "align": align,
                            "sortable": true,
                            "format": "",
                            "metadata": metadata,
                            "entity_ref": entity_ref,
                        })
                    })
                    .collect()
            } else {
                columns
                    .iter()
                    .map(|c| {
                        let key = c.get("key").and_then(|v| v.as_str()).unwrap_or("");
                        let col_type = c.get("type").and_then(|v| v.as_str()).unwrap_or("string");
                        let is_measure = c
                            .get("is_measure")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let metadata = c.get("metadata").cloned().unwrap_or_else(|| json!({}));

                        // Find custom label template from dimensions in query_spec
                        let mut label_template = None;
                        if let Some(qs) = query_spec {
                            if let Some(dims) = qs.get("dimensions").and_then(|d| d.as_array()) {
                                for d in dims {
                                    if d.get("attribute").and_then(|a| a.as_str()) == Some(key) {
                                        label_template =
                                            d.get("label_template").and_then(|t| t.as_str());
                                        break;
                                    }
                                }
                            }
                        }

                        // If label_template is not found, check if it's a metric name
                        let mut metric_name = None;
                        if let Some(qs) = query_spec {
                            if let Some(metrics) = qs.get("metrics").and_then(|m| m.as_array()) {
                                for m in metrics {
                                    if m.get("name").and_then(|n| n.as_str()) == Some(key) {
                                        metric_name = Some(key);
                                        break;
                                    }
                                }
                            }
                        }

                        // Determine label
                        let label = if let Some(mn) = metric_name {
                            mn.to_string()
                        } else {
                            derive_semantic_label(key, label_template)
                        };

                        // Alignment: metrics/numerical RIGHT, categorical/temporal LEFT
                        let align = if col_type == "number" || is_measure {
                            "RIGHT"
                        } else {
                            "LEFT"
                        };

                        json!({
                            "key": key,
                            "label": label,
                            "type": col_type,
                            "align": align,
                            "sortable": true,
                            "format": "",
                            "metadata": metadata,
                            "entity_ref": entity_ref,
                        })
                    })
                    .collect()
            };

            json!({"table": {"columns": cols, "row_actions": [], "global_links": []}})
        }
    };

    let final_payload = json!({"type": viz_type, "payload": payload});
    tracing::debug!(
        "DEBUG NORMALIZER: viz_type={}, payload={}",
        viz_type,
        final_payload
    );
    obj.insert("viz_ext".to_string(), final_payload);
}

/// Enriquece la inteligencia TIME_SHIFT, BENCHMARK o SMART.
/// [PORTED_FROM: (enrich-viz-intelligence chunk)]
fn enrich_viz_intelligence(body: &mut Value) {
    let already_has = body
        .pointer("/viz_ext/payload/signal/intelligence")
        .is_some();
    if already_has {
        return;
    }

    let signal_val = body
        .pointer("/viz_ext/payload/signal/value")
        .and_then(|v| v.as_f64());

    // 1. Buscar previous_value en el signal ya construido
    let prev_in_signal = body
        .pointer("/viz_ext/payload/signal/previous_value")
        .and_then(|v| v.as_f64());

    // 2. Fallback: buscar en la primera data row (inyectado por executor KPI)
    let prev_in_row = body
        .pointer("/data/0/previous_value")
        .and_then(|v| v.as_f64());

    // 3. Fallback BENCHMARK: benchmark_value en row
    let benchmark_in_row = body
        .pointer("/data/0/benchmark_value")
        .and_then(|v| v.as_f64());

    let prev_val = prev_in_signal.or(prev_in_row).or(benchmark_in_row);

    // SMART stats: z_score e is_anomaly (Z > 2.0 = anomalía gaussiana)
    let first_row = body.pointer("/data/0").cloned();
    let z_score = first_row
        .as_ref()
        .and_then(|r| r.as_object())
        .and_then(|obj| {
            obj.iter().find_map(|(k, v)| {
                if k.starts_with("z_score_") {
                    v.as_f64()
                } else {
                    None
                }
            })
        });

    if let Some(curr) = signal_val {
        // Propagate previous_value al signal si no estaba
        if prev_val.is_some() {
            if let Some(obj) = body.pointer_mut("/viz_ext/payload/signal") {
                if let Some(m) = obj.as_object_mut() {
                    m.entry("previous_value")
                        .or_insert(json!(prev_val.unwrap_or(0.0)));
                }
            }
        }

        if let Some(prev) = prev_val {
            let delta = curr - prev;
            let pct = if prev == 0.0 {
                if curr > 0.0 {
                    100.0
                } else if curr < 0.0 {
                    -100.0
                } else {
                    0.0
                }
            } else {
                100.0 * delta / prev.abs()
            };
            let dir = if delta > 0.001 {
                "up"
            } else if delta < -0.001 {
                "down"
            } else {
                "neutral"
            };
            let label = format!("{}{:.1}%", if pct > 0.0 { "+" } else { "" }, pct);
            let is_anomaly = z_score.map(|z| z.abs() > 2.0).unwrap_or(false);

            if let Some(obj) = body.pointer_mut("/viz_ext/payload/signal") {
                if let Some(m) = obj.as_object_mut() {
                    let mut intel = serde_json::Map::new();
                    intel.insert("direction".to_string(), json!(dir));
                    intel.insert("percentage".to_string(), json!(pct));
                    intel.insert("delta_abs".to_string(), json!(delta));
                    intel.insert("previous_value".to_string(), json!(prev));
                    intel.insert("label".to_string(), json!(label));
                    intel.insert("is_anomaly".to_string(), json!(is_anomaly));
                    if let Some(z) = z_score {
                        intel.insert("z_score".to_string(), json!(z));
                    }
                    intel.insert("represents_initial".to_string(), json!(prev == 0.0));
                    m.insert("intelligence".to_string(), Value::Object(intel));
                }
            }
        }
    }
}
