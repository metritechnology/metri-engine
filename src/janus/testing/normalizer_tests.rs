use crate::janus::normalizer::{normalize_response, normalize_chunk, helpers, ResponseType};
use serde_json::json;

#[test]
fn ensure_status_adds_success_true() {
    let mut body = json!({"data": []});
    helpers::ensure_status(&mut body, true);
    assert_eq!(body["status"]["success"], json!(true));
}

#[test]
fn ensure_status_adds_error_fields() {
    let mut body = json!({"code": "EAV_001", "reason": "not found"});
    helpers::ensure_status(&mut body, false);
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

// ── KPI + TIME_SHIFT → IntelligenceSignal completo ─────────────────────────
#[test]
fn kpi_with_time_shift_produces_full_intelligence_signal() {
    let body = json!({
        "query_key":   "kpi_activos",
        "entity_type": "asset",
        "data": [
            {
                "count_id": 15.0,          // valor actual
                "current_count_id": 15.0,  // renombrado por run_comparisons (TIME_SHIFT)
                "prev_0_count_id": 10.0,   // período anterior
                "previous_value": 10.0     // inyectado por executor KPI
            }
        ],
        "columns": [{"key": "count_id", "label": "Total Activos"}],
        "channel": "oltp",
        "output_cast": "KPI",
        "viz": "kpi"
    });

    let result = normalize_chunk(&body);

    let signal_val = result.pointer("/viz_ext/payload/signal/value")
        .and_then(|v| v.as_f64())
        .expect("signal.value debe existir");
    assert!((signal_val - 15.0).abs() < 0.01, "signal.value debe ser 15.0, got {signal_val}");

    let prev_val = result.pointer("/viz_ext/payload/signal/previous_value")
        .and_then(|v| v.as_f64())
        .expect("signal.previous_value debe existir");
    assert!((prev_val - 10.0).abs() < 0.01, "previous_value debe ser 10.0, got {prev_val}");

    let intel = result.pointer("/viz_ext/payload/signal/intelligence")
        .expect("IntelligenceSignal debe existir");

    assert_eq!(intel["direction"], json!("up"), "direction debe ser 'up'");

    let pct = intel["percentage"].as_f64().expect("percentage debe ser número");
    assert!((pct - 50.0).abs() < 0.01, "percentage debe ser 50.0, got {pct}");

    let delta = intel["delta_abs"].as_f64().expect("delta_abs debe ser número");
    assert!((delta - 5.0).abs() < 0.01, "delta_abs debe ser 5.0, got {delta}");

    let label = intel["label"].as_str().expect("label debe ser string");
    assert!(label.contains("50"), "label debe contener '50', got '{label}'");
    assert!(label.starts_with('+'), "label debe empezar con '+', got '{label}'");

    assert_eq!(intel["is_anomaly"], json!(false), "is_anomaly debe ser false sin SMART");
}

// ── KPI con BENCHMARK → IntelligenceSignal vs target fijo ─────────────────
#[test]
fn kpi_with_benchmark_produces_intelligence_signal() {
    let body = json!({
        "query_key":   "kpi_wos",
        "entity_type": "work_order",
        "data": [
            {
                "count_id": 20.0,
                "benchmark_value": 25.0,  // target mensual
                "previous_value": 25.0    // inyectado por executor
            }
        ],
        "columns": [{"key": "count_id", "label": "Órdenes de Trabajo"}],
        "channel": "oltp",
        "output_cast": "KPI",
        "viz": "kpi"
    });

    let result = normalize_chunk(&body);

    let intel = result.pointer("/viz_ext/payload/signal/intelligence")
        .expect("IntelligenceSignal debe existir");

    assert_eq!(intel["direction"], json!("down"), "direction debe ser 'down'");
    let pct = intel["percentage"].as_f64().expect("percentage debe ser número");
    assert!((pct + 20.0).abs() < 0.01, "percentage debe ser -20.0, got {pct}");
    assert_eq!(intel["is_anomaly"], json!(false));
}

// ── KPI con SMART z_score → is_anomaly = true ─────────────────────────────
#[test]
fn kpi_with_smart_anomaly_sets_is_anomaly_true() {
    let body = json!({
        "query_key":   "kpi_downtime",
        "entity_type": "downtime_log",
        "data": [
            {
                "sum_duration": 500.0,       // valor actual (outlier)
                "previous_value": 100.0,     // media histórica
                "mean_sum_duration": 100.0,
                "std_sum_duration": 15.0,
                "z_score_sum_duration": 26.7 // Z > 2.0 → anomalía
            }
        ],
        "columns": [{"key": "sum_duration", "label": "Tiempo Inactivo"}],
        "channel": "oltp",
        "output_cast": "KPI",
        "viz": "gauge"
    });

    let result = normalize_chunk(&body);

    let intel = result.pointer("/viz_ext/payload/signal/intelligence")
        .expect("IntelligenceSignal debe existir");

    assert_eq!(intel["is_anomaly"], json!(true),
        "is_anomaly debe ser true para z_score > 2.0");

    let z = intel["z_score"].as_f64().expect("z_score debe estar en intelligence");
    assert!(z > 2.0, "z_score debe ser > 2.0, got {z}");

    assert_eq!(intel["represents_initial"], json!(false));
}

// ── KPI puro (sin comparisons) → signal válido sin intelligence ────────────
#[test]
fn kpi_without_comparisons_has_valid_signal_no_intelligence() {
    let body = json!({
        "query_key":   "kpi_parts",
        "entity_type": "part",
        "data": [
            { "count_id": 42.0 }
        ],
        "columns": [{"key": "count_id", "label": "Total Parts"}],
        "channel": "oltp",
        "output_cast": "KPI",
        "viz": "kpi"
    });

    let result = normalize_chunk(&body);

    let val = result.pointer("/viz_ext/payload/signal/value")
        .and_then(|v| v.as_f64())
        .expect("signal.value debe existir");
    assert!((val - 42.0).abs() < 0.01, "signal.value debe ser 42.0, got {val}");

    let intel = result.pointer("/viz_ext/payload/signal/intelligence");
    assert!(intel.is_none(),
        "IntelligenceSignal NO debe existir sin previous_value");
}

// ── KPI down trend: valor actual menor que previo ──────────────────────────
#[test]
fn kpi_down_trend_direction_and_negative_pct() {
    let body = json!({
        "query_key":   "kpi_revenue",
        "entity_type": "labor_log",
        "data": [
            {
                "sum_hours": 80.0,
                "previous_value": 100.0
            }
        ],
        "columns": [{"key": "sum_hours"}],
        "channel": "oltp",
        "output_cast": "KPI",
        "viz": "indicator"
    });

    let result = normalize_chunk(&body);
    let intel = result.pointer("/viz_ext/payload/signal/intelligence")
        .expect("IntelligenceSignal debe existir");

    assert_eq!(intel["direction"], json!("down"));
    let pct = intel["percentage"].as_f64().unwrap();
    assert!((pct + 20.0).abs() < 0.01, "pct debe ser -20.0, got {pct}");
    let delta = intel["delta_abs"].as_f64().unwrap();
    assert!((delta + 20.0).abs() < 0.01, "delta_abs debe ser -20.0, got {delta}");
}

// ── ChartDecoration Contract Tests ─────────────────────────────────────────────
#[test]
fn chart_bar_has_safe_defaults() {
    let body = json!({
        "query_key":   "bar_chart",
        "entity_type": "labor_log",
        "data": [["2024-01", 500.0]],
        "columns": [
            {"key": "period", "label": "Período"},
            {"key": "sum_cost", "label": "Costo Total"}
        ],
        "channel": "oltp",
        "output_cast": "TABLE",
        "viz": "bar"
    });

    let result = normalize_chunk(&body);
    let chart = result.pointer("/viz_ext/payload/chart")
        .expect("ChartDecoration debe existir para viz=bar");

    assert_eq!(chart["x_dimension"], json!("period"),
        "x_dimension debe ser la primera columna 'period'");
    assert_eq!(chart["y_dimensions"], json!(["sum_cost"]),
        "y_dimensions debe contener 'sum_cost'");

    assert_eq!(chart["show_legend"],  json!(true),  "show_legend default debe ser true");
    assert_eq!(chart["show_tooltip"], json!(true),  "show_tooltip default debe ser true");
    assert_eq!(chart["stacked"], json!(false), "stacked default debe ser false");
    assert_eq!(chart["smooth"],  json!(false), "smooth default debe ser false");
}

#[test]
fn timeseries_auto_activates_fill_gaps() {
    let body = json!({
        "query_key":   "ts_chart",
        "entity_type": "sale",
        "data": [["2024-W01", 1000.0]],
        "columns": [
            {"key": "bucket",      "label": "Período"},
            {"key": "sum_revenue", "label": "Ingresos"}
        ],
        "channel": "oltp",
        "output_cast": "TIMESERIES",
        "viz": "line"
    });

    let result = normalize_chunk(&body);
    let chart = result.pointer("/viz_ext/payload/chart")
        .expect("ChartDecoration debe existir para viz=line");

    assert_eq!(chart["fill_gaps"], json!(true),
        "fill_gaps debe ser true para TIMESERIES output_cast");
    assert_eq!(chart["x_dimension"], json!("bucket"));
    assert_eq!(chart["y_dimensions"], json!(["sum_revenue"]));
}

#[test]
fn chart_decoration_full_override_all_10_fields() {
    let body = json!({
        "query_key":   "prod_chart",
        "entity_type": "production_log",
        "data": [["2024-01-15", 1200.0, 800.0]],
        "columns": [
            {"key": "fecha",      "label": "Fecha"},
            {"key": "produccion", "label": "Producción"},
            {"key": "consumo",    "label": "Consumo"}
        ],
        "channel": "oltp",
        "output_cast": "TIMESERIES",
        "viz": "bar",
        "decoration": {
            "color_scheme":   "industrial_blue",
            "show_legend":    false,
            "show_tooltip":   false,
            "title":          "Reporte de Producción",
            "stacked":        true,
            "smooth":         true,
            "label_template": "{{fecha}}: {{produccion}} unidades",
            "fill_gaps":      true
        }
    });

    let result = normalize_chunk(&body);
    let chart = result.pointer("/viz_ext/payload/chart")
        .expect("ChartDecoration debe existir");

    assert_eq!(chart["x_dimension"], json!("fecha"));
    assert_eq!(chart["y_dimensions"], json!(["produccion", "consumo"]));

    assert_eq!(chart["color_scheme"],  json!("industrial_blue"), "color_scheme debe propagarse");
    assert_eq!(chart["show_legend"],   json!(false), "show_legend override debe ser false");
    assert_eq!(chart["show_tooltip"],  json!(false), "show_tooltip override debe ser false");
    assert_eq!(chart["title"],         json!("Reporte de Producción"), "title debe propagarse");

    assert_eq!(chart["stacked"], json!(true), "stacked debe ser true");
    assert_eq!(chart["smooth"],  json!(true), "smooth debe ser true");

    assert_eq!(chart["label_template"],
        json!("{{fecha}}: {{produccion}} unidades"),
        "label_template debe llegar intacto con placeholders Mustache sin interpolar");

    assert_eq!(chart["fill_gaps"], json!(true), "fill_gaps debe ser true");
}

#[test]
fn area_chart_stacked_and_smooth_hooks() {
    let body = json!({
        "query_key": "area_test",
        "entity_type": "incident",
        "data": [["W01", 5.0]],
        "columns": [
            {"key": "week",     "label": "Semana"},
            {"key": "failures", "label": "Fallas"}
        ],
        "channel": "oltp",
        "output_cast": "TABLE",
        "viz": "area",
        "decoration": { "stacked": true, "smooth": true }
    });

    let result = normalize_chunk(&body);
    let chart = result.pointer("/viz_ext/payload/chart")
        .expect("ChartDecoration para viz=area");

    assert_eq!(chart["stacked"], json!(true), "stacked debe ser true para area chart");
    assert_eq!(chart["smooth"],  json!(true), "smooth debe ser true para area chart");
    assert_eq!(chart["show_legend"],  json!(true),  "show_legend mantiene default true");
    assert_eq!(chart["show_tooltip"], json!(true), "show_tooltip mantiene default true");
}

#[test]
fn scatter_chart_label_template_reaches_frontend_intact() {
    let body = json!({
        "query_key": "scatter_test",
        "entity_type": "asset",
        "data": [["Bomba A", 3500.0]],
        "columns": [
            {"key": "asset_name", "label": "Activo"},
            {"key": "total_cost", "label": "Costo"}
        ],
        "channel": "oltp",
        "output_cast": "TABLE",
        "viz": "scatter",
        "decoration": { "label_template": "{{asset_name}} - {{total_cost}} USD" }
    });

    let result = normalize_chunk(&body);
    let chart = result.pointer("/viz_ext/payload/chart")
        .expect("ChartDecoration para viz=scatter");

    let lt = chart["label_template"].as_str()
        .expect("label_template debe ser string");
    assert_eq!(lt, "{{asset_name}} - {{total_cost}} USD",
        "label_template debe llegar intacto con placeholders Mustache sin interpolar");
}

#[test]
fn pie_generates_breakdown_not_chart_decoration() {
    let body = json!({
        "query_key": "pie_test",
        "entity_type": "asset",
        "data": [
            ["Mecánica", 3000.0],
            ["Eléctrica", 1500.0]
        ],
        "columns": [
            {"key": "area_name", "label": "Área"},
            {"key": "sum_cost",  "label": "Costo"}
        ],
        "channel": "oltp",
        "output_cast": "PIE",
        "viz": "pie"
    });

    let result = normalize_chunk(&body);

    assert!(result.pointer("/viz_ext/payload/breakdown").is_some(),
        "breakdown debe existir para viz=pie");

    assert!(result.pointer("/viz_ext/payload/chart").is_none(),
        "ChartDecoration NO debe existir para viz=pie — incompatible con BreakdownSignal");
}

#[test]
fn line_chart_multi_series_3_y_dimensions() {
    let body = json!({
        "query_key": "multi_series",
        "entity_type": "financial",
        "data": [["2024-01", 10000.0, 7000.0, 3000.0]],
        "columns": [
            {"key": "month",       "label": "Mes"},
            {"key": "sum_revenue", "label": "Ingresos"},
            {"key": "sum_cost",    "label": "Costos"},
            {"key": "margin",      "label": "Margen"}
        ],
        "channel": "oltp",
        "output_cast": "TIMESERIES",
        "viz": "line"
    });

    let result = normalize_chunk(&body);
    let chart = result.pointer("/viz_ext/payload/chart")
        .expect("ChartDecoration para viz=line multi-series");

    assert_eq!(chart["x_dimension"], json!("month"),
        "x_dimension debe ser 'month' (primera columna)");

    let y_dims = chart["y_dimensions"].as_array()
        .expect("y_dimensions debe ser array");
    assert_eq!(y_dims.len(), 3, "debe haber 3 y_dimensions para multi-series");
    assert!(y_dims.contains(&json!("sum_revenue")));
    assert!(y_dims.contains(&json!("sum_cost")));
    assert!(y_dims.contains(&json!("margin")));
}

#[test]
fn table_column_enriched_metadata_verification() {
    let body = json!({
        "query_key": "table_assets_q",
        "entity_type": "asset",
        "data": [],
        "columns": [
            {"key": "name", "type": "string"},
            {"key": "status", "type": "string"},
            {"key": "criticality", "type": "string"},
            {"key": "health_score", "type": "number"},
            {"key": "Cantidad", "type": "number", "is_measure": true}
        ],
        "channel": "oltp",
        "output_cast": "table",
        "viz": "pivot",
        "query_spec": {
            "entity": "asset",
            "dimensions": [
                {"attribute": "name", "label_template": "Nombre: {{name}}"},
                {"attribute": "status", "label_template": "Estado: {{status}}"},
                {"attribute": "criticality"},
                {"attribute": "health_score"}
            ],
            "metrics": [
                {"attribute": "id", "name": "Cantidad", "fn": "count"}
            ]
        }
    });

    let result = normalize_chunk(&body);
    let table = result.pointer("/viz_ext/payload/table")
        .expect("table payload debe existir");
    let columns = table["columns"].as_array()
        .expect("columns debe ser un array");

    assert_eq!(columns.len(), 5);

    assert_eq!(columns[0]["key"], json!("name"));
    assert_eq!(columns[0]["label"], json!("Nombre"));
    assert_eq!(columns[0]["type"], json!("string"));
    assert_eq!(columns[0]["align"], json!("LEFT"));
    assert_eq!(columns[0]["sortable"], json!(true));
    assert_eq!(columns[0]["entity_ref"], json!("asset"));

    assert_eq!(columns[1]["key"], json!("status"));
    assert_eq!(columns[1]["label"], json!("Estado"));
    assert_eq!(columns[1]["type"], json!("string"));
    assert_eq!(columns[1]["align"], json!("LEFT"));
    assert_eq!(columns[1]["sortable"], json!(true));
    assert_eq!(columns[1]["entity_ref"], json!("asset"));

    assert_eq!(columns[2]["key"], json!("criticality"));
    assert_eq!(columns[2]["label"], json!("Criticality"));
    assert_eq!(columns[2]["type"], json!("string"));
    assert_eq!(columns[2]["align"], json!("LEFT"));
    assert_eq!(columns[2]["sortable"], json!(true));
    assert_eq!(columns[2]["entity_ref"], json!("asset"));

    assert_eq!(columns[3]["key"], json!("health_score"));
    assert_eq!(columns[3]["label"], json!("Health Score"));
    assert_eq!(columns[3]["type"], json!("number"));
    assert_eq!(columns[3]["align"], json!("RIGHT"));
    assert_eq!(columns[3]["sortable"], json!(true));
    assert_eq!(columns[3]["entity_ref"], json!("asset"));

    assert_eq!(columns[4]["key"], json!("Cantidad"));
    assert_eq!(columns[4]["label"], json!("Cantidad"));
    assert_eq!(columns[4]["type"], json!("number"));
    assert_eq!(columns[4]["align"], json!("RIGHT"));
    assert_eq!(columns[4]["sortable"], json!(true));
    assert_eq!(columns[4]["entity_ref"], json!("asset"));
}
