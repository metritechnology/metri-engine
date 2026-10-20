//! Suite exhaustiva de cobertura para el motor VIZ de Janus y gRPC:
//! `janus::normalizer::query`, `janus::normalizer::helpers`, `janus::router::post_processor`,
//! `janus::multi_series` y `grpc::handlers::query_support::map_viz_meta`.

use serde_json::{json, Map};
use std::collections::HashMap;

use crate::grpc::handlers::query_support::map_viz_meta;
use crate::grpc::pb::viz_meta::Payload as PbVizPayload;
use crate::janus::fbs::{
    AggregationFunction, DimensionDefinitionT, HierarchyContextT, MetricDefinitionT,
};
use crate::janus::multi_series::{outer_join_data, partition_standalone};
use crate::janus::normalizer::{
    helpers::{
        capitalize_key, derive_semantic_label, ensure_status, hint_to_static, infer_viz_type,
        safe_double,
    },
    normalize_chunk, normalize_unary, ResponseType,
};
use crate::janus::router::post_processor::{
    apply_label_templates, build_query_spec, derive_columns, inject_hierarchy_children,
    is_auth_caller, redact_sensitive_attributes,
};
use crate::janus::router::CedarCtx;

#[test]
fn viz_normalizer_helpers_and_response_types_100_percent() {
    // 1. safe_double across all JSON types
    assert_eq!(safe_double(&json!(42.5)), 42.5);
    assert_eq!(safe_double(&json!("19.75")), 19.75);
    assert_eq!(safe_double(&json!("not_a_number")), 0.0);
    assert_eq!(safe_double(&json!(null)), 0.0);
    assert_eq!(safe_double(&json!(true)), 0.0);

    // 2. infer_viz_type & hint_to_static
    assert_eq!(infer_viz_type(Some("KPI"), None), "indicator");
    assert_eq!(infer_viz_type(Some("PIE"), None), "pie");
    assert_eq!(infer_viz_type(Some("TIMESERIES"), None), "line");
    assert_eq!(infer_viz_type(Some("BUBBLE"), None), "scatter");
    assert_eq!(infer_viz_type(Some("TABLE"), None), "table");
    assert_eq!(infer_viz_type(Some("CSV_EXPORT"), None), "table");
    assert_eq!(infer_viz_type(None, None), "table");

    for (hint, expected) in [
        ("indicator", "indicator"),
        ("kpi", "indicator"),
        ("gauge", "indicator"),
        ("pie", "pie"),
        ("donut", "pie"),
        ("line", "line"),
        ("bar", "bar"),
        ("area", "area"),
        ("scatter", "scatter"),
        ("timeseries", "line"),
        ("tree", "tree"),
        ("pivot", "table"),
    ] {
        assert_eq!(hint_to_static(hint), expected);
        assert_eq!(infer_viz_type(Some("TABLE"), Some(hint)), expected);
    }

    // 3. capitalize_key & derive_semantic_label
    assert_eq!(capitalize_key("work_order_cost"), "Work Order Cost");
    assert_eq!(capitalize_key(""), "");
    assert_eq!(
        derive_semantic_label("asset_name", Some("Activo: {{asset_name}}")),
        "Activo"
    );
    assert_eq!(
        derive_semantic_label("asset_name", Some("Etiqueta Fija")),
        "Etiqueta Fija"
    );
    assert_eq!(
        derive_semantic_label("asset_name", Some("{{asset_name}}")),
        "Asset Name"
    );
    assert_eq!(derive_semantic_label("asset_name", Some("")), "Asset Name");
    assert_eq!(derive_semantic_label("asset_name", None), "Asset Name");

    // 4. ensure_status edge cases (scalar no-op, detail fallback)
    let mut scalar = json!(123);
    ensure_status(&mut scalar, true);
    assert_eq!(scalar, json!(123));

    let mut err_detail = json!({"detail": "fallo detallado"});
    ensure_status(&mut err_detail, false);
    assert_eq!(err_detail["status"]["error_code"], json!("INTERNAL_ERROR"));
    assert_eq!(
        err_detail["status"]["error_message"],
        json!("fallo detallado")
    );

    // 5. ResponseType::infer & normalize_unary across all variants
    assert_eq!(ResponseType::infer(&json!("scalar")), ResponseType::Default);
    assert_eq!(
        ResponseType::infer(&json!({"ingested_count": 5})),
        ResponseType::Bulk
    );
    assert_eq!(
        ResponseType::infer(&json!({"entity_id": "e1"})),
        ResponseType::Transaction
    );
    assert_eq!(
        ResponseType::infer(&json!({"values": ["a", "b"]})),
        ResponseType::Explore
    );
    assert_eq!(
        ResponseType::infer(&json!({"matched_rules": []})),
        ResponseType::MatchRules
    );
    assert_eq!(
        ResponseType::infer(&json!({"responses": []})),
        ResponseType::MatchRulesBatch
    );

    let unary_explore = normalize_unary(&json!({"values": ["x"]}));
    assert_eq!(unary_explore["status"]["success"], json!(true));
}

#[test]
fn viz_normalizer_kpi_sparkline_history_thresholds_and_intelligence_edge_cases() {
    // 1. Multi-row KPI sparkline history + query_spec unnamed metric + unit/entity_ref + thresholds
    let body_sparkline = json!({
        "query_key": "kpi_spark",
        "tenant_id": "tnt_01",
        "channel": "olap",
        "execution_time_ms": 42,
        "cache": {"hit": true, "remaining_secs": 180},
        "output_cast": "KPI",
        "viz": "indicator",
        "thresholds": [{"value": 80.0, "color": "red"}],
        "query_spec": {
            "metrics": [{"attribute": "temp", "fn": "avg"}]
        },
        "columns": [
            {"key": "avg_temp", "unit": "°C", "entity_ref": "iot_reading", "is_measure": true}
        ],
        "data": [
            {"avg_temp": 70.0},
            {"avg_temp": 75.0},
            {"avg_temp": 85.0}
        ]
    });
    let res = normalize_chunk(&body_sparkline);
    assert_eq!(res["metadata"]["engine"], json!("olap"));
    assert_eq!(res["metadata"]["cache_hits"], json!(1));
    assert_eq!(res["metadata"]["cache_ttl_seconds"], json!(180));
    assert_eq!(res["metadata"]["execution_time_ms"], json!(42));
    assert_eq!(
        res.pointer("/viz_ext/payload/signal/value"),
        Some(&json!(85.0))
    );
    assert_eq!(
        res.pointer("/viz_ext/payload/signal/history"),
        Some(&json!([70.0, 75.0, 85.0]))
    );
    assert_eq!(
        res.pointer("/viz_ext/payload/signal/unit"),
        Some(&json!("°C"))
    );
    assert_eq!(
        res.pointer("/viz_ext/payload/signal/entity_ref"),
        Some(&json!("iot_reading"))
    );
    assert!(res.pointer("/viz_ext/payload/signal/thresholds").is_some());

    // 2. KPI with existing metadata (execution_time_ms = 0 updated from root) + array row [[ts, val]] + prev == 0.0 edge cases
    for (curr_val, expected_pct, expected_dir) in [
        (50.0, 100.0, "up"),
        (-25.0, -100.0, "down"),
        (0.0, 0.0, "neutral"),
    ] {
        let body_zero_prev = json!({
            "metadata": {"execution_time_ms": 0},
            "execution_time_ms": 15,
            "output_cast": "KPI",
            "data": [[1_700_000_000.0, curr_val]],
            "viz_ext": {
                "type": "indicator",
                "payload": {
                    "signal": {
                        "value": curr_val,
                        "previous_value": 0.0
                    }
                }
            }
        });
        let r = normalize_chunk(&body_zero_prev);
        assert_eq!(r["metadata"]["execution_time_ms"], json!(15));
        let intel = r.pointer("/viz_ext/payload/signal/intelligence").unwrap();
        assert_eq!(intel["percentage"], json!(expected_pct));
        assert_eq!(intel["direction"], json!(expected_dir));
        assert_eq!(intel["represents_initial"], json!(true));
    }

    // 3. KPI with is_measure column lookup (no query_spec) + already_has intelligence early return
    let body_is_measure = json!({
        "output_cast": "KPI",
        "columns": [{"key": "m_val", "is_measure": true}],
        "data": [{"m_val": 100.0, "previous_value": 100.0001}]
    });
    let r_neutral = normalize_chunk(&body_is_measure);
    assert_eq!(
        r_neutral.pointer("/viz_ext/payload/signal/intelligence/direction"),
        Some(&json!("neutral"))
    );
    // Calling normalize_chunk again hits `already_has` early return
    let r_again = normalize_chunk(&r_neutral);
    assert_eq!(
        r_again.pointer("/viz_ext/payload/signal/intelligence/direction"),
        Some(&json!("neutral"))
    );
}

#[test]
fn viz_normalizer_pie_object_rows_json_chart_override_tree_and_table_branches() {
    // 1. PIE with object rows, is_dimension/is_measure columns, plus fallback rows (null/empty/scalar)
    let pie_obj_body = json!({
        "output_cast": "PIE",
        "viz": "donut",
        "columns": [
            {"key": "category", "is_dimension": true},
            {"key": "total_cost", "is_measure": true}
        ],
        "data": [
            {"category": "HVAC", "total_cost": 1200.0},
            {"category": null, "total_cost": 300.0},
            [null, 150.0],
            "invalid_row"
        ]
    });
    let pie_res = normalize_chunk(&pie_obj_body);
    let signals = pie_res
        .pointer("/viz_ext/payload/breakdown/signals")
        .unwrap();
    assert_eq!(signals["HVAC"]["value"], json!(1200.0));
    assert_eq!(signals["slice_1"]["value"], json!(300.0));
    assert_eq!(signals["slice_2"]["value"], json!(150.0));
    assert_eq!(signals["slice_3"]["value"], json!(0.0));

    // 2. Chart with JSON string in `viz` override: explicit `fill_gaps: false` overrides `TIMESERIES`
    let chart_json_override = json!({
        "output_cast": "TIMESERIES",
        "viz": r#"{"type": "bar", "fill_gaps": false, "horizontal": true, "x_axis_label_template": "X:{{x}}", "y_axis_label_template": "Y:{{y}}"}"#,
        "columns": [{"key": "day"}, {"key": "count"}],
        "data": [{"day": "2026-01-01", "count": 10}]
    });
    let chart_res = normalize_chunk(&chart_json_override);
    assert_eq!(chart_res["viz_ext"]["type"], json!("bar"));
    let chart_payload = &chart_res["viz_ext"]["payload"]["chart"];
    assert!(chart_payload.get("fill_gaps").is_none());
    assert_eq!(chart_payload["horizontal"], json!(true));
    assert_eq!(chart_payload["x_axis_label_template"], json!("X:{{x}}"));
    assert_eq!(chart_payload["y_axis_label_template"], json!("Y:{{y}}"));

    // Chart with JSON string override where `fill_gaps: true` is explicit on non-TIMESERIES
    let chart_json_fill_true = json!({
        "output_cast": "TABLE",
        "viz": r#"{"type": "line", "fill_gaps": true}"#,
        "columns": [{"key": "day"}, {"key": "count"}],
        "data": []
    });
    let chart_res2 = normalize_chunk(&chart_json_fill_true);
    assert_eq!(
        chart_res2["viz_ext"]["payload"]["chart"]["fill_gaps"],
        json!(true)
    );

    // Chart with JSON string override without `fill_gaps` key on `TIMESERIES`
    let chart_json_auto_fill = json!({
        "output_cast": "TIMESERIES",
        "viz": r#"{"type": "area", "smooth": true}"#,
        "columns": [{"key": "day"}, {"key": "count"}],
        "data": []
    });
    let chart_res3 = normalize_chunk(&chart_json_auto_fill);
    assert_eq!(
        chart_res3["viz_ext"]["payload"]["chart"]["fill_gaps"],
        json!(true)
    );

    // 3. TREE viz: with hierarchy.parent_field + icon column vs fallback parent_*_id without icon
    let tree_with_icon = json!({
        "output_cast": "TABLE",
        "viz": "tree",
        "hierarchy": {"parent_field": "custom_parent_id"},
        "columns": [
            {"key": "id"},
            {"key": "title"},
            {"key": "icon"},
            {"key": "custom_parent_id"}
        ],
        "data": []
    });
    let tree_res1 = normalize_chunk(&tree_with_icon);
    let t1 = &tree_res1["viz_ext"]["payload"]["tree"];
    assert_eq!(t1["parent_id_key"], json!("custom_parent_id"));
    assert_eq!(t1["label_key"], json!("title"));
    assert_eq!(t1["icon_key"], json!("icon"));

    let tree_fallback = json!({
        "output_cast": "TABLE",
        "viz": "tree",
        "columns": [
            {"key": "code"},
            {"key": "parent_location_id"}
        ],
        "data": []
    });
    let tree_res2 = normalize_chunk(&tree_fallback);
    let t2 = &tree_res2["viz_ext"]["payload"]["tree"];
    assert_eq!(t2["parent_id_key"], json!("parent_location_id"));
    assert_eq!(t2["label_key"], json!("code"));
    assert!(t2.get("icon_key").is_none());

    // 4. TABLE without query_spec (`has_spec = false`) and with unnamed metric in query_spec
    let table_no_spec = json!({
        "entity_type": "work_order",
        "output_cast": "TABLE",
        "columns": [
            {"key": "code", "type": "string"},
            {"key": "total_cost", "type": "number", "is_measure": true}
        ],
        "data": []
    });
    let tbl_res1 = normalize_chunk(&table_no_spec);
    let cols1 = tbl_res1["viz_ext"]["payload"]["table"]["columns"]
        .as_array()
        .unwrap();
    assert_eq!(cols1.len(), 2);
    assert_eq!(cols1[0]["align"], json!("LEFT"));
    assert_eq!(cols1[1]["align"], json!("RIGHT"));

    let table_unnamed_metric = json!({
        "output_cast": "TABLE",
        "query_spec": {
            "entity": "work_order",
            "metrics": [{"attribute": "cost", "fn": "sum"}]
        },
        "columns": [],
        "data": []
    });
    let tbl_res2 = normalize_chunk(&table_unnamed_metric);
    let cols2 = tbl_res2["viz_ext"]["payload"]["table"]["columns"]
        .as_array()
        .unwrap();
    assert_eq!(cols2[0]["key"], json!("sum_cost"));
    assert_eq!(cols2[0]["align"], json!("RIGHT"));
}

#[test]
fn viz_post_processor_and_multi_series_100_percent() {
    // 1. apply_label_templates: skips Cartesian (including JSON viz hint), applies to TABLE/PIE/CSV_EXPORT
    let dims = vec![DimensionDefinitionT {
        attribute: Some("asset_name".to_string()),
        label_template: Some("Equipo: {{asset_name}}".to_string()),
        ..Default::default()
    }];
    let mut rows_cartesian = vec![json!({"asset_name": "Chiller 1"})];
    apply_label_templates(
        &mut rows_cartesian,
        &dims,
        3,
        &Some(r#"{"type":"bar"}"#.to_string()),
    );
    assert_eq!(rows_cartesian[0]["asset_name"], json!("Chiller 1"));

    let mut rows_table = vec![json!({"asset_name": "Chiller 1"})];
    apply_label_templates(&mut rows_table, &dims, 3, &Some("table".to_string()));
    assert_eq!(rows_table[0]["asset_name"], json!("Equipo: Chiller 1"));

    // 2. inject_hierarchy_children: with parent_field Some vs None
    let mut rows_h1 = vec![json!({"id": "1"})];
    let h_with_parent = HierarchyContextT {
        parent_field: Some("parent_id".to_string()),
        current_node_id: None,
        inject_has_children: true,
    };
    inject_hierarchy_children(&mut rows_h1, Some(&h_with_parent));
    assert_eq!(rows_h1[0]["has_children"], json!(true));

    let mut rows_h2 = vec![json!({"id": "1"})];
    let h_without_parent = HierarchyContextT {
        parent_field: None,
        current_node_id: None,
        inject_has_children: true,
    };
    inject_hierarchy_children(&mut rows_h2, Some(&h_without_parent));
    assert_eq!(rows_h2[0]["has_children"], json!(false));

    // 3. derive_columns & build_query_spec across all aggregation codes (1..=5, 99)
    let metrics: Vec<MetricDefinitionT> = [1, 2, 3, 4, 5, 99]
        .into_iter()
        .map(|code| MetricDefinitionT {
            entity: Some("work_order".to_string()),
            attribute: Some("cost".to_string()),
            aggregation: AggregationFunction(code),
            name: None,
            ..Default::default()
        })
        .collect();
    let qs = build_query_spec("work_order", &dims, &metrics);
    assert_eq!(qs["entity"], json!("work_order"));
    assert_eq!(qs["metrics"].as_array().unwrap().len(), 6);

    let sample_rows = vec![json!({"asset_name": "A", "sum_cost": 10.0})];
    let derived = derive_columns(&sample_rows, &dims, &metrics);
    assert_eq!(derived.len(), 2);

    // 4. redact_sensitive_attributes & is_auth_caller
    if crate::codice::registry::global_opt().is_none() {
        let models_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config/models");
        if let Ok((reg, _)) = crate::codice::CodeRegistry::build(&models_dir) {
            let _ = std::panic::catch_unwind(|| crate::codice::init_global(reg));
        }
    }
    assert!(is_auth_caller("metri-auth", &[]));
    assert!(is_auth_caller(
        "regular-user",
        &["role_super_master".to_string()]
    ));
    assert!(!is_auth_caller("regular-user", &["viewer".to_string()]));

    let normal_ctx = CedarCtx {
        tenant_id: "t1".to_string(),
        user_id: "regular-user".to_string(),
        roles: vec!["viewer".to_string()],
        is_super_master: false,
        cross_tenant_scope: String::new(),
        domain_boundaries: json!({}),
    };
    let auth_ctx = CedarCtx {
        user_id: "metri-auth".to_string(),
        ..normal_ctx.clone()
    };

    let mut user_rows = vec![json!({
        "id": "u1",
        "password_hash": "secret123",
        "user/password_hash": "secret123"
    })];
    redact_sensitive_attributes(&mut user_rows, "user", &auth_ctx);
    assert!(user_rows[0].get("password_hash").is_some());

    redact_sensitive_attributes(&mut user_rows, "user", &normal_ctx);
    assert!(user_rows[0].get("password_hash").is_none());
    assert!(user_rows[0].get("user/password_hash").is_none());

    // Unknown entity is a safe no-op
    redact_sensitive_attributes(&mut user_rows, "non_existent_entity", &normal_ctx);

    // 5. multi_series: outer_join_data & partition_standalone
    let mut single_map = HashMap::new();
    single_map.insert("q1".to_string(), vec![json!({"x": 1})]);
    assert_eq!(outer_join_data(single_map), vec![json!({"x": 1})]);

    let mut multi_map = HashMap::new();
    multi_map.insert(
        "q1".to_string(),
        vec![json!({"bucket": "2026-01", "tags": ["a"], "active": true, "n": null, "v": 10})],
    );
    multi_map.insert(
        "q2".to_string(),
        vec![json!({"bucket": "2026-02", "v": 20})],
    );
    let joined = outer_join_data(multi_map);
    assert_eq!(joined.len(), 2);

    let mut q_map = Map::new();
    q_map.insert("q1".to_string(), json!({}));
    q_map.insert("q2".to_string(), json!({}));
    q_map.insert("q3".to_string(), json!({}));
    assert_eq!(partition_standalone(q_map.clone(), &[]).len(), 3);
    let standalone = partition_standalone(
        q_map,
        &[json!({"query_keys": ["q1"]}), json!({"query-keys": ["q2"]})],
    );
    assert_eq!(standalone.len(), 1);
    assert!(standalone.contains_key("q3"));
}

#[test]
fn grpc_map_viz_meta_all_5_protobuf_payloads_and_empty_fallbacks() {
    assert!(map_viz_meta(None).is_none());

    // 1. Signal + Intelligence
    let sig_json = json!({
        "type": "indicator",
        "payload": {
            "signal": {
                "value": 150.5,
                "previous_value": 100.0,
                "unit": "COP",
                "status_label": "OK",
                "entity_ref": "work_order",
                "intelligence": {
                    "direction": "up",
                    "percentage": 50.5,
                    "delta_abs": 50.5,
                    "previous_value": 100.0,
                    "label": "+50.5%",
                    "is_anomaly": true,
                    "z_score": 3.1,
                    "represents_initial": false
                }
            }
        }
    });
    let pb_sig = map_viz_meta(Some(&sig_json)).unwrap();
    assert_eq!(pb_sig.r#type, "indicator");
    match pb_sig.payload.unwrap() {
        PbVizPayload::Signal(s) => {
            assert_eq!(s.value, 150.5);
            assert_eq!(s.previous_value, Some(100.0));
            assert_eq!(s.unit, "COP");
            let intel = s.intelligence.unwrap();
            assert_eq!(intel.direction, "up");
            assert!(intel.is_anomaly);
            assert_eq!(intel.z_score, 3.1);
        }
        other => panic!("Expected Signal payload, got {other:?}"),
    }

    // 2. ChartDecoration (all 13 fields)
    let chart_json = json!({
        "type": "bar",
        "payload": {
            "chart": {
                "x_dimension": "month",
                "y_dimensions": ["cost", "budget"],
                "color_scheme": "viridis",
                "show_legend": false,
                "show_tooltip": true,
                "title": "Costos Mensuales",
                "stacked": true,
                "smooth": true,
                "label_template": "{{month}}: {{cost}}",
                "fill_gaps": true,
                "x_axis_label_template": "Mes {{month}}",
                "y_axis_label_template": "$ {{cost}}",
                "horizontal": true
            }
        }
    });
    let pb_chart = map_viz_meta(Some(&chart_json)).unwrap();
    match pb_chart.payload.unwrap() {
        PbVizPayload::Chart(c) => {
            assert_eq!(c.x_dimension, "month");
            assert_eq!(c.y_dimensions, vec!["cost", "budget"]);
            assert_eq!(c.color_scheme, "viridis");
            assert!(!c.show_legend);
            assert!(c.show_tooltip);
            assert!(c.stacked);
            assert!(c.smooth);
            assert!(c.fill_gaps);
            assert!(c.horizontal);
            assert_eq!(c.x_axis_label_template, "Mes {{month}}");
            assert_eq!(c.y_axis_label_template, "$ {{cost}}");
        }
        other => panic!("Expected Chart payload, got {other:?}"),
    }

    // 3. BreakdownSignal
    let breakdown_json = json!({
        "type": "pie",
        "payload": {
            "breakdown": {
                "signals": {
                    "HVAC": {"value": 60.0},
                    "ELEC": {"value": 40.0}
                }
            }
        }
    });
    let pb_bd = map_viz_meta(Some(&breakdown_json)).unwrap();
    match pb_bd.payload.unwrap() {
        PbVizPayload::Breakdown(b) => {
            assert_eq!(b.signals.len(), 2);
            assert_eq!(b.signals["HVAC"].value, 60.0);
        }
        other => panic!("Expected Breakdown payload, got {other:?}"),
    }

    // 4. TableMeta
    let table_json = json!({
        "type": "table",
        "payload": {
            "table": {
                "columns": [
                    {"key": "code", "label": "Código", "type": "string", "sortable": true, "format": ""}
                ]
            }
        }
    });
    let pb_tbl = map_viz_meta(Some(&table_json)).unwrap();
    match pb_tbl.payload.unwrap() {
        PbVizPayload::Table(t) => {
            assert_eq!(t.columns.len(), 1);
            assert_eq!(t.columns[0].key, "code");
            assert_eq!(t.columns[0].label, "Código");
        }
        other => panic!("Expected Table payload, got {other:?}"),
    }

    // 5. TreeMeta
    let tree_json = json!({
        "type": "tree",
        "payload": {
            "tree": {
                "id_key": "id",
                "parent_id_key": "parent_id",
                "label_key": "name",
                "has_children_key": "has_children",
                "icon_key": "icon"
            }
        }
    });
    let pb_tree = map_viz_meta(Some(&tree_json)).unwrap();
    match pb_tree.payload.unwrap() {
        PbVizPayload::Tree(tr) => {
            assert_eq!(tr.id_key, "id");
            assert_eq!(tr.parent_id_key, "parent_id");
            assert_eq!(tr.label_key, "name");
            assert_eq!(tr.has_children_key, "has_children");
            assert_eq!(tr.icon_key, "icon");
        }
        other => panic!("Expected Tree payload, got {other:?}"),
    }

    // 6. Unknown / missing payload branch
    let empty_payload = map_viz_meta(Some(&json!({"type": "custom", "payload": {}}))).unwrap();
    assert!(empty_payload.payload.is_none());
    let no_payload = map_viz_meta(Some(&json!({"type": "custom"}))).unwrap();
    assert!(no_payload.payload.is_none());
}
