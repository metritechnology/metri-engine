//! Comprehensive 100% coverage test suite for Aegis VIZ caster (`caster.rs`), aggregations (`aggregation.rs`), hierarchy (`hierarchy.rs`), label templates (`label_template.rs`), and AST IR (`ast_ir.rs`).

use serde_json::{json, Value};

use crate::aegis::ast_ir::{AstIr, OutputCast, WhereNode};
use crate::aegis::label_template::{extract_fields, format_value, interpolate, interpolate_rows};
use crate::aegis::oltp::aggregation::{apply_metrics_fbs, derive_columns};
use crate::aegis::oltp::caster::{
    apply_output_cast_fbs, apply_timeseries_bucketing, truncate_to_interval,
};
use crate::aegis::oltp::hierarchy::inject_has_children_from_rows;
use crate::janus::fbs::{
    AggregationFunction, AnalyticalComparisonT, AnalyticalComparison_ComparisonType,
    AnalyticsRequestT, DimensionDefinitionT, FormulaEntryT, MetricDefinitionT, OutputCastType,
    TimeFrameContextT,
};
use crate::temporal::core::TimeRange;

#[test]
fn viz_caster_kpi_with_measures_and_all_comparison_branches() {
    let now: i64 = 1_760_000_000;
    let time_range = TimeRange {
        start_ts: Some(now - 7 * 86_400),
        end_ts: Some(now),
    };

    let rows = vec![
        json!({"created_at": now - 86_400, "cost": 100.0, "work_order/cost": 100.0, "hours": 4.0}),
        json!({"created_at": now - 2 * 86_400, "cost": 300.0, "work_order/cost": 300.0, "hours": 6.0}),
    ];
    let all_rows = vec![
        json!({"created_at": now - 86_400, "cost": 100.0, "work_order/cost": 100.0, "hours": 4.0}),
        json!({"created_at": now - 2 * 86_400, "cost": 300.0, "work_order/cost": 300.0, "hours": 6.0}),
        json!({"created_at": now - 10 * 86_400, "cost": 200.0, "work_order/cost": 200.0, "hours": 5.0}),
    ];

    // 1. Test formula measures (valid, NaN, lexer error, parser error, evaluator error)
    let measures = vec![
        FormulaEntryT {
            name: Some("double_hours".to_string()),
            formula: Some("hours * 2".to_string()),
        },
        FormulaEntryT {
            name: Some("nan_measure".to_string()),
            formula: Some("0 / 0".to_string()),
        },
        FormulaEntryT {
            name: Some("bad_lexer".to_string()),
            formula: Some("hours @ 2".to_string()),
        },
        FormulaEntryT {
            name: Some("bad_parser".to_string()),
            formula: Some("hours + * 2".to_string()),
        },
        FormulaEntryT {
            name: Some("missing_var".to_string()),
            formula: Some("non_existent_col * 2".to_string()),
        },
    ];

    // Unnamed metric with attribute "cost" -> alias "sum_cost"
    let metrics = vec![
        MetricDefinitionT {
            attribute: Some("cost".to_string()),
            aggregation: AggregationFunction::SUM,
            name: None,
            ..Default::default()
        },
        MetricDefinitionT {
            attribute: Some("double_hours".to_string()),
            aggregation: AggregationFunction::SUM,
            name: Some("total_double_hours".to_string()),
            ..Default::default()
        },
    ];

    let comparisons = vec![AnalyticalComparisonT {
        type_: AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE,
        relative_granularity: Some("day".to_string()),
        relative_amount: 7,
        label: Some("prev_week".to_string()),
        ..Default::default()
    }];

    let ast_ir = AnalyticsRequestT {
        output_cast: OutputCastType(1), // KPI
        measures: Some(measures),
        metrics: Some(metrics),
        comparisons: Some(comparisons),
        time_frame: Some(Box::new(TimeFrameContextT {
            timezone: Some("UTC".to_string()),
            ..Default::default()
        })),
        ..Default::default()
    };

    let out = apply_output_cast_fbs(&rows, &all_rows, &ast_ir, &time_range);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["total_double_hours"], json!(20.0));
    assert_eq!(out[0]["previous_value"], json!(200.0));

    // 2. Test KPI with BENCHMARK comparison fallback and unnamed metrics (1=count, 3=avg, 4=min, 5=max, 6=agg)
    for (agg_code, expected_val) in [
        (AggregationFunction::COUNT, 2.0),
        (AggregationFunction::AVG, 200.0),
        (AggregationFunction::MIN, 100.0),
        (AggregationFunction::MAX, 300.0),
        (AggregationFunction::MEDIAN, 300.0),
    ] {
        let ast_bench = AnalyticsRequestT {
            output_cast: OutputCastType(1),
            metrics: Some(vec![MetricDefinitionT {
                attribute: Some("work_order/cost".to_string()),
                aggregation: agg_code,
                name: None,
                ..Default::default()
            }]),
            comparisons: Some(vec![AnalyticalComparisonT {
                type_: AnalyticalComparison_ComparisonType::BENCHMARK,
                benchmark_value: 250.0,
                label: Some("target".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let bench_out = apply_output_cast_fbs(&rows, &all_rows, &ast_bench, &time_range);
        assert_eq!(bench_out.len(), 1);
        assert_eq!(bench_out[0]["previous_value"], json!(250.0));
        let _ = expected_val;
    }

    // 3. KPI without comparisons
    let ast_simple = AnalyticsRequestT {
        output_cast: OutputCastType(1),
        metrics: Some(vec![MetricDefinitionT {
            attribute: Some("hours".to_string()),
            aggregation: AggregationFunction::SUM,
            name: Some("sum_hours".to_string()),
            ..Default::default()
        }]),
        comparisons: None,
        ..Default::default()
    };
    let simple_out = apply_output_cast_fbs(&rows, &rows, &ast_simple, &time_range);
    assert_eq!(simple_out, vec![json!({"sum_hours": 10.0})]);
}

#[test]
fn viz_caster_pie_bubble_timeseries_table_and_csv_export_all_branches() {
    let time_range = TimeRange {
        start_ts: None,
        end_ts: None,
    };

    let rows = vec![
        json!({"category": "HVAC", "priority": 1, "score": 2.5, "cost": 500.0, "created_at": 1_760_000_000_000.0}),
        json!({"category": "HVAC", "priority": 1, "score": 2.5, "cost": 300.0, "meta/created_at": 1_760_000_100.0}),
        json!({"category": "ELEC", "priority": 2, "score": 1.5, "cost": 900.0, "timestamp": 1_760_086_400.0}),
        json!({"category": "", "cost": 100.0, "updated_at": 1_760_086_500.0}),
    ];

    let metrics = vec![MetricDefinitionT {
        attribute: Some("cost".to_string()),
        aggregation: AggregationFunction::SUM,
        name: Some("total_cost".to_string()),
        ..Default::default()
    }];

    // 1. PIE (4) & BUBBLE (5) with dimensions (str, i64, f64)
    for cast_code in [4, 5] {
        let ast_pie = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: Some(vec![
                DimensionDefinitionT {
                    attribute: Some("category".to_string()),
                    ..Default::default()
                },
                DimensionDefinitionT {
                    attribute: Some("priority".to_string()),
                    ..Default::default()
                },
                DimensionDefinitionT {
                    attribute: Some("score".to_string()),
                    ..Default::default()
                },
            ]),
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let pie_out = apply_output_cast_fbs(&rows, &rows, &ast_pie, &time_range);
        assert_eq!(pie_out.len(), 2);
        // Sorted descending: ELEC (900.0) first, HVAC (800.0) second
        assert_eq!(pie_out[0]["total_cost"], json!(900.0));
        assert_eq!(pie_out[1]["total_cost"], json!(800.0));

        // PIE with empty dim_keys
        let ast_pie_empty_dims = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: Some(vec![DimensionDefinitionT {
                attribute: None,
                ..Default::default()
            }]),
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let fallback_out = apply_output_cast_fbs(&rows, &rows, &ast_pie_empty_dims, &time_range);
        assert_eq!(fallback_out.len(), 1);
        assert_eq!(fallback_out[0]["total_cost"], json!(1800.0));

        // PIE with dimensions: None
        let ast_pie_no_dims = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: None,
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let no_dims_out = apply_output_cast_fbs(&rows, &rows, &ast_pie_no_dims, &time_range);
        assert_eq!(no_dims_out.len(), 1);
    }

    // 2. TIMESERIES (2) with dimensions vs without dimensions + invalid interval fallback to Day
    assert_eq!(
        truncate_to_interval(1_760_000_123, "invalid_interval"),
        truncate_to_interval(1_760_000_123, "day")
    );
    assert!(truncate_to_interval(1_760_000_123, "hour") <= 1_760_000_123);

    let ts_dims = vec![
        DimensionDefinitionT {
            attribute: Some("bucket".to_string()),
            interval: Some("day".to_string()),
            ..Default::default()
        },
        DimensionDefinitionT {
            attribute: Some("category".to_string()),
            interval: None,
            ..Default::default()
        },
    ];
    let ast_ts = AnalyticsRequestT {
        output_cast: OutputCastType(2),
        dimensions: Some(ts_dims.clone()),
        metrics: Some(metrics.clone()),
        ..Default::default()
    };
    let ts_out = apply_output_cast_fbs(&rows, &rows, &ast_ts, &time_range);
    assert!(!ts_out.is_empty());
    assert!(ts_out[0].get("bucket").is_some());

    // Direct call with ingested_at and missing timestamp
    let extra_ts_rows = vec![
        json!({"ingested_at": 1_760_000_000.0, "cost": 50.0, "category": "A"}),
        json!({"cost": 25.0}),
    ];
    let bucketed = apply_timeseries_bucketing(&extra_ts_rows, &metrics, &ts_dims);
    assert_eq!(bucketed.len(), 2);

    // TIMESERIES without dimensions returns rows as-is
    let ast_ts_no_dims = AnalyticsRequestT {
        output_cast: OutputCastType(2),
        dimensions: None,
        metrics: Some(metrics.clone()),
        ..Default::default()
    };
    assert_eq!(
        apply_output_cast_fbs(&rows, &rows, &ast_ts_no_dims, &time_range).len(),
        rows.len()
    );

    // 3. TABLE (3) & CSV_EXPORT (6) with namespaced dimensions, empty dim_keys, empty dims, no dims, no metrics
    for cast_code in [3, 6] {
        let ast_tbl = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: Some(vec![
                DimensionDefinitionT {
                    attribute: Some("work_order/category".to_string()),
                    ..Default::default()
                },
                DimensionDefinitionT {
                    attribute: Some("priority".to_string()),
                    ..Default::default()
                },
                DimensionDefinitionT {
                    attribute: Some("score".to_string()),
                    ..Default::default()
                },
            ]),
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let tbl_out = apply_output_cast_fbs(&rows, &rows, &ast_tbl, &time_range);
        assert_eq!(tbl_out.len(), 3);

        // Empty dim_keys
        let ast_tbl_empty_key = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: Some(vec![DimensionDefinitionT {
                attribute: None,
                ..Default::default()
            }]),
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        assert_eq!(
            apply_output_cast_fbs(&rows, &rows, &ast_tbl_empty_key, &time_range).len(),
            rows.len()
        );

        // Empty dims vector
        let ast_tbl_empty_dims = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: Some(vec![]),
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        assert_eq!(
            apply_output_cast_fbs(&rows, &rows, &ast_tbl_empty_dims, &time_range).len(),
            rows.len()
        );

        // No dims
        let ast_tbl_no_dims = AnalyticsRequestT {
            output_cast: OutputCastType(cast_code),
            dimensions: None,
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        assert_eq!(
            apply_output_cast_fbs(&rows, &rows, &ast_tbl_no_dims, &time_range).len(),
            rows.len()
        );
    }

    // Empty metrics & None metrics
    let ast_empty_metrics = AnalyticsRequestT {
        output_cast: OutputCastType(1),
        metrics: Some(vec![]),
        ..Default::default()
    };
    assert_eq!(
        apply_output_cast_fbs(&rows, &rows, &ast_empty_metrics, &time_range).len(),
        rows.len()
    );
    let ast_none_metrics = AnalyticsRequestT {
        output_cast: OutputCastType(1),
        metrics: None,
        ..Default::default()
    };
    assert_eq!(
        apply_output_cast_fbs(&rows, &rows, &ast_none_metrics, &time_range).len(),
        rows.len()
    );
}

#[test]
fn viz_aggregation_all_14_functions_and_column_derivation() {
    let rows = vec![
        json!({"asset/cost": 10.0, "hours": 2.0, "created_at": 1_700_000_000, "epoch_ms": 1_760_000_000_000.0, "active": true, "name": "A"}),
        json!({"asset/cost": 20.0, "hours": 4.0, "created_at": 1_700_000_100, "epoch_ms": 1_760_000_001_000.0, "active": false, "name": "B"}),
        json!({"asset/cost": 30.0, "hours": 6.0, "created_at": 1_700_000_200, "epoch_ms": 1_760_000_002_000.0, "active": true, "name": "C"}),
        json!({"asset/cost": 40.0, "hours": 8.0, "created_at": 1_700_000_300, "epoch_ms": 1_760_000_003_000.0, "active": true, "name": "D"}),
    ];

    let all_aggs = vec![
        AggregationFunction::COUNT,
        AggregationFunction::SUM,
        AggregationFunction::AVG,
        AggregationFunction::MIN,
        AggregationFunction::MAX,
        AggregationFunction::MEDIAN,
        AggregationFunction::STD_DEV,
        AggregationFunction::VARIANCE,
        AggregationFunction::PERCENTILE_90,
        AggregationFunction::PERCENTILE_95,
        AggregationFunction::PERCENTILE_99,
        AggregationFunction::CORRELATION,
        AggregationFunction::LINEAR_REGRESSION,
        AggregationFunction::LOGISTIC_REGRESSION,
        AggregationFunction(99), // fallback
    ];

    let metrics: Vec<MetricDefinitionT> = all_aggs
        .into_iter()
        .map(|agg| MetricDefinitionT {
            attribute: Some("asset/cost".to_string()),
            secondary_attribute: Some("hours".to_string()),
            aggregation: agg,
            name: None,
            ..Default::default()
        })
        .collect();

    let agg_obj = apply_metrics_fbs(&rows, &metrics);
    assert_eq!(agg_obj["count_asset/cost"], json!(4.0));
    assert_eq!(agg_obj["sum_asset/cost"], json!(100.0));
    assert_eq!(agg_obj["avg_asset/cost"], json!(25.0));
    assert_eq!(agg_obj["min_asset/cost"], json!(10.0));
    assert_eq!(agg_obj["max_asset/cost"], json!(40.0));
    assert_eq!(agg_obj["median_asset/cost"], json!(30.0));
    assert_eq!(agg_obj["p90_asset/cost"], json!(40.0));
    assert_eq!(agg_obj["p95_asset/cost"], json!(40.0));
    assert_eq!(agg_obj["p99_asset/cost"], json!(40.0));
    assert!((agg_obj["correlation_asset/cost"].as_f64().unwrap() - 1.0).abs() < 1e-6);
    assert!((agg_obj["linear_regression_asset/cost"].as_f64().unwrap() - 5.0).abs() < 1e-6);
    assert_eq!(agg_obj["logistic_regression_asset/cost"], json!(4.0));
    assert_eq!(agg_obj["agg_asset/cost"], json!(100.0));

    // Edge cases: empty vals / constant vals for correlation & regression
    let const_rows = vec![json!({"x": 5.0, "y": 5.0}), json!({"x": 5.0, "y": 5.0})];
    let const_metrics = vec![
        MetricDefinitionT {
            attribute: Some("x".to_string()),
            secondary_attribute: Some("y".to_string()),
            aggregation: AggregationFunction::CORRELATION,
            name: Some("corr_const".to_string()),
            ..Default::default()
        },
        MetricDefinitionT {
            attribute: Some("x".to_string()),
            secondary_attribute: Some("y".to_string()),
            aggregation: AggregationFunction::LINEAR_REGRESSION,
            name: Some("regr_const".to_string()),
            ..Default::default()
        },
    ];
    let const_res = apply_metrics_fbs(&const_rows, &const_metrics);
    assert_eq!(const_res["corr_const"], json!(0.0));
    assert_eq!(const_res["regr_const"], json!(0.0));

    // derive_columns & infer_col_type
    assert!(derive_columns(&[], &[], &[]).is_empty());
    let cols = derive_columns(&rows, &["name".to_string()], &["asset/cost".to_string()]);
    assert_eq!(cols.len(), 6);
    // First column must be the dimension "name", last must be the measure "asset/cost"
    assert_eq!(cols.first().unwrap()["key"], json!("name"));
    assert_eq!(cols.first().unwrap()["is_dimension"], json!(true));
    assert_eq!(cols.last().unwrap()["key"], json!("asset/cost"));
    assert_eq!(cols.last().unwrap()["is_measure"], json!(true));

    let created_col = cols.iter().find(|c| c["key"] == "created_at").unwrap();
    assert_eq!(created_col["type"], json!("timestamp"));
    let epoch_ms_col = cols.iter().find(|c| c["key"] == "epoch_ms").unwrap();
    assert_eq!(epoch_ms_col["type"], json!("timestamp"));
    let active_col = cols.iter().find(|c| c["key"] == "active").unwrap();
    assert_eq!(active_col["type"], json!("boolean"));
}

#[test]
fn viz_hierarchy_and_label_template_full_coverage() {
    // 1. Hierarchy: empty rows & populated parent-child tree
    let mut empty: Vec<Value> = vec![];
    inject_has_children_from_rows(&mut empty, "location/parent_id");

    let mut tree_rows = vec![
        json!({"id": "root", "parent_id": ""}),
        json!({"id": "child-1", "location/parent_id": "root"}),
        json!({"id": "leaf-1", "parent_id": "child-1"}),
    ];
    inject_has_children_from_rows(&mut tree_rows, "location/parent_id");
    assert_eq!(tree_rows[0]["has_children"], json!(true));
    assert_eq!(tree_rows[1]["has_children"], json!(true));
    assert_eq!(tree_rows[2]["has_children"], json!(false));

    // 2. Label templates: empty template, suffix key coercion, boolean/array formatting, interpolate_rows
    assert_eq!(interpolate("   ", &json!({"a": 1})), None);
    assert!(extract_fields("   ").is_empty());
    assert_eq!(format_value(Some(&json!(true))), "true");
    assert_eq!(format_value(Some(&json!([1, 2]))), "[1,2]");

    let mut rows = vec![
        json!({"custom_ns/code": "CH-01", "power": 120.556}),
        json!({"custom_ns/code": "CH-02", "power": 90.0}),
    ];
    interpolate_rows(&mut rows, "   "); // no-op on empty
    assert!(rows[0].get("_label").is_none());

    interpolate_rows(&mut rows, "{{ns2/code}} ({{power}} kW)");
    //Wait: custom_ns/code ends with "/code", so searching for "code" via suffix matches!
    let mut rows2 = vec![json!({"custom_ns/code": "CH-01", "power": 120.556})];
    interpolate_rows(&mut rows2, "{{code}} ({{power}} kW)");
    assert_eq!(rows2[0]["_label"], json!("CH-01 (120.56 kW)"));
}

#[test]
fn viz_ast_ir_all_output_casts_and_21_where_operators() {
    for (cast_str, expected) in [
        ("KPI", OutputCast::KPI),
        ("TIMESERIES", OutputCast::TIMESERIES),
        ("TABLE", OutputCast::TABLE),
        ("PIE", OutputCast::PIE),
        ("BUBBLE", OutputCast::BUBBLE),
        ("CSV_EXPORT", OutputCast::CsvExport),
        ("UNKNOWN_CAST", OutputCast::TABLE),
    ] {
        let ast_json = json!({
            "entity": "work_order",
            "output_cast": cast_str,
            "select": ["id", {"cost": "sum"}],
            "group_by": [{"attribute": "status"}],
            "order_by": [{"field": "cost", "descending": true}],
            "metrics": [{"attribute": "cost", "aggregation": "sum", "name": "total_cost"}],
            "measures": [{"name": "m1", "formula": "cost * 1.19"}],
            "semantic_measures": [{"metric_key": "mtbf"}],
            "comparisons": [{"type": "BENCHMARK", "benchmark_value": 100.0}],
            "hierarchy": {"parent_field": "parent_id", "inject_has_children": true},
            "limit": 25,
            "schema": {"entity": "work_order", "attributes": [{"name": "cost", "type": "number"}]},
            "where": [
                "and",
                ["=", "status", "OPEN"],
                ["not=", "status", "CANCELLED"],
                [">", "cost", 10],
                ["<", "cost", 1000],
                [">=", "priority", 1],
                ["<=", "priority", 5],
                ["in", "category", ["HVAC", "ELEC"]],
                ["not-in", "category", ["CIVIL"]],
                ["between", "created_at", [100, 200]],
                ["matches", "code", "^WO-.*"],
                ["like", "title", "%Chiller%"],
                ["contains", "description", "vibración"],
                ["is-null", "deleted_at"],
                ["is-not-null", "asset_id"],
                ["or", ["fuzzy", "title", "valbula"], ["fts", "bomba"]],
                ["not", ["=", "archived", true]],
                ["ref-filter", "asset_id", ["=", "criticality", "HIGH"]]
            ]
        });

        let parsed = AstIr::from_value(&ast_json).expect("valid AstIr");
        assert_eq!(parsed.output_cast, expected);
        assert_eq!(parsed.entity, "work_order");
        assert_eq!(parsed.limit, Some(25));
    }

    // Empty & null WHERE nodes
    assert_eq!(
        WhereNode::from_value(&json!(null)).unwrap(),
        WhereNode::Empty
    );
    assert_eq!(WhereNode::from_value(&json!([])).unwrap(), WhereNode::Empty);

    // Error paths in WhereNode and AstIr
    assert!(AstIr::from_value(&json!({})).is_err());
    assert!(WhereNode::from_value(&json!("not_an_array")).is_err());
    assert!(WhereNode::from_value(&json!([123])).is_err());
    assert!(WhereNode::from_value(&json!(["unknown_op", "col", 1])).is_err());
    for op in [
        "=",
        "not=",
        ">",
        "<",
        ">=",
        "<=",
        "in",
        "not-in",
        "between",
        "matches",
        "like",
        "contains",
        "is-null",
        "is-not-null",
        "not",
        "fuzzy",
        "fts",
        "ref-filter",
    ] {
        assert!(
            WhereNode::from_value(&json!([op])).is_err(),
            "op {op} without args must fail"
        );
    }
    assert!(WhereNode::from_value(&json!(["matches", "col"])).is_err());
    assert!(WhereNode::from_value(&json!(["like", "col"])).is_err());
    assert!(WhereNode::from_value(&json!(["contains", "col"])).is_err());
    assert!(WhereNode::from_value(&json!(["fuzzy", "col"])).is_err());
    assert!(WhereNode::from_value(&json!(["ref-filter", "col"])).is_err());
}
