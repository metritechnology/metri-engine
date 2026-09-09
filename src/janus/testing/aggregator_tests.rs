//! Tests for `janus::aggregator`.
use crate::janus::aggregator::apply_output_cast;
use serde_json::json;

#[test]
fn kpi_sum_aggregation() {
    let rows = vec![
        json!({"cost": 100.0, "status": "OPEN"}),
        json!({"cost": 200.0, "status": "CLOSED"}),
        json!({"cost": 300.0, "status": "OPEN"}),
    ];
    let ast = json!({
        "output_cast": "KPI",
        "metrics": [{"field": "cost", "aggregation": "SUM", "alias": "total_cost"}]
    });
    let result = apply_output_cast(rows, &ast);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["total_cost"], json!(600.0));
}

#[test]
fn table_passes_rows_through() {
    let rows = vec![json!({"a": 1}), json!({"a": 2})];
    let ast = json!({"output_cast": "TABLE"});
    let result = apply_output_cast(rows.clone(), &ast);
    assert_eq!(result.len(), 2);
}
