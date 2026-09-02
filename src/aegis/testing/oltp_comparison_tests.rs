use crate::aegis::oltp::comparison::*;
use crate::aegis::oltp::aggregation::apply_metrics_fbs;
use crate::janus::fbs::{MetricDefinitionT, AggregationFunction, AnalyticalComparisonT, AnalyticalComparison_ComparisonType};
use crate::temporal::core::TimeRange;
use serde_json::{Value, json};

fn make_row(created_at: i64, value: f64) -> Value {
    json!({ "created_at": created_at, "revenue": value })
}

fn make_metric(attr: &str, name: &str) -> MetricDefinitionT {
    MetricDefinitionT {
        attribute: Some(attr.to_string()),
        aggregation: AggregationFunction::SUM,
        name: Some(name.to_string()),
        ..Default::default()
    }
}

fn epoch_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(1_700_000_000)
}

#[test]
fn test_time_shift_relative() {
    let now = epoch_now();
    // Período actual: últimos 7 días
    let cs = now - 7 * 86400;
    let ce = now;
    // Rows: 3 en período actual + 3 hace 7-14 días (prev)
    let rows = vec![
        make_row(now - 1 * 86400, 100.0),
        make_row(now - 3 * 86400, 200.0),
        make_row(now - 5 * 86400, 150.0),
        make_row(now - 8 * 86400, 50.0),  // prev
        make_row(now - 10 * 86400, 70.0), // prev
        make_row(now - 12 * 86400, 80.0), // prev
    ];
    let metric = make_metric("revenue", "sum_revenue");
    let tf = TimeRange { start_ts: Some(cs), end_ts: Some(ce) };

    // Métricas actuales (período principal)
    let current_rows: Vec<Value> = rows.iter()
        .filter(|r| {
            let ts = r["created_at"].as_i64().unwrap_or(0);
            ts >= cs && ts <= ce
        })
        .cloned().collect();
    let current_m = apply_metrics_fbs(&current_rows, &[metric.clone()]);
    let current_map = match current_m { Value::Object(m) => m, _ => panic!("expected object") };

    let comp = AnalyticalComparisonT {
        type_: AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE,
        relative_granularity: Some("day".to_string()),
        relative_amount: 7,
        label: Some("prev_week".to_string()),
        ..Default::default()
    };

    let result = run_comparisons(&rows, &[metric], &[comp], Some(&tf), current_map, "UTC");

    assert!(result.contains_key("current_sum_revenue"), "debe tener current_X cuando hay TIME_SHIFT");
    assert!(result.contains_key("prev_0_sum_revenue"), "debe tener prev_0_X");
    let prev_sum = result["prev_0_sum_revenue"].as_f64().unwrap_or(0.0);
    assert!((prev_sum - 200.0).abs() < 0.01, "prev_sum debe ser 50+70+80=200, got {prev_sum}");
}

#[test]
fn test_benchmark_inline() {
    let rows = vec![make_row(0, 500.0)];
    let metric = make_metric("revenue", "sum_revenue");
    let comp = AnalyticalComparisonT {
        type_: AnalyticalComparison_ComparisonType::BENCHMARK,
        benchmark_value: 1000.0,
        label: Some("target".to_string()),
        ..Default::default()
    };
    let current_m = apply_metrics_fbs(&rows, &[metric.clone()]);
    let current_map = match current_m { Value::Object(m) => m, _ => panic!() };
    let result = run_comparisons(&rows, &[metric], &[comp], None, current_map, "UTC");

    assert!(result.contains_key("benchmark_target"));
    assert_eq!(result["benchmark_target"].as_f64().unwrap(), 1000.0);
}

#[test]
fn test_smart_z_score() {
    let now = epoch_now();
    // Histórico: 91 días de data con valores conocidos
    let mut rows = Vec::new();
    for i in 1..=90 {
        rows.push(make_row(now - i * 86400, 100.0)); // histórico uniforme
    }
    // Actual: valor muy alejado de la media
    rows.push(make_row(now - 86400 / 2, 200.0)); // 1 día ago

    let metric = make_metric("revenue", "sum_revenue");
    let tf = TimeRange { start_ts: Some(now - 86400), end_ts: Some(now) };

    let current_m = apply_metrics_fbs(&vec![rows.last().unwrap().clone()], &[metric.clone()]);
    let current_map = match current_m { Value::Object(m) => m, _ => panic!() };

    let comp = AnalyticalComparisonT {
        type_: AnalyticalComparison_ComparisonType::SMART,
        ..Default::default()
    };
    let result = run_comparisons(&rows, &[metric], &[comp], Some(&tf), current_map, "UTC");

    assert!(result.contains_key("mean_sum_revenue"), "debe tener mean_X");
    assert!(result.contains_key("std_sum_revenue"), "debe tener std_X");
    let mean = result["mean_sum_revenue"].as_f64().unwrap_or(0.0);
    // Con 90 rows de 100.0, la media debería ser ~100.0
    assert!((mean - 100.0).abs() < 1.0, "mean debe ser ~100.0, got {mean}");
}
