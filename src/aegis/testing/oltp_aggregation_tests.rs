//! Tests for `aegis::oltp::aggregation`.
use crate::aegis::oltp::aggregation::*;
use serde_json::json;

#[test]
fn count_star_with_no_attribute() {
    use crate::janus::fbs::{AggregationFunction, MetricDefinitionT};
    let rows = vec![
        json!({"status": "ACTIVE"}),
        json!({"status": "ACTIVE"}),
        json!({"status": "INACTIVE"}),
    ];
    let metrics = vec![MetricDefinitionT {
        aggregation: AggregationFunction::COUNT,
        name: Some("total".to_string()),
        ..Default::default()
    }];
    let result = apply_metrics_fbs(&rows, &metrics);
    assert_eq!(result["total"], json!(3.0));
}

#[test]
fn sum_with_attribute() {
    use crate::janus::fbs::{AggregationFunction, MetricDefinitionT};
    let rows = vec![
        json!({"area": 100.0}),
        json!({"area": 200.0}),
        json!({"area": 300.0}),
    ];
    let metrics = vec![MetricDefinitionT {
        aggregation: AggregationFunction::SUM,
        attribute: Some("area".to_string()),
        name: Some("total_area".to_string()),
        ..Default::default()
    }];
    let result = apply_metrics_fbs(&rows, &metrics);
    assert_eq!(result["total_area"], json!(600.0));
}

#[test]
fn filtered_count() {
    use crate::janus::fbs::{
        AggregationFunction, FilterCriteriaT, FilterNodeT, FilterOperator, FilterValueT,
        MetricDefinitionT,
    };
    let rows = vec![
        json!({"status": "ACTIVE",   "area": 100.0}),
        json!({"status": "ACTIVE",   "area": 200.0}),
        json!({"status": "INACTIVE", "area": 300.0}),
    ];
    let filter = FilterNodeT {
        criteria: Some(Box::new(FilterCriteriaT {
            field: Some("status".to_string()),
            op_ref: FilterOperator::EQ,
            value: Some(Box::new(FilterValueT {
                string_val: Some("ACTIVE".to_string()),
                number_val: 0.0,
                bool_val: false,
                list_val: None,
                range_values: None,
                timestamp_val: 0,
            })),
        })),
        group: None,
    };
    let metrics = vec![MetricDefinitionT {
        aggregation: AggregationFunction::COUNT,
        name: Some("active_count".to_string()),
        filter: Some(Box::new(filter)),
        ..Default::default()
    }];
    let result = apply_metrics_fbs(&rows, &metrics);
    assert_eq!(result["active_count"], json!(2.0));
}

#[test]
fn test_custom_strategy_ocp() {
    struct CustomMaxStrategy;
    impl AggregationStrategy for CustomMaxStrategy {
        fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
            vals.iter().copied().fold(f64::NEG_INFINITY, f64::max)
        }
    }
    let vals = vec![10.0, 42.0, 5.0];
    let strategy = CustomMaxStrategy;
    assert_eq!(strategy.compute(&vals, &[]), 42.0);
}
