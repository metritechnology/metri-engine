//! Pruebas unitarias para `janus::plan_selector`.
use crate::janus::plan_selector::{select_plan, EavQueryPlan};
use serde_json::json;

#[test]
fn selects_point_lookup_when_ulid_present() {
    let ast = json!({
        "entity": "asset",
        "where": ["=", "entity/ulid", "01JXYZ"]
    });
    match select_plan(&ast) {
        EavQueryPlan::PointLookup { entity_id } => assert_eq!(entity_id, "01JXYZ"),
        other => panic!("Expected PointLookup, got {:?}", other),
    }
}

#[test]
fn selects_aevt_scan_when_no_filters() {
    let ast = json!({
        "entity": "asset",
        "where": ["=", "tenant/id", "tnt_01"]
    });
    match select_plan(&ast) {
        EavQueryPlan::AevtScan { entity_type, .. } => assert_eq!(entity_type, "asset"),
        other => panic!("Expected AevtScan, got {:?}", other),
    }
}

#[test]
fn selects_avet_single_for_indexed_filter() {
    let ast = json!({
        "entity": "work_order",
        "where": ["and", ["=", "tenant/id", "tnt_01"], ["=", "work_order/status", "OPEN"]]
    });
    match select_plan(&ast) {
        EavQueryPlan::AvetSingleFilter { attr_name, .. } => {
            assert_eq!(attr_name, "work_order/status");
        }
        other => panic!("Expected AvetSingleFilter, got {:?}", other),
    }
}

#[test]
fn selects_avet_range_for_overdue_work_order_and_merges_bounds() {
    use crate::eav::types::datom::DatomValue;
    use crate::eav::types::encoding::AvetBound;

    let ast = json!({
        "entity": "work_order",
        "where": [
            "and",
            ["=", "tenant/id", "tnt_01"],
            ["=", "work_order/status", "IN_PROGRESS"],
            [">=", "work_order/due_date", 1_700_000_000_000i64],
            ["<", "work_order/due_date", 1_730_000_000_000i64]
        ]
    });
    match select_plan(&ast) {
        EavQueryPlan::AvetRangeFilter {
            attr_name,
            lower,
            upper,
            eq_filters,
        } => {
            assert_eq!(attr_name, "work_order/due_date");
            assert_eq!(
                lower,
                AvetBound::Inclusive(DatomValue::Instant(1_700_000_000_000))
            );
            assert_eq!(
                upper,
                AvetBound::Exclusive(DatomValue::Instant(1_730_000_000_000))
            );
            assert_eq!(
                eq_filters,
                vec![(
                    "work_order/status".to_string(),
                    DatomValue::Str("IN_PROGRESS".to_string())
                )]
            );
        }
        other => panic!("Expected AvetRangeFilter, got {:?}", other),
    }
}

#[test]
fn selects_avet_range_fbs_for_lt_and_between() {
    use crate::eav::types::datom::DatomValue;
    use crate::eav::types::encoding::AvetBound;
    use crate::janus::fbs;
    use crate::janus::plan_selector::select_plan_fbs;

    let req = fbs::AnalyticsRequestT {
        entity: Some("work_order".to_string()),
        filters: Some(vec![
            fbs::FilterNodeT {
                criteria: Some(Box::new(fbs::FilterCriteriaT {
                    field: Some("work_order/due_date".to_string()),
                    op_ref: fbs::FilterOperator::LT,
                    value: Some(Box::new(fbs::FilterValueT {
                        number_val: 1_730_000_000_000.0,
                        ..Default::default()
                    })),
                })),
                group: None,
            },
            fbs::FilterNodeT {
                criteria: Some(Box::new(fbs::FilterCriteriaT {
                    field: Some("work_order/status".to_string()),
                    op_ref: fbs::FilterOperator::EQ,
                    value: Some(Box::new(fbs::FilterValueT {
                        string_val: Some("OPEN".to_string()),
                        ..Default::default()
                    })),
                })),
                group: None,
            },
        ]),
        ..Default::default()
    };

    match select_plan_fbs(&req) {
        EavQueryPlan::AvetRangeFilter {
            attr_name,
            lower,
            upper,
            eq_filters,
        } => {
            assert_eq!(attr_name, "due_date");
            assert_eq!(lower, AvetBound::Unbounded);
            assert_eq!(
                upper,
                AvetBound::Exclusive(DatomValue::Instant(1_730_000_000_000))
            );
            assert_eq!(
                eq_filters,
                vec![("status".to_string(), DatomValue::Str("OPEN".to_string()))]
            );
        }
        other => panic!("Expected AvetRangeFilter from FBS, got {:?}", other),
    }
}
