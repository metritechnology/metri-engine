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
