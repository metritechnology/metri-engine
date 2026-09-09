//! Tests for `aegis::oltp::compiler`.
use crate::aegis::oltp::compiler::*;
use crate::eav::reader::query::NativeQueryPlan;
use serde_json::json;

#[test]
fn test_resolve_ts_field() {
    let schema = json!({
        "attributes": [
            {"name": "event_ts", "type": "epoch"},
            {"name": "value", "type": "number"}
        ]
    });
    assert_eq!(resolve_ts_field("asset", Some(&schema)), "asset/event_ts");
    assert_eq!(resolve_ts_field("asset", None), "meta/created_at");
}

#[test]
fn test_compile_native_plan_point_lookup() {
    let ast = json!({
        "entity": "asset",
        "where": ["=", "entity/ulid", "01JTEST"]
    });
    let plan = compile_native_plan(&ast, "tnt_01");
    assert!(matches!(plan, NativeQueryPlan::PointLookup { entity_id } if entity_id == "01JTEST"));
}

#[test]
fn test_compile_native_plan_fts() {
    let ast = json!({
        "entity": "asset",
        "search": "bomba hidraulica"
    });
    let plan = compile_native_plan(&ast, "tnt_01");
    assert!(matches!(plan, NativeQueryPlan::FtsSearch { term, .. } if term == "bomba hidraulica"));
}
