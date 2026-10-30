//! Pruebas unitarias del compilador de AST IR hacia planes nativos EAV (`aegis::oltp::compiler`).
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

#[test]
fn test_compile_native_plan_avet_range_overdue() {
    use crate::eav::types::datom::DatomValue;
    use crate::eav::types::encoding::AvetBound;

    let ast = json!({
        "entity": "work_order",
        "where": ["and", ["<", "work_order/due_date", 1_730_000_000_000i64], ["=", "work_order/status", "IN_PROGRESS"]]
    });
    let plan = compile_native_plan(&ast, "tnt_01");
    match plan {
        NativeQueryPlan::AvetRange {
            tenant_id,
            attr_name,
            lower,
            upper,
            eq_filters,
            entity_type,
        } => {
            assert_eq!(tenant_id, "tnt_01");
            assert_eq!(attr_name, "work_order/due_date");
            assert_eq!(lower, AvetBound::Unbounded);
            assert_eq!(
                upper,
                AvetBound::Exclusive(DatomValue::Instant(1_730_000_000_000))
            );
            assert_eq!(eq_filters.len(), 1);
            assert_eq!(entity_type.as_deref(), Some("work_order"));
        }
        other => panic!("Expected NativeQueryPlan::AvetRange, got {:?}", other),
    }
}
