//! Tests for `infrastructure::local_s3_query_engine`.
// infrastructure/tests/local_s3_query_engine_tests.rs
//
// Corpus de caracterización de la Fase 5 (PLAN_CORRECCIONES_PENDIENTES):
// fijar el comportamiento ACTUAL de los helpers puros del motor local ANTES
// de descomponer `execute_single_query`. Si una extracción cambia un valor
// aquí, el commit lo justifica o se revierte (Regla 04 del plan mayor).

use crate::codice::registry::AttrType;
use crate::infrastructure::local_s3_query_engine::*;
use serde_json::json;
use std::collections::HashMap;

// ── extract_entity_from_sql ─────────────────────────────────────────────────

#[test]
fn entidad_desde_from_calificado_y_desnudo() {
    assert_eq!(
        extract_entity_from_sql("SELECT id FROM metri_olap.audit_log WHERE x = 1"),
        Some("audit_log".to_string())
    );
    assert_eq!(
        extract_entity_from_sql("SELECT id FROM audit_log LIMIT 10"),
        Some("audit_log".to_string())
    );
}

#[test]
fn sufijos_raw_y_rollup_se_recortan() {
    assert_eq!(
        extract_entity_from_sql("SELECT v FROM metri_olap.meter_reading_raw"),
        Some("meter_reading".to_string())
    );
    assert_eq!(
        extract_entity_from_sql("SELECT v FROM meter_reading_rollup"),
        Some("meter_reading".to_string())
    );
}

#[test]
fn select_anidado_no_es_entidad() {
    assert_eq!(extract_entity_from_sql("SELECT 1"), None);
}

// ── extract_projected_columns ───────────────────────────────────────────────

#[test]
fn proyeccion_simple_con_alias() {
    let cols = extract_projected_columns("SELECT id AS rid, action_type FROM t");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].alias, "rid");
    assert_eq!(cols[0].expr, "id");
    assert!(!cols[0].is_aggregate);
    assert_eq!(cols[1].alias, "action_type");
}

#[test]
fn count_star_es_agregado() {
    let cols = extract_projected_columns("SELECT COUNT(*) AS total FROM t");
    assert_eq!(cols.len(), 1);
    assert!(cols[0].is_aggregate);
    assert_eq!(cols[0].agg_fn.as_deref(), Some("COUNT"));
    assert_eq!(cols[0].agg_field.as_deref(), Some("*"));
    assert_eq!(cols[0].alias, "total");
}

#[test]
fn sum_de_campo_con_alias() {
    let cols = extract_projected_columns("SELECT SUM(reading_value) AS suma FROM t");
    assert!(cols[0].is_aggregate);
    assert_eq!(cols[0].agg_fn.as_deref(), Some("SUM"));
    assert_eq!(cols[0].agg_field.as_deref(), Some("reading_value"));
}

#[test]
fn date_trunc_es_columna_no_agregada() {
    // El motor trata date_trunc como columna de grupo, no como agregado.
    let cols = extract_projected_columns(
        "SELECT date_trunc('day', created_at) AS bucket, COUNT(*) AS c FROM t GROUP BY bucket",
    );
    assert_eq!(cols.len(), 2);
    assert!(!cols[0].is_aggregate);
    assert!(cols[0].expr.contains("date_trunc"));
    assert_eq!(cols[0].alias, "bucket");
    assert!(cols[1].is_aggregate);
}

// ── extract_limit_from_sql ──────────────────────────────────────────────────

#[test]
fn limit_presente_y_ausente() {
    assert_eq!(extract_limit_from_sql("SELECT x FROM t LIMIT 42"), Some(42));
    assert_eq!(extract_limit_from_sql("SELECT x FROM t"), None);
}

// ── extract_filters_from_sql ────────────────────────────────────────────────

#[test]
fn filtro_de_igualdad_string_y_numerico() {
    let f = extract_filters_from_sql("SELECT x FROM t WHERE tenant_id = 'tnt_01' AND seq = 7");
    assert_eq!(f.get("tenant_id"), Some(&json!("tnt_01")));
    assert_eq!(f.get("seq"), Some(&json!(7)));
}

#[test]
fn filtro_in_de_lista() {
    let f = extract_filters_from_sql("SELECT x FROM t WHERE action_type IN ('CREATE', 'DELETE')");
    match f.get("action_type") {
        Some(serde_json::Value::Array(vals)) => {
            assert_eq!(vals, &vec![json!("CREATE"), json!("DELETE")]);
        }
        other => panic!("esperaba array, obtuve {other:?}"),
    }
}

#[test]
fn filtro_booleano() {
    let f = extract_filters_from_sql("SELECT x FROM t WHERE retryable = true");
    assert_eq!(f.get("retryable"), Some(&json!(true)));
}

// ── extract_timestamp_range_from_sql ────────────────────────────────────────

#[test]
fn rango_con_fechas_iso() {
    let (start, end) = extract_timestamp_range_from_sql(
        "SELECT x FROM t WHERE created_at >= '2026-08-01' AND created_at <= '2026-08-31'",
    );
    let s = start.expect("start");
    let e = end.expect("end");
    assert_eq!(s, 1785542400, "2026-08-01T00:00:00Z en segundos");
    assert_eq!(e, 1788220799, "2026-08-31T23:59:59Z en segundos");
}

#[test]
fn rango_con_epoch_y_columna_timestamp() {
    let (start, end) = extract_timestamp_range_from_sql(
        "SELECT x FROM t WHERE timestamp >= 1785542400 AND timestamp <= 1788220799",
    );
    assert_eq!(start, Some(1785542400));
    assert_eq!(end, Some(1788220799));
}

#[test]
fn comparaciones_sobre_columnas_no_temporales_se_ignoran() {
    let (start, end) = extract_timestamp_range_from_sql("SELECT x FROM t WHERE reading_value >= 5");
    assert_eq!(start, None);
    assert_eq!(end, None);
}

// ── truncate_timestamp ──────────────────────────────────────────────────────

#[test]
fn truncado_a_dia_y_hora() {
    // 2026-08-01T13:45:30Z = 1785590730 s
    let ts = 1785590730;
    assert_eq!(truncate_timestamp(ts, "day"), 1785542400);
    assert_eq!(truncate_timestamp(ts, "hour"), 1785589200);
    // En milisegundos devuelve milisegundos.
    assert_eq!(truncate_timestamp(ts * 1000, "day"), 1785542400 * 1000);
}

// ── evaluate_column + coerción ──────────────────────────────────────────────

fn types() -> HashMap<String, AttrType> {
    let mut m = HashMap::new();
    m.insert("reading_value".to_string(), AttrType::Decimal);
    m.insert("created_at".to_string(), AttrType::Epoch);
    m.insert("retryable".to_string(), AttrType::Boolean);
    m
}

#[test]
fn coercion_decimal_string_a_numero() {
    let col = ProjectedColumn {
        expr: "reading_value".to_string(),
        alias: "reading_value".to_string(),
        is_aggregate: false,
        agg_fn: None,
        agg_field: None,
    };
    let obj = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(json!({
        "reading_value": "12.5"
    }))
    .unwrap();
    assert_eq!(evaluate_column(&col, &obj, &types()), json!(12.5));
}

#[test]
fn campo_ausente_es_null() {
    let col = ProjectedColumn {
        expr: "no_esta".to_string(),
        alias: "no_esta".to_string(),
        is_aggregate: false,
        agg_fn: None,
        agg_field: None,
    };
    let obj = serde_json::Map::new();
    assert_eq!(
        evaluate_column(&col, &obj, &types()),
        serde_json::Value::Null
    );
}

// ── parse_cte_queries + evaluate_final_expression ───────────────────────────

#[test]
fn cte_de_comparacion_se_parsea() {
    let sql = "WITH current_data AS (SELECT bucket, v FROM t WHERE x = 1), \
               prev_0 AS (SELECT bucket, v FROM t WHERE x = 2) \
               SELECT c.v, prev_0.v FROM current_data c, prev_0";
    let Some((ctes, final_select)) = parse_cte_queries(sql) else {
        panic!("debería parsear la CTE");
    };
    assert_eq!(ctes.len(), 2);
    assert!(ctes.contains_key("current_data"));
    assert!(ctes.contains_key("prev_0"));
    assert!(ctes["current_data"].starts_with("SELECT"));
    assert!(final_select.starts_with("SELECT"));
}

#[test]
fn sql_normal_no_es_cte() {
    assert!(parse_cte_queries("SELECT x FROM t").is_none());
}

#[test]
fn expresion_final_lee_current_por_indice() {
    let mut row = HashMap::new();
    row.insert("v".to_string(), json!(3));
    let prev: HashMap<String, Vec<HashMap<String, serde_json::Value>>> = HashMap::new();
    let smart: HashMap<String, Vec<HashMap<String, serde_json::Value>>> = HashMap::new();
    let val = evaluate_final_expression("c.v", Some(&row), &prev, &smart, 0);
    assert_eq!(val, json!(3));
}

#[test]
fn expresion_final_lee_prev_por_indice() {
    let mut prev_row = HashMap::new();
    prev_row.insert("v".to_string(), json!(9));
    let mut prev = HashMap::new();
    prev.insert("prev_0".to_string(), vec![prev_row]);
    let smart: HashMap<String, Vec<HashMap<String, serde_json::Value>>> = HashMap::new();
    let val = evaluate_final_expression("prev_0.v", None, &prev, &smart, 0);
    assert_eq!(val, json!(9));
}

#[test]
fn expresion_final_sin_dato_es_null() {
    let prev: HashMap<String, Vec<HashMap<String, serde_json::Value>>> = HashMap::new();
    let smart: HashMap<String, Vec<HashMap<String, serde_json::Value>>> = HashMap::new();
    let val = evaluate_final_expression("c.no_esta", None, &prev, &smart, 0);
    assert_eq!(val, serde_json::Value::Null);
}
