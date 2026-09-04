// infrastructure/local_s3_query_engine/pipeline.rs — Fase 5 (commit C)
//
// Responsabilidad 2 del motor local: el pipeline en memoria sobre los
// registros crudos del lake — filtros de igualdad/IN, rango temporal,
// agregación por grupo y proyección no agregada. Todo puro: registros
// entra, filas salen — sin I/O. MOVIMIENTO PURO desde
// `execute_single_query` + red nueva que fija su semántica.

use serde_json::Value;
use std::collections::HashMap;

use super::sql_parse::{clean_expr_field, ProjectedColumn};
use crate::codice::registry::AttrType;
use chrono::{Datelike, TimeZone, Timelike};

// ── Estado de agregación ────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct GroupState {
    pub group_values: Vec<Value>,
    pub agg_states: Vec<AggState>,
}

#[derive(Clone)]
pub(crate) struct AggState {
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub has_values: bool,
}

// ── Filtros y rango temporal ────────────────────────────────────────────────

/// Aplica filtros de igualdad/IN y el rango temporal sobre los registros
/// crudos, con tope de escaneo. MOVIMIENTO PURO del bloque 3 de
/// `execute_single_query`.
pub(crate) fn apply_filters_and_range(
    raw_records: &[serde_json::Map<String, Value>],
    filters: &HashMap<String, Value>,
    time_range: (Option<i64>, Option<i64>),
    attr_types: &HashMap<String, AttrType>,
    max_scan_records: usize,
) -> Vec<serde_json::Map<String, Value>> {
    let (start_ts, end_ts) = time_range;
    let mut fail_log_count = 0;
    let mut filtered_records = Vec::new();
    for obj_map in raw_records {
        if filtered_records.len() >= max_scan_records {
            break;
        }

        // Aplicar filtros en memoria (igualdad e IN de campos de filtros)
        let mut matches_filters = true;
        for (col, filter_val) in filters {
            let cleaned_col = clean_expr_field(col);
            if cleaned_col == "1" || cleaned_col == "true" {
                continue;
            }
            if let Some(record_val) = obj_map.get(&cleaned_col) {
                let coerced = coerce_value(record_val, &cleaned_col, attr_types);
                match filter_val {
                    Value::Array(arr) => {
                        if !arr.contains(&coerced) {
                            if fail_log_count < 10 {
                                tracing::info!("[LocalS3QueryEngine] Filtro IN falló: col={} cleaned_col={} record_val={:?} coerced={:?} filter_val={:?}", col, cleaned_col, record_val, coerced, filter_val);
                                fail_log_count += 1;
                            }
                            matches_filters = false;
                            break;
                        }
                    }
                    _ => {
                        if coerced != *filter_val {
                            if fail_log_count < 10 {
                                tracing::info!("[LocalS3QueryEngine] Filtro EQ falló: col={} cleaned_col={} record_val={:?} coerced={:?} filter_val={:?}", col, cleaned_col, record_val, coerced, filter_val);
                                fail_log_count += 1;
                            }
                            matches_filters = false;
                            break;
                        }
                    }
                }
            } else {
                match filter_val {
                    Value::Array(arr) => {
                        if !arr.contains(&Value::Null) {
                            if fail_log_count < 10 {
                                tracing::info!("[LocalS3QueryEngine] Filtro IN (no col) falló: col={} cleaned_col={} filter_val={:?}", col, cleaned_col, filter_val);
                                fail_log_count += 1;
                            }
                            matches_filters = false;
                            break;
                        }
                    }
                    _ => {
                        if *filter_val != Value::Null {
                            if fail_log_count < 10 {
                                tracing::info!("[LocalS3QueryEngine] Filtro EQ (no col) falló: col={} cleaned_col={} filter_val={:?}", col, cleaned_col, filter_val);
                                fail_log_count += 1;
                            }
                            matches_filters = false;
                            break;
                        }
                    }
                }
            }
        }
        if !matches_filters {
            continue;
        }

        // Filtrar por rango de tiempo si se especifica
        if start_ts.is_some() || end_ts.is_some() {
            let record_ts = obj_map
                .get("timestamp")
                .or_else(|| obj_map.get("created_at"))
                .and_then(|v| match v {
                    Value::Number(n) => n.as_i64(),
                    Value::String(s) => s.parse::<i64>().ok(),
                    _ => None,
                });
            if let Some(r_ts) = record_ts {
                let r_ts_sec = if r_ts > 100000000000 { r_ts / 1000 } else { r_ts };
                if let Some(start) = start_ts {
                    let start_sec = if start > 100000000000 { start / 1000 } else { start };
                    if r_ts_sec < start_sec {
                        continue;
                    }
                }
                if let Some(end) = end_ts {
                    let end_sec = if end > 100000000000 { end / 1000 } else { end };
                    if r_ts_sec > end_sec {
                        continue;
                    }
                }
            }
        }

        filtered_records.push(obj_map.clone());
    }
    filtered_records
}

// ── Agregación ──────────────────────────────────────────────────────────────

/// Agregación por grupo (COUNT/SUM/AVG/MIN/MAX) sobre los registros ya
/// filtrados. MOVIMIENTO PURO de la rama `any_is_aggregate` de
/// `execute_single_query`.
pub(crate) fn aggregate(
    raw_records: &[serde_json::Map<String, Value>],
    projected_info: &[ProjectedColumn],
    attr_types: &HashMap<String, AttrType>,
) -> Vec<HashMap<String, Value>> {
    let group_cols: Vec<&ProjectedColumn> =
        projected_info.iter().filter(|c| !c.is_aggregate).collect();
    let agg_cols: Vec<&ProjectedColumn> =
        projected_info.iter().filter(|c| c.is_aggregate).collect();

    let mut groups: HashMap<Vec<String>, GroupState> = HashMap::new();

    for obj_map in raw_records {
        let mut group_key = Vec::new();
        let mut group_values = Vec::new();
        for col in &group_cols {
            let val = evaluate_column(col, obj_map, attr_types);
            group_key.push(serde_json::to_string(&val).unwrap_or_default());
            group_values.push(val);
        }

        let state = groups.entry(group_key).or_insert_with(|| GroupState {
            group_values,
            agg_states: vec![
                AggState {
                    count: 0,
                    sum: 0.0,
                    min: f64::MAX,
                    max: f64::MIN,
                    has_values: false,
                };
                agg_cols.len()
            ],
        });

        for (i, col) in agg_cols.iter().enumerate() {
            let agg_state = &mut state.agg_states[i];
            let field_name = col.agg_field.as_deref().unwrap_or("*");
            let cleaned_field = clean_expr_field(field_name);

            if col.agg_fn.as_deref() == Some("COUNT") {
                let should_count = if field_name == "*" {
                    true
                } else {
                    obj_map
                        .get(&cleaned_field)
                        .map(|v| !v.is_null())
                        .unwrap_or(false)
                };
                if should_count {
                    agg_state.count += 1;
                    agg_state.has_values = true;
                }
            } else {
                let val_opt = obj_map.get(&cleaned_field).and_then(|v| match v {
                    Value::Number(n) => n.as_f64(),
                    Value::String(s) => s.parse::<f64>().ok(),
                    Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
                    _ => None,
                });

                if let Some(v) = val_opt {
                    match col.agg_fn.as_deref() {
                        Some("SUM") => {
                            agg_state.sum += v;
                            agg_state.has_values = true;
                        }
                        Some("AVG") => {
                            agg_state.sum += v;
                            agg_state.count += 1;
                            agg_state.has_values = true;
                        }
                        Some("MIN") => {
                            if v < agg_state.min {
                                agg_state.min = v;
                            }
                            agg_state.has_values = true;
                        }
                        Some("MAX") => {
                            if v > agg_state.max {
                                agg_state.max = v;
                            }
                            agg_state.has_values = true;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let is_groups_empty = groups.is_empty();
    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    for state in groups.into_values() {
        let mut row = HashMap::new();
        for (i, col) in group_cols.iter().enumerate() {
            let val = &state.group_values[i];
            row.insert(col.alias.clone(), val.clone());
        }
        for (i, col) in agg_cols.iter().enumerate() {
            let agg_state = &state.agg_states[i];
            let val = match col.agg_fn.as_deref() {
                Some("COUNT") => Value::Number(serde_json::Number::from(agg_state.count)),
                Some("SUM") => {
                    if agg_state.has_values {
                        Value::Number(
                            serde_json::Number::from_f64(agg_state.sum)
                                .unwrap_or(serde_json::Number::from(0)),
                        )
                    } else {
                        Value::Null
                    }
                }
                Some("AVG") => {
                    if agg_state.has_values && agg_state.count > 0 {
                        let avg = agg_state.sum / (agg_state.count as f64);
                        Value::Number(
                            serde_json::Number::from_f64(avg)
                                .unwrap_or(serde_json::Number::from(0)),
                        )
                    } else {
                        Value::Null
                    }
                }
                Some("MIN") => {
                    if agg_state.has_values {
                        Value::Number(
                            serde_json::Number::from_f64(agg_state.min)
                                .unwrap_or(serde_json::Number::from(0)),
                        )
                    } else {
                        Value::Null
                    }
                }
                Some("MAX") => {
                    if agg_state.has_values {
                        Value::Number(
                            serde_json::Number::from_f64(agg_state.max)
                                .unwrap_or(serde_json::Number::from(0)),
                        )
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };
            row.insert(col.alias.clone(), val);
        }
        rows.push(row);
    }
    if is_groups_empty && group_cols.is_empty() {
        let mut row = HashMap::new();
        for col in &agg_cols {
            let val = match col.agg_fn.as_deref() {
                Some("COUNT") => Value::Number(serde_json::Number::from(0)),
                _ => Value::Null,
            };
            row.insert(col.alias.clone(), val);
        }
        rows.push(row);
    }
    rows
}

// ── Proyección no agregada ──────────────────────────────────────────────────

/// Proyección fila a fila sin agregación. MOVIMIENTO PURO de la rama else de
/// `execute_single_query`.
pub(crate) fn project_rows(
    raw_records: &[serde_json::Map<String, Value>],
    projected_info: &[ProjectedColumn],
    attr_types: &HashMap<String, AttrType>,
) -> Vec<HashMap<String, Value>> {
    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    for obj_map in raw_records {
        let mut row = HashMap::new();
        for col in projected_info {
            let field_val = evaluate_column(col, obj_map, attr_types);
            row.insert(col.alias.clone(), field_val);
        }
        rows.push(row);
    }
    rows
}

// ── Evaluación de columnas y coerción ───────────────────────────────────────

/// Trunca el timestamp (ms o sec) al intervalo indicado.
pub(crate) fn truncate_timestamp(ts_ms_or_sec: i64, interval: &str) -> i64 {
    let ts_sec = if ts_ms_or_sec > 100000000000 {
        ts_ms_or_sec / 1000
    } else {
        ts_ms_or_sec
    };

    let dt = chrono::Utc.timestamp_opt(ts_sec, 0).unwrap();
    let truncated_dt = match interval.to_lowercase().as_str() {
        "minute" => dt
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "hour" => dt
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "day" => dt
            .with_hour(0)
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "week" => {
            let days_from_monday = match dt.weekday() {
                chrono::Weekday::Mon => 0,
                chrono::Weekday::Tue => 1,
                chrono::Weekday::Wed => 2,
                chrono::Weekday::Thu => 3,
                chrono::Weekday::Fri => 4,
                chrono::Weekday::Sat => 5,
                chrono::Weekday::Sun => 6,
            };
            let truncated = dt - chrono::Duration::days(days_from_monday);
            truncated
                .with_hour(0)
                .and_then(|t| t.with_minute(0))
                .and_then(|t| t.with_second(0))
                .and_then(|t| t.with_nanosecond(0))
                .unwrap_or(dt)
        }
        "month" => dt
            .with_day(1)
            .and_then(|t| t.with_hour(0))
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "year" => dt
            .with_month(1)
            .and_then(|t| t.with_day(1))
            .and_then(|t| t.with_hour(0))
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        _ => dt,
    };

    if ts_ms_or_sec > 100000000000 {
        truncated_dt.timestamp() * 1000
    } else {
        truncated_dt.timestamp()
    }
}

/// Evalúa el valor de la columna para un registro de S3.
pub(crate) fn evaluate_column(
    col: &ProjectedColumn,
    obj_map: &serde_json::Map<String, Value>,
    attr_types: &HashMap<String, AttrType>,
) -> Value {
    let lower_expr = col.expr.to_lowercase();
    if lower_expr.contains("date_trunc") {
        let interval = if lower_expr.contains("'minute'") {
            "minute"
        } else if lower_expr.contains("'hour'") {
            "hour"
        } else if lower_expr.contains("'week'") {
            "week"
        } else if lower_expr.contains("'month'") {
            "month"
        } else if lower_expr.contains("'year'") {
            "year"
        } else {
            "day"
        };

        let field_name = if lower_expr.contains("created_at") {
            "created_at"
        } else if lower_expr.contains("event_ts") {
            "event_ts"
        } else {
            "timestamp"
        };

        if let Some(raw_val) = obj_map.get(field_name) {
            let ts = match raw_val {
                Value::Number(n) => n.as_i64().unwrap_or(0),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                _ => 0,
            };
            if ts > 0 {
                let truncated = truncate_timestamp(ts, interval);
                return Value::Number(serde_json::Number::from(truncated));
            }
        }
        Value::Null
    } else {
        let field_name = clean_expr_field(&col.expr);
        if let Some(val) = obj_map.get(&field_name) {
            coerce_value(val, &field_name, attr_types)
        } else {
            Value::Null
        }
    }
}

/// Construye un mapa de nombre_campo → AttrType desde el schema Codice.
pub(crate) fn build_attr_type_map(entity: &str) -> HashMap<String, AttrType> {
    let mut map = HashMap::new();
    if let Some(reg) = crate::codice::registry::global_opt() {
        if let Some(attrs) = reg.get_attributes(entity) {
            for attr in attrs.iter() {
                map.insert(attr.name.clone(), attr.attr_type.clone());
            }
        }
    }
    map.insert("id".to_string(), AttrType::String);
    map.insert("_tenant".to_string(), AttrType::String);
    map.insert("tenant_id".to_string(), AttrType::String);
    map.insert("created_at".to_string(), AttrType::Epoch);
    map
}

/// Coerción de valor JSON usando el schema Codice.
pub(crate) fn coerce_value(val: &Value, col: &str, attr_types: &HashMap<String, AttrType>) -> Value {
    let attr_type = attr_types.get(col);

    match attr_type {
        Some(AttrType::Epoch) => match val {
            Value::Number(_) => val.clone(),
            Value::String(s) => s.parse::<i64>().map(Value::from).unwrap_or(val.clone()),
            _ => val.clone(),
        },
        Some(AttrType::Number) | Some(AttrType::Decimal) => match val {
            Value::Number(_) => val.clone(),
            Value::String(s) => {
                if let Ok(n) = s.parse::<f64>() {
                    Value::from(n)
                } else {
                    val.clone()
                }
            }
            _ => val.clone(),
        },
        Some(AttrType::Boolean) => match val {
            Value::Bool(_) => val.clone(),
            Value::String(s) => Value::Bool(s.eq_ignore_ascii_case("true")),
            _ => val.clone(),
        },
        _ => val.clone(),
    }
}

/// Extrae timestamp de un row para ordenamiento.
pub(crate) fn extract_ts(row: &HashMap<String, Value>) -> i64 {
    row.get("timestamp")
        .or_else(|| row.get("created_at"))
        .or_else(|| row.get("bucket"))
        .or_else(|| row.get("current_bucket"))
        .and_then(|v| match v {
            Value::Number(n) => n.as_i64(),
            Value::String(s) => s.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

// ── Red propia del pipeline (Fase 5) ────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: serde_json::Value) -> serde_json::Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    fn col(expr: &str, alias: &str, agg: Option<(&str, &str)>) -> ProjectedColumn {
        ProjectedColumn {
            expr: expr.to_string(),
            alias: alias.to_string(),
            is_aggregate: agg.is_some(),
            agg_fn: agg.map(|f| f.0.to_string()),
            agg_field: agg.map(|f| f.1.to_string()),
        }
    }

    fn types() -> HashMap<String, AttrType> {
        let mut m = HashMap::new();
        m.insert("reading_value".to_string(), AttrType::Decimal);
        m.insert("created_at".to_string(), AttrType::Epoch);
        m.insert("action_type".to_string(), AttrType::String);
        m.insert("timestamp".to_string(), AttrType::Epoch);
        m
    }

    #[test]
    fn filtro_eq_coercido_y_in_de_lista() {
        let records = vec![
            obj(json!({"action_type": "CREATE", "v": 1})),
            obj(json!({"action_type": "DELETE", "v": 2})),
            obj(json!({"action_type": "CREATE", "v": 3})),
        ];
        let mut filters = HashMap::new();
        filters.insert("action_type".to_string(), json!("CREATE"));
        let out = apply_filters_and_range(&records, &filters, (None, None), &types(), 100);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r["action_type"] == json!("CREATE")));

        let mut filters = HashMap::new();
        filters.insert(
            "action_type".to_string(),
            json!(["DELETE", "UPDATE"]),
        );
        let out = apply_filters_and_range(&records, &filters, (None, None), &types(), 100);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn filtro_sobre_columna_ausente_solo_pasa_con_null_o_lista_con_null() {
        let records = vec![obj(json!({"v": 1})), obj(json!({"action_type": "X"}))];
        let mut filters = HashMap::new();
        filters.insert("action_type".to_string(), json!("CREATE"));
        let out = apply_filters_and_range(&records, &filters, (None, None), &types(), 100);
        assert_eq!(out.len(), 0, "EQ sobre columna ausente no matchea nunca");

        let mut filters = HashMap::new();
        filters.insert("action_type".to_string(), json!([serde_json::Value::Null]));
        let out = apply_filters_and_range(&records, &filters, (None, None), &types(), 100);
        assert_eq!(out.len(), 1, "IN con Null matchea el registro sin columna");
    }

    #[test]
    fn rango_temporal_normaliza_ms_y_registros_sin_ts_pasan() {
        let records = vec![
            obj(json!({"created_at": 1785590730000i64})), // ms, dentro del día
            obj(json!({"created_at": 1785475200})),       // sec, 2026-07-30 → fuera
            obj(json!({"v": 1})),                         // sin timestamp → pasa
        ];
        let out = apply_filters_and_range(
            &records,
            &HashMap::new(),
            (Some(1785542400), Some(1785628799)), // 2026-08-01
            &types(),
            100,
        );
        assert_eq!(out.len(), 2, "el de julio se va; el sin-ts se queda");
    }

    #[test]
    fn tope_de_escaneo_corta_el_resultado() {
        let records: Vec<_> = (0..10)
            .map(|i| obj(json!({"i": i, "action_type": "CREATE"})))
            .collect();
        let mut filters = HashMap::new();
        filters.insert("action_type".to_string(), json!("CREATE"));
        let out = apply_filters_and_range(&records, &filters, (None, None), &types(), 4);
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn agregacion_count_y_sum_por_grupo() {
        let records = vec![
            obj(json!({"action_type": "CREATE", "reading_value": "10"})),
            obj(json!({"action_type": "CREATE", "reading_value": "5"})),
            obj(json!({"action_type": "DELETE", "reading_value": 3})),
        ];
        let cols = vec![
            col("action_type", "action_type", None),
            col("COUNT(*)", "total", Some(("COUNT", "*"))),
            col("SUM(reading_value)", "suma", Some(("SUM", "reading_value"))),
        ];
        let rows = aggregate(&records, &cols, &types());
        assert_eq!(rows.len(), 2);
        let create = rows.iter().find(|r| r["action_type"] == json!("CREATE")).unwrap();
        assert_eq!(create["total"], json!(2));
        assert_eq!(create["suma"], json!(15.0));
        let delete = rows.iter().find(|r| r["action_type"] == json!("DELETE")).unwrap();
        assert_eq!(delete["suma"], json!(3.0));
    }

    #[test]
    fn avg_min_max_sobre_valores_coercidos() {
        let records = vec![
            obj(json!({"reading_value": "4"})),
            obj(json!({"reading_value": 8})),
            obj(json!({"reading_value": "6"})),
        ];
        let cols = vec![
            col("AVG(reading_value)", "avg", Some(("AVG", "reading_value"))),
            col("MIN(reading_value)", "min", Some(("MIN", "reading_value"))),
            col("MAX(reading_value)", "max", Some(("MAX", "reading_value"))),
        ];
        let rows = aggregate(&records, &cols, &types());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["avg"], json!(6.0));
        assert_eq!(rows[0]["min"], json!(4.0));
        assert_eq!(rows[0]["max"], json!(8.0));
    }

    #[test]
    fn agregacion_sin_grupos_ni_filas_devuelve_count_cero() {
        let cols = vec![
            col("COUNT(*)", "total", Some(("COUNT", "*"))),
            col("SUM(v)", "suma", Some(("SUM", "v"))),
        ];
        let rows = aggregate(&[], &cols, &types());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["total"], json!(0));
        assert_eq!(rows[0]["suma"], serde_json::Value::Null);
    }

    #[test]
    fn min_max_sin_valores_son_null() {
        let records = vec![obj(json!({"reading_value": "no-numero"}))];
        let cols = vec![col("MIN(reading_value)", "min", Some(("MIN", "reading_value")))];
        let rows = aggregate(&records, &cols, &types());
        assert_eq!(rows[0]["min"], serde_json::Value::Null);
    }

    #[test]
    fn proyeccion_simple_y_date_trunc() {
        let records = vec![obj(json!({
            "action_type": "CREATE",
            "created_at": 1785590730, // 2026-08-01T13:45:30Z
        }))];
        let cols = vec![
            col("action_type", "action_type", None),
            col("date_trunc('day', created_at)", "bucket", None),
        ];
        let rows = project_rows(&records, &cols, &types());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["action_type"], json!("CREATE"));
        assert_eq!(rows[0]["bucket"], json!(1785542400));
    }

    #[test]
    fn extract_ts_prefiere_timestamp_luego_created_at() {
        let mut row = HashMap::new();
        row.insert("created_at".to_string(), json!(123));
        assert_eq!(extract_ts(&row), 123);
        row.insert("timestamp".to_string(), json!(456));
        assert_eq!(extract_ts(&row), 456);
        assert_eq!(extract_ts(&HashMap::new()), 0);
    }
}
