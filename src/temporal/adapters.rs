// [PORTED_FROM: src/metri/temporal/adapters.clj]
// temporal/adapters.rs — Adaptadores temporales: TimeRange → cláusulas engine-específicas.
//
// SRP: traducir {start_ts, end_ts} epoch-s a la sintaxis de cada motor.
//
// Adaptadores disponibles:
//   to_datalog_clauses  → cláusulas Datahike (convierte a ms con s_to_ms)
//   to_honey_clause     → HoneySQL BETWEEN para Athena/SQL (serde_json)
//   to_bucket_fn        → fn de bucketing Clojure para OLTP TIMESERIES

use crate::temporal::core::{s_to_ms, truncate_to_unit, CalUnit, TimeRange};
use serde_json::{json, Value};

// ── OLTP: Datahike Datalog ────────────────────────────────────────────────────

/// Representa una cláusula Datalog para Datahike.
/// Ejemplo: [?e :created_at ?ts1] [>= ?ts1 1735689600000]
#[derive(Debug, Clone)]
pub struct DatalogClause {
    pub binding: String,       // "[?e :created_at ?ts1]"
    pub lower: Option<String>, // "[>= ?ts1 <ms>]"
    pub upper: Option<String>, // "[<= ?ts1 <ms>]"
}

/// TimeRange × ts_field × counter → DatalogClause para Datahike.
///
/// IMPORTANTE: Datahike almacena timestamps como epoch-MILISEGUNDOS.
/// s_to_ms convierte epoch-s → epoch-ms.
///
/// Retorna None si time_range es None o ambos límites son None.
///
/// Parámetros:
///   time_range  {start_ts, end_ts} epoch-s
///   ts_field    nombre del atributo — ej. "_created_at", "inventory_movement/reading_ts"
///   counter     índice para generar vars únicas (?ts1, ?ts2, …)
pub fn to_datalog_clauses(
    time_range: &TimeRange,
    ts_field: &str,
    counter: usize,
) -> Option<DatalogClause> {
    if time_range.start_ts.is_none() && time_range.end_ts.is_none() {
        return None;
    }

    let var_name = format!("?ts{}", counter);
    let binding = format!("[?e :{} {}]", ts_field, var_name);

    let lower = time_range
        .start_ts
        .map(|s| format!("[>= {} {}]", var_name, s_to_ms(s)));
    let upper = time_range
        .end_ts
        .map(|e| format!("[<= {} {}]", var_name, s_to_ms(e)));

    Some(DatalogClause {
        binding,
        lower,
        upper,
    })
}

/// Serializa DatalogClause a Vec<String> de cláusulas Datalog completas.
pub fn datalog_clause_to_parts(clause: &DatalogClause) -> Vec<String> {
    let mut parts = vec![clause.binding.clone()];
    if let Some(l) = &clause.lower {
        parts.push(l.clone());
    }
    if let Some(u) = &clause.upper {
        parts.push(u.clone());
    }
    parts
}

// ── OLAP: HoneySQL Athena ─────────────────────────────────────────────────────

/// TimeRange × col_name → cláusula WHERE en formato serde_json Value.
///
/// Si solo hay start_ts → [">=", col, start_ts]
/// Si solo hay end_ts   → ["<=", col, end_ts]
/// Si ambos             → ["and", [">=", ...], ["<=", ...]]
/// Si nil / ambos nil   → null (sin cláusula temporal)
///
/// Los valores son epoch-segundos (i64) directamente.
/// Athena compara timestamps numéricos vía from_unixtime() en el WHERE.
pub fn to_honey_clause(time_range: &TimeRange, col: &str) -> Value {
    let col_v = Value::String(col.to_string());

    match (time_range.start_ts, time_range.end_ts) {
        (Some(s), Some(e)) => json!(["and", [">=", col_v, s], ["<=", col_v.clone(), e]]),
        (Some(s), None) => json!([">=", col_v, s]),
        (None, Some(e)) => json!(["<=", col_v, e]),
        (None, None) => Value::Null,
    }
}

// ── OLTP: Bucket fn para TIMESERIES in-memory ─────────────────────────────────

/// Configuración de bucketing para TIMESERIES in-memory.
#[derive(Debug, Clone)]
pub struct BucketConfig {
    pub interval: String, // "minute" | "hour" | "day" | "week" | "month" | "quarter" | "year"
    pub timezone: String,
}

/// Aplica bucketing a un epoch-valor (puede ser ms si viene de Datahike).
/// Normaliza a segundos antes de truncar con truncate_to_unit.
///
/// Retorna el epoch-segundos truncado, o None si epoch_val es 0.
pub fn apply_bucket(config: &BucketConfig, epoch_val: i64) -> Option<i64> {
    if epoch_val == 0 {
        return None;
    }
    let unit = config.interval.parse::<CalUnit>().unwrap();
    // Normalizar: si > 1e11 → está en ms → convertir a s
    let epoch_s = if epoch_val > 100_000_000_000 {
        epoch_val / 1_000
    } else {
        epoch_val
    };
    Some(truncate_to_unit(epoch_s, unit, &config.timezone))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_datalog_clauses_both_bounds() {
        let r = TimeRange {
            start_ts: Some(1_000),
            end_ts: Some(2_000),
        };
        let c = to_datalog_clauses(&r, "_created_at", 1).unwrap();
        assert!(c.binding.contains("?ts1"));
        assert!(c.lower.as_ref().unwrap().contains("1000000")); // s→ms
        assert!(c.upper.as_ref().unwrap().contains("2000000"));
    }

    #[test]
    fn to_datalog_clauses_none_when_no_bounds() {
        let r = TimeRange {
            start_ts: None,
            end_ts: None,
        };
        assert!(to_datalog_clauses(&r, "_created_at", 1).is_none());
    }

    #[test]
    fn to_honey_clause_both_bounds() {
        let r = TimeRange {
            start_ts: Some(1_000),
            end_ts: Some(2_000),
        };
        let v = to_honey_clause(&r, "created_at");
        assert_eq!(v[0], "and");
    }

    #[test]
    fn to_honey_clause_null_on_no_bounds() {
        let r = TimeRange {
            start_ts: None,
            end_ts: None,
        };
        assert!(to_honey_clause(&r, "created_at").is_null());
    }

    #[test]
    fn apply_bucket_normalizes_ms() {
        let cfg = BucketConfig {
            interval: "day".to_string(),
            timezone: "UTC".to_string(),
        };
        // 2026-05-14 15:30:45 UTC en ms
        let epoch_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, 14)
            .unwrap()
            .and_hms_opt(15, 30, 45)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let result = apply_bucket(&cfg, epoch_ms).unwrap();
        let dt = chrono::DateTime::from_timestamp(result, 0).unwrap();
        assert_eq!(dt.time(), chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    }
}
