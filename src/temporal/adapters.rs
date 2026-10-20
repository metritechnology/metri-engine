//! Adaptadores temporales para traducir rangos `TimeRange` a predicados y buckets de consulta.
//!
//! Convierte límites `{start_ts, end_ts}` en segundos epoch a las estructuras de filtrado
//! requeridas por los ejecutores de lectura sobre datoms EAV / `.mbf` y agrupadores de
//! series temporales (`TIMESERIES`) en memoria:
//! - `to_datalog_clauses`: Genera cláusulas de enlace y cotas en milisegundos epoch.
//! - `to_honey_clause`: Construye árboles de predicados JSON compatibles con `WhereNode`.
//! - `apply_bucket`: Trunca marcas temporales al inicio del intervalo según la zona horaria.

use crate::temporal::core::{s_to_ms, truncate_to_unit, CalUnit, TimeRange};
use serde_json::{json, Value};

// ── OLTP: Predicados Datalog sobre Datoms ─────────────────────────────────────

/// Representa una cláusula de filtrado temporal en formato Datalog.
///
/// Ejemplo: `[?e :created_at ?ts1]` junto con `[>= ?ts1 1735689600000]`.
#[derive(Debug, Clone)]
pub struct DatalogClause {
    /// Patrón de enlace EAV que vincula la entidad con la variable temporal (ej. `"[?e :created_at ?ts1]"`).
    pub binding: String, // "[?e :created_at ?ts1]"
    /// Predicado de límite inferior en milisegundos epoch (ej. `"[>= ?ts1 <ms>]"`).
    pub lower: Option<String>, // "[>= ?ts1 <ms>]"
    /// Predicado de límite superior en milisegundos epoch (ej. `"[<= ?ts1 <ms>]"`).
    pub upper: Option<String>, // "[<= ?ts1 <ms>]"
}

/// Convierte un `TimeRange`, nombre de atributo temporal e índice en una `DatalogClause`.
///
/// Los datoms almacenan marcas de tiempo como milisegundos epoch (`i64`), por lo que
/// `s_to_ms` convierte los límites de segundos a milisegundos.
///
/// Retorna `None` si ambos límites de `time_range` son `None`.
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

/// Serializa una `DatalogClause` en un vector ordenado de fragmentos Datalog (`Vec<String>`).
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

// ── Predicados AST JSON (WhereNode) ───────────────────────────────────────────

/// Traduce un `TimeRange` y nombre de columna a una expresión de filtro JSON (`Value`).
///
/// - Si solo existe `start_ts`: `[">=", col, start_ts]`
/// - Si solo existe `end_ts`: `["<=", col, end_ts]`
/// - Si ambos existen: `["and", [">=", col, start_ts], ["<=", col, end_ts]]`
/// - Si ambos son `None`: `Value::Null` (sin filtro temporal)
///
/// Los valores emitidos están expresados en segundos epoch (`i64`).
pub fn to_honey_clause(time_range: &TimeRange, col: &str) -> Value {
    let col_v = Value::String(col.to_string());

    match (time_range.start_ts, time_range.end_ts) {
        (Some(s), Some(e)) => json!(["and", [">=", col_v, s], ["<=", col_v.clone(), e]]),
        (Some(s), None) => json!([">=", col_v, s]),
        (None, Some(e)) => json!(["<=", col_v, e]),
        (None, None) => Value::Null,
    }
}

// ── OLTP: Agrupación temporal (Bucketing) para TIMESERIES en memoria ──────────

/// Configuración de agrupación por intervalos temporales para series de tiempo en memoria.
#[derive(Debug, Clone)]
pub struct BucketConfig {
    /// Unidad de intervalo temporal (`"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`).
    pub interval: String, // "minute" | "hour" | "day" | "week" | "month" | "quarter" | "year"
    /// Identificador IANA de la zona horaria aplicable al truncamiento (ej. `"UTC"`, `"America/Bogota"`).
    pub timezone: String,
}

/// Aplica agrupación por intervalo (`bucketing`) a un valor epoch en segundos o milisegundos.
///
/// Normaliza automáticamente valores mayores a `100_000_000_000` (milisegundos) a segundos
/// antes de truncar con `truncate_to_unit`. Retorna `None` si `epoch_val` es `0` o si el
/// intervalo no es válido.
pub fn apply_bucket(config: &BucketConfig, epoch_val: i64) -> Option<i64> {
    if epoch_val == 0 {
        return None;
    }
    let unit = config.interval.parse::<CalUnit>().ok()?;
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
