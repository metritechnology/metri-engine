// [PORTED_FROM: src/metri/aegis/datalog/comparison.clj]
// aegis/oltp/comparison.rs — AnalyticalComparison: 5 tipos del contrato §4.
//
// SRP: computar comparaciones temporales sobre rows EAV en memoria — sin I/O.
//
// Estrategias (paralelo al path Clojure/OLAP):
//   TIME_SHIFT_RELATIVE  → desplazo período actual por N × granularidad
//   TIME_SHIFT_SHORTCUT  → 12 atajos nombrados del contrato
//   TIME_SHIFT_ABSOLUTE  → ventana explícita absolute_start_ts / absolute_end_ts
//   SMART                → histórico 90d → mean/std → z_score por métrica
//   BENCHMARK            → valor inline en resultado (sin query adicional)
//
// Diferencia respecto al path OLAP (Athena/SQL):
//   SQL   → CTEs WITH prev_0 / smart_N + CROSS JOIN / LEFT JOIN en bucket
//   Rust  → rows ya hidratados en memoria + filtro temporal inline
//
// Resultado: mapa plano (serde_json::Value::Object) fusionable con las métricas actuales.
//   { "current_sum_revenue": 1234,
//     "prev_0_sum_revenue":  1100,
//     "benchmark_target":    1000.0,
//     "mean_sum_revenue":    1050.0,
//     "std_sum_revenue":     80.0,
//     "z_score_sum_revenue": 2.3 }

use serde_json::{Value, json};
use tracing::warn;

use crate::janus::fbs::{
    AnalyticalComparisonT, AnalyticalComparison_ComparisonType, MetricDefinitionT, AnalyticalComparison_ShiftShortcut,
};
use crate::aegis::oltp::aggregation::apply_metrics_fbs;
use crate::temporal::core::TimeRange;

// ── Helpers de período ────────────────────────────────────────────────────────

/// Traduce granularidad de texto a segundos.
/// [PORTED_FROM: (case gran ...) en comparison-period]
fn gran_secs(gran: &str) -> i64 {
    match gran {
        "minute"  => 60,
        "hour"    => 3600,
        "day"     => 86400,
        "week"    => 7 * 86400,
        "month"   => 30 * 86400,
        "quarter" => 91 * 86400,
        "year"    => 365 * 86400,
        _         => 86400, // fallback: day
    }
}

/// Resuelve el ShiftShortcut a (prev_start, prev_end) dado el período actual.
/// [PORTED_FROM: (shortcut->period shortcut cs ce)]
fn shortcut_to_period(shortcut: i8, cs: i64, ce: i64) -> Option<(i64, i64)> {
    let duration = ce - cs;
    let shift = |s: i64| (cs - s, ce - s);

    // Mapeo numérico del enum AnalyticalComparison_ShiftShortcut generado
    match shortcut {
        // PREVIOUS_PERIOD (0 o 1 según fbs)
        s if s == AnalyticalComparison_ShiftShortcut::PREVIOUS_PERIOD.0 as i8 => {
            Some((cs - duration, cs))
        }
        s if s == AnalyticalComparison_ShiftShortcut::SAME_PERIOD_LAST_YEAR.0 as i8 => {
            Some(shift(365 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::SAME_PERIOD_LAST_QUARTER.0 as i8 => {
            Some(shift(91 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::SAME_PERIOD_LAST_MONTH.0 as i8 => {
            Some(shift(30 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::SAME_DAY_LAST_WEEK.0 as i8 => {
            Some(shift(7 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::SAME_DAY_LAST_MONTH.0 as i8 => {
            Some(shift(30 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::SAME_DAY_LAST_YEAR.0 as i8 => {
            Some(shift(365 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::YESTERDAY_LAST_YEAR.0 as i8 => {
            Some((cs - 366 * 86400, cs - 365 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::YESTERDAY_LAST_MONTH.0 as i8 => {
            Some((cs - 31 * 86400, cs - 30 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::YESTERDAY_LAST_WEEK.0 as i8 => {
            Some((cs - 8 * 86400, cs - 7 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::TODAY_LAST_YEAR.0 as i8 => {
            Some(shift(365 * 86400))
        }
        s if s == AnalyticalComparison_ShiftShortcut::TODAY_LAST_MONTH.0 as i8 => {
            Some(shift(30 * 86400))
        }
        _ => None,
    }
}

/// Resuelve el AnalyticalComparison a (prev_start, prev_end) o None para BENCHMARK/SMART.
/// [PORTED_FROM: (comparison->period comp {:keys [start-ts end-ts]})]
fn comparison_to_period(
    comp: &AnalyticalComparisonT,
    cs: i64,
    ce: i64,
) -> Option<(i64, i64)> {
    let ctype = comp.type_;

    match ctype {
        // TIME_SHIFT_RELATIVE
        t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE => {
            let gran   = comp.relative_granularity.as_deref().unwrap_or("day");
            let amount = comp.relative_amount as i64;
            let secs   = gran_secs(gran) * amount;
            Some((cs - secs, ce - secs))
        }
        // TIME_SHIFT_SHORTCUT
        t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_SHORTCUT => {
            shortcut_to_period(comp.shortcut.0 as i8, cs, ce)
        }
        // TIME_SHIFT_ABSOLUTE
        t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_ABSOLUTE => {
            let ps = comp.absolute_start_ts;
            let pe = comp.absolute_end_ts;
            if ps > 0 && pe > 0 {
                Some((ps, pe))
            } else {
                None
            }
        }
        // BENCHMARK / SMART → no generan query shifted
        _ => None,
    }
}

// ── Filtro temporal in-memory ─────────────────────────────────────────────────

/// Filtra rows por ventana temporal [prev_start, prev_end].
/// Prueba múltiples campos de timestamp por orden de prioridad.
/// [PORTED_FROM: (run-shifted-query db base-clauses ... prev-start prev-end ts-field)]
fn filter_rows_by_window<'a>(
    rows: &'a [Value],
    prev_start: i64,
    prev_end: i64,
) -> Vec<&'a Value> {
    const TS_CANDIDATES: &[&str] = &[
        "created_at", "meta/created_at", "timestamp", "updated_at", "ingested_at",
    ];
    rows.iter()
        .filter(|row| {
            let ts_raw = TS_CANDIDATES.iter()
                .find_map(|f| row.get(*f).and_then(|v| v.as_f64()))
                .unwrap_or(0.0);
            // Normalizar ms → s si > 1e11
            let ts_secs = if ts_raw > 1e11 { (ts_raw / 1000.0) as i64 } else { ts_raw as i64 };
            ts_secs >= prev_start && ts_secs <= prev_end
        })
        .collect()
}

// ── SMART: estadísticas históricas ───────────────────────────────────────────

/// Calcula mean/std/z_score para cada métrica sobre un conjunto histórico de rows.
/// [PORTED_FROM: (compute-smart-stats hist-rows metrics current-metric-map)]
fn compute_smart_stats(
    hist_rows: &[Value],
    metrics: &[MetricDefinitionT],
    current_metrics: &serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    let mut stats = serde_json::Map::new();

    for m in metrics {
        let attr  = m.attribute.as_deref().unwrap_or("");
        let alias = build_metric_alias(m);

        // Recolectar valores numéricos del atributo en el histórico
        let nums: Vec<f64> = hist_rows.iter()
            .filter_map(|r| {
                r.get(attr)
                    .or_else(|| r.get(attr.split('/').last().unwrap_or(attr)))
                    .and_then(|v| v.as_f64())
            })
            .collect();

        let n = nums.len();
        if n == 0 { continue; }

        // Media
        let mean: f64 = nums.iter().sum::<f64>() / n as f64;
        stats.insert(format!("mean_{alias}"), json!(mean));

        // Desviación estándar (Bessel: n-1 para muestra)
        if n > 1 {
            let variance = nums.iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / (n - 1) as f64;
            let std_dev = variance.sqrt();
            stats.insert(format!("std_{alias}"), json!(std_dev));

            // Z-score del valor actual
            if std_dev > 0.0 {
                if let Some(curr) = current_metrics.get(&alias).and_then(|v| v.as_f64()) {
                    let z = (curr - mean) / std_dev;
                    stats.insert(format!("z_score_{alias}"), json!(z));
                }
            }
        }
    }

    stats
}

/// Construye el alias de métrica igual que apply_metrics_fbs.
/// [PORTED_FROM: (str/lower-case (name fn-kw)) "_" (name attr-kw))]
fn build_metric_alias(m: &MetricDefinitionT) -> String {
    if let Some(name) = m.name.as_deref().filter(|s| !s.is_empty()) {
        return name.to_string();
    }
    let fn_str = format!("{:?}", m.aggregation).to_lowercase();
    let attr   = m.attribute.as_deref().unwrap_or("total")
        .split('/').last().unwrap_or("total");
    format!("{fn_str}_{attr}")
}

// ── API pública ───────────────────────────────────────────────────────────────

/// Ejecuta todas las AnalyticalComparison y retorna un mapa plano fusionable
/// con las métricas actuales.
///
/// Parámetros:
///   all_rows        — filas completas del período principal (para filter_rows_by_window)
///   metrics         — [MetricDefinition] (igual que en query principal)
///   comparisons     — [AnalyticalComparison] del AST IR
///   resolved_tf     — TimeRange del período actual (None = sin ventana temporal)
///   current_metrics — { alias → value } de apply_metrics_fbs período actual
///
/// Retorna mapa plano:
///   { "current_sum_revenue": 1234,   ← renombrado si hay TIME_SHIFT
///     "prev_0_sum_revenue":  1100,
///     "benchmark_target":    1000.0,
///     "mean_sum_revenue":    1050.0,
///     "std_sum_revenue":     80.0,
///     "z_score_sum_revenue": 2.3 }
///
/// [PORTED_FROM: (run-comparisons db base-clauses in-sym->val pull-pattern
///                metrics comparisons resolved-tf current-metrics ts-field)]
pub fn run_comparisons(
    all_rows: &[Value],
    metrics: &[MetricDefinitionT],
    comparisons: &[AnalyticalComparisonT],
    resolved_tf: Option<&TimeRange>,
    current_metrics: serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let cs = resolved_tf.and_then(|t| t.start_ts).unwrap_or(0);
    let ce = resolved_tf.and_then(|t| t.end_ts).unwrap_or(now_s);

    // Si hay algún TIME_SHIFT, renombramos current_metrics a current_X
    // [PORTED_FROM: (if (some #(time-shift? (:type %)) comparisons) rename-current ...)]
    let has_time_shift = comparisons.iter().any(|c| {
        c.type_ == AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE
            || c.type_ == AnalyticalComparison_ComparisonType::TIME_SHIFT_SHORTCUT
            || c.type_ == AnalyticalComparison_ComparisonType::TIME_SHIFT_ABSOLUTE
    });

    let mut result: serde_json::Map<String, Value> = if has_time_shift {
        current_metrics.iter()
            .map(|(k, v)| (format!("current_{k}"), v.clone()))
            .collect()
    } else {
        current_metrics.clone()
    };

    for (idx, comp) in comparisons.iter().enumerate() {
        let label = comp.label.as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("")
            .to_string();

        match comp.type_ {

            // ── TIME_SHIFT_* → filtrar rows por período desplazado ───────────
            t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE
              || t == AnalyticalComparison_ComparisonType::TIME_SHIFT_SHORTCUT
              || t == AnalyticalComparison_ComparisonType::TIME_SHIFT_ABSOLUTE =>
            {
                match comparison_to_period(comp, cs, ce) {
                    Some((prev_start, prev_end)) => {
                        let prev_rows_refs = filter_rows_by_window(all_rows, prev_start, prev_end);
                        let prev_rows_owned: Vec<Value> =
                            prev_rows_refs.into_iter().cloned().collect();
                        let prev_m = apply_metrics_fbs(&prev_rows_owned, metrics);
                        let prefix = format!("prev_{idx}_");
                        if let Value::Object(map) = prev_m {
                            for (k, v) in map {
                                result.insert(format!("{prefix}{k}"), v);
                            }
                        }
                    }
                    None => {
                        warn!(
                            "[Aegis Cmp] No se pudo resolver período para comp[{idx}] type={:?} label={label}",
                            comp.type_
                        );
                    }
                }
            }

            // ── BENCHMARK → valor inline (sin query) ────────────────────────
            t if t == AnalyticalComparison_ComparisonType::BENCHMARK => {
                let bk_key = if label.is_empty() {
                    "benchmark_value".to_string()
                } else {
                    format!("benchmark_{label}")
                };
                result.insert(bk_key, json!(comp.benchmark_value));
            }

            // ── SMART → histórico 90d → z-score ─────────────────────────────
            t if t == AnalyticalComparison_ComparisonType::SMART => {
                let hist_start = cs - 90 * 86400;
                let hist_rows_refs = filter_rows_by_window(all_rows, hist_start, cs);
                let hist_rows_owned: Vec<Value> =
                    hist_rows_refs.into_iter().cloned().collect();
                let stats = compute_smart_stats(&hist_rows_owned, metrics, &current_metrics);
                result.extend(stats);
            }

            // Tipo desconocido
            _ => {
                warn!(
                    "[Aegis Cmp] AnalyticalComparison tipo desconocido: {:?} label={label}",
                    comp.type_
                );
            }
        }
    }

    result
}

// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::janus::fbs::{MetricDefinitionT, AggregationFunction, AnalyticalComparisonT, AnalyticalComparison_ComparisonType};
    use crate::temporal::core::TimeRange;

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

    // ── Test 1: TIME_SHIFT_RELATIVE ───────────────────────────────────────────
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

        let result = run_comparisons(&rows, &[metric], &[comp], Some(&tf), current_map);

        assert!(result.contains_key("current_sum_revenue"), "debe tener current_X cuando hay TIME_SHIFT");
        assert!(result.contains_key("prev_0_sum_revenue"), "debe tener prev_0_X");
        let prev_sum = result["prev_0_sum_revenue"].as_f64().unwrap_or(0.0);
        assert!((prev_sum - 200.0).abs() < 0.01, "prev_sum debe ser 50+70+80=200, got {prev_sum}");
    }

    // ── Test 2: BENCHMARK ─────────────────────────────────────────────────────
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
        let result = run_comparisons(&rows, &[metric], &[comp], None, current_map);

        assert!(result.contains_key("benchmark_target"));
        assert_eq!(result["benchmark_target"].as_f64().unwrap(), 1000.0);
    }

    // ── Test 3: SMART z-score ─────────────────────────────────────────────────
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
        let result = run_comparisons(&rows, &[metric], &[comp], Some(&tf), current_map);

        assert!(result.contains_key("mean_sum_revenue"), "debe tener mean_X");
        assert!(result.contains_key("std_sum_revenue"), "debe tener std_X");
        let mean = result["mean_sum_revenue"].as_f64().unwrap_or(0.0);
        // Con 90 rows de 100.0, la media debería ser ~100.0
        assert!((mean - 100.0).abs() < 1.0, "mean debe ser ~100.0, got {mean}");
    }
}
