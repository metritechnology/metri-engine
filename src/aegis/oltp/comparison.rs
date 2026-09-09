//! AnalyticalComparison — the five temporal comparison strategies.
//!
//! AnalyticalComparison: 5 tipos del contrato §4.
//!
//! SRP: computar comparaciones temporales sobre rows EAV en memoria — sin I/O.
//!
//! # Origin
//! Estrategias (paralelo al path el stack anterior/OLAP):
//! TIME_SHIFT_RELATIVE  → shift_by_calendar(N × granularidad) via temporal::comparison
//! TIME_SHIFT_SHORTCUT  → resolve_shortcut via temporal::comparison (bisiesto-safe)
//! TIME_SHIFT_ABSOLUTE  → ventana explícita absolute_start_ts / absolute_end_ts
//! SMART                → smart_history_window(90d) via temporal::comparison → z_score
//! BENCHMARK            → valor inline en resultado (sin query adicional)
//!
//! Diferencia respecto al path OLAP (Athena/SQL):
//! SQL   → CTEs WITH prev_0 / smart_N + CROSS JOIN / LEFT JOIN en bucket
//! Rust  → rows ya hidratados en memoria + filtro temporal inline
//!
//! Resolución de períodos:
//! ✅ Delegada a temporal::comparison (shift_by_calendar, chrono) — única SSOT
//! ✅ Bisiesto-safe, DST-aware, timezone propagada
//! ✅ No más aritmética fija (30*86400, 365*86400) en este módulo

use serde_json::{json, Value};
use tracing::warn;

use crate::aegis::oltp::aggregation::apply_metrics_fbs;
use crate::janus::fbs::{
    AnalyticalComparisonT, AnalyticalComparison_ComparisonType, AnalyticalComparison_ShiftShortcut,
    MetricDefinitionT,
};
use crate::temporal::comparison::{
    resolve_comparison_period, smart_history_window, AnalyticalComparison as TempComparison,
    ComparisonType, ShiftShortcut,
};
use crate::temporal::core::TimeRange;

// ── Helpers de conversión FBS → temporal::comparison types ───────────────────

/// Convierte AnalyticalComparisonT (FBS) → TempComparison (temporal::comparison).
/// Permite delegar la resolución de período a la SSOT temporal sin duplicar lógica.
fn fbs_to_temp_comparison(comp: &AnalyticalComparisonT) -> TempComparison {
    TempComparison {
        comp_type: match comp.type_ {
            t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE => {
                ComparisonType::TimeShiftRelative
            }
            t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_SHORTCUT => {
                ComparisonType::TimeShiftShortcut
            }
            t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_ABSOLUTE => {
                ComparisonType::TimeShiftAbsolute
            }
            t if t == AnalyticalComparison_ComparisonType::BENCHMARK => ComparisonType::Benchmark,
            t if t == AnalyticalComparison_ComparisonType::SMART => ComparisonType::Smart,
            _ => ComparisonType::Unspecified,
        },
        relative_granularity: comp
            .relative_granularity
            .clone()
            .unwrap_or_else(|| "day".to_string()),
        relative_amount: i64::max(comp.relative_amount as i64, 1),
        shortcut: match comp.shortcut {
            s if s == AnalyticalComparison_ShiftShortcut::PREVIOUS_PERIOD => {
                ShiftShortcut::PreviousPeriod
            }
            s if s == AnalyticalComparison_ShiftShortcut::SAME_PERIOD_LAST_YEAR => {
                ShiftShortcut::SamePeriodLastYear
            }
            s if s == AnalyticalComparison_ShiftShortcut::SAME_PERIOD_LAST_QUARTER => {
                ShiftShortcut::SamePeriodLastQuarter
            }
            s if s == AnalyticalComparison_ShiftShortcut::SAME_PERIOD_LAST_MONTH => {
                ShiftShortcut::SamePeriodLastMonth
            }
            s if s == AnalyticalComparison_ShiftShortcut::SAME_DAY_LAST_WEEK => {
                ShiftShortcut::SameDayLastWeek
            }
            s if s == AnalyticalComparison_ShiftShortcut::SAME_DAY_LAST_MONTH => {
                ShiftShortcut::SameDayLastMonth
            }
            s if s == AnalyticalComparison_ShiftShortcut::SAME_DAY_LAST_YEAR => {
                ShiftShortcut::SameDayLastYear
            }
            s if s == AnalyticalComparison_ShiftShortcut::YESTERDAY_LAST_YEAR => {
                ShiftShortcut::YesterdayLastYear
            }
            s if s == AnalyticalComparison_ShiftShortcut::YESTERDAY_LAST_MONTH => {
                ShiftShortcut::YesterdayLastMonth
            }
            s if s == AnalyticalComparison_ShiftShortcut::YESTERDAY_LAST_WEEK => {
                ShiftShortcut::YesterdayLastWeek
            }
            s if s == AnalyticalComparison_ShiftShortcut::TODAY_LAST_YEAR => {
                ShiftShortcut::TodayLastYear
            }
            s if s == AnalyticalComparison_ShiftShortcut::TODAY_LAST_MONTH => {
                ShiftShortcut::TodayLastMonth
            }
            _ => ShiftShortcut::Unspecified,
        },
        absolute_start_ts: if comp.absolute_start_ts != 0 {
            Some(comp.absolute_start_ts)
        } else {
            None
        },
        absolute_end_ts: if comp.absolute_end_ts != 0 {
            Some(comp.absolute_end_ts)
        } else {
            None
        },
    }
}

// ── Filtro temporal in-memory ─────────────────────────────────────────────────

/// Filtra rows por ventana temporal [prev_start, prev_end].
/// Prueba múltiples campos de timestamp por orden de prioridad.
fn filter_rows_by_window<'a>(rows: &'a [Value], prev_start: i64, prev_end: i64) -> Vec<&'a Value> {
    const TS_CANDIDATES: &[&str] = &[
        "created_at",
        "meta/created_at",
        "timestamp",
        "updated_at",
        "ingested_at",
    ];
    rows.iter()
        .filter(|row| {
            let ts_raw = TS_CANDIDATES
                .iter()
                .find_map(|f| row.get(*f).and_then(|v| v.as_f64()))
                .unwrap_or(0.0);
            // Normalizar ms → s si > 1e11
            let ts_secs = if ts_raw > 1e11 {
                (ts_raw / 1000.0) as i64
            } else {
                ts_raw as i64
            };
            ts_secs >= prev_start && ts_secs <= prev_end
        })
        .collect()
}

// ── SMART: estadísticas históricas ───────────────────────────────────────────

/// Calcula mean/std/z_score para cada métrica sobre un conjunto histórico de rows.
fn compute_smart_stats(
    hist_rows: &[Value],
    metrics: &[MetricDefinitionT],
    current_metrics: &serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    let mut stats = serde_json::Map::new();

    for m in metrics {
        let attr = m.attribute.as_deref().unwrap_or("");
        let alias = build_metric_alias(m);

        // Recolectar valores numéricos del atributo en el histórico
        let nums: Vec<f64> = hist_rows
            .iter()
            .filter_map(|r| {
                r.get(attr)
                    .or_else(|| r.get(attr.split('/').next_back().unwrap_or(attr)))
                    .and_then(|v| v.as_f64())
            })
            .collect();

        let n = nums.len();
        if n == 0 {
            continue;
        }

        // Media
        let mean: f64 = nums.iter().sum::<f64>() / n as f64;
        stats.insert(format!("mean_{alias}"), json!(mean));

        // Desviación estándar (Bessel: n-1 para muestra)
        if n > 1 {
            let variance = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
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
fn build_metric_alias(m: &MetricDefinitionT) -> String {
    if let Some(name) = m.name.as_deref().filter(|s| !s.is_empty()) {
        return name.to_string();
    }
    let fn_str = format!("{:?}", m.aggregation).to_lowercase();
    let attr = m
        .attribute
        .as_deref()
        .unwrap_or("total")
        .split('/')
        .next_back()
        .unwrap_or("total");
    format!("{fn_str}_{attr}")
}

// ── API pública ───────────────────────────────────────────────────────────────

/// Ejecuta todas las AnalyticalComparison y retorna un mapa plano fusionable
/// con las métricas actuales.
///
/// Parámetros:
///   all_rows        — filas completas del período principal (para filter_rows_by_window)
///   metrics         — `MetricDefinition` (igual que en query principal)
///   comparisons     — `AnalyticalComparison` del AST IR
///   resolved_tf     — TimeRange del período actual (None = sin ventana temporal)
///   current_metrics — { alias → value } de apply_metrics_fbs período actual
///   tz              — timezone string (ej: "UTC", "America/Mexico_City")
///                     propagada a temporal::comparison para shift bisiesto-safe
///
/// Resolución de períodos delegada a `temporal::comparison::resolve_comparison_period`
/// (shift_by_calendar — chrono — bisiesto-safe, DST-aware).
///
///                metrics comparisons resolved-tf current-metrics ts-field)]
pub fn run_comparisons(
    all_rows: &[Value],
    metrics: &[MetricDefinitionT],
    comparisons: &[AnalyticalComparisonT],
    resolved_tf: Option<&TimeRange>,
    current_metrics: serde_json::Map<String, Value>,
    tz: &str,
) -> serde_json::Map<String, Value> {
    let now_s = chrono::Utc::now().timestamp();
    let cs = resolved_tf.and_then(|t| t.start_ts).unwrap_or(0);
    let ce = resolved_tf.and_then(|t| t.end_ts).unwrap_or(now_s);
    let tf = TimeRange {
        start_ts: Some(cs),
        end_ts: Some(ce),
    };

    // Si hay algún TIME_SHIFT, renombramos current_metrics a current_X
    let has_time_shift = comparisons.iter().any(|c| {
        c.type_ == AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE
            || c.type_ == AnalyticalComparison_ComparisonType::TIME_SHIFT_SHORTCUT
            || c.type_ == AnalyticalComparison_ComparisonType::TIME_SHIFT_ABSOLUTE
    });

    let mut result: serde_json::Map<String, Value> = if has_time_shift {
        current_metrics
            .iter()
            .map(|(k, v)| (format!("current_{k}"), v.clone()))
            .collect()
    } else {
        current_metrics.clone()
    };

    for (idx, comp) in comparisons.iter().enumerate() {
        // CLJ: (or (:label comp) (str "comp_" idx))
        let label: String = comp
            .label
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("comp_{idx}"));

        match comp.type_ {
            // ── TIME_SHIFT_* → delegar a temporal::comparison (bisiesto-safe) ─
            // [CLJ: (:TIME_SHIFT_RELATIVE :TIME_SHIFT_SHORTCUT :TIME_SHIFT_ABSOLUTE)]
            t if t == AnalyticalComparison_ComparisonType::TIME_SHIFT_RELATIVE
                || t == AnalyticalComparison_ComparisonType::TIME_SHIFT_SHORTCUT
                || t == AnalyticalComparison_ComparisonType::TIME_SHIFT_ABSOLUTE =>
            {
                let temp_comp = fbs_to_temp_comparison(comp);
                match resolve_comparison_period(&temp_comp, &tf, tz) {
                    Some(period) => {
                        let prev_rows: Vec<Value> =
                            filter_rows_by_window(all_rows, period.prev_start, period.prev_end)
                                .into_iter()
                                .cloned()
                                .collect();
                        // CLJ: (str "prev_" idx "_") como prefix
                        let prefix = format!("prev_{idx}_");
                        if let Value::Object(map) = apply_metrics_fbs(&prev_rows, metrics) {
                            for (k, v) in map {
                                result.insert(format!("{prefix}{k}"), v);
                            }
                        }
                    }
                    None => {
                        warn!(
                            "[Aegis Cmp] No se pudo resolver período para comp[{idx}] \
                             type={:?} label={label}",
                            comp.type_
                        );
                    }
                }
            }

            // ── BENCHMARK → columna inline sin query ────────────────────────
            // [CLJ: (keyword (str "benchmark_" (or lbl "value")))]
            t if t == AnalyticalComparison_ComparisonType::BENCHMARK => {
                // CLJ default label es "comp_{idx}" pero para benchmark usamos "value"
                // // cuando el label real está vacío, igual que el stack anterior:
                // (or (:label comp) "value") — aquí label ya tiene fallback "comp_{idx}"
                // pero el CLJ original usa "value" como default específico de BENCHMARK.
                let bk_label = comp
                    .label
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("value");
                let bk_key = format!("benchmark_{bk_label}");
                // CLJ: (double (or (:benchmark-value comp) 0.0))
                result.insert(bk_key, json!(comp.benchmark_value as f64));
            }

            // ── SMART → smart_history_window via temporal::comparison ─────────
            // [CLJ: hist-start = (- cs (* 90 86400)) via temporal/smart-history-window]
            t if t == AnalyticalComparison_ComparisonType::SMART => {
                let hist_window = smart_history_window(&tf, None);
                let hist_rows: Vec<Value> = filter_rows_by_window(
                    all_rows,
                    hist_window.start_ts.unwrap_or(cs - 90 * 86_400),
                    hist_window.end_ts.unwrap_or(cs),
                )
                .into_iter()
                .cloned()
                .collect();
                let stats = compute_smart_stats(&hist_rows, metrics, &current_metrics);
                result.extend(stats);
            }

            // :COMPARISON_TYPE_UNSPECIFIED → log y omitir [CLJ paridad]
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
