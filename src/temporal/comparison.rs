// temporal/comparison.rs — Resolución de AnalyticalComparison → períodos de comparación.
//
// CORRECCIONES vs implementación anterior:
//   1. Todos los shortcuts usan shift_by_calendar (chrono) — bisiesto-safe
//   2. SAME_PERIOD_LAST_YEAR ya no usa 365*86400 fijo
//   3. SAME_PERIOD_LAST_QUARTER ya no usa 91*86400 fijo
//   4. Timezone propagada en todos los shortcuts
//   5. TIME_SHIFT_RELATIVE usa shift_by_calendar — 'month' es mes real

use crate::temporal::core::{
    shift_by_calendar, CalUnit, ComparisonPeriod, TimeRange, SMART_HISTORY_DAYS,
};

// ── Tipos del proto ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonType {
    TimeShiftRelative,
    TimeShiftShortcut,
    TimeShiftAbsolute,
    Benchmark,
    Smart,
    Unspecified,
}

impl ComparisonType {
    pub fn from_proto(v: i32) -> Self {
        match v {
            1 => Self::TimeShiftRelative,
            2 => Self::TimeShiftShortcut,
            3 => Self::TimeShiftAbsolute,
            4 => Self::Benchmark,
            5 => Self::Smart,
            _ => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShiftShortcut {
    PreviousPeriod,
    SamePeriodLastYear,
    SamePeriodLastQuarter,
    SamePeriodLastMonth,
    SameDayLastWeek,
    SameDayLastMonth,
    SameDayLastYear,
    YesterdayLastYear,
    YesterdayLastMonth,
    YesterdayLastWeek,
    TodayLastYear,
    TodayLastMonth,
    Unspecified,
}

impl ShiftShortcut {
    pub fn from_proto(v: i32) -> Self {
        match v {
            1 => Self::PreviousPeriod,
            2 => Self::SamePeriodLastYear,
            3 => Self::SamePeriodLastQuarter,
            4 => Self::SamePeriodLastMonth,
            5 => Self::SameDayLastWeek,
            6 => Self::SameDayLastMonth,
            7 => Self::SameDayLastYear,
            8 => Self::YesterdayLastYear,
            9 => Self::YesterdayLastMonth,
            10 => Self::YesterdayLastWeek,
            11 => Self::TodayLastYear,
            12 => Self::TodayLastMonth,
            _ => Self::Unspecified,
        }
    }
}

/// Representa un AnalyticalComparison del proto.
#[derive(Debug, Clone)]
pub struct AnalyticalComparison {
    pub comp_type: ComparisonType,
    pub relative_granularity: String, // "day", "week", "month", "quarter", "year"
    pub relative_amount: i64,
    pub shortcut: ShiftShortcut,
    pub absolute_start_ts: Option<i64>, // epoch-s
    pub absolute_end_ts: Option<i64>,   // epoch-s
}

// ── Helpers privados ──────────────────────────────────────────────────────────

fn now_s() -> i64 {
    chrono::Utc::now().timestamp()
}

// ── Shortcut resolver ─────────────────────────────────────────────────────────

/// Resuelve un ShiftShortcut → ComparisonPeriod.
/// Usa shift_by_calendar (chrono) → correcto para bisiestos y meses reales.
/// Retorna None para shortcuts no reconocidos.
pub fn resolve_shortcut(
    shortcut: ShiftShortcut,
    range: &TimeRange,
    tz: &str,
) -> Option<ComparisonPeriod> {
    let now = now_s();
    let cs = range.start_ts.unwrap_or(0);
    let ce = range.end_ts.unwrap_or(now);
    let dur = ce - cs;

    // shift_simétrico: desplaza ambos extremos por la misma cantidad de calendario
    let shift_cal = |amount: i64, unit: CalUnit| ComparisonPeriod {
        prev_start: shift_by_calendar(cs, -amount, unit, tz),
        prev_end: shift_by_calendar(ce, -amount, unit, tz),
    };

    Some(match shortcut {
        // El período previo de exactamente la misma duración (aritmético)
        ShiftShortcut::PreviousPeriod => ComparisonPeriod {
            prev_start: cs - dur,
            prev_end: cs,
        },

        // Shifts de 1 año — bisiesto-safe vía chrono
        ShiftShortcut::SamePeriodLastYear => shift_cal(1, CalUnit::Year),
        ShiftShortcut::SameDayLastYear => shift_cal(1, CalUnit::Year),
        ShiftShortcut::TodayLastYear => shift_cal(1, CalUnit::Year),
        ShiftShortcut::YesterdayLastYear => ComparisonPeriod {
            prev_start: shift_by_calendar(cs - 86_400, -1, CalUnit::Year, tz),
            prev_end: shift_by_calendar(cs, -1, CalUnit::Year, tz),
        },

        // Shifts de 1 trimestre — chrono (91d sería incorrecto)
        ShiftShortcut::SamePeriodLastQuarter => shift_cal(1, CalUnit::Quarter),

        // Shifts de 1 mes — chrono (30d sería incorrecto para meses cortos)
        ShiftShortcut::SamePeriodLastMonth => shift_cal(1, CalUnit::Month),
        ShiftShortcut::SameDayLastMonth => shift_cal(1, CalUnit::Month),
        ShiftShortcut::TodayLastMonth => shift_cal(1, CalUnit::Month),
        ShiftShortcut::YesterdayLastMonth => ComparisonPeriod {
            prev_start: shift_by_calendar(cs - 86_400, -1, CalUnit::Month, tz),
            prev_end: shift_by_calendar(cs, -1, CalUnit::Month, tz),
        },

        // Shifts de 1 semana (7 días — aritmético)
        ShiftShortcut::SameDayLastWeek => shift_cal(1, CalUnit::Week),
        ShiftShortcut::YesterdayLastWeek => ComparisonPeriod {
            prev_start: shift_by_calendar(cs - 86_400, -1, CalUnit::Week, tz),
            prev_end: shift_by_calendar(cs, -1, CalUnit::Week, tz),
        },

        ShiftShortcut::Unspecified => return None,
    })
}

// ── API pública ───────────────────────────────────────────────────────────────

/// Resuelve un AnalyticalComparison × TimeRange × tz → ComparisonPeriod | None.
///
/// Retorna None para BENCHMARK y SMART (no producen ventana temporal — se manejan en el caller).
///
/// Tipos:
///   TimeShiftRelative  → desplaza por N × granularidad con shift_by_calendar
///   TimeShiftShortcut  → shortcuts nombrados con chrono (bisiesto-safe)
///   TimeShiftAbsolute  → ventana explícita (absolute_start_ts / absolute_end_ts, epoch-s)
///   Benchmark          → None (columna inline, sin CTE)
///   Smart              → None (CTE histórico, manejado por caller)
pub fn resolve_comparison_period(
    comp: &AnalyticalComparison,
    time_range: &TimeRange,
    tz: &str,
) -> Option<ComparisonPeriod> {
    let now = now_s();

    match comp.comp_type {
        ComparisonType::TimeShiftRelative => {
            let unit = comp.relative_granularity.parse::<CalUnit>().unwrap();
            let amount = comp.relative_amount.max(1);
            Some(ComparisonPeriod {
                prev_start: shift_by_calendar(time_range.start_ts.unwrap_or(0), -amount, unit, tz),
                prev_end: shift_by_calendar(time_range.end_ts.unwrap_or(now), -amount, unit, tz),
            })
        }

        ComparisonType::TimeShiftShortcut => resolve_shortcut(comp.shortcut, time_range, tz),

        ComparisonType::TimeShiftAbsolute => Some(ComparisonPeriod {
            prev_start: comp.absolute_start_ts.unwrap_or(0),
            prev_end: comp.absolute_end_ts.unwrap_or(now),
        }),

        // BENCHMARK y SMART no generan ventana temporal propia
        ComparisonType::Benchmark => None,
        ComparisonType::Smart => None,
        ComparisonType::Unspecified => None,
    }
}

// ── Smart history window ───────────────────────────────────────────────────────

/// Genera la ventana histórica para la estrategia SMART (anomaly detection).
/// Retorna TimeRange para el período de N días antes del inicio del período actual.
/// Por defecto usa SMART_HISTORY_DAYS (90 días).
pub fn smart_history_window(range: &TimeRange, history_days: Option<i64>) -> TimeRange {
    let now = now_s();
    let cs = range.start_ts.unwrap_or(now);
    let days = history_days.unwrap_or(SMART_HISTORY_DAYS);
    TimeRange {
        start_ts: Some(cs - days * 86_400),
        end_ts: Some(cs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    fn range(s: i64, e: i64) -> TimeRange {
        TimeRange {
            start_ts: Some(s),
            end_ts: Some(e),
        }
    }

    #[test]
    fn previous_period_is_symmetric() {
        let r = range(1_000_000, 1_086_400); // 1 día
        let p = resolve_shortcut(ShiftShortcut::PreviousPeriod, &r, "UTC").unwrap();
        assert_eq!(p.prev_start, 1_000_000 - 86_400);
        assert_eq!(p.prev_end, 1_000_000);
    }

    #[test]
    fn relative_shift_1_month() {
        let comp = AnalyticalComparison {
            comp_type: ComparisonType::TimeShiftRelative,
            relative_granularity: "month".to_string(),
            relative_amount: 1,
            shortcut: ShiftShortcut::Unspecified,
            absolute_start_ts: None,
            absolute_end_ts: None,
        };
        // 2026-05-01 → shift -1 mes → 2026-04-01
        let epoch_may1 = chrono::NaiveDate::from_ymd_opt(2026, 5, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let r = range(epoch_may1, epoch_may1 + 86_400);
        let p = resolve_comparison_period(&comp, &r, "UTC").unwrap();
        let dt = chrono::DateTime::from_timestamp(p.prev_start, 0).unwrap();
        assert_eq!(dt.month(), 4);
        assert_eq!(dt.day(), 1);
    }

    #[test]
    fn smart_history_window_default_90_days() {
        let now = chrono::Utc::now().timestamp();
        let r = range(now, now + 86_400);
        let h = smart_history_window(&r, None);
        assert_eq!(h.end_ts.unwrap(), now);
        assert_eq!(h.start_ts.unwrap(), now - 90 * 86_400);
    }

    #[test]
    fn benchmark_returns_none() {
        let comp = AnalyticalComparison {
            comp_type: ComparisonType::Benchmark,
            relative_granularity: "day".to_string(),
            relative_amount: 1,
            shortcut: ShiftShortcut::Unspecified,
            absolute_start_ts: None,
            absolute_end_ts: None,
        };
        let r = range(0, 86_400);
        assert!(resolve_comparison_period(&comp, &r, "UTC").is_none());
    }
}
