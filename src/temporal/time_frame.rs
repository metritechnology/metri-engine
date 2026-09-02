// [PORTED_FROM: src/metri/temporal/time_frame.clj]
// temporal/time_frame.rs — Resuelve TimeFrameContext → {start_ts, end_ts} epoch-segundos.
//
// SSOT — reemplaza metres.aegis.time-frame en Clojure.
//
// CORRECCIONES vs la implementación anterior:
//   1. CUSTOM_RANGE: el proto envía epoch-MILISEGUNDOS → se convierte con ms_to_s
//   2. Todos los shifts usan shift_by_calendar (chrono) — bisiesto-safe, DST-aware
//   3. ALL_TIME retorna {start_ts: None, end_ts: None} de forma explícita
//   4. Tipo no reconocido retorna None (loggeable upstream)
//
// 29 tipos soportados (28 relativos + CUSTOM_RANGE).

use crate::temporal::core::{self as t, ms_to_s, shift_by_calendar, CalUnit, TimeRange};

/// Representa un TimeFrameContext del proto (campos renombrados a snake_case Rust).
#[derive(Debug, Clone)]
pub struct TimeFrameCtx {
    pub tf_type: TimeFrameType,
    pub n_value: i64,
    /// start_ts en epoch-MILISEGUNDOS (como define el proto)
    pub start_ts_ms: i64,
    /// end_ts en epoch-MILISEGUNDOS
    pub end_ts_ms: i64,
    pub timezone: String,
}

/// Mapeo del enum TimeFrameContext.TimeFilterType del proto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeFrameType {
    CustomRange,
    Today,
    Yesterday,
    Tomorrow,
    LastNMinutes,
    LastNHours,
    LastNDays,
    NextNDays,
    ThisWeek,
    LastWeek,
    NextWeek,
    LastNWeeks,
    NextNWeeks,
    WeekToDate,
    ThisMonth,
    LastMonth,
    NextMonth,
    LastNMonths,
    NextNMonths,
    MonthToDate,
    ThisQuarter,
    LastQuarter,
    LastNQuarters,
    QuarterToDate,
    ThisYear,
    LastYear,
    LastNYears,
    YearToDate,
    AllTime,
    Unspecified,
}

impl TimeFrameType {
    /// Convierte el valor numérico del enum proto al tipo Rust.
    pub fn from_proto(v: i32) -> Self {
        match v {
            1 => Self::CustomRange,
            2 => Self::Today,
            3 => Self::Yesterday,
            4 => Self::Tomorrow,
            5 => Self::LastNMinutes,
            6 => Self::LastNHours,
            7 => Self::LastNDays,
            8 => Self::NextNDays,
            9 => Self::ThisWeek,
            10 => Self::LastWeek,
            11 => Self::NextWeek,
            12 => Self::LastNWeeks,
            13 => Self::NextNWeeks,
            14 => Self::WeekToDate,
            15 => Self::ThisMonth,
            16 => Self::LastMonth,
            17 => Self::NextMonth,
            18 => Self::LastNMonths,
            19 => Self::NextNMonths,
            20 => Self::MonthToDate,
            21 => Self::ThisQuarter,
            22 => Self::LastQuarter,
            23 => Self::LastNQuarters,
            24 => Self::QuarterToDate,
            25 => Self::ThisYear,
            26 => Self::LastYear,
            27 => Self::LastNYears,
            28 => Self::YearToDate,
            29 => Self::AllTime,
            _ => Self::Unspecified,
        }
    }
}

// ── API pública ───────────────────────────────────────────────────────────────

/// Resuelve un TimeFrameCtx → TimeRange {start_ts, end_ts} en epoch-segundos.
///
/// CUSTOM_RANGE: el proto define start_ts / end_ts como epoch-MILISEGUNDOS (i64).
/// Esta función convierte automáticamente con ms_to_s.
///
/// Retorna None si el tipo no está especificado / no reconocido.
/// Retorna TimeRange { start_ts: None, end_ts: None } para ALL_TIME.
pub fn resolve_time_frame(tf: &TimeFrameCtx) -> Option<TimeRange> {
    let tz = &tf.timezone;
    let now = chrono::Utc::now().timestamp();
    let n = tf.n_value.max(1);

    let td = || t::truncate_to_unit(now, CalUnit::Day, tz);
    let wk = || t::truncate_to_unit(now, CalUnit::Week, tz);
    let mo = || t::truncate_to_unit(now, CalUnit::Month, tz);
    let qt = || t::truncate_to_unit(now, CalUnit::Quarter, tz);
    let yr = || t::truncate_to_unit(now, CalUnit::Year, tz);

    // Helpers de shift nombrados (igual que el Clojure)
    let d = |base: i64, delta: i64| shift_by_calendar(base, delta, CalUnit::Day, tz);
    let m = |base: i64, delta: i64| shift_by_calendar(base, delta, CalUnit::Month, tz);
    let y = |base: i64, delta: i64| shift_by_calendar(base, delta, CalUnit::Year, tz);
    let w = |base: i64, delta: i64| shift_by_calendar(base, delta, CalUnit::Week, tz);

    let range = match tf.tf_type {
        // CUSTOM_RANGE: proto envía epoch-ms → convertir a epoch-s
        TimeFrameType::CustomRange => TimeRange {
            start_ts: Some(ms_to_s(tf.start_ts_ms)),
            end_ts: Some(ms_to_s(tf.end_ts_ms)),
        },

        // ── Diario / Horario ───────────────────────────────────────────────
        TimeFrameType::Today => TimeRange {
            start_ts: Some(td()),
            end_ts: Some(d(td(), 1)),
        },
        TimeFrameType::Yesterday => TimeRange {
            start_ts: Some(d(td(), -1)),
            end_ts: Some(td()),
        },
        TimeFrameType::Tomorrow => TimeRange {
            start_ts: Some(d(td(), 1)),
            end_ts: Some(d(td(), 2)),
        },
        TimeFrameType::LastNMinutes => TimeRange {
            start_ts: Some(now - n * 60),
            end_ts: Some(now),
        },
        TimeFrameType::LastNHours => TimeRange {
            start_ts: Some(now - n * 3600),
            end_ts: Some(now),
        },
        TimeFrameType::LastNDays => TimeRange {
            start_ts: Some(d(td(), -n)),
            end_ts: Some(now),
        },
        TimeFrameType::NextNDays => TimeRange {
            start_ts: Some(now),
            end_ts: Some(d(now, n)),
        },

        // ── Semanal ────────────────────────────────────────────────────────
        TimeFrameType::ThisWeek => TimeRange {
            start_ts: Some(wk()),
            end_ts: Some(now),
        },
        TimeFrameType::LastWeek => TimeRange {
            start_ts: Some(w(wk(), -1)),
            end_ts: Some(wk()),
        },
        TimeFrameType::NextWeek => TimeRange {
            start_ts: Some(w(wk(), 1)),
            end_ts: Some(w(wk(), 2)),
        },
        TimeFrameType::LastNWeeks => TimeRange {
            start_ts: Some(w(wk(), -n)),
            end_ts: Some(now),
        },
        TimeFrameType::NextNWeeks => TimeRange {
            start_ts: Some(now),
            end_ts: Some(w(now, n)),
        },
        TimeFrameType::WeekToDate => TimeRange {
            start_ts: Some(wk()),
            end_ts: Some(now),
        },

        // ── Mensual ────────────────────────────────────────────────────────
        TimeFrameType::ThisMonth => TimeRange {
            start_ts: Some(mo()),
            end_ts: Some(now),
        },
        TimeFrameType::LastMonth => TimeRange {
            start_ts: Some(m(mo(), -1)),
            end_ts: Some(mo()),
        },
        TimeFrameType::NextMonth => TimeRange {
            start_ts: Some(m(mo(), 1)),
            end_ts: Some(m(mo(), 2)),
        },
        TimeFrameType::LastNMonths => TimeRange {
            start_ts: Some(m(mo(), -n)),
            end_ts: Some(now),
        },
        TimeFrameType::NextNMonths => TimeRange {
            start_ts: Some(now),
            end_ts: Some(m(now, n)),
        },
        TimeFrameType::MonthToDate => TimeRange {
            start_ts: Some(mo()),
            end_ts: Some(now),
        },

        // ── Trimestral ─────────────────────────────────────────────────────
        TimeFrameType::ThisQuarter => TimeRange {
            start_ts: Some(qt()),
            end_ts: Some(now),
        },
        TimeFrameType::LastQuarter => TimeRange {
            start_ts: Some(m(qt(), -3)),
            end_ts: Some(qt()),
        },
        TimeFrameType::LastNQuarters => TimeRange {
            start_ts: Some(m(qt(), -3 * n)),
            end_ts: Some(now),
        },
        TimeFrameType::QuarterToDate => TimeRange {
            start_ts: Some(qt()),
            end_ts: Some(now),
        },

        // ── Anual ──────────────────────────────────────────────────────────
        TimeFrameType::ThisYear => TimeRange {
            start_ts: Some(yr()),
            end_ts: Some(now),
        },
        TimeFrameType::LastYear => TimeRange {
            start_ts: Some(y(yr(), -1)),
            end_ts: Some(yr()),
        },
        TimeFrameType::LastNYears => TimeRange {
            start_ts: Some(y(yr(), -n)),
            end_ts: Some(now),
        },
        TimeFrameType::YearToDate => TimeRange {
            start_ts: Some(yr()),
            end_ts: Some(now),
        },

        // ── Sin filtro temporal ────────────────────────────────────────────
        TimeFrameType::AllTime => TimeRange {
            start_ts: None,
            end_ts: None,
        },

        // Tipo no reconocido → None (loggeable upstream)
        TimeFrameType::Unspecified => return None,
    };

    Some(range)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(tf_type: TimeFrameType) -> TimeFrameCtx {
        TimeFrameCtx {
            tf_type,
            n_value: 7,
            start_ts_ms: 1_600_000_000_000,
            end_ts_ms: 1_700_000_000_000,
            timezone: "UTC".to_string(),
        }
    }

    #[test]
    fn custom_range_converts_ms_to_s() {
        let tf = ctx(TimeFrameType::CustomRange);
        let r = resolve_time_frame(&tf).unwrap();
        assert_eq!(r.start_ts.unwrap(), 1_600_000_000);
        assert_eq!(r.end_ts.unwrap(), 1_700_000_000);
    }

    #[test]
    fn today_has_start_lt_end() {
        let r = resolve_time_frame(&ctx(TimeFrameType::Today)).unwrap();
        assert!(r.start_ts.unwrap() < r.end_ts.unwrap());
    }

    #[test]
    fn all_time_returns_none_bounds() {
        let r = resolve_time_frame(&ctx(TimeFrameType::AllTime)).unwrap();
        assert!(r.start_ts.is_none());
        assert!(r.end_ts.is_none());
    }

    #[test]
    fn unspecified_returns_none() {
        let r = resolve_time_frame(&ctx(TimeFrameType::Unspecified));
        assert!(r.is_none());
    }

    #[test]
    fn last_n_days_n_respected() {
        let mut tf = ctx(TimeFrameType::LastNDays);
        tf.n_value = 30;
        let r = resolve_time_frame(&tf).unwrap();
        let diff_days = (r.end_ts.unwrap() - r.start_ts.unwrap()) / 86_400;
        // Debe ser ~30 días (puede diferir 1 día por horario de verano)
        assert!((29..=31).contains(&diff_days));
    }
}
