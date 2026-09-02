// [PORTED_FROM: src/metri/temporal/core.clj]
// temporal/core.rs — Primitivas temporales canónicas.
//
// Invariantes (idénticas al Clojure):
//   - TODOS los valores internos son epoch-SEGUNDOS (i64), salvo sufijo _ms.
//   - CUSTOM_RANGE del proto llega en epoch-MILISEGUNDOS → usar ms_to_s antes de procesar.
//   - shift_by_calendar usa chrono → bisiesto-safe, DST-aware.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike};
use chrono_tz::Tz;

// ── Constantes de Escala ──────────────────────────────────────────────────────

/// Factor de conversión ms → s.
/// Datahike almacena timestamps como epoch-ms (i64).
/// El proto TimeFrameContext.start_ts / end_ts son epoch-ms.
/// Toda la lógica interna de Metri Engine opera en epoch-s.
pub const MS_PER_SECOND: i64 = 1_000;

/// Ventana histórica por defecto para la estrategia SMART (anomaly detection).
/// El CTE SMART calcula AVG + STDDEV_SAMP sobre los últimos N días antes del período actual.
pub const SMART_HISTORY_DAYS: i64 = 90;

// ── Conversiones ms ↔ s ──────────────────────────────────────────────────────

/// Convierte epoch-milisegundos → epoch-segundos (i64).
/// Uso: CUSTOM_RANGE del proto, timestamps de Datahike.
#[inline]
pub fn ms_to_s(ms: i64) -> i64 {
    ms / MS_PER_SECOND
}

/// Convierte epoch-segundos → epoch-milisegundos (i64).
/// Uso: cláusulas Datalog para Datahike.
#[inline]
pub fn s_to_ms(s: i64) -> i64 {
    s * MS_PER_SECOND
}

// ── Helpers de Timezone ───────────────────────────────────────────────────────

/// Parsea el string de timezone. Retorna UTC si no se reconoce.
pub fn parse_tz(tz: &str) -> Tz {
    tz.parse::<Tz>().unwrap_or(chrono_tz::UTC)
}

/// Convierte epoch-segundos → DateTime en la timezone dada.
pub fn epoch_to_zdt(epoch_secs: i64, tz: Tz) -> DateTime<Tz> {
    tz.timestamp_opt(epoch_secs, 0)
        .single()
        .unwrap_or_else(|| chrono_tz::UTC.timestamp_opt(epoch_secs, 0).unwrap())
}

/// Trunca un DateTime al inicio del día (00:00:00) en su timezone.
pub fn day0(dt: DateTime<Tz>) -> DateTime<Tz> {
    dt.with_hour(0)
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_nanosecond(0))
        .unwrap_or(dt)
}

// ── shift_by_calendar ─────────────────────────────────────────────────────────

/// Unidad de calendario para shift_by_calendar y truncate_to_unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl std::str::FromStr for CalUnit {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            _ => Self::Day,
        })
    }
}

/// Desplaza epoch-segundos por `amount` unidades de `unit` en la timezone `tz`.
/// Usa chrono real → correcto para años bisiestos, meses de diferente longitud y DST.
/// `amount` puede ser negativo (pasado) o positivo (futuro).
/// Retorna epoch-segundos (i64).
pub fn shift_by_calendar(epoch_secs: i64, amount: i64, unit: CalUnit, tz: &str) -> i64 {
    let tz_parsed = parse_tz(tz);
    let dt = epoch_to_zdt(epoch_secs, tz_parsed);

    let shifted = match unit {
        CalUnit::Minute => dt + Duration::minutes(amount),
        CalUnit::Hour => dt + Duration::hours(amount),
        CalUnit::Day => dt + Duration::days(amount),
        CalUnit::Week => dt + Duration::weeks(amount),
        CalUnit::Month => shift_months(dt, amount),
        CalUnit::Quarter => shift_months(dt, amount * 3),
        CalUnit::Year => shift_years(dt, amount),
    };

    shifted.timestamp()
}

fn shift_months(dt: DateTime<Tz>, months: i64) -> DateTime<Tz> {
    let total_months = dt.month0() as i64 + months;
    let year_delta = total_months.div_euclid(12);
    let new_month = (total_months.rem_euclid(12) + 1) as u32;
    let new_year = dt.year() + year_delta as i32;

    // Clamp day al máximo del mes destino (ej. 31 enero → 28/29 febrero)
    let max_day = days_in_month(new_year, new_month);
    let new_day = dt.day().min(max_day);

    let naive = NaiveDate::from_ymd_opt(new_year, new_month, new_day)
        .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), dt.second()))
        .expect("shift_months: fecha inválida");

    dt.timezone()
        .from_local_datetime(&naive)
        .single()
        .unwrap_or_else(|| dt.timezone().from_utc_datetime(&naive))
}

fn shift_years(dt: DateTime<Tz>, years: i64) -> DateTime<Tz> {
    shift_months(dt, years * 12)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 30,
    }
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

// ── truncate_to_unit ──────────────────────────────────────────────────────────

/// Trunca epoch-segundos al inicio del período dado, respetando la timezone.
/// Equivalente a Athena date_trunc() ejecutado en Rust.
///
/// Usado en:
///   - OLTP executor (bucketing in-memory de TIMESERIES)
///   - Referencia canónica para validar el SQL generado en OLAP
///
/// Retorna epoch-segundos (i64).
pub fn truncate_to_unit(epoch_secs: i64, unit: CalUnit, tz: &str) -> i64 {
    match unit {
        // Truncados aritméticos sin chrono (UTC-safe para minutos/horas)
        CalUnit::Minute => (epoch_secs / 60) * 60,
        CalUnit::Hour => (epoch_secs / 3600) * 3600,

        // Truncados de calendario — requieren ZonedDateTime para DST-correctness
        _ => {
            let tz_parsed = parse_tz(tz);
            let dt = epoch_to_zdt(epoch_secs, tz_parsed);

            let truncated = match unit {
                CalUnit::Day => day0(dt),

                CalUnit::Week => {
                    // Inicio de semana = Lunes (ISO 8601)
                    let days_from_monday = dt.weekday().num_days_from_monday() as i64;
                    let monday = dt - Duration::days(days_from_monday);
                    day0(monday)
                }

                CalUnit::Month => {
                    let first = dt.with_day(1).unwrap_or(dt);
                    day0(first)
                }

                CalUnit::Quarter => {
                    let qm = ((dt.month() - 1) / 3) * 3 + 1;
                    let first = dt.with_month(qm).and_then(|d| d.with_day(1)).unwrap_or(dt);
                    day0(first)
                }

                CalUnit::Year => {
                    let first = dt.with_ordinal(1).unwrap_or(dt);
                    day0(first)
                }

                // Minutas/Horas ya manejadas arriba — fallback safe
                _ => dt,
            };

            truncated.timestamp()
        }
    }
}

// ── parse_athena_ts ───────────────────────────────────────────────────────────

/// Parsea un valor de timestamp proveniente de Athena → epoch-segundos (i64).
///
/// Soporta:
///   - Número i64/f64 → conversión directa (to_unixtime retorna f64)
///   - '2026-03-04 00:00:00.000'     → LocalDateTime UTC → epoch-s
///   - '2026-03-04 00:00:00.000 UTC' → strip timezone suffix
///   - '2026-03-04'                  → LocalDate UTC start-of-day
///   - epoch como string '1772582400' → i64::parse
///
/// Retorna None si no puede parsear.
pub fn parse_athena_ts(s: &str) -> Option<i64> {
    let s = s.trim().trim_end_matches(" UTC").trim();

    // Intento 1: epoch numérico como string
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Some(f as i64);
    }

    // Intento 2: LocalDateTime con diferentes precisiones de subsegundo
    let datetime_fmts = ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"];
    for fmt in &datetime_fmts {
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ndt.and_utc().timestamp());
        }
    }

    // Intento 3: LocalDate → inicio del día UTC
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(nd.and_hms_opt(0, 0, 0)?.and_utc().timestamp());
    }

    None
}

// ── Helpers de período de comparación ────────────────────────────────────────

/// Par de timestamps para un rango temporal.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
}

/// Resultado de un cálculo de período de comparación.
#[derive(Debug, Clone, Copy)]
pub struct ComparisonPeriod {
    pub prev_start: i64,
    pub prev_end: i64,
}

/// Desplaza el par {start_ts, end_ts} (epoch-s) hacia el pasado por `amount` unidades de `unit`.
/// Usa shift_by_calendar → bisiesto-safe, DST-aware.
pub fn shift_period(range: &TimeRange, amount: i64, unit: CalUnit, tz: &str) -> ComparisonPeriod {
    let now = chrono::Utc::now().timestamp();
    ComparisonPeriod {
        prev_start: shift_by_calendar(range.start_ts.unwrap_or(0), -amount, unit, tz),
        prev_end: shift_by_calendar(range.end_ts.unwrap_or(now), -amount, unit, tz),
    }
}

/// Calcula la duración en segundos entre dos epoch-segundos.
pub fn duration_seconds(start_ts: Option<i64>, end_ts: Option<i64>) -> i64 {
    let now = chrono::Utc::now().timestamp();
    end_ts.unwrap_or(now) - start_ts.unwrap_or(0)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ms_to_s_rounds_down() {
        assert_eq!(ms_to_s(1_500), 1);
        assert_eq!(ms_to_s(2_000), 2);
    }

    #[test]
    fn s_to_ms_round_trip() {
        assert_eq!(s_to_ms(ms_to_s(5_000)), 5_000);
    }

    #[test]
    fn shift_by_calendar_adds_days() {
        // 2026-01-01 00:00:00 UTC = 1735689600
        let epoch = 1_735_689_600_i64;
        let shifted = shift_by_calendar(epoch, 30, CalUnit::Day, "UTC");
        // +30 días = 2026-01-31 00:00:00
        assert_eq!(shifted - epoch, 30 * 86_400);
    }

    #[test]
    fn shift_by_calendar_month_clamps_day() {
        // 2026-01-31 → +1 month → 2026-02-28 (no hay Feb 31)
        let epoch = chrono::NaiveDate::from_ymd_opt(2026, 1, 31)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let shifted = shift_by_calendar(epoch, 1, CalUnit::Month, "UTC");
        let result = chrono::DateTime::from_timestamp(shifted, 0).unwrap();
        assert_eq!(result.day(), 28);
        assert_eq!(result.month(), 2);
    }

    #[test]
    fn truncate_to_unit_day() {
        // 2026-05-14 15:30:45 UTC
        let epoch = chrono::NaiveDate::from_ymd_opt(2026, 5, 14)
            .unwrap()
            .and_hms_opt(15, 30, 45)
            .unwrap()
            .and_utc()
            .timestamp();
        let trunc = truncate_to_unit(epoch, CalUnit::Day, "UTC");
        let result = chrono::DateTime::from_timestamp(trunc, 0).unwrap();
        assert_eq!(result.hour(), 0);
        assert_eq!(result.minute(), 0);
    }

    #[test]
    fn parse_athena_ts_handles_all_formats() {
        assert!(parse_athena_ts("1735689600").is_some());
        assert!(parse_athena_ts("2026-01-01 00:00:00.000").is_some());
        assert!(parse_athena_ts("2026-01-01 00:00:00.000 UTC").is_some());
        assert!(parse_athena_ts("2026-01-01").is_some());
        assert!(parse_athena_ts("not-a-date").is_none());
    }
}
