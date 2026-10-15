//! Zone 3 IoT time-series compression, UN/CEFACT SI unit normalization, and multi-resolution pyramids.

use serde::{Deserialize, Serialize};

/// UN/CEFACT physical unit codes used in `meter_reading.json` and `iot_alert_rule.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UncefactUnit {
    Celsius,          // "CEL" -> SI base: Celsius
    Fahrenheit,       // "FAH" -> converted to Celsius: (F - 32) * 5 / 9
    Kelvin,           // "KEL" -> converted to Celsius: K - 273.15
    Bar,              // "BAR" -> SI base pressure in Bar
    Psi,              // "PSI" -> converted to Bar: PSI * 0.0689475729
    Kilopascal,       // "KPA" -> converted to Bar: KPA * 0.01
    KilowattHour,     // "KWH" -> SI base energy in kWh
    MegawattHour,     // "MWH" -> converted to kWh: MWH * 1000.0
    MillimeterPerSec, // "MMS" -> Vibration velocity mm/s
}

impl UncefactUnit {
    /// Parses a UN/CEFACT string code (`"CEL"`, `"FAH"`, `"BAR"`, `"PSI"`, etc.).
    pub fn from_code(code: &str) -> Self {
        match code.trim().to_ascii_uppercase().as_str() {
            "FAH" => Self::Fahrenheit,
            "KEL" => Self::Kelvin,
            "BAR" => Self::Bar,
            "PSI" => Self::Psi,
            "KPA" => Self::Kilopascal,
            "KWH" => Self::KilowattHour,
            "MWH" => Self::MegawattHour,
            "MMS" | "MM_S" => Self::MillimeterPerSec,
            _ => Self::Celsius,
        }
    }

    /// Normalizes a raw physical reading into its canonical SI domain unit (`y_SI = a * x + b`).
    #[inline]
    pub fn normalize_to_si(self, raw_value: f64) -> f64 {
        match self {
            Self::Celsius | Self::Bar | Self::KilowattHour | Self::MillimeterPerSec => raw_value,
            Self::Fahrenheit => (raw_value - 32.0) * (5.0 / 9.0),
            Self::Kelvin => raw_value - 273.15,
            Self::Psi => raw_value * 0.068_947_572_931_683_6,
            Self::Kilopascal => raw_value * 0.01,
            Self::MegawattHour => raw_value * 1000.0,
        }
    }
}

/// Delta-of-Delta + Chimp128 XOR compressed time-series stream for an IoT sensor metric.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Chimp128SeriesBlock {
    pub asset_row_id: u32,
    pub metric_code: String,
    pub count: usize,
    pub base_timestamp_ms: i64,
    pub first_delta_ms: i64,
    pub delta_of_deltas_ms: Vec<i64>,
    pub base_value_bits: u64,
    pub xor_residuals: Vec<u64>,
    /// Pre-aggregated hourly rollup buckets `(hour_bucket_ms, count, sum_si, min_si, max_si, last_si)`.
    pub hourly_pyramid: Vec<(i64, u64, f64, f64, f64, f64)>,
}

impl Chimp128SeriesBlock {
    pub fn new(asset_row_id: u32, metric_code: impl Into<String>) -> Self {
        Self {
            asset_row_id,
            metric_code: metric_code.into(),
            count: 0,
            base_timestamp_ms: 0,
            first_delta_ms: 0,
            delta_of_deltas_ms: Vec::new(),
            base_value_bits: 0,
            xor_residuals: Vec::new(),
            hourly_pyramid: Vec::new(),
        }
    }

    /// Appends a sensor reading `(timestamp_ms, raw_value, unit)`, automatically normalizing to SI
    /// and compressing via Delta-of-Delta + XOR residuals.
    pub fn push_reading(&mut self, timestamp_ms: i64, raw_value: f64, unit: UncefactUnit) {
        let si_val = unit.normalize_to_si(raw_value);
        let val_bits = si_val.to_bits();

        if self.count == 0 {
            self.base_timestamp_ms = timestamp_ms;
            self.base_value_bits = val_bits;
        } else if self.count == 1 {
            self.first_delta_ms = timestamp_ms - self.base_timestamp_ms;
            let prev_bits = self.base_value_bits;
            self.xor_residuals.push(val_bits ^ prev_bits);
        } else {
            // Reconstruct last two timestamps to compute second derivative Delta^2
            let mut prev_t = self.base_timestamp_ms + self.first_delta_ms;
            let mut prev_delta = self.first_delta_ms;
            for &d2 in &self.delta_of_deltas_ms {
                prev_delta += d2;
                prev_t += prev_delta;
            }
            let current_delta = timestamp_ms - prev_t;
            self.delta_of_deltas_ms.push(current_delta - prev_delta);

            let mut prev_bits = self.base_value_bits;
            for &xr in &self.xor_residuals {
                prev_bits ^= xr;
            }
            self.xor_residuals.push(val_bits ^ prev_bits);
        }
        self.count += 1;

        // Update hourly multi-resolution pyramid
        let hour_bucket = (timestamp_ms / 3_600_000) * 3_600_000;
        if let Some(last_bucket) = self.hourly_pyramid.last_mut() {
            if last_bucket.0 == hour_bucket {
                last_bucket.1 += 1;
                last_bucket.2 += si_val;
                if si_val < last_bucket.3 {
                    last_bucket.3 = si_val;
                }
                if si_val > last_bucket.4 {
                    last_bucket.4 = si_val;
                }
                last_bucket.5 = si_val;
                return;
            }
        }
        self.hourly_pyramid
            .push((hour_bucket, 1, si_val, si_val, si_val, si_val));
    }

    /// Decompresses all `(timestamp_ms, si_value)` readings losslessly.
    pub fn decompress_all(&self) -> Vec<(i64, f64)> {
        if self.count == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.count);
        let mut cur_t = self.base_timestamp_ms;
        let mut cur_bits = self.base_value_bits;
        out.push((cur_t, f64::from_bits(cur_bits)));

        if self.count >= 2 {
            let mut cur_delta = self.first_delta_ms;
            cur_t += cur_delta;
            cur_bits ^= self.xor_residuals[0];
            out.push((cur_t, f64::from_bits(cur_bits)));

            for i in 0..self.delta_of_deltas_ms.len() {
                cur_delta += self.delta_of_deltas_ms[i];
                cur_t += cur_delta;
                cur_bits ^= self.xor_residuals[i + 1];
                out.push((cur_t, f64::from_bits(cur_bits)));
            }
        }
        out
    }

    /// Returns the fraction of periodic readings whose Delta-of-Delta is 0 (1-bit compression rate).
    pub fn zero_delta_of_delta_ratio(&self) -> f64 {
        if self.delta_of_deltas_ms.is_empty() {
            return 1.0;
        }
        let zeros = self
            .delta_of_deltas_ms
            .iter()
            .filter(|&&d2| d2 == 0)
            .count();
        zeros as f64 / self.delta_of_deltas_ms.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chimp128_lossless_roundtrip_and_uncefact_unit_normalization() {
        let mut block = Chimp128SeriesBlock::new(10, "TEMPERATURE_HEAD");
        // Push 1st reading in Celsius (20.0 C), 2nd reading in Fahrenheit (86.0 F == 30.0 C), 3rd in Kelvin (313.15 K == 40.0 C)
        block.push_reading(1_700_000_000_000, 20.0, UncefactUnit::Celsius);
        block.push_reading(1_700_000_060_000, 86.0, UncefactUnit::Fahrenheit);
        block.push_reading(1_700_000_120_000, 313.15, UncefactUnit::Kelvin);

        // Regular 60s interval means delta-of-delta is 0 (100% 1-bit timestamp compression!)
        assert!((block.zero_delta_of_delta_ratio() - 1.0).abs() < 1e-12);

        let points = block.decompress_all();
        assert_eq!(points.len(), 3);
        assert!((points[0].1 - 20.0).abs() < 1e-11);
        assert!((points[1].1 - 30.0).abs() < 1e-11);
        assert!((points[2].1 - 40.0).abs() < 1e-11);
        assert_eq!(block.hourly_pyramid.len(), 1);
        assert!((block.hourly_pyramid[0].2 - 90.0).abs() < 1e-11);
    }
}
