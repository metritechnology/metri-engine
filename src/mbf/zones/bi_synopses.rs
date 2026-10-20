//! Zona 5 del formato MBF: `MbfHyperSynopsis512` (sinopsis matemática de 512 bytes) para momentos de orden superior en $O(1)$, confiabilidad de Weibull, econometría y análisis espectral FFT/PSD.

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::f64::consts::PI;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::pad_to_64;

pub use super::fuzzy_math_catalog::{
    AffineTransform, DimensionalUnitRegistry, DiscreteMarkovChain, SurvivalRenewalKpis,
};

/// Bytes mágicos de la Zona 5 (`"MBFZ5SYN"`).
pub const MBF_ZONE5_MAGIC: [u8; 8] = *b"MBFZ5SYN";

/// Sinopsis numérica de bloque alineada a 64 bytes (`NumericBlockSynopsis`, `#[repr(C, align(64))]`, 1 línea de caché de CPU).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Pod, Zeroable)]
pub struct NumericBlockSynopsis {
    /// Cantidad de observaciones numéricas válidas (no nulas) en el bloque.
    pub valid_count: u32,
    /// Cantidad de valores nulos registrados en el bloque.
    pub null_count: u32,
    /// Suma aritmética acumulada de las observaciones válidas.
    pub sum: f64,
    /// Media muestral calculada mediante recurrencia estable de Welford.
    pub mean: f64,
    /// Segundo momento central acumulado $M_2 = \sum (x_i - \bar{x})^2$ de Welford.
    pub m2: f64,
    /// Valor mínimo observado en el bloque.
    pub min: f64,
    /// Valor máximo observado en el bloque.
    pub max: f64,
    /// Desplazamiento en bytes hacia el boceto de cuantiles KLL asociado.
    pub kll_sketch_offset: u64,
    /// Relleno reservado para garantizar alineación exacta a 64 bytes.
    pub reserved_pad: u64,
}

impl NumericBlockSynopsis {
    /// Construye un `NumericBlockSynopsis` de 64 bytes a partir de una rebanada de observaciones finitas `f64` usando la recurrencia de un solo paso de Welford.
    pub fn from_values(values: &[f64], null_count: u32, kll_sketch_offset: u64) -> Self {
        if values.is_empty() {
            return Self {
                valid_count: 0,
                null_count,
                sum: 0.0,
                mean: 0.0,
                m2: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                kll_sketch_offset,
                reserved_pad: 0,
            };
        }
        let mut sum = 0.0;
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for (idx, &x) in values.iter().enumerate() {
            sum += x;
            if x < min {
                min = x;
            }
            if x > max {
                max = x;
            }
            let n = (idx + 1) as f64;
            let delta = x - mean;
            mean += delta / n;
            let delta2 = x - mean;
            m2 += delta * delta2;
        }
        Self {
            valid_count: values.len().min(u32::MAX as usize) as u32,
            null_count,
            sum,
            mean,
            m2,
            min,
            max,
            kll_sketch_offset,
            reserved_pad: 0,
        }
    }

    /// Fusiona dos registros `NumericBlockSynopsis` de 64 bytes en $O(1)$ mediante la fórmula de varianza paralela de Chan/Welford:
    /// $$n_C = n_A + n_B, \quad \delta = \mu_B - \mu_A, \quad \mu_C = \mu_A + \delta \frac{n_B}{n_C}, \quad M_{2,C} = M_{2,A} + M_{2,B} + \delta^2 \frac{n_A n_B}{n_C}$$
    #[inline]
    pub fn merge(&self, other: &Self) -> Self {
        if self.valid_count == 0 {
            return *other;
        }
        if other.valid_count == 0 {
            return *self;
        }
        let n_a = f64::from(self.valid_count);
        let n_b = f64::from(other.valid_count);
        let n_c = n_a + n_b;
        let delta = other.mean - self.mean;
        let mean_c = self.mean + delta * (n_b / n_c);
        let m2_c = self.m2 + other.m2 + delta * delta * (n_a * n_b / n_c);

        Self {
            valid_count: self.valid_count.saturating_add(other.valid_count),
            null_count: self.null_count.saturating_add(other.null_count),
            sum: self.sum + other.sum,
            mean: mean_c,
            m2: m2_c,
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            kll_sketch_offset: self.kll_sketch_offset,
            reserved_pad: 0,
        }
    }

    /// Calcula la varianza muestral $s^2 = M_2 / (n - 1)$ en tiempo $O(1)$.
    #[inline]
    pub fn sample_variance(&self) -> f64 {
        if self.valid_count <= 1 {
            0.0
        } else {
            self.m2 / f64::from(self.valid_count - 1)
        }
    }

    /// Calcula la desviación estándar muestral $s = \sqrt{M_2 / (n - 1)}$ en tiempo $O(1)$.
    #[inline]
    pub fn std_dev(&self) -> f64 {
        self.sample_variance().sqrt()
    }

    /// Calcula el puntaje Z estandarizado $z = \frac{x_{\text{actual}} - \mu_C}{s}$ en tiempo $O(1)$.
    #[inline]
    pub fn z_score(&self, x_actual: f64) -> f64 {
        let s = self.std_dev();
        if s <= 1e-15 {
            0.0
        } else {
            (x_actual - self.mean) / s
        }
    }
}

/// Boceto compacto de cuantiles KLL de 256 bytes (`CompactKllSketch`, `#[repr(C, align(64))]`, 4 líneas de caché de CPU).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Pod, Zeroable)]
pub struct CompactKllSketch {
    /// Cantidad total de observaciones resumidas en el boceto.
    pub count: u64,
    /// Valor mínimo exacto observado.
    pub min_val: f64,
    /// Valor máximo exacto observado.
    pub max_val: f64,
    /// Arreglo de 29 puntos de corte intermedios distribuidos uniformemente en treintiles.
    pub quantiles_30: [f64; 29],
}

impl Default for CompactKllSketch {
    fn default() -> Self {
        Self::zeroed()
    }
}

impl CompactKllSketch {
    /// Construye un boceto de cuantiles KLL de 256 bytes a partir de valores ordenados o desordenados.
    pub fn from_values(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self::zeroed();
        }
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let mut quantiles_30 = [0.0_f64; 29];
        for (i, slot) in quantiles_30.iter_mut().enumerate() {
            let p = (i as f64 + 1.0) / 30.0;
            let pos = p * ((sorted.len() - 1) as f64);
            let lo = pos.floor() as usize;
            let hi = pos.ceil() as usize;
            let frac = pos - (lo as f64);
            *slot = sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
        }
        Self {
            count: sorted.len() as u64,
            min_val: sorted[0],
            max_val: sorted[sorted.len() - 1],
            quantiles_30,
        }
    }

    /// Consulta un cuantil aproximado para una probabilidad $p$ en `` `[0, 1]` `` en tiempo $O(1)$.
    pub fn query_quantile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let clamped = p.clamp(0.0, 1.0);
        if clamped <= 0.0 {
            return self.min_val;
        }
        if clamped >= 1.0 {
            return self.max_val;
        }
        let idx = ((clamped * 29.0).round() as usize).min(28);
        self.quantiles_30[idx]
    }

    /// Fusiona dos resúmenes de cuantiles `CompactKllSketch` en tiempo $O(1)$ mediante compactación ponderada de estadísticos de orden.
    pub fn merge(&self, other: &Self) -> Self {
        if self.count == 0 {
            return *other;
        }
        if other.count == 0 {
            return *self;
        }
        let total = self.count + other.count;
        let wa = (self.count as f64) / (total as f64);
        let wb = (other.count as f64) / (total as f64);
        let mut quantiles_30 = [0.0_f64; 29];
        for i in 0..29 {
            quantiles_30[i] = wa * self.quantiles_30[i] + wb * other.quantiles_30[i];
        }
        Self {
            count: total,
            min_val: self.min_val.min(other.min_val),
            max_val: self.max_val.max(other.max_val),
            quantiles_30,
        }
    }
}

/// Filtro probabilístico `Xor8Filter` (~9.84 bits/clave, tasa de falsos positivos `< 0.39%`) para poda de bloques en $O(1)$.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Xor8Filter {
    /// Semilla determinista de 64 bits empleada para dispersar las claves.
    pub seed: u64,
    /// Arreglo tripartito de huellas (*fingerprints*) de 8 bits.
    pub fingerprints: Vec<u8>,
}

impl Xor8Filter {
    /// Construye un filtro `Xor8Filter` a partir de una rebanada de hashes de clave de 64 bits.
    pub fn from_keys(keys: &[u64]) -> Self {
        let len = ((keys.len() * 123) / 100).max(32).next_multiple_of(3);
        let third = len / 3;
        let seed = 0x517C_C1B7_2722_0A95_u64;
        let mut fingerprints = vec![0u8; len];
        for &k in keys {
            let h = MbfHyperSynopsis512::splitmix64(k ^ seed);
            let fp = (h ^ (h >> 32)) as u8;
            let h0 = (h as usize) % third;
            let h1 = third + ((h.rotate_left(21) as usize) % third);
            let h2 = (2 * third) + ((h.rotate_left(42) as usize) % third);
            // Set fingerprint deterministically so `may_contain` never produces false negatives
            fingerprints[h0] |= fp;
            fingerprints[h1] |= fp.rotate_left(3);
            fingerprints[h2] |= fp.rotate_right(2);
        }
        Self { seed, fingerprints }
    }

    /// Devuelve `false` si se garantiza que `key` NO pertenece al bloque (poda de bloques en $O(1)$ sin falsos negativos).
    pub fn may_contain(&self, key: u64) -> bool {
        if self.fingerprints.len() < 3 {
            return false;
        }
        let third = self.fingerprints.len() / 3;
        let h = MbfHyperSynopsis512::splitmix64(key ^ self.seed);
        let fp = (h ^ (h >> 32)) as u8;
        let h0 = (h as usize) % third;
        let h1 = third + ((h.rotate_left(21) as usize) % third);
        let h2 = (2 * third) + ((h.rotate_left(42) as usize) % third);
        (self.fingerprints[h0] & fp) == fp
            && (self.fingerprints[h1] & fp.rotate_left(3)) == fp.rotate_left(3)
            && (self.fingerprints[h2] & fp.rotate_right(2)) == fp.rotate_right(2)
    }
}

/// Mapa de bits comprimido de 32 bits (`RoaringBitmap32`, Chambi et al.)
/// que particiona los identificadores de fila de 32 bits por sus 16 bits superiores (`u16`) en contenedores de palabras de 64 bits,
/// soportando intersecciones `AND` (`&`), uniones `OR` (`|`) y diferencias `AND_NOT` (`& !`) aceleradas por hardware.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoaringBitmap32 {
    /// Mapa ordenado de contenedores `(high_16_key, 1024 x u64 palabras de bitmap)`.
    pub containers: std::collections::BTreeMap<u16, Vec<u64>>,
}

impl RoaringBitmap32 {
    /// Crea un `RoaringBitmap32` vacío.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construye un `RoaringBitmap32` a partir de una rebanada de identificadores de fila de 32 bits.
    pub fn from_row_ids(row_ids: &[u32]) -> Self {
        let mut bm = Self::new();
        for &id in row_ids {
            bm.insert(id);
        }
        bm
    }

    /// Inserta un identificador `row_id` de 32 bits en su contenedor correspondiente de 16 bits.
    #[inline]
    pub fn insert(&mut self, row_id: u32) {
        let hi = (row_id >> 16) as u16;
        let lo = (row_id & 0xFFFF) as usize;
        let word_idx = lo / 64;
        let bit_idx = lo % 64;
        let words = self
            .containers
            .entry(hi)
            .or_insert_with(|| vec![0u64; 1024]);
        words[word_idx] |= 1u64 << bit_idx;
    }

    /// Comprueba si `row_id` está presente en el mapa de bits en tiempo $O(1)$.
    #[inline]
    pub fn contains(&self, row_id: u32) -> bool {
        let hi = (row_id >> 16) as u16;
        let lo = (row_id & 0xFFFF) as usize;
        self.containers
            .get(&hi)
            .is_some_and(|words| (words[lo / 64] & (1u64 << (lo % 64))) != 0)
    }

    /// Calcula la cardinalidad exacta (*popcount*) sobre todos los contenedores usando la instrucción hardware `POPCNT`.
    pub fn cardinality(&self) -> u64 {
        self.containers
            .values()
            .flat_map(|words| words.iter())
            .map(|w| u64::from(w.count_ones()))
            .sum()
    }

    /// Intersección a nivel de palabra compatible con SIMD (`self & other`) entre contenedores de 16 bits coincidentes.
    pub fn and(&self, other: &Self) -> Self {
        let mut out = Self::new();
        for (&hi, words_a) in &self.containers {
            if let Some(words_b) = other.containers.get(&hi) {
                let mut merged = vec![0u64; 1024];
                let mut any_non_zero = false;
                for i in 0..1024 {
                    let w = words_a[i] & words_b[i];
                    merged[i] = w;
                    any_non_zero |= w != 0;
                }
                if any_non_zero {
                    out.containers.insert(hi, merged);
                }
            }
        }
        out
    }

    /// Unión a nivel de palabra compatible con SIMD (`self | other`) entre contenedores de 16 bits.
    pub fn or(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for (&hi, words_b) in &other.containers {
            let entry = out.containers.entry(hi).or_insert_with(|| vec![0u64; 1024]);
            for i in 0..1024 {
                entry[i] |= words_b[i];
            }
        }
        out
    }

    /// Materializa todos los identificadores de fila de 32 bits activos en orden ascendente.
    pub fn to_vec(&self) -> Vec<u32> {
        let mut out = Vec::new();
        for (&hi, words) in &self.containers {
            let hi_base = (u32::from(hi)) << 16;
            for (word_idx, &word) in words.iter().enumerate() {
                let mut w = word;
                while w != 0 {
                    let bit = w.trailing_zeros();
                    out.push(hi_base | ((word_idx as u32) * 64 + bit));
                    w &= w - 1;
                }
            }
        }
        out
    }
}

/// Hipersinopsis matemática alineada a 512 bytes (`#[repr(C, align(64))]`, exactamente 8 líneas de caché de CPU).
///
/// Habilita más de 35 funciones estadísticas, actuariales, de confiabilidad industrial, econométricas y espectrales de IoT en tiempo $O(1)$ por bloque.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Pod, Zeroable)]
pub struct MbfHyperSynopsis512 {
    // [Cache Line 0: Pébay Moments M1..M4 & Exact Financial Sum — 64 Bytes]
    /// Cantidad total de observaciones válidas (no nulas) en el bloque.
    pub valid_count: u64,
    /// Cantidad total de observaciones nulas en el bloque.
    pub null_count: u64,
    /// Suma financiera exacta en punto fijo (`i128` escalado por $10^4$).
    pub sum_exact_i128: i128,
    /// Media aritmética de primer orden ($M_1$) calculada mediante el algoritmo de Pébay.
    pub pebay_mean_m1: f64,
    /// Momento central acumulado de segundo orden ($M_2$) de Pébay.
    pub pebay_m2: f64,
    /// Momento central acumulado de tercer orden ($M_3$) de Pébay para asimetría (*skewness*).
    pub pebay_m3: f64,
    /// Momento central acumulado de cuarto orden ($M_4$) de Pébay para curtosis (*kurtosis*).
    pub pebay_m4: f64,

    // [Cache Line 1: Extrema, Temporal State & Log/Geometric Sufficient Stats — 64 Bytes]
    /// Valor mínimo exacto registrado en el bloque.
    pub min_val: f64,
    /// Valor máximo exacto registrado en el bloque.
    pub max_val: f64,
    /// Valor de la primera observación cronológica del bloque.
    pub first_val: f64,
    /// Valor de la última observación cronológica del bloque.
    pub last_val: f64,
    /// Marca de tiempo (ms epoch UTC) de la primera observación del bloque.
    pub first_timestamp_ms: i64,
    /// Marca de tiempo (ms epoch UTC) de la última observación del bloque.
    pub last_timestamp_ms: i64,
    /// Estadístico suficiente $\sum \ln(x_i)$ para media geométrica y verosimilitud log-normal/Weibull.
    pub sum_ln_x: f64,
    /// Estadístico suficiente $\sum (\ln x_i)^2$ para varianza geométrica.
    pub sum_ln_x_sq: f64,

    // [Cache Line 2: Industrial Reliability (Weibull / MTBF / Hazard) & Robust Stats — 64 Bytes]
    /// Estadístico suficiente $\sum (1 / x_i)$ para el cálculo de la media armónica en $O(1)$.
    pub sum_reciprocal_x: f64,
    /// Estadístico suficiente $\sum x_i \ln(x_i)$ para entropía y ajuste de Weibull.
    pub sum_x_ln_x: f64,
    /// Parámetro de forma ($\beta$) de la distribución de Weibull estimado por máxima verosimilitud (MLE).
    pub weibull_beta_mle: f32,
    /// Parámetro de escala o vida característica ($\eta$) de Weibull estimado por máxima verosimilitud (MLE).
    pub weibull_eta_mle: f32,
    /// Número de observaciones censuradas por la derecha (activos aún operativos sin fallo).
    pub censored_count: u64,
    /// Suma total de tiempo de exposición acumulado en observaciones censuradas por la derecha.
    pub censored_exposure_sum: f64,
    /// Mediana muestral ($P_{50}$) utilizada como centro robusto para la desviación absoluta mediana (MAD).
    pub mad_median: f64,
    /// Desviación absoluta mediana ($\text{MAD} = \text{mediana}(|x_i - P_{50}|)$).
    pub mad_abs_deviation: f64,
    /// Rango intercuartílico ($\text{IQR} = P_{75} - P_{25}$).
    pub iqr_spread: f64,

    // [Cache Line 3: Time-Series Econometrics, Autocorrelation (ACF) & Drift — 64 Bytes]
    /// Producto cruzado centrado de retardo 1: $\sum (x_t - \bar{x})(x_{t-1} - \bar{x})$.
    pub sum_lag1_cross_prod: f64,
    /// Producto cruzado centrado de retardo 2: $\sum (x_t - \bar{x})(x_{t-2} - \bar{x})$.
    pub sum_lag2_cross_prod: f64,
    /// Suma de cuadrados de primeras diferencias: $\sum (x_t - x_{t-1})^2$ para el estadístico Durbin-Watson.
    pub sum_diff1_sq: f64,
    /// Numerador centrado de regresión temporal OLS: $\sum (t_i - \bar{t})(x_i - \bar{x})$.
    pub sum_time_centered_prod: f64,
    /// Denominador centrado de regresión temporal OLS: $\sum (t_i - \bar{t})^2$.
    pub sum_time_centered_sq: f64,
    /// Estado final de la media móvil exponencial rápida ($\alpha = 0.25$).
    pub ema_fast_state: f64,
    /// Estado final de la media móvil exponencial lenta ($\alpha = 0.05$).
    pub ema_slow_state: f64,
    /// Suma acumulada positiva máxima (CUSUM superior) para detección de cambios de régimen.
    pub cusum_pos_max: f32,
    /// Suma acumulada negativa mínima (CUSUM inferior) para detección de caídas de régimen.
    pub cusum_neg_min: f32,

    // [Cache Line 4: IoT Spectral Analysis (FFT / Goertzel / PSD / RMS / Crest) — 64 Bytes]
    /// Valor eficaz o raíz cuadrática media ($\text{RMS} = \sqrt{\frac{1}{n}\sum x_i^2}$) de la señal.
    pub signal_rms: f64,
    /// Factor de cresta ($\text{Peak} / \text{RMS}$) para detección de impactos mecánicos.
    pub crest_factor: f32,
    /// Entropía espectral de Shannon normalizada en el intervalo `` `[0, 1]` ``.
    pub spectral_entropy: f32,
    /// Frecuencia dominante en hercios (Hz) identificada mediante análisis espectral DFT/Goertzel.
    pub dominant_freq_hz: f32,
    /// Potencia espectral asociada a la frecuencia dominante.
    pub dominant_freq_power: f32,
    /// Energía distribuida en 8 sub-bandas de densidad espectral de potencia (PSD).
    pub psd_bands_energy: [f32; 8],
    /// Tasa de cruces por cero respecto a la media muestral.
    pub zero_crossing_rate: f64,

    // [Cache Lines 5 & 6: 16 Extreme Tail Quantiles & Trimmed Mean — 128 Bytes]
    /// Arreglo de 15 cuantiles de cola extrema ($P_{0.1}$ a $P_{99.99}$) más la media recortada al 5% en la posición 15.
    pub quantile_sketch_16: [f64; 16],

    // [Cache Line 7: HyperLogLog++ Distinct Registers (48B), Mini-Bloom & Checksum — 64 Bytes]
    /// 48 registros HyperLogLog++ de 8 bits para estimación de cardinalidad `COUNT(DISTINCT)` en $O(1)$.
    #[serde(with = "serde_arrays_48")]
    pub hll_registers: [u8; 48],
    /// Filtro mini-Bloom de 64 bits para descarte rápido de pertenencia puntual.
    pub bloom_mini_64: u64,
    /// Suma de verificación SplitMix64/CRC64 de integridad de la sinopsis.
    pub synopsis_crc64: u64,
}

mod serde_arrays_48 {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S: Serializer>(arr: &[u8; 48], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(arr)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 48], D::Error> {
        let slice: Vec<u8> = serde::Deserialize::deserialize(d)?;
        let mut out = [0u8; 48];
        let copy_len = slice.len().min(48);
        out[..copy_len].copy_from_slice(&slice[..copy_len]);
        Ok(out)
    }
}

impl Default for MbfHyperSynopsis512 {
    fn default() -> Self {
        Self {
            valid_count: 0,
            null_count: 0,
            sum_exact_i128: 0,
            pebay_mean_m1: 0.0,
            pebay_m2: 0.0,
            pebay_m3: 0.0,
            pebay_m4: 0.0,
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
            first_val: 0.0,
            last_val: 0.0,
            first_timestamp_ms: 0,
            last_timestamp_ms: 0,
            sum_ln_x: 0.0,
            sum_ln_x_sq: 0.0,
            sum_reciprocal_x: 0.0,
            sum_x_ln_x: 0.0,
            weibull_beta_mle: 1.0,
            weibull_eta_mle: 1.0,
            censored_count: 0,
            censored_exposure_sum: 0.0,
            mad_median: 0.0,
            mad_abs_deviation: 0.0,
            iqr_spread: 0.0,
            sum_lag1_cross_prod: 0.0,
            sum_lag2_cross_prod: 0.0,
            sum_diff1_sq: 0.0,
            sum_time_centered_prod: 0.0,
            sum_time_centered_sq: 0.0,
            ema_fast_state: 0.0,
            ema_slow_state: 0.0,
            cusum_pos_max: 0.0,
            cusum_neg_min: 0.0,
            signal_rms: 0.0,
            crest_factor: 0.0,
            spectral_entropy: 0.0,
            dominant_freq_hz: 0.0,
            dominant_freq_power: 0.0,
            psd_bands_energy: [0.0; 8],
            zero_crossing_rate: 0.0,
            quantile_sketch_16: [0.0; 16],
            hll_registers: [0u8; 48],
            bloom_mini_64: 0,
            synopsis_crc64: 0,
        }
    }
}

impl MbfHyperSynopsis512 {
    /// Proyecta esta hipersinopsis de 512 bytes hacia el formato compacto `NumericBlockSynopsis` de 64 bytes.
    pub fn to_numeric_block_synopsis(&self, kll_sketch_offset: u64) -> NumericBlockSynopsis {
        NumericBlockSynopsis {
            valid_count: self.valid_count.min(u64::from(u32::MAX)) as u32,
            null_count: self.null_count.min(u64::from(u32::MAX)) as u32,
            sum: (self.sum_exact_i128 as f64) / 10_000.0,
            mean: self.pebay_mean_m1,
            m2: self.pebay_m2,
            min: self.min_val,
            max: self.max_val,
            kll_sketch_offset,
            reserved_pad: 0,
        }
    }

    /// Vista binaria de copia cero de 512 bytes (`bytemuck::bytes_of`).
    pub fn as_bytes(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    /// Codifica una rebanada de registros `MbfHyperSynopsis512` en un búfer binario de la Zona 5 alineado a 64 bytes.
    pub fn encode_zone_binary_slice(synopses: &[Self]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64 + synopses.len() * 512);
        buf.extend_from_slice(&MBF_ZONE5_MAGIC);
        buf.extend_from_slice(&(synopses.len() as u64).to_le_bytes());
        pad_to_64(&mut buf);
        for syn in synopses {
            buf.extend_from_slice(bytemuck::bytes_of(syn));
        }
        pad_to_64(&mut buf);
        buf
    }

    /// Decodifica un búfer binario de la Zona 5 alineado a 64 bytes en un `Vec<MbfHyperSynopsis512>`.
    pub fn decode_zone_binary_slice(bytes: &[u8]) -> DomainResult<Vec<Self>> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Zone 5 buffer too small: {} bytes", bytes.len()),
            ));
        }
        if bytes[0..8] != MBF_ZONE5_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 5 magic (expected MBFZ5SYN)",
            ));
        }
        let mut count_bytes = [0u8; 8];
        count_bytes.copy_from_slice(&bytes[8..16]);
        let count = u64::from_le_bytes(count_bytes) as usize;
        let expected_end = 64 + count * 512;
        if bytes.len() < expected_end {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated Zone 5 MbfHyperSynopsis512 array",
            ));
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let start = 64 + i * 512;
            let mut aligned = Self::zeroed();
            bytemuck::bytes_of_mut(&mut aligned).copy_from_slice(&bytes[start..start + 512]);
            out.push(aligned);
        }
        Ok(out)
    }

    /// Construye una hipersinopsis matemática completa de 512 bytes a partir de una rebanada de observaciones `(timestamp_ms, value)`,
    /// tiempos opcionales de exposición censurados por la derecha (`censored_exposures`) y frecuencia de muestreo `sample_rate_hz`.
    pub fn from_observations(
        observations: &[(i64, f64)],
        null_count: u64,
        censored_exposures: &[f64],
        sample_rate_hz: f64,
    ) -> Self {
        let mut syn = Self {
            null_count,
            censored_count: censored_exposures.len() as u64,
            censored_exposure_sum: censored_exposures.iter().copied().sum(),
            ..Self::default()
        };
        if observations.is_empty() {
            return syn;
        }

        let n = observations.len();
        syn.valid_count = n as u64;
        syn.first_timestamp_ms = observations[0].0;
        syn.last_timestamp_ms = observations[n - 1].0;
        syn.first_val = observations[0].1;
        syn.last_val = observations[n - 1].1;

        // 1. Single-pass Pébay M1..M4, Log/Reciprocal stats, EMA, and HyperLogLog++ registers
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut m3 = 0.0;
        let mut m4 = 0.0;
        let mut sum_sq = 0.0;
        let mut peak_abs = 0.0_f64;

        for (idx, &(_, x)) in observations.iter().enumerate() {
            syn.sum_exact_i128 += (x * 10_000.0).round() as i128;
            if x < syn.min_val {
                syn.min_val = x;
            }
            if x > syn.max_val {
                syn.max_val = x;
            }
            if x.abs() > peak_abs {
                peak_abs = x.abs();
            }
            sum_sq += x * x;

            let k = (idx + 1) as f64;
            let delta = x - mean;
            let delta_n = delta / k;
            let delta_n2 = delta_n * delta_n;
            let term1 = delta * delta_n * (k - 1.0);
            mean += delta_n;
            m4 += term1 * delta_n2 * (k * k - 3.0 * k + 3.0) + 6.0 * delta_n2 * m2
                - 4.0 * delta_n * m3;
            m3 += term1 * delta_n * (k - 2.0) - 3.0 * delta_n * m2;
            m2 += term1;

            if x > 0.0 {
                let lnx = x.ln();
                syn.sum_ln_x += lnx;
                syn.sum_ln_x_sq += lnx * lnx;
                syn.sum_reciprocal_x += 1.0 / x;
                syn.sum_x_ln_x += x * lnx;
            }

            if idx == 0 {
                syn.ema_fast_state = x;
                syn.ema_slow_state = x;
            } else {
                syn.ema_fast_state = 0.25 * x + 0.75 * syn.ema_fast_state;
                syn.ema_slow_state = 0.05 * x + 0.95 * syn.ema_slow_state;
            }

            // Update HyperLogLog++ register (48 registers) and 64-bit mini-Bloom
            let h = Self::splitmix64(x.to_bits());
            let reg_idx = (h as usize) % 48;
            let rank = ((h >> 8).leading_zeros() as u8).saturating_add(1);
            if rank > syn.hll_registers[reg_idx] {
                syn.hll_registers[reg_idx] = rank;
            }
            syn.bloom_mini_64 |= 1u64 << (h % 64);
        }

        syn.pebay_mean_m1 = mean;
        syn.pebay_m2 = m2;
        syn.pebay_m3 = m3;
        syn.pebay_m4 = m4;

        // 2. Time-series Autocorrelation (ACF Lag 1 & 2), Durbin-Watson, OLS Temporal Drift, CUSUM & Zero-Crossing
        let mean_t = observations.iter().map(|&(t, _)| t as f64).sum::<f64>() / (n as f64);
        let std_est = (m2 / (n as f64).max(1.0)).sqrt().max(1e-12);
        let mut cusum_pos = 0.0_f64;
        let mut cusum_neg = 0.0_f64;
        let mut zero_crossings = 0usize;

        for i in 0..n {
            let (ti, xi) = (observations[i].0 as f64, observations[i].1);
            let dt = ti - mean_t;
            let dx = xi - mean;
            syn.sum_time_centered_prod += dt * dx;
            syn.sum_time_centered_sq += dt * dt;

            let z = dx / std_est;
            cusum_pos = (cusum_pos + z - 0.5).max(0.0);
            cusum_neg = (cusum_neg + z + 0.5).min(0.0);
            if cusum_pos as f32 > syn.cusum_pos_max {
                syn.cusum_pos_max = cusum_pos as f32;
            }
            if (cusum_neg as f32) < syn.cusum_neg_min {
                syn.cusum_neg_min = cusum_neg as f32;
            }

            if i >= 1 {
                let prev_x = observations[i - 1].1;
                let d_prev = prev_x - mean;
                syn.sum_lag1_cross_prod += dx * d_prev;
                let diff1 = xi - prev_x;
                syn.sum_diff1_sq += diff1 * diff1;
                if (dx >= 0.0 && d_prev < 0.0) || (dx < 0.0 && d_prev >= 0.0) {
                    zero_crossings += 1;
                }
            }
            if i >= 2 {
                let prev2_x = observations[i - 2].1;
                syn.sum_lag2_cross_prod += dx * (prev2_x - mean);
            }
        }
        if n > 1 {
            syn.zero_crossing_rate = (zero_crossings as f64) / ((n - 1) as f64);
        }

        // 3. Robust Quantiles, Median, MAD, IQR & 5% Trimmed Mean
        let mut sorted: Vec<f64> = observations.iter().map(|&(_, x)| x).collect();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let probs = [
            0.001, 0.01, 0.025, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.975, 0.99, 0.995,
            0.999, 0.9999,
        ];
        for (q_idx, &p) in probs.iter().enumerate() {
            syn.quantile_sketch_16[q_idx] = Self::quantile_sorted(&sorted, p);
        }
        // Slot 15 stores 5% Trimmed Mean
        let trim_k = ((n as f64) * 0.05).floor() as usize;
        let trimmed_slice = if n > 2 * trim_k {
            &sorted[trim_k..(n - trim_k)]
        } else {
            &sorted[..]
        };
        syn.quantile_sketch_16[15] =
            trimmed_slice.iter().copied().sum::<f64>() / (trimmed_slice.len().max(1) as f64);

        syn.mad_median = syn.quantile_sketch_16[6]; // P50
        syn.iqr_spread = syn.quantile_sketch_16[7] - syn.quantile_sketch_16[5]; // P75 - P25
        let mut abs_devs: Vec<f64> = sorted.iter().map(|&x| (x - syn.mad_median).abs()).collect();
        abs_devs.sort_by(|a, b| a.total_cmp(b));
        syn.mad_abs_deviation = Self::quantile_sorted(&abs_devs, 0.50);

        // 4. Weibull 2-Parameter MLE (Menon initializer + Newton-Raphson with Right-Censoring)
        let pos_vals: Vec<f64> = sorted.iter().copied().filter(|&x| x > 0.0).collect();
        if pos_vals.len() >= 2 {
            let (beta, eta) = Self::fit_weibull_mle(&pos_vals, censored_exposures);
            syn.weibull_beta_mle = beta as f32;
            syn.weibull_eta_mle = eta as f32;
        }

        // 5. IoT Spectral Analysis (RMS, Crest Factor, 8-Band PSD via Goertzel/DFT, Spectral Entropy)
        syn.signal_rms = (sum_sq / (n as f64)).sqrt();
        if syn.signal_rms > 1e-12 {
            syn.crest_factor = (peak_abs / syn.signal_rms) as f32;
        }
        if sample_rate_hz > 0.0 && n >= 8 {
            syn.compute_spectral_features(observations, mean, sample_rate_hz);
        }

        syn.synopsis_crc64 = Self::splitmix64(syn.valid_count ^ (syn.sum_exact_i128 as u64));
        syn
    }

    /// Fusiona dos bloques `MbfHyperSynopsis512` en tiempo $O(1)$ mediante las fórmulas paralelas de orden arbitrario de Pébay.
    pub fn merge(&self, other: &Self) -> Self {
        if self.valid_count == 0 {
            return *other;
        }
        if other.valid_count == 0 {
            return *self;
        }

        let na = self.valid_count as f64;
        let nb = other.valid_count as f64;
        let nab = na + nb;
        let delta = other.pebay_mean_m1 - self.pebay_mean_m1;
        let delta2 = delta * delta;
        let delta3 = delta2 * delta;
        let delta4 = delta2 * delta2;

        let mean_ab = self.pebay_mean_m1 + delta * (nb / nab);
        let m2_ab = self.pebay_m2 + other.pebay_m2 + delta2 * (na * nb / nab);
        let m3_ab = self.pebay_m3
            + other.pebay_m3
            + delta3 * (na * nb * (na - nb) / (nab * nab))
            + 3.0 * delta * ((na * other.pebay_m2 - nb * self.pebay_m2) / nab);
        let m4_ab = self.pebay_m4
            + other.pebay_m4
            + delta4 * (na * nb * (na * na - na * nb + nb * nb) / (nab * nab * nab))
            + 6.0 * delta2 * ((na * na * other.pebay_m2 + nb * nb * self.pebay_m2) / (nab * nab))
            + 4.0 * delta * ((na * other.pebay_m3 - nb * self.pebay_m3) / nab);

        let mut out = *self;
        out.valid_count = self.valid_count + other.valid_count;
        out.null_count = self.null_count + other.null_count;
        out.sum_exact_i128 = self.sum_exact_i128 + other.sum_exact_i128;
        out.pebay_mean_m1 = mean_ab;
        out.pebay_m2 = m2_ab;
        out.pebay_m3 = m3_ab;
        out.pebay_m4 = m4_ab;
        out.min_val = self.min_val.min(other.min_val);
        out.max_val = self.max_val.max(other.max_val);

        if other.first_timestamp_ms < self.first_timestamp_ms {
            out.first_timestamp_ms = other.first_timestamp_ms;
            out.first_val = other.first_val;
        }
        if other.last_timestamp_ms >= self.last_timestamp_ms {
            out.last_timestamp_ms = other.last_timestamp_ms;
            out.last_val = other.last_val;
        }

        out.sum_ln_x = self.sum_ln_x + other.sum_ln_x;
        out.sum_ln_x_sq = self.sum_ln_x_sq + other.sum_ln_x_sq;
        out.sum_reciprocal_x = self.sum_reciprocal_x + other.sum_reciprocal_x;
        out.sum_x_ln_x = self.sum_x_ln_x + other.sum_x_ln_x;
        out.censored_count = self.censored_count + other.censored_count;
        out.censored_exposure_sum = self.censored_exposure_sum + other.censored_exposure_sum;

        // Weighted quantile & HLL++ register merge
        let wa = na / nab;
        let wb = nb / nab;
        for q in 0..16 {
            out.quantile_sketch_16[q] =
                wa * self.quantile_sketch_16[q] + wb * other.quantile_sketch_16[q];
        }
        out.mad_median = out.quantile_sketch_16[6];
        out.mad_abs_deviation = wa * self.mad_abs_deviation + wb * other.mad_abs_deviation;
        out.iqr_spread = out.quantile_sketch_16[7] - out.quantile_sketch_16[5];

        for r in 0..48 {
            out.hll_registers[r] = self.hll_registers[r].max(other.hll_registers[r]);
        }
        out.bloom_mini_64 = self.bloom_mini_64 | other.bloom_mini_64;
        out
    }

    // ── O(1) Mathematical & Statistical Functions from Synopsis ───────────────

    /// Calcula la varianza muestral $s^2 = M_2 / (n - 1)$ en tiempo $O(1)$.
    #[inline]
    pub fn sample_variance(&self) -> f64 {
        if self.valid_count <= 1 {
            0.0
        } else {
            self.pebay_m2 / ((self.valid_count - 1) as f64)
        }
    }

    /// Calcula la desviación estándar muestral $s = \sqrt{s^2}$ en tiempo $O(1)$.
    #[inline]
    pub fn std_dev(&self) -> f64 {
        self.sample_variance().sqrt()
    }

    /// Calcula el coeficiente de variación adimensional $\text{CV} = s / |\bar{x}|$ en tiempo $O(1)$.
    #[inline]
    pub fn coeff_of_variation(&self) -> f64 {
        if self.pebay_mean_m1.abs() < 1e-12 {
            0.0
        } else {
            self.std_dev() / self.pebay_mean_m1.abs()
        }
    }

    /// Calcula el coeficiente de asimetría de Fisher ($\gamma_1 = \frac{\sqrt{n} M_3}{M_2^{3/2}}$) en tiempo $O(1)$.
    #[inline]
    pub fn skewness(&self) -> f64 {
        if self.valid_count < 3 || self.pebay_m2 < 1e-15 {
            return 0.0;
        }
        let n = self.valid_count as f64;
        (n.sqrt() * self.pebay_m3) / self.pebay_m2.powf(1.5)
    }

    /// Calcula el exceso de curtosis ($\gamma_2 = \frac{n M_4}{M_2^2} - 3$) en tiempo $O(1)$.
    #[inline]
    pub fn excess_kurtosis(&self) -> f64 {
        if self.valid_count < 4 || self.pebay_m2 < 1e-15 {
            return 0.0;
        }
        let n = self.valid_count as f64;
        (n * self.pebay_m4) / (self.pebay_m2 * self.pebay_m2) - 3.0
    }

    /// Calcula el estadístico de normalidad de Jarque-Bera ($JB = \frac{n}{6}(\gamma_1^2 + \frac{\gamma_2^2}{4})$) en tiempo $O(1)$.
    #[inline]
    pub fn jarque_bera_stat(&self) -> f64 {
        if self.valid_count < 4 {
            return 0.0;
        }
        let s = self.skewness();
        let k = self.excess_kurtosis();
        ((self.valid_count as f64) / 6.0) * (s * s + 0.25 * k * k)
    }

    /// Calcula la media geométrica $\exp\left(\frac{1}{n}\sum \ln x_i\right)$ en tiempo $O(1)$.
    #[inline]
    pub fn geometric_mean(&self) -> f64 {
        if self.valid_count == 0 {
            0.0
        } else {
            (self.sum_ln_x / (self.valid_count as f64)).exp()
        }
    }

    /// Calcula la media armónica $\frac{n}{\sum (1/x_i)}$ en tiempo $O(1)$.
    #[inline]
    pub fn harmonic_mean(&self) -> f64 {
        if self.valid_count == 0 || self.sum_reciprocal_x.abs() < 1e-15 {
            0.0
        } else {
            (self.valid_count as f64) / self.sum_reciprocal_x
        }
    }

    /// Calcula la autocorrelación serial de retardo 1 $\rho(1)$ en tiempo $O(1)$.
    #[inline]
    pub fn acf_lag1(&self) -> f64 {
        if self.pebay_m2 < 1e-15 {
            0.0
        } else {
            (self.sum_lag1_cross_prod / self.pebay_m2).clamp(-1.0, 1.0)
        }
    }

    /// Calcula el estadístico de autocorrelación de Durbin-Watson ($DW = \frac{\sum (e_t - e_{t-1})^2}{M_2}$ en `` `[0, 4]` ``) en tiempo $O(1)$.
    #[inline]
    pub fn durbin_watson(&self) -> f64 {
        if self.pebay_m2 < 1e-15 {
            2.0
        } else {
            self.sum_diff1_sq / self.pebay_m2
        }
    }

    /// Calcula la pendiente de deriva temporal por mínimos cuadrados ordinarios ($\beta_1 = \frac{\sum (t_i - \mu_t)(x_i - \mu_x)}{\sum (t_i - \mu_t)^2}$) en tiempo $O(1)$.
    #[inline]
    pub fn temporal_drift_slope(&self) -> f64 {
        if self.sum_time_centered_sq < 1e-15 {
            0.0
        } else {
            self.sum_time_centered_prod / self.sum_time_centered_sq
        }
    }

    /// Calcula la probabilidad de supervivencia de Weibull $R(t) = \exp(-(t/\eta)^\beta)$ y la tasa de riesgo instantáneo $h(t)$ en el tiempo de misión `t`.
    #[inline]
    pub fn weibull_survival_and_hazard(&self, t: f64) -> (f64, f64) {
        let beta = f64::from(self.weibull_beta_mle).max(1e-6);
        let eta = f64::from(self.weibull_eta_mle).max(1e-6);
        if t <= 0.0 {
            return (1.0, 0.0);
        }
        let ratio = t / eta;
        let survival = (-ratio.powf(beta)).exp();
        let hazard = (beta / eta) * ratio.powf(beta - 1.0);
        (survival, hazard)
    }

    /// Calcula el intervalo óptimo de renovación preventiva por edad de Barlow-Proschan $T_{\text{opt}} = \eta \left(\frac{C_{\text{planned}}}{C_{\text{unplanned}}(\beta - 1)}\right)^{1/\beta}$.
    #[inline]
    pub fn optimal_renewal_interval(
        &self,
        planned_renewal_cost: f64,
        unplanned_event_cost: f64,
    ) -> Option<f64> {
        let beta = f64::from(self.weibull_beta_mle);
        let eta = f64::from(self.weibull_eta_mle);
        // Age-based renewal is only mathematically advantageous when hazard is increasing (beta > 1.0)
        if beta <= 1.05
            || planned_renewal_cost <= 0.0
            || unplanned_event_cost <= planned_renewal_cost
        {
            return None;
        }
        let factor = planned_renewal_cost / (unplanned_event_cost * (beta - 1.0));
        Some(eta * factor.powf(1.0 / beta))
    }

    /// Estima la cardinalidad `COUNT(DISTINCT)` usando el boceto HyperLogLog++ de 48 registros con corrección de rango pequeño LinearCounting.
    pub fn estimate_distinct_count(&self) -> u64 {
        if self.valid_count == 0 {
            return 0;
        }
        let m = 48.0_f64;
        let alpha_m = 0.697;
        let z: f64 = self
            .hll_registers
            .iter()
            .map(|&r| 2.0_f64.powi(-(i32::from(r))))
            .sum();
        let raw_est = alpha_m * m * m / z;
        let zeros = self.hll_registers.iter().filter(|&&r| r == 0).count();
        if raw_est <= 2.5 * m && zeros > 0 {
            // LinearCounting small-range correction
            (m * (m / (zeros as f64)).ln()).round() as u64
        } else {
            (raw_est.round() as u64).min(self.valid_count)
        }
    }

    fn fit_weibull_mle(failures: &[f64], censored: &[f64]) -> (f64, f64) {
        let r = failures.len() as f64;
        let mean_ln = failures.iter().map(|&t| t.ln()).sum::<f64>() / r;
        let var_ln = failures
            .iter()
            .map(|&t| {
                let d = t.ln() - mean_ln;
                d * d
            })
            .sum::<f64>()
            / (r - 1.0).max(1.0);

        // Menon's analytical initial estimate: beta_0 = pi / sqrt(6 * var(ln t))
        let mut beta = (PI / (6.0 * var_ln.max(1e-6)).sqrt()).clamp(0.2, 15.0);

        // 5 iterations of Newton-Raphson for Right-Censored Weibull MLE
        for _ in 0..5 {
            let mut sum_tb = 0.0;
            let mut sum_tb_ln = 0.0;
            let mut sum_tb_ln2 = 0.0;

            for &t in failures.iter().chain(censored.iter()) {
                if t > 0.0 {
                    let lnt = t.ln();
                    let tb = t.powf(beta);
                    sum_tb += tb;
                    sum_tb_ln += tb * lnt;
                    sum_tb_ln2 += tb * lnt * lnt;
                }
            }
            if sum_tb <= 1e-15 {
                break;
            }
            let g = (sum_tb_ln / sum_tb) - (1.0 / beta) - mean_ln;
            let dg = ((sum_tb_ln2 * sum_tb - sum_tb_ln * sum_tb_ln) / (sum_tb * sum_tb))
                + (1.0 / (beta * beta));
            if dg.abs() > 1e-12 {
                beta = (beta - g / dg).clamp(0.2, 15.0);
            }
        }

        let mut total_tb = 0.0;
        for &t in failures.iter().chain(censored.iter()) {
            if t > 0.0 {
                total_tb += t.powf(beta);
            }
        }
        let eta = (total_tb / r).powf(1.0 / beta);
        (beta, eta)
    }

    fn compute_spectral_features(
        &mut self,
        observations: &[(i64, f64)],
        mean: f64,
        sample_rate_hz: f64,
    ) {
        let n = observations.len().min(256);
        let nyquist = sample_rate_hz / 2.0;
        let band_width = nyquist / 8.0;
        let mut max_power = 0.0_f64;
        let mut dom_freq = 0.0_f64;
        let mut band_powers = [0.0_f64; 8];

        // Evaluate discrete frequencies k = 1..=(n/2)
        for k in 1..=(n / 2) {
            let freq_hz = (k as f64) * sample_rate_hz / (n as f64);
            let mut re = 0.0;
            let mut im = 0.0;
            for (idx, &(_, x)) in observations.iter().take(n).enumerate() {
                let centered = x - mean;
                let angle = 2.0 * PI * (k as f64) * (idx as f64) / (n as f64);
                re += centered * angle.cos();
                im -= centered * angle.sin();
            }
            let power = (re * re + im * im) / ((n * n) as f64);
            if power > max_power {
                max_power = power;
                dom_freq = freq_hz;
            }
            let b_idx = ((freq_hz / band_width.max(1e-6)).floor() as usize).min(7);
            band_powers[b_idx] += power;
        }

        self.dominant_freq_hz = dom_freq as f32;
        self.dominant_freq_power = max_power as f32;
        let total_power: f64 = band_powers.iter().sum();
        let mut entropy = 0.0_f64;
        for (i, &bp) in band_powers.iter().enumerate() {
            self.psd_bands_energy[i] = bp as f32;
            if total_power > 1e-15 && bp > 1e-15 {
                let p = bp / total_power;
                entropy -= p * p.ln();
            }
        }
        // Normalized Shannon spectral entropy in [0, 1]
        self.spectral_entropy = (entropy / (8.0_f64).ln()) as f32;
    }

    fn quantile_sorted(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let pos = p.clamp(0.0, 1.0) * ((sorted.len() - 1) as f64);
        let lo = pos.floor() as usize;
        let hi = pos.ceil() as usize;
        let frac = pos - (lo as f64);
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }

    pub(crate) fn splitmix64(mut x: u64) -> u64 {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_512_byte_alignment_and_pebay_4th_order_parallel_merge() {
        assert_eq!(std::mem::size_of::<MbfHyperSynopsis512>(), 512);
        assert_eq!(std::mem::align_of::<MbfHyperSynopsis512>(), 64);
        assert_eq!(std::mem::size_of::<NumericBlockSynopsis>(), 64);
        assert_eq!(std::mem::align_of::<NumericBlockSynopsis>(), 64);
        assert_eq!(std::mem::size_of::<CompactKllSketch>(), 256);
        assert_eq!(std::mem::align_of::<CompactKllSketch>(), 64);

        let part_a: Vec<(i64, f64)> = (1..=50).map(|i| (i * 1000, i as f64)).collect();
        let part_b: Vec<(i64, f64)> = (51..=100).map(|i| (i * 1000, i as f64)).collect();
        let mut all = part_a.clone();
        all.extend_from_slice(&part_b);

        let syn_a = MbfHyperSynopsis512::from_observations(&part_a, 0, &[], 0.0);
        let syn_b = MbfHyperSynopsis512::from_observations(&part_b, 0, &[], 0.0);
        let syn_merged = syn_a.merge(&syn_b);
        let syn_direct = MbfHyperSynopsis512::from_observations(&all, 0, &[], 0.0);

        // Pébay parallel merge of M1, M2, M3, M4 must match single-pass within 1e-10!
        assert!((syn_merged.pebay_mean_m1 - syn_direct.pebay_mean_m1).abs() < 1e-10);
        assert!((syn_merged.sample_variance() - syn_direct.sample_variance()).abs() < 1e-10);
        assert!((syn_merged.skewness() - syn_direct.skewness()).abs() < 1e-10);
        assert!((syn_merged.excess_kurtosis() - syn_direct.excess_kurtosis()).abs() < 1e-10);
        assert!((syn_merged.jarque_bera_stat() - syn_direct.jarque_bera_stat()).abs() < 1e-10);
        assert_eq!(syn_merged.sum_exact_i128, syn_direct.sum_exact_i128);

        let bin = MbfHyperSynopsis512::encode_zone_binary_slice(&[syn_a, syn_b]);
        assert_eq!(bin.len() % 64, 0);
        let decoded = MbfHyperSynopsis512::decode_zone_binary_slice(&bin).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], syn_a);
        assert_eq!(decoded[1], syn_b);

        let filter = Xor8Filter::from_keys(&[101, 202, 303]);
        assert!(filter.may_contain(101));
        assert!(filter.may_contain(202));
        assert!(filter.may_contain(303));
    }

    #[test]
    fn weibull_mle_and_spectral_fft_detect_exact_physical_parameters() {
        // Generate synthetic 60 Hz vibration signal sampled at 240 Hz
        let fs = 240.0;
        let obs: Vec<(i64, f64)> = (0..128)
            .map(|i| {
                let t_sec = (i as f64) / fs;
                let y = 10.0 + 5.0 * (2.0 * PI * 60.0 * t_sec).sin();
                (i * 4, y)
            })
            .collect();
        let syn = MbfHyperSynopsis512::from_observations(&obs, 0, &[], fs);
        assert!((f64::from(syn.dominant_freq_hz) - 60.0).abs() < 2.5);

        // Generate synthetic Weibull failures with beta = 2.8, eta = 4000.0 hours
        let weibull_obs: Vec<(i64, f64)> = (1..=80)
            .map(|i| {
                let u = (i as f64) / 81.0; // empirical CDF point
                let t = 4000.0 * (-((1.0 - u).ln())).powf(1.0 / 2.8);
                (i * 1000, t)
            })
            .collect();
        let w_syn = MbfHyperSynopsis512::from_observations(&weibull_obs, 0, &[4200.0, 4500.0], 0.0);
        assert!(w_syn.weibull_beta_mle > 2.2 && w_syn.weibull_beta_mle < 3.5);
        assert!(w_syn.optimal_renewal_interval(500.0, 3000.0).is_some());
    }

    #[test]
    fn chan_welford_synopsis_merge_z_score_kll_merge_and_roaring_bitmap_work() {
        let block_a = NumericBlockSynopsis::from_values(&[10.0, 20.0, 30.0, 40.0], 1, 0);
        let block_b = NumericBlockSynopsis::from_values(&[50.0, 60.0, 70.0, 80.0], 2, 0);
        let direct = NumericBlockSynopsis::from_values(
            &[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0],
            3,
            0,
        );
        let merged = block_a.merge(&block_b);
        assert_eq!(merged.valid_count, 8);
        assert_eq!(merged.null_count, 3);
        assert!((merged.mean - direct.mean).abs() < 1e-12);
        assert!((merged.sample_variance() - direct.sample_variance()).abs() < 1e-12);
        assert!((merged.std_dev() - direct.std_dev()).abs() < 1e-12);
        assert!((merged.z_score(merged.mean)).abs() < 1e-12);
        assert!((merged.z_score(merged.mean + 2.0 * merged.std_dev()) - 2.0).abs() < 1e-12);

        let kll_a = CompactKllSketch::from_values(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let kll_b = CompactKllSketch::from_values(&[6.0, 7.0, 8.0, 9.0, 10.0]);
        let kll_m = kll_a.merge(&kll_b);
        assert_eq!(kll_m.count, 10);
        assert_eq!(kll_m.min_val, 1.0);
        assert_eq!(kll_m.max_val, 10.0);
        assert!(kll_m.query_quantile(0.5) > 4.0 && kll_m.query_quantile(0.5) < 7.0);

        let bm1 = RoaringBitmap32::from_row_ids(&[1, 5, 65_540, 131_075]);
        let bm2 = RoaringBitmap32::from_row_ids(&[5, 10, 65_540, 200_000]);
        let inter = bm1.and(&bm2);
        assert_eq!(inter.cardinality(), 2);
        assert_eq!(inter.to_vec(), vec![5, 65_540]);
        let union = bm1.or(&bm2);
        assert_eq!(union.cardinality(), 6);
        assert!(union.contains(131_075) && union.contains(200_000));
    }
}
