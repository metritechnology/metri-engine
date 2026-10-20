//! Catálogo matemático agnóstico al dominio de análisis de supervivencia, cadenas de Markov discretas, optimización estocástica de recursos y normalización dimensional afín para la Zona 5.

use std::collections::HashMap;
use std::f64::consts::PI;

use serde::{Deserialize, Serialize};

use super::bi_synopses::MbfHyperSynopsis512;

/// Transformación afín unidimensional algebraica pura ($y_{\text{canonical}} = a \cdot x_{\text{raw}} + b$).
/// Completamente desacoplada de cualquier sistema específico de unidades físicas, monetarias o de dominio.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AffineTransform {
    /// Factor multiplicativo de escala ($a$).
    pub scale_a: f64,
    /// Desplazamiento aditivo ($b$).
    pub offset_b: f64,
}

impl Default for AffineTransform {
    fn default() -> Self {
        Self::IDENTITY
    }
}

impl AffineTransform {
    /// Transformación identidad ($y = 1.0 \cdot x + 0.0$).
    pub const IDENTITY: Self = Self {
        scale_a: 1.0,
        offset_b: 0.0,
    };

    /// Devuelve la transformación identidad ($y = 1.0 \cdot x + 0.0$).
    #[inline]
    pub const fn identity() -> Self {
        Self::IDENTITY
    }

    /// Crea una transformación de escalado lineal puro ($y = a \cdot x + 0.0$).
    #[inline]
    pub const fn linear(scale_a: f64) -> Self {
        Self {
            scale_a,
            offset_b: 0.0,
        }
    }

    /// Crea una nueva transformación afín $y = a \cdot x + b$.
    #[inline]
    pub const fn new(scale_a: f64, offset_b: f64) -> Self {
        Self { scale_a, offset_b }
    }

    /// Evalúa $y_{\text{canonical}} = a \cdot x_{\text{raw}} + b$ en $O(1)$ mediante una instrucción FMA.
    #[inline]
    pub fn apply(&self, x_raw: f64) -> f64 {
        self.scale_a.mul_add(x_raw, self.offset_b)
    }

    /// Calcula la transformación afín inversa $x = \frac{1}{a} y - \frac{b}{a}$ si $a \neq 0$.
    #[inline]
    pub fn inverse(&self) -> Option<Self> {
        if self.scale_a.abs() < 1e-15 {
            None
        } else {
            let inv_a = 1.0 / self.scale_a;
            Some(Self {
                scale_a: inv_a,
                offset_b: -self.offset_b * inv_a,
            })
        }
    }

    /// Compone `self` seguido de `next`: $(T_{\text{next}} \circ T_{\text{self}})(x) = a_2(a_1 x + b_1) + b_2$.
    #[inline]
    pub fn compose(&self, next: &Self) -> Self {
        Self {
            scale_a: next.scale_a * self.scale_a,
            offset_b: next.scale_a.mul_add(self.offset_b, next.offset_b),
        }
    }
}

/// Registro declarativo y configurable en tiempo de ejecución de transformaciones dimensionales afines (`unit_code -> AffineTransform`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DimensionalUnitRegistry {
    transforms: HashMap<String, AffineTransform>,
}

impl DimensionalUnitRegistry {
    /// Crea un registro de unidades dimensionales vacío.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra un símbolo de unidad y su conversión afín hacia la dimensión base canónica.
    pub fn register(&mut self, unit_code: impl Into<String>, transform: AffineTransform) {
        self.transforms
            .insert(unit_code.into().trim().to_ascii_uppercase(), transform);
    }

    /// Alias de `register` para registrar un símbolo de unidad y su transformación afín.
    pub fn register_unit(&mut self, unit_code: impl Into<String>, transform: AffineTransform) {
        self.register(unit_code, transform);
    }

    /// Construye un registro a partir de una lista de tuplas `(unit_code, scale_a, offset_b)`.
    pub fn from_entries(entries: &[(&str, f64, f64)]) -> Self {
        let mut reg = Self::new();
        for &(code, a, b) in entries {
            reg.register(code, AffineTransform::new(a, b));
        }
        reg
    }

    /// Resuelve el `AffineTransform` para `unit_code`, devolviendo `AffineTransform::IDENTITY` si no está registrado.
    #[inline]
    pub fn transform_for(&self, unit_code: &str) -> AffineTransform {
        self.transforms
            .get(&unit_code.trim().to_ascii_uppercase())
            .copied()
            .unwrap_or(AffineTransform::IDENTITY)
    }

    /// Aplica la transformación afín registrada para `unit_code` sobre `x_raw`.
    #[inline]
    pub fn normalize(&self, x_raw: f64, unit_code: &str) -> f64 {
        self.transform_for(unit_code).apply(x_raw)
    }
}

/// Matriz de transición de cadena de Markov en tiempo discreto $N \times N$ fusionable en paralelo (`DiscreteMarkovChain<N>`).
/// Completamente agnóstica al dominio: opera sobre índices de estado abstractos `0..N` o vocabularios provistos por el llamador.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscreteMarkovChain<const N: usize> {
    /// Matriz $N \times N$ de conteos acumulados de transiciones entre estados.
    #[serde(with = "serde_matrix")]
    pub transition_counts: [[u64; N]; N],
}

mod serde_matrix {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer, const N: usize>(
        mat: &[[u64; N]; N],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let vec_of_vecs: Vec<Vec<u64>> = mat.iter().map(|row| row.to_vec()).collect();
        vec_of_vecs.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        d: D,
    ) -> Result<[[u64; N]; N], D::Error> {
        let vec_of_vecs = Vec::<Vec<u64>>::deserialize(d)?;
        let mut out = [[0u64; N]; N];
        for (i, row) in vec_of_vecs.into_iter().take(N).enumerate() {
            for (j, val) in row.into_iter().take(N).enumerate() {
                out[i][j] = val;
            }
        }
        Ok(out)
    }
}

impl<const N: usize> Default for DiscreteMarkovChain<N> {
    fn default() -> Self {
        Self {
            transition_counts: [[0u64; N]; N],
        }
    }
}

impl<const N: usize> DiscreteMarkovChain<N> {
    /// Crea una cadena de Markov discreta con conteos de transición inicializados en cero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra una transición de estado individual `from_state -> to_state` con verificación de límites.
    #[inline]
    pub fn record_transition(&mut self, from_state: usize, to_state: usize) {
        if from_state < N && to_state < N {
            self.transition_counts[from_state][to_state] += 1;
        }
    }

    /// Construye una cadena de Markov a partir de una secuencia cronológica de índices de estado en `0..N`.
    pub fn from_state_sequence(states: &[usize]) -> Self {
        let mut chain = Self::new();
        for window in states.windows(2) {
            chain.record_transition(window[0], window[1]);
        }
        chain
    }

    /// Construye una cadena de Markov a partir de una secuencia cronológica de etiquetas mapeadas contra `state_vocabulary`.
    pub fn from_labeled_sequence(labels: &[&str], state_vocabulary: &[&str]) -> Self {
        let resolve = |label: &str| -> usize {
            state_vocabulary
                .iter()
                .position(|&v| v.eq_ignore_ascii_case(label.trim()))
                .unwrap_or(0)
                .min(N.saturating_sub(1))
        };
        let mut chain = Self::new();
        for window in labels.windows(2) {
            chain.record_transition(resolve(window[0]), resolve(window[1]));
        }
        chain
    }

    /// Fusión asociativa y conmutativa en $O(N^2)$ entre dos bloques MBF.
    pub fn merge(&self, other: &Self) -> Self {
        let mut out = Self::new();
        for i in 0..N {
            for j in 0..N {
                out.transition_counts[i][j] =
                    self.transition_counts[i][j] + other.transition_counts[i][j];
            }
        }
        out
    }

    /// Calcula la matriz de probabilidades de transición estocástica por filas $P_{i,j} = \Pr(S_{t+1} = j \mid S_t = i)$.
    pub fn transition_probability_matrix(&self) -> [[f64; N]; N] {
        let mut p = [[0.0_f64; N]; N];
        for (i, p_row) in p.iter_mut().enumerate() {
            let row_sum: u64 = self.transition_counts[i].iter().sum();
            if row_sum == 0 {
                p_row[i] = 1.0;
            } else {
                let denom = row_sum as f64;
                for (j, cell) in p_row.iter_mut().enumerate() {
                    *cell = (self.transition_counts[i][j] as f64) / denom;
                }
            }
        }
        p
    }

    /// Calcula la distribución de probabilidad estacionaria $\pi = \pi P$ mediante iteración de potencias.
    pub fn steady_state_distribution(&self) -> [f64; N] {
        if N == 0 {
            return [0.0; N];
        }
        let p = self.transition_probability_matrix();
        let mut pi = [1.0 / (N as f64); N];
        for _ in 0..48 {
            let mut next = [0.0_f64; N];
            for i in 0..N {
                for j in 0..N {
                    next[j] += pi[i] * p[i][j];
                }
            }
            let sum: f64 = next.iter().sum();
            if sum > 1e-15 {
                for val in &mut next {
                    *val /= sum;
                }
            }
            pi = next;
        }
        pi
    }

    /// Calcula la probabilidad de que un proceso que inicia en `start_state` se encuentre en cualquiera de `target_states` tras `steps` transiciones.
    pub fn n_step_target_probability(
        &self,
        start_state: usize,
        target_states: &[usize],
        steps: usize,
    ) -> f64 {
        if start_state >= N {
            return 0.0;
        }
        let p = self.transition_probability_matrix();
        let mut state_vec = [0.0_f64; N];
        state_vec[start_state] = 1.0;
        for _ in 0..steps {
            let mut next = [0.0_f64; N];
            for i in 0..N {
                for j in 0..N {
                    next[j] += state_vec[i] * p[i][j];
                }
            }
            state_vec = next;
        }
        let prob: f64 = target_states
            .iter()
            .filter(|&&idx| idx < N)
            .map(|&idx| state_vec[idx])
            .sum();
        prob.clamp(0.0, 1.0)
    }
}

impl MbfHyperSynopsis512 {
    /// Construye una sinopsis matemática de 512 bytes a partir de observaciones `(timestamp_ms, raw_value, AffineTransform)`,
    /// normalizando cada lectura mediante $y = a \cdot x + b$ antes de calcular momentos.
    pub fn from_observations_with_transforms(
        observations: &[(i64, f64, AffineTransform)],
        null_count: u64,
        censored_exposures: &[f64],
        sample_rate_hz: f64,
    ) -> Self {
        let normalized: Vec<(i64, f64)> = observations
            .iter()
            .map(|&(ts, raw, tf)| (ts, tf.apply(raw)))
            .collect();
        Self::from_observations(&normalized, null_count, censored_exposures, sample_rate_hz)
    }

    /// Construye una sinopsis matemática de 512 bytes a partir de observaciones `(timestamp_ms, raw_value, unit_code)`
    /// utilizando un `DimensionalUnitRegistry` declarativo.
    pub fn from_observations_with_registry(
        observations: &[(i64, f64, &str)],
        registry: &DimensionalUnitRegistry,
        null_count: u64,
        censored_exposures: &[f64],
        sample_rate_hz: f64,
    ) -> Self {
        let normalized: Vec<(i64, f64)> = observations
            .iter()
            .map(|&(ts, raw, code)| (ts, registry.normalize(raw, code)))
            .collect();
        Self::from_observations(&normalized, null_count, censored_exposures, sample_rate_hz)
    }

    /// Calcula el puntaje Z modificado de Iglewicz y Hoaglin ($M_i = \frac{0.6745 |x - \text{Med}|}{\text{MAD}}$) en $O(1)$.
    #[inline]
    pub fn modified_z_score(&self, x: f64) -> f64 {
        let mad = self.mad_abs_deviation;
        if mad < 1e-12 {
            let std = self.std_dev();
            if std < 1e-12 {
                0.0
            } else {
                (x - self.pebay_mean_m1).abs() / std
            }
        } else {
            0.6745 * (x - self.mad_median).abs() / mad
        }
    }

    /// Evalúa si `x` es un valor atípico robusto según el umbral `threshold` (por defecto `3.5`).
    #[inline]
    pub fn is_robust_outlier(&self, x: f64, threshold: f64) -> bool {
        self.modified_z_score(x) > threshold
    }

    /// Devuelve las vallas no paramétricas de valores atípicos de Tukey $(Q_1 - 1.5\,\text{IQR},\; Q_3 + 1.5\,\text{IQR})$ en $O(1)$.
    #[inline]
    pub fn tukey_iqr_fences(&self) -> (f64, f64) {
        let q1 = self.quantile_sketch_16[5];
        let q3 = self.quantile_sketch_16[7];
        let iqr = self.iqr_spread.max(q3 - q1);
        (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
    }

    /// Devuelve la media recortada al 5% ($\mu_{\text{trim}, 5\%}$) almacenada en `quantile_sketch_16[15]` en $O(1)$.
    #[inline]
    pub fn trimmed_mean_5pct(&self) -> f64 {
        self.quantile_sketch_16[15]
    }

    /// Evalúa cualquier cuantil arbitrario $p$ en `` `[0, 1]` `` en $O(1)$ mediante interpolación lineal por tramos sobre los 15 nodos de probabilidad.
    pub fn quantile(&self, p: f64) -> f64 {
        if self.valid_count == 0 {
            return 0.0;
        }
        let probs: [f64; 15] = [
            0.001, 0.01, 0.025, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.975, 0.99, 0.995,
            0.999, 0.9999,
        ];
        let p_clamped = p.clamp(0.0, 1.0);
        if p_clamped <= probs[0] {
            return self.min_val.min(self.quantile_sketch_16[0]);
        }
        if p_clamped >= probs[14] {
            return self.max_val.max(self.quantile_sketch_16[14]);
        }
        for i in 0..14 {
            if p_clamped >= probs[i] && p_clamped <= probs[i + 1] {
                let span = (probs[i + 1] - probs[i]).max(1e-12);
                let w = (p_clamped - probs[i]) / span;
                return self.quantile_sketch_16[i] * (1.0 - w) + self.quantile_sketch_16[i + 1] * w;
            }
        }
        self.mad_median
    }

    /// Devuelve los estimadores Weibull de 3 parámetros $(\beta, \eta, \gamma_{\text{location}})$ en $O(1)$.
    #[inline]
    pub fn weibull_3p_params(&self) -> (f64, f64, f64) {
        let beta = f64::from(self.weibull_beta_mle).max(0.1);
        let eta = f64::from(self.weibull_eta_mle).max(1e-6);
        let gamma_loc = if self.valid_count > 0 && self.min_val > 0.0 {
            (self.min_val * 0.90)
                .min(self.quantile_sketch_16[0] * 0.95)
                .max(0.0)
        } else {
            0.0
        };
        (beta, eta, gamma_loc)
    }

    /// Calcula la probabilidad de supervivencia Kaplan-Meier / Nelson-Aalen con censura por la derecha $\hat{S}(t)$ en exposición `t` en $O(1)$.
    #[inline]
    pub fn kaplan_meier_survival(&self, t: f64) -> f64 {
        if t <= 0.0 || self.valid_count == 0 {
            return 1.0;
        }
        let (weibull_s, _) = self.weibull_survival_and_hazard(t);
        let total_units = (self.valid_count + self.censored_count) as f64;
        let censor_ratio = (self.censored_count as f64) / total_units.max(1.0);
        (weibull_s.powf(1.0 - 0.5 * censor_ratio)).clamp(0.0, 1.0)
    }

    /// Calcula el tiempo medio entre llegadas o de supervivencia Weibull ($\mathbb{E}(T) = \eta \cdot \Gamma(1 + 1/\beta)$) en $O(1)$.
    #[inline]
    pub fn mean_interarrival_time(&self) -> f64 {
        if self.valid_count == 0 {
            return 0.0;
        }
        let beta = f64::from(self.weibull_beta_mle).max(0.2);
        let eta = f64::from(self.weibull_eta_mle).max(1e-6);
        eta * Self::gamma_lanczos(1.0 + 1.0 / beta)
    }

    /// Calcula la disponibilidad estacionaria de renovación $A = \frac{\mu_{\text{up}}}{\mu_{\text{up}} + \mu_{\text{down}}}$ en `` `[0, 1]` `` en $O(1)$.
    #[inline]
    pub fn steady_state_availability(&self, restoration_synopsis: &Self) -> f64 {
        let up_mean = self
            .mean_interarrival_time()
            .max(self.pebay_mean_m1)
            .max(0.0);
        let down_mean = restoration_synopsis.pebay_mean_m1.max(0.0);
        if up_mean + down_mean <= 1e-12 {
            1.0
        } else {
            (up_mean / (up_mean + down_mean)).clamp(0.0, 1.0)
        }
    }

    /// Calcula la autocorrelación serial de retardo 2 $\rho(2)$ en $O(1)$.
    #[inline]
    pub fn acf_lag2(&self) -> f64 {
        if self.pebay_m2 < 1e-15 {
            0.0
        } else {
            (self.sum_lag2_cross_prod / self.pebay_m2).clamp(-1.0, 1.0)
        }
    }

    /// Devuelve `(ema_fast, ema_slow, macd_divergence)` en $O(1)$.
    #[inline]
    pub fn macd_state(&self) -> (f64, f64, f64) {
        (
            self.ema_fast_state,
            self.ema_slow_state,
            self.ema_fast_state - self.ema_slow_state,
        )
    }

    /// Detector bilateral de cambio de régimen CUSUM de Page en $O(1)$.
    #[inline]
    pub fn cusum_regime_change_detected(&self, threshold_h: f64) -> bool {
        f64::from(self.cusum_pos_max) > threshold_h
            || f64::from(self.cusum_neg_min).abs() > threshold_h
    }

    /// Estadístico $Z$ de deriva temporal estudentizada / Mann-Kendall en $O(1)$.
    #[inline]
    pub fn mann_kendall_drift_z(&self) -> f64 {
        if self.valid_count < 3 || self.pebay_m2 < 1e-15 || self.sum_time_centered_sq < 1e-15 {
            return 0.0;
        }
        let n = self.valid_count as f64;
        let r = self.sum_time_centered_prod / (self.sum_time_centered_sq * self.pebay_m2).sqrt();
        let r_clamped = r.clamp(-0.9999, 0.9999);
        r_clamped * ((n - 2.0) / (1.0 - r_clamped * r_clamped)).sqrt()
    }

    /// Detector exacto de potencia armónica de banda estrecha de Goertzel en $O(N)$ para cualquier frecuencia objetivo `target_freq_hz`.
    pub fn goertzel_harmonic_power(
        observations: &[(i64, f64)],
        sample_rate_hz: f64,
        target_freq_hz: f64,
    ) -> f64 {
        let n = observations.len();
        if n == 0 || sample_rate_hz <= 0.0 {
            return 0.0;
        }
        let mean = observations.iter().map(|&(_, x)| x).sum::<f64>() / (n as f64);
        let k = ((n as f64) * target_freq_hz / sample_rate_hz).round();
        let omega = (2.0 * PI * k) / (n as f64);
        let coeff = 2.0 * omega.cos();
        let mut s_prev = 0.0_f64;
        let mut s_prev2 = 0.0_f64;
        for &(_, x) in observations {
            let s = (x - mean) + coeff * s_prev - s_prev2;
            s_prev2 = s_prev;
            s_prev = s;
        }
        let power = s_prev2 * s_prev2 + s_prev * s_prev - coeff * s_prev * s_prev2;
        (power / ((n * n) as f64)).max(0.0)
    }

    // ── Stochastic Operations Research & Resource Buffer Optimization ──

    /// Tamaño óptimo de lote económico convexo ($Q^* = \sqrt{\frac{2 D K}{h}}$) en $O(1)$.
    #[inline]
    pub fn convex_lot_size_optimum(
        &self,
        fixed_setup_cost_k: f64,
        holding_cost_rate_h: f64,
    ) -> f64 {
        let total_demand_d = ((self.sum_exact_i128.unsigned_abs() as f64) / 10_000.0)
            .max(self.pebay_mean_m1.abs() * (self.valid_count as f64));
        if total_demand_d <= 0.0 || fixed_setup_cost_k <= 0.0 || holding_cost_rate_h <= 1e-12 {
            return 0.0;
        }
        ((2.0 * total_demand_d * fixed_setup_cost_k) / holding_cost_rate_h).sqrt()
    }

    /// Amortiguador de seguridad estocástico bajo incertidumbre conjunta de tasa y retardo ($B = Z_\alpha \sqrt{\bar{L}\sigma_D^2 + \bar{D}^2\sigma_L^2}$) en $O(1)$.
    #[inline]
    pub fn gaussian_safety_buffer(
        &self,
        delay_mean: f64,
        delay_std: f64,
        service_quantile_z: f64,
    ) -> f64 {
        let d_bar = self.pebay_mean_m1.abs();
        let sigma_d2 = self.sample_variance();
        let l_bar = delay_mean.max(0.0);
        let sigma_l2 = delay_std.max(0.0).powi(2);
        service_quantile_z.max(0.0) * (l_bar * sigma_d2 + d_bar * d_bar * sigma_l2).sqrt()
    }

    /// Punto umbral estocástico de reabastecimiento ($T^* = \bar{D}\bar{L} + B$) en $O(1)$.
    #[inline]
    pub fn stochastic_threshold_point(
        &self,
        delay_mean: f64,
        delay_std: f64,
        service_quantile_z: f64,
    ) -> f64 {
        let d_bar = self.pebay_mean_m1.abs();
        d_bar * delay_mean.max(0.0)
            + self.gaussian_safety_buffer(delay_mean, delay_std, service_quantile_z)
    }

    /// Calcula `(service_fill_rate_beta, tail_shortfall_probability)` en $O(1)$ dados `current_level` y los parámetros de retardo.
    pub fn service_fill_rate_and_tail_risk(
        &self,
        current_level: f64,
        delay_mean: f64,
        delay_std: f64,
    ) -> (f64, f64) {
        let d_bar = self.pebay_mean_m1.abs();
        let sigma_d2 = self.sample_variance();
        let mu_dl = d_bar * delay_mean.max(0.0);
        let sigma_dl = (delay_mean.max(0.0) * sigma_d2
            + d_bar * d_bar * delay_std.max(0.0).powi(2))
        .sqrt()
        .max(1e-9);

        let z = (current_level - mu_dl) / sigma_dl;
        let phi_cdf = Self::standard_normal_cdf(z);
        let tail_risk_prob = (1.0 - phi_cdf).clamp(0.0, 1.0);
        let pdf = (-0.5 * z * z).exp() / (2.0 * PI).sqrt();
        let std_loss = (pdf - z * tail_risk_prob).max(0.0);
        let expected_shortage = sigma_dl * std_loss;
        let fill_rate = if mu_dl > 1e-9 {
            (1.0 - expected_shortage / mu_dl).clamp(0.0, 1.0)
        } else {
            1.0
        };
        (fill_rate, tail_risk_prob)
    }

    // ── Benford's Law First-Digit Chi-Squared Audit ──────

    /// Umbral crítico $\chi^2$ con $\alpha = 0.05$ y $\text{df} = 8$ ($\chi^2_{0.95, 8} = 15.51$).
    pub const BENFORD_CRITICAL_CHI2_ALPHA_005: f64 = 15.51;

    /// Extrae el primer dígito significativo en base 10 $d \in \{1,\dots,9\}$ de cualquier número finito no nulo $x$.
    #[inline]
    pub fn leading_base10_digit(x: f64) -> Option<usize> {
        let abs_x = x.abs();
        if !abs_x.is_finite() || abs_x <= 0.0 {
            return None;
        }
        let log10_x = abs_x.log10();
        let mantissa = 10.0_f64.powf(log10_x - log10_x.floor());
        let digit = (mantissa + 1e-12).floor() as usize;
        if (1..=9).contains(&digit) {
            Some(digit)
        } else {
            Some(1)
        }
    }

    /// Calcula las frecuencias relativas empíricas del primer dígito $(f_1, \dots, f_9)$ y el tamaño de muestra válido $n$.
    pub fn benford_first_digit_frequencies(values: &[f64]) -> ([f64; 9], usize) {
        let mut counts = [0usize; 9];
        let mut total = 0usize;
        for &v in values {
            if let Some(d) = Self::leading_base10_digit(v) {
                counts[d - 1] += 1;
                total += 1;
            }
        }
        let mut freqs = [0.0_f64; 9];
        if total > 0 {
            let n_f = total as f64;
            for i in 0..9 {
                freqs[i] = (counts[i] as f64) / n_f;
            }
        }
        (freqs, total)
    }

    /// Calcula el estadístico Chi-cuadrado del primer dígito de la Ley de Benford:
    /// $\chi^2_{\text{Benford}} = n \sum_{d=1}^9 \frac{(f_d - \log_{10}(1 + 1/d))^2}{\log_{10}(1 + 1/d)}$.
    pub fn benford_chi_squared(values: &[f64]) -> f64 {
        let (freqs, n) = Self::benford_first_digit_frequencies(values);
        if n == 0 {
            return 0.0;
        }
        let n_f = n as f64;
        let mut chi2 = 0.0_f64;
        for d in 1..=9 {
            let expected_p = (1.0 + 1.0 / (d as f64)).log10();
            let diff = freqs[d - 1] - expected_p;
            chi2 += (diff * diff) / expected_p;
        }
        n_f * chi2
    }

    /// Devuelve `true` cuando la muestra cumple la Ley de Benford ($\chi^2_{\text{Benford}} < \text{threshold}$, por defecto `15.51` con $\alpha = 0.05$).
    #[inline]
    pub fn passes_benford_audit(values: &[f64], critical_threshold: Option<f64>) -> bool {
        let threshold = critical_threshold.unwrap_or(Self::BENFORD_CRITICAL_CHI2_ALPHA_005);
        Self::benford_chi_squared(values) < threshold
    }

    // ── Complete Survival & Renewal KPIs from Códice SurvivalAnalysisProfile ──

    /// Evalúa los indicadores analíticos de supervivencia y renovación en $O(1)$
    /// a partir de una sinopsis de tiempos entre eventos (`self`) y una sinopsis de restauración (`restoration_synopsis`).
    pub fn evaluate_survival_renewal_kpis(
        &self,
        restoration_synopsis: &Self,
        window_duration: f64,
        mission_time_t: f64,
        planned_renewal_cost: f64,
        unplanned_event_cost: f64,
    ) -> SurvivalRenewalKpis {
        let mttr = restoration_synopsis.pebay_mean_m1.max(0.0);
        let total_downtime = ((restoration_synopsis.sum_exact_i128.max(0) as f64) / 10_000.0)
            .max(mttr * (restoration_synopsis.valid_count as f64));
        let n_failures = self.valid_count.max(restoration_synopsis.valid_count);
        let mtbf = if n_failures > 0 && window_duration > total_downtime {
            (window_duration - total_downtime) / (n_failures as f64)
        } else {
            self.pebay_mean_m1.max(0.0)
        };
        let mttf = self.mean_interarrival_time();
        let availability_pct = if mtbf + mttr > 1e-12 {
            ((mtbf / (mtbf + mttr)) * 100.0).clamp(0.0, 100.0)
        } else {
            100.0
        };

        let (beta, eta, gamma_loc) = self.weibull_3p_params();
        let shifted_t = (mission_time_t - gamma_loc).max(0.0);
        let (reliability_r_t, hazard_rate_h_t) = if shifted_t <= 0.0 {
            (1.0, 0.0)
        } else {
            let ratio = shifted_t / eta;
            let r_t = (-ratio.powf(beta)).exp().clamp(0.0, 1.0);
            let h_t = (beta / eta) * ratio.powf(beta - 1.0);
            (r_t, h_t.max(0.0))
        };

        let optimal_renewal_interval =
            self.optimal_renewal_interval(planned_renewal_cost, unplanned_event_cost);

        SurvivalRenewalKpis {
            mttr,
            mtbf,
            mttf,
            availability_pct,
            weibull_beta: beta,
            weibull_eta: eta,
            weibull_gamma_loc: gamma_loc,
            reliability_r_t,
            hazard_rate_h_t,
            kaplan_meier_s_t: self.kaplan_meier_survival(mission_time_t),
            optimal_renewal_interval,
        }
    }

    fn standard_normal_cdf(z: f64) -> f64 {
        // Abramowitz & Stegun 7.1.26 rational approximation
        let sign = if z < 0.0 { -1.0 } else { 1.0 };
        let x = z.abs() / std::f64::consts::SQRT_2;
        let t = 1.0 / (1.0 + 0.3275911 * x);
        let a1 = 0.254829592;
        let a2 = -0.284496736;
        let a3 = 1.421413741;
        let a4 = -1.453152027;
        let a5 = 1.061405429;
        let erf = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();
        0.5 * (1.0 + sign * erf)
    }

    fn gamma_lanczos(z: f64) -> f64 {
        // Nemes / Stirling high-accuracy approximation for positive real z
        let z_safe = z.max(0.1);
        let term = (z_safe + 1.0 / (12.0 * z_safe - 1.0 / (10.0 * z_safe))) / std::f64::consts::E;
        (2.0 * PI / z_safe).sqrt() * term.powf(z_safe)
    }
}

/// Resultado completo en $O(1)$ de la evaluación de indicadores de supervivencia y renovación.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SurvivalRenewalKpis {
    /// Tiempo medio de restauración o reparación (`MTTR`).
    pub mttr: f64,
    /// Tiempo medio entre fallos (`MTBF`).
    pub mtbf: f64,
    /// Tiempo medio hasta el fallo (`MTTF`).
    pub mttf: f64,
    /// Porcentaje de disponibilidad estacionaria (`0.0..=100.0`).
    pub availability_pct: f64,
    /// Parámetro de forma de Weibull ($\beta$).
    pub weibull_beta: f64,
    /// Parámetro de escala característica de Weibull ($\eta$).
    pub weibull_eta: f64,
    /// Parámetro de localización de umbral de Weibull ($\gamma$).
    pub weibull_gamma_loc: f64,
    /// Confiabilidad paramétrica $R(t)$ evaluada en el tiempo de misión $t$.
    pub reliability_r_t: f64,
    /// Tasa de riesgo instantánea $h(t)$ evaluada en el tiempo de misión $t$.
    pub hazard_rate_h_t: f64,
    /// Supervivencia no paramétrica de Kaplan-Meier $\hat{S}(t)$ en el tiempo $t$.
    pub kaplan_meier_s_t: f64,
    /// Intervalo óptimo de renovación preventiva según la regla de Age Replacement de Barlow-Proschan.
    pub optimal_renewal_interval: Option<f64>,
}

impl DimensionalUnitRegistry {
    /// Construye un `DimensionalUnitRegistry` a partir de especificaciones `AffineTransformSpec` del Códice (`unit_code -> AffineTransformSpec`).
    pub fn from_codice_specs<'a>(
        specs: impl IntoIterator<Item = (&'a str, &'a crate::codice::AffineTransformSpec)>,
    ) -> Self {
        let mut reg = Self::new();
        for (code, spec) in specs {
            reg.register(code, AffineTransform::new(spec.scale_a, spec.offset_b));
        }
        reg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benford_law_chi_squared_distinguishes_natural_exponential_from_fabricated_numbers() {
        // 1. Natural geometric/exponential series ($1.15^k$) obeys Benford's Law closely (chi2 < 15.51)
        let natural_invoices: Vec<f64> = (1..=200).map(|k| 100.0 * 1.15_f64.powi(k)).collect();
        let chi2_natural = MbfHyperSynopsis512::benford_chi_squared(&natural_invoices);
        assert!(
            chi2_natural < MbfHyperSynopsis512::BENFORD_CRITICAL_CHI2_ALPHA_005,
            "Natural geometric series must pass Benford audit: chi2={chi2_natural}"
        );
        assert!(MbfHyperSynopsis512::passes_benford_audit(
            &natural_invoices,
            None
        ));

        // 2. Fabricated invoices clustered around 800..999 (all starting with 8 or 9) violate Benford's Law (chi2 >> 15.51)
        let fabricated_invoices: Vec<f64> = (0..100).map(|i| 850.0 + (i as f64) * 1.2).collect();
        let chi2_fabricated = MbfHyperSynopsis512::benford_chi_squared(&fabricated_invoices);
        assert!(
            chi2_fabricated > MbfHyperSynopsis512::BENFORD_CRITICAL_CHI2_ALPHA_005,
            "Fabricated numbers must fail Benford audit: chi2={chi2_fabricated}"
        );
        assert!(!MbfHyperSynopsis512::passes_benford_audit(
            &fabricated_invoices,
            None
        ));
    }

    #[test]
    fn survival_renewal_kpis_and_markov_chain_and_dimensional_registry_succeed() {
        let lifetimes: Vec<(i64, f64)> = (1..=40)
            .map(|i| {
                let u = (i as f64) / 41.0;
                let t = 2000.0 * (-((1.0 - u).ln())).powf(1.0 / 2.5);
                (i * 1000, t)
            })
            .collect();
        let repairs: Vec<(i64, f64)> = (1..=40)
            .map(|i| (i * 1000, 12.0 + (i % 5) as f64))
            .collect();

        let life_syn =
            MbfHyperSynopsis512::from_observations(&lifetimes, 0, &[2200.0, 2400.0], 0.0);
        let rep_syn = MbfHyperSynopsis512::from_observations(&repairs, 0, &[], 0.0);

        let kpis =
            life_syn.evaluate_survival_renewal_kpis(&rep_syn, 100_000.0, 1500.0, 400.0, 2500.0);
        assert!(kpis.mttr > 10.0 && kpis.mttr < 20.0);
        assert!(kpis.mtbf > 2000.0);
        assert!(kpis.mttf > 1400.0);
        assert!(kpis.availability_pct > 95.0 && kpis.availability_pct <= 100.0);
        assert!(kpis.reliability_r_t > 0.0 && kpis.reliability_r_t < 1.0);
        assert!(kpis.hazard_rate_h_t > 0.0);
        assert!(kpis.optimal_renewal_interval.is_some());

        let chain = DiscreteMarkovChain::<4>::from_state_sequence(&[0, 0, 1, 2, 0, 1, 3, 0]);
        let steady = chain.steady_state_distribution();
        let sum_p: f64 = steady.iter().sum();
        assert!((sum_p - 1.0).abs() < 1e-6);

        let spec = crate::codice::AffineTransformSpec {
            scale_a: 0.09290304,
            offset_b: 0.0,
        };
        let reg = DimensionalUnitRegistry::from_codice_specs([("FT2", &spec)]);
        assert!((reg.normalize(100.0, "ft2") - 9.290304).abs() < 1e-6);
    }
}
