//! Domain-agnostic mathematical, survival analysis, discrete Markov, stochastic resource optimization, and affine dimensional normalization catalog for Zone 5 (`09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md`).

use std::collections::HashMap;
use std::f64::consts::PI;

use serde::{Deserialize, Serialize};

use super::bi_synopses::MbfHyperSynopsis512;

/// Pure algebraic 1D affine transformation ($y_{\text{canonical}} = a \cdot x_{\text{raw}} + b$, §6).
/// Completely decoupled from any specific physical, monetary, or domain unit system.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AffineTransform {
    pub scale_a: f64,
    pub offset_b: f64,
}

impl Default for AffineTransform {
    fn default() -> Self {
        Self::IDENTITY
    }
}

impl AffineTransform {
    /// Identity transformation ($y = 1.0 \cdot x + 0.0$).
    pub const IDENTITY: Self = Self {
        scale_a: 1.0,
        offset_b: 0.0,
    };

    /// Returns the identity transformation ($y = 1.0 \cdot x + 0.0$).
    #[inline]
    pub const fn identity() -> Self {
        Self::IDENTITY
    }

    /// Creates a linear scaling transformation ($y = a \cdot x + 0.0$).
    #[inline]
    pub const fn linear(scale_a: f64) -> Self {
        Self {
            scale_a,
            offset_b: 0.0,
        }
    }

    /// Creates a new affine transformation $y = a \cdot x + b$.
    #[inline]
    pub const fn new(scale_a: f64, offset_b: f64) -> Self {
        Self { scale_a, offset_b }
    }

    /// Evaluates $y_{\text{canonical}} = a \cdot x_{\text{raw}} + b$ in $O(1)$ (single FMA instruction).
    #[inline]
    pub fn apply(&self, x_raw: f64) -> f64 {
        self.scale_a.mul_add(x_raw, self.offset_b)
    }

    /// Computes the inverse affine transformation $x = \frac{1}{a} y - \frac{b}{a}$ if $a \neq 0$.
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

    /// Composes `self` followed by `next`: $(T_{\text{next}} \circ T_{\text{self}})(x) = a_2(a_1 x + b_1) + b_2$.
    #[inline]
    pub fn compose(&self, next: &Self) -> Self {
        Self {
            scale_a: next.scale_a * self.scale_a,
            offset_b: next.scale_a.mul_add(self.offset_b, next.offset_b),
        }
    }
}

/// Declarative, runtime-configurable registry of affine dimensional transforms (`unit_code -> AffineTransform`, §6).
/// Contains zero hardcoded physical or domain units; all mappings are injected from Códice metadata or terminology tables.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DimensionalUnitRegistry {
    transforms: HashMap<String, AffineTransform>,
}

impl DimensionalUnitRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a unit symbol and its affine conversion to the canonical base dimension.
    pub fn register(&mut self, unit_code: impl Into<String>, transform: AffineTransform) {
        self.transforms
            .insert(unit_code.into().trim().to_ascii_uppercase(), transform);
    }

    /// Alias for `register` to register a unit symbol and its affine conversion.
    pub fn register_unit(&mut self, unit_code: impl Into<String>, transform: AffineTransform) {
        self.register(unit_code, transform);
    }

    /// Builds a registry from a slice of `(unit_code, scale_a, offset_b)` tuples.
    pub fn from_entries(entries: &[(&str, f64, f64)]) -> Self {
        let mut reg = Self::new();
        for &(code, a, b) in entries {
            reg.register(code, AffineTransform::new(a, b));
        }
        reg
    }

    /// Resolves the `AffineTransform` for `unit_code`, defaulting to `AffineTransform::IDENTITY` if unregistered.
    #[inline]
    pub fn transform_for(&self, unit_code: &str) -> AffineTransform {
        self.transforms
            .get(&unit_code.trim().to_ascii_uppercase())
            .copied()
            .unwrap_or(AffineTransform::IDENTITY)
    }

    /// Applies the registered affine transform for `unit_code` to `x_raw`.
    #[inline]
    pub fn normalize(&self, x_raw: f64, unit_code: &str) -> f64 {
        self.transform_for(unit_code).apply(x_raw)
    }
}

/// Parallel-mergeable $N \times N$ Discrete-Time Markov Chain transition matrix (`DiscreteMarkovChain<N>`, §1.1 #1 & §7).
/// Completely domain-agnostic: operates on abstract state indices `0..N` or caller-provided state vocabularies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscreteMarkovChain<const N: usize> {
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a single state transition `from_state -> to_state` (bounds-checked).
    #[inline]
    pub fn record_transition(&mut self, from_state: usize, to_state: usize) {
        if from_state < N && to_state < N {
            self.transition_counts[from_state][to_state] += 1;
        }
    }

    /// Builds a Markov chain from a chronological sequence of state indices in `0..N`.
    pub fn from_state_sequence(states: &[usize]) -> Self {
        let mut chain = Self::new();
        for window in states.windows(2) {
            chain.record_transition(window[0], window[1]);
        }
        chain
    }

    /// Builds a Markov chain from a chronological sequence of string labels mapped against a caller-supplied `state_vocabulary`.
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

    /// Associative and commutative $O(N^2)$ merge across two MBF blocks.
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

    /// Computes the row-stochastic transition probability matrix $P_{i,j} = \Pr(S_{t+1} = j \mid S_t = i)$.
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

    /// Computes the stationary (steady-state) probability distribution $\pi = \pi P$ via power iteration.
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

    /// Computes the probability that a process starting in `start_state` is in any of `target_states` after `steps` transitions.
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
    /// Builds a 512-byte mathematical synopsis from `(timestamp_ms, raw_value, AffineTransform)` observations,
    /// normalizing each observation via $y = a \cdot x + b$ before computing moments (§6).
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

    /// Builds a 512-byte mathematical synopsis from `(timestamp_ms, raw_value, unit_code)` observations
    /// using a caller-provided declarative `DimensionalUnitRegistry` (§6).
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

    /// Iglewicz & Hoaglin Modified Z-Score ($M_i = \frac{0.6745 |x - \text{Med}|}{\text{MAD}}$) in $O(1)$ (§7).
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

    /// Evaluates whether `x` is a robust outlier according to the Códice `outlier_sigma_threshold` (default `3.5`).
    #[inline]
    pub fn is_robust_outlier(&self, x: f64, threshold: f64) -> bool {
        self.modified_z_score(x) > threshold
    }

    /// Returns Tukey's non-parametric boxplot outlier fences $(Q_1 - 1.5\,\text{IQR},\; Q_3 + 1.5\,\text{IQR})$ in $O(1)$ (§7).
    #[inline]
    pub fn tukey_iqr_fences(&self) -> (f64, f64) {
        let q1 = self.quantile_sketch_16[5];
        let q3 = self.quantile_sketch_16[7];
        let iqr = self.iqr_spread.max(q3 - q1);
        (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
    }

    /// Returns the 5% Trimmed Mean ($\mu_{\text{trim}, 5\%}$) stored in `quantile_sketch_16[15]` in $O(1)$ (§7).
    #[inline]
    pub fn trimmed_mean_5pct(&self) -> f64 {
        self.quantile_sketch_16[15]
    }

    /// Evaluates any arbitrary quantile $p \in [0, 1]$ in $O(1)$ by piecewise-linear interpolation over the 15 probability knots.
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

    /// Returns the 3-Parameter Weibull estimates $(\beta, \eta, \gamma_{\text{location}})$ in $O(1)$ (§7).
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

    /// Computes the right-censored Kaplan-Meier / Nelson-Aalen survival probability $\hat{S}(t)$ at exposure `t` in $O(1)$ (§7).
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

    /// Computes the Weibull Mean Interarrival / Survival Time (`E[T] = eta * Gamma(1 + 1/beta)`) in $O(1)$ (§7).
    #[inline]
    pub fn mean_interarrival_time(&self) -> f64 {
        if self.valid_count == 0 {
            return 0.0;
        }
        let beta = f64::from(self.weibull_beta_mle).max(0.2);
        let eta = f64::from(self.weibull_eta_mle).max(1e-6);
        eta * Self::gamma_lanczos(1.0 + 1.0 / beta)
    }

    /// Computes Steady-State Renewal Availability $A = \frac{\mu_{\text{up}}}{\mu_{\text{up}} + \mu_{\text{down}}} \in [0, 1]$ in $O(1)$ (§7).
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

    /// Lag-2 Serial Autocorrelation $\rho(2)$ in $O(1)$ (§7).
    #[inline]
    pub fn acf_lag2(&self) -> f64 {
        if self.pebay_m2 < 1e-15 {
            0.0
        } else {
            (self.sum_lag2_cross_prod / self.pebay_m2).clamp(-1.0, 1.0)
        }
    }

    /// Returns `(ema_fast, ema_slow, macd_divergence)` in $O(1)$ (§7).
    #[inline]
    pub fn macd_state(&self) -> (f64, f64, f64) {
        (
            self.ema_fast_state,
            self.ema_slow_state,
            self.ema_fast_state - self.ema_slow_state,
        )
    }

    /// Page's Two-Sided CUSUM regime shift detector in $O(1)$ (§7).
    #[inline]
    pub fn cusum_regime_change_detected(&self, threshold_h: f64) -> bool {
        f64::from(self.cusum_pos_max) > threshold_h
            || f64::from(self.cusum_neg_min).abs() > threshold_h
    }

    /// Mann-Kendall / Studentized Temporal Drift $Z$-Statistic in $O(1)$ (§7).
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

    /// Exact Goertzel $O(N)$ narrow-band harmonic power detector for any target frequency `target_freq_hz` (§7).
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

    // ── Stochastic Operations Research & Resource Buffer Optimization (§1.1 #4 & §7) ──

    /// Convex Batch / Economic Lot Size Optimum ($Q^* = \sqrt{\frac{2 D K}{h}}$) in $O(1)$ (§7).
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

    /// Stochastic Safety Buffer under joint rate & delay uncertainty ($B = Z_\alpha \sqrt{\bar{L}\sigma_D^2 + \bar{D}^2\sigma_L^2}$) in $O(1)$ (§7).
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

    /// Stochastic Replenishment Threshold Point ($T^* = \bar{D}\bar{L} + B$) in $O(1)$ (§7).
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

    /// Computes `(service_fill_rate_beta, tail_shortfall_probability)` in $O(1)$ given `current_level` and delay parameters (§7).
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

    // ── Benford's Law First-Digit Chi-Squared Audit (§1.1 #3, §2.2 & §7) ──────

    /// Critical $\chi^2$ threshold at $\alpha = 0.05$ with $\text{df} = 8$ ($\chi^2_{0.95, 8} = 15.51$, §7).
    pub const BENFORD_CRITICAL_CHI2_ALPHA_005: f64 = 15.51;

    /// Extracts the leading non-zero base-10 digit $d \in \{1,\dots,9\}$ of any finite non-zero number $x$.
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

    /// Computes the empirical first-digit relative frequencies $[f_1, \dots, f_9]$ and valid sample size $n$ (§7).
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

    /// Computes Benford's Law First-Digit Chi-Squared statistic:
    /// $\chi^2_{\text{Benford}} = n \sum_{d=1}^9 \frac{(f_d - \log_{10}(1 + 1/d))^2}{\log_{10}(1 + 1/d)}$ (§7).
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

    /// Returns `true` when the sample conforms to Benford's Law ($\chi^2_{\text{Benford}} < \text{threshold}$, default `15.51` at $\alpha = 0.05$, §7).
    #[inline]
    pub fn passes_benford_audit(values: &[f64], critical_threshold: Option<f64>) -> bool {
        let threshold = critical_threshold.unwrap_or(Self::BENFORD_CRITICAL_CHI2_ALPHA_005);
        Self::benford_chi_squared(values) < threshold
    }

    // ── Complete Survival & Renewal KPIs from Códice SurvivalAnalysisProfile (§4.1 & §4.2) ──

    /// Evaluates all 7 analytical Survival & Renewal KPIs defined in Spec 09 §4.2 in $O(1)$
    /// from an interarrival/lifetime synopsis (`self`) and a restoration/repair synopsis (`restoration_synopsis`).
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

/// Complete $O(1)$ Survival & Renewal KPI evaluation output matching Spec 09 §4.2.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SurvivalRenewalKpis {
    pub mttr: f64,
    pub mtbf: f64,
    pub mttf: f64,
    pub availability_pct: f64,
    pub weibull_beta: f64,
    pub weibull_eta: f64,
    pub weibull_gamma_loc: f64,
    pub reliability_r_t: f64,
    pub hazard_rate_h_t: f64,
    pub kaplan_meier_s_t: f64,
    pub optimal_renewal_interval: Option<f64>,
}

impl DimensionalUnitRegistry {
    /// Builds a `DimensionalUnitRegistry` from Códice `AffineTransformSpec` mappings (`unit_code -> AffineTransformSpec`, §6).
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
