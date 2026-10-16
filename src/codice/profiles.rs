//! Declarative mathematical, survival analysis, spectral, cross-covariance, spatial, and fuzzy search profiles for the Códice (`09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md`).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn default_moments_order() -> u8 {
    4
}

fn default_true() -> bool {
    true
}

fn default_symspell_distance() -> u8 {
    2
}

fn default_bm25_weight() -> f32 {
    1.0
}

fn default_psd_bands() -> u8 {
    8
}

fn default_sample_rate_hz() -> f64 {
    100.0
}

fn default_synopsis_tier() -> String {
    "hyper_512b".to_string()
}

fn default_outlier_sigma() -> f64 {
    3.5
}

fn default_cusum_k() -> f64 {
    0.5
}

fn default_cusum_h() -> f64 {
    4.0
}

/// Declarative affine transformation $y_{\text{canonical}} = a \cdot x_{\text{raw}} + b$ (§6).
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub struct AffineTransformSpec {
    pub scale_a: f64,
    pub offset_b: f64,
}

impl Default for AffineTransformSpec {
    fn default() -> Self {
        Self {
            scale_a: 1.0,
            offset_b: 0.0,
        }
    }
}

/// Familias de distribución probabilística pre-ajustables en `MbfHyperSynopsis512` (§2.1 & §2.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum DistributionFamily {
    #[default]
    #[serde(
        rename = "gaussian",
        alias = "NORMAL",
        alias = "NORMAL_GAUSSIAN",
        alias = "normal_gaussian"
    )]
    NormalGaussian,
    #[serde(
        rename = "weibull",
        alias = "WEIBULL_2P",
        alias = "weibull_2p",
        alias = "WEIBULL"
    )]
    Weibull2P,
    #[serde(rename = "weibull_3p", alias = "WEIBULL_3P")]
    Weibull3P,
    #[serde(rename = "lognormal", alias = "LOGNORMAL", alias = "log_normal")]
    LogNormal,
    #[serde(rename = "exponential", alias = "EXPONENTIAL")]
    Exponential,
    #[serde(rename = "poisson", alias = "POISSON")]
    Poisson,
    #[serde(rename = "gamma", alias = "GAMMA")]
    Gamma,
    #[serde(rename = "pareto", alias = "PARETO")]
    Pareto,
}

/// Perfil de análisis espectral en frecuencia para flujos de series de tiempo (§2.1 & §2.2).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SpectralProfile {
    #[serde(default = "default_true")]
    pub enable_fft: bool,
    #[serde(default = "default_sample_rate_hz")]
    pub sample_rate_hz: f64,
    #[serde(default = "default_psd_bands", alias = "num_psd_bands")]
    pub psd_bands: u8,
    #[serde(default)]
    pub goertzel_target_hz: Vec<f64>,
    #[serde(default = "default_true")]
    pub compute_rms_and_crest_factor: bool,
    #[serde(default = "default_true")]
    pub compute_spectral_entropy: bool,
}

/// Perfil matemático y estadístico avanzado por atributo compilado en el Códice (§2.1 & §2.2).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MathProfile {
    #[serde(default = "default_synopsis_tier")]
    pub synopsis_tier: String,
    #[serde(default)]
    pub distribution_family: DistributionFamily,
    #[serde(default = "default_outlier_sigma")]
    pub outlier_sigma_threshold: f64,
    #[serde(default = "default_true")]
    pub enable_cusum: bool,
    #[serde(default = "default_cusum_k")]
    pub cusum_slack_k: f64,
    #[serde(default = "default_cusum_h")]
    pub cusum_threshold_h: f64,
    #[serde(default)]
    pub enable_benford_audit: bool,
    #[serde(default)]
    pub enable_weibull_mle: bool,
    #[serde(default)]
    pub unit_field_ref: Option<String>,
    #[serde(default)]
    pub affine_to_canonical: Option<AffineTransformSpec>,
    #[serde(default, alias = "spectral_fft_bands")]
    pub spectral_profile: Option<SpectralProfile>,
    #[serde(default)]
    pub precision: Option<String>,
    #[serde(default = "default_moments_order")]
    pub central_moments_order: u8,
    #[serde(default)]
    pub fenwick_prefix_sum: bool,
    #[serde(default)]
    pub quantiles: Vec<String>,
    #[serde(default)]
    pub timeseries_lag_autocorrelation: Vec<u16>,
    #[serde(default)]
    pub ema_alphas: Vec<f64>,
    #[serde(default)]
    pub outlier_detectors: Vec<String>,
    #[serde(default)]
    pub distribution_fits: Vec<DistributionFamily>,
}

impl Default for MathProfile {
    fn default() -> Self {
        Self {
            synopsis_tier: default_synopsis_tier(),
            distribution_family: DistributionFamily::NormalGaussian,
            outlier_sigma_threshold: default_outlier_sigma(),
            enable_cusum: true,
            cusum_slack_k: default_cusum_k(),
            cusum_threshold_h: default_cusum_h(),
            enable_benford_audit: false,
            enable_weibull_mle: false,
            unit_field_ref: None,
            affine_to_canonical: None,
            spectral_profile: None,
            precision: Some("FixedDecimal128_Scale4".to_string()),
            central_moments_order: 4,
            fenwick_prefix_sum: false,
            quantiles: vec![
                "KLL_SKETCH_256".to_string(),
                "DDSKETCH_EXTREME_TAIL".to_string(),
            ],
            timeseries_lag_autocorrelation: vec![1, 2],
            ema_alphas: vec![0.05, 0.20, 0.50],
            outlier_detectors: vec!["MODIFIED_Z_SCORE_MAD".to_string(), "IQR_TUKEY".to_string()],
            distribution_fits: vec![DistributionFamily::NormalGaussian],
        }
    }
}

/// Algoritmos fonéticos y de normalización léxica agnósticos de dominio en Zona 5B (§2.1 & §2.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum PhoneticAlgorithm {
    /// Codificador fonético romance (b==v, c/s/z, ll/y, h muda, ch, ñ).
    #[default]
    #[serde(
        rename = "romance_phonetic",
        alias = "ROMANCE_PHONETIC",
        alias = "SPANISH_METAPHONE"
    )]
    RomancePhonetic,
    /// Double Metaphone (Lawrence Philips) para términos anglosajones/germánicos.
    #[serde(rename = "double_metaphone", alias = "DOUBLE_METAPHONE")]
    DoubleMetaphone,
    /// Normalización de códigos alfanuméricos con resolución de homóglifos ópticos (`O`<->`0`, `I`/`l`<->`1`).
    #[serde(
        rename = "alphanumeric_code_normalizer",
        alias = "ALPHANUMERIC_CODE_NORMALIZER"
    )]
    AlphanumericCodeNormalizer,
    /// Sin expansión fonética (para códigos estrictos).
    #[serde(rename = "none", alias = "NONE")]
    None,
}

/// Perfil de búsqueda difusa (Fuzzy), fonética, léxica (BM25F) y semántica por atributo (§2.1 & §2.2).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FuzzySearchProfile {
    #[serde(
        default = "default_symspell_distance",
        alias = "symspell_max_edit_distance"
    )]
    pub max_edit_distance: u8,
    #[serde(default = "default_true")]
    pub enable_symspell: bool,
    #[serde(default = "default_true", alias = "trigram_bitmap")]
    pub enable_trigrams: bool,
    #[serde(default = "default_true")]
    pub bigram_bitmap: bool,
    #[serde(default = "default_true")]
    pub fst_lexicon: bool,
    #[serde(default)]
    pub phonetic_algorithm: PhoneticAlgorithm,
    #[serde(default)]
    pub phonetic_algorithms: Vec<PhoneticAlgorithm>,
    #[serde(default = "default_true")]
    pub alphanumeric_code_normalizer: bool,
    #[serde(default, alias = "synonym_terminology_uri")]
    pub synonym_dictionary_uri: Option<String>,
    #[serde(default = "default_bm25_weight", alias = "bm25f_weight")]
    pub bm25_field_boost: f32,
}

impl Default for FuzzySearchProfile {
    fn default() -> Self {
        Self {
            max_edit_distance: 2,
            enable_symspell: true,
            enable_trigrams: true,
            bigram_bitmap: true,
            fst_lexicon: true,
            phonetic_algorithm: PhoneticAlgorithm::RomancePhonetic,
            phonetic_algorithms: vec![
                PhoneticAlgorithm::RomancePhonetic,
                PhoneticAlgorithm::DoubleMetaphone,
                PhoneticAlgorithm::AlphanumericCodeNormalizer,
            ],
            alphanumeric_code_normalizer: true,
            synonym_dictionary_uri: None,
            bm25_field_boost: 1.0,
        }
    }
}

/// Perfil de análisis de supervivencia, procesos de renovación y riesgo estocástico a nivel de entidad (§2.1 & §2.2).
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct SurvivalAnalysisProfile {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, alias = "subject_ref_attr")]
    pub subject_ref_field: String,
    #[serde(default)]
    pub terminal_event_values: Vec<String>,
    #[serde(default)]
    pub censored_event_values: Vec<String>,
    #[serde(default, alias = "timestamp_attr")]
    pub event_epoch_field: String,
    #[serde(default, alias = "completed_at_attr")]
    pub recovery_epoch_field: Option<String>,
    #[serde(default, alias = "duration_attr")]
    pub restoration_duration_field: String,
    #[serde(default = "default_true")]
    pub fit_weibull_3p: bool,
    #[serde(default = "default_true")]
    pub enable_kaplan_meier_censoring: bool,
    #[serde(default)]
    pub event_filter: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub distributions: Vec<DistributionFamily>,
}

/// Perfil de matriz de co-momentos multivariados en Zona 6B (`MbfCrossCovarianceMatrix`, §2.1 & §4.3).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct CrossCovarianceProfile {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, alias = "measure_attributes")]
    pub measure_fields: Vec<String>,
}

/// Perfil de inteligencia geoespacial Uber H3 + Morton Z-Order + R-Tree MBR en Zona 5B (§2.1 & §5.1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct SpatialProfile {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, alias = "latitude_attr")]
    pub lat_field: String,
    #[serde(default, alias = "longitude_attr")]
    pub lon_field: String,
    #[serde(default)]
    pub h3_resolutions: Vec<u8>,
    #[serde(default = "default_true", alias = "rtree_bounding_box")]
    pub morton_z_order: bool,
}
