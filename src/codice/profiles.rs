//! Perfiles declarativos matemáticos, de análisis de supervivencia, espectrales, de covarianza cruzada, geoespaciales y de búsqueda difusa del Códice.

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

/// Transformación afín declarativa $y_{\text{canonical}} = a \cdot x_{\text{raw}} + b$ hacia unidades canónicas SI.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub struct AffineTransformSpec {
    /// Factor de escala multiplicativo $a$ de la transformación afín.
    pub scale_a: f64,
    /// Desplazamiento aditivo $b$ de la transformación afín.
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

/// Familias de distribución probabilística pre-ajustables en `MbfHyperSynopsis512`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum DistributionFamily {
    /// Distribución normal gaussiana simétrica ($\mu, \sigma^2$).
    #[default]
    #[serde(
        rename = "gaussian",
        alias = "NORMAL",
        alias = "NORMAL_GAUSSIAN",
        alias = "normal_gaussian"
    )]
    NormalGaussian,
    /// Distribución de Weibull de 2 parámetros (forma $\beta$, escala $\eta$).
    #[serde(
        rename = "weibull",
        alias = "WEIBULL_2P",
        alias = "weibull_2p",
        alias = "WEIBULL"
    )]
    Weibull2P,
    /// Distribución de Weibull de 3 parámetros con umbral de vida libre de fallos $\gamma$.
    #[serde(rename = "weibull_3p", alias = "WEIBULL_3P")]
    Weibull3P,
    /// Distribución log-normal para tiempos de reparación y variables asimétricas positivas.
    #[serde(rename = "lognormal", alias = "LOGNORMAL", alias = "log_normal")]
    LogNormal,
    /// Distribución exponencial con tasa de riesgo constante $\lambda$.
    #[serde(rename = "exponential", alias = "EXPONENTIAL")]
    Exponential,
    /// Distribución discreta de Poisson para conteo de eventos raros por intervalo.
    #[serde(rename = "poisson", alias = "POISSON")]
    Poisson,
    /// Distribución Gamma para sumas de tiempos entre eventos exponenciales.
    #[serde(rename = "gamma", alias = "GAMMA")]
    Gamma,
    /// Distribución de Pareto de cola pesada para análisis ABC y concentración de costos.
    #[serde(rename = "pareto", alias = "PARETO")]
    Pareto,
}

/// Perfil de análisis espectral en frecuencia para flujos de series de tiempo.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SpectralProfile {
    /// Indica si se calcula la Transformada Rápida de Fourier (FFT) sobre ventanas de telemetría.
    #[serde(default = "default_true")]
    pub enable_fft: bool,
    /// Frecuencia de muestreo nominal de la señal en hercios (Hz).
    #[serde(default = "default_sample_rate_hz")]
    pub sample_rate_hz: f64,
    /// Número de bandas de densidad espectral de potencia (PSD) a sintetizar.
    #[serde(default = "default_psd_bands", alias = "num_psd_bands")]
    pub psd_bands: u8,
    /// Frecuencias objetivo específicas (Hz) monitoreadas mediante el algoritmo de Goertzel.
    #[serde(default)]
    pub goertzel_target_hz: Vec<f64>,
    /// Indica si se computan el valor eficaz (RMS) y el factor de cresta de la señal.
    #[serde(default = "default_true")]
    pub compute_rms_and_crest_factor: bool,
    /// Indica si se computa la entropía espectral de Shannon sobre el espectro de potencia.
    #[serde(default = "default_true")]
    pub compute_spectral_entropy: bool,
}

/// Perfil matemático y estadístico avanzado por atributo compilado en el Códice.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MathProfile {
    /// Nivel de estructura de sinopsis estadística en disco (ej. `"hyper_512b"`).
    #[serde(default = "default_synopsis_tier")]
    pub synopsis_tier: String,
    /// Familia de distribución probabilística primaria del atributo.
    #[serde(default)]
    pub distribution_family: DistributionFamily,
    /// Umbral en desviaciones estándar ($\sigma$) para detección de valores atípicos.
    #[serde(default = "default_outlier_sigma")]
    pub outlier_sigma_threshold: f64,
    /// Habilita el detector secuencial de cambio de régimen CUSUM tabular.
    #[serde(default = "default_true")]
    pub enable_cusum: bool,
    /// Parámetro de holgura ($k$) del acumulador CUSUM en unidades de desviación estándar.
    #[serde(default = "default_cusum_k")]
    pub cusum_slack_k: f64,
    /// Umbral de decisión ($h$) del acumulador CUSUM para disparar alarma de deriva.
    #[serde(default = "default_cusum_h")]
    pub cusum_threshold_h: f64,
    /// Habilita la auditoría antifraude de primer dígito según la Ley de Benford.
    #[serde(default)]
    pub enable_benford_audit: bool,
    /// Habilita la estimación de máxima verosimilitud (MLE) para ajuste de Weibull.
    #[serde(default)]
    pub enable_weibull_mle: bool,
    /// Nombre del atributo hermano que almacena el código de unidad de medida (UN/CEFACT).
    #[serde(default)]
    pub unit_field_ref: Option<String>,
    /// Especificación opcional de transformación afín hacia la unidad canónica del sistema.
    #[serde(default)]
    pub affine_to_canonical: Option<AffineTransformSpec>,
    /// Perfil espectral opcional para atributos de vibración o telemetría de alta frecuencia.
    #[serde(default, alias = "spectral_fft_bands")]
    pub spectral_profile: Option<SpectralProfile>,
    /// Representación numérica de precisión fija (ej. `"FixedDecimal128_Scale4"`).
    #[serde(default)]
    pub precision: Option<String>,
    /// Orden máximo de los momentos centrales de Welford/Pébay mantenidos (hasta orden 4).
    #[serde(default = "default_moments_order")]
    pub central_moments_order: u8,
    /// Indica si se mantiene un árbol de Fenwick (Binary Indexed Tree) de sumas prefijas.
    #[serde(default)]
    pub fenwick_prefix_sum: bool,
    /// Estructuras de bosquejo de cuantiles habilitadas (ej. KLL Sketch, DDSketch).
    #[serde(default)]
    pub quantiles: Vec<String>,
    /// Retardos ($k$) para el cálculo incremental de autocorrelación en series temporales.
    #[serde(default)]
    pub timeseries_lag_autocorrelation: Vec<u16>,
    /// Coeficientes de suavizamiento ($\alpha$) para medias móviles exponenciales (EMA).
    #[serde(default)]
    pub ema_alphas: Vec<f64>,
    /// Detectores robustos de anomalías habilitados (ej. Modified Z-Score MAD, IQR Tukey).
    #[serde(default)]
    pub outlier_detectors: Vec<String>,
    /// Lista de familias de distribución candidatas para ajuste simultáneo de bondad de ajuste.
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

/// Algoritmos fonéticos y de normalización léxica agnósticos de dominio en Zona 5B.
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

/// Perfil de búsqueda difusa (Fuzzy), fonética, léxica (BM25F) y semántica por atributo.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FuzzySearchProfile {
    /// Distancia máxima de edición Damerau-Levenshtein permitida en búsquedas SymSpell.
    #[serde(
        default = "default_symspell_distance",
        alias = "symspell_max_edit_distance"
    )]
    pub max_edit_distance: u8,
    /// Habilita la indexación de borrados simétricos (SymSpell) para corrección ortográfica $O(1)$.
    #[serde(default = "default_true")]
    pub enable_symspell: bool,
    /// Habilita el índice de trigramas de caracteres para similitud léxica y subcadenas.
    #[serde(default = "default_true", alias = "trigram_bitmap")]
    pub enable_trigrams: bool,
    /// Habilita el filtro de mapa de bits de bigramas de 64 bits para poda rápida de candidatos.
    #[serde(default = "default_true")]
    pub bigram_bitmap: bool,
    /// Habilita la compilación del léxico en un transductor de estados finitos (FST).
    #[serde(default = "default_true")]
    pub fst_lexicon: bool,
    /// Algoritmo fonético primario aplicado a los tokens del atributo.
    #[serde(default)]
    pub phonetic_algorithm: PhoneticAlgorithm,
    /// Tubería de algoritmos fonéticos secundarios aplicados en cascada.
    #[serde(default)]
    pub phonetic_algorithms: Vec<PhoneticAlgorithm>,
    /// Habilita la normalización de códigos técnicos e identificadores con resolución de homóglifos.
    #[serde(default = "default_true")]
    pub alphanumeric_code_normalizer: bool,
    /// URI opcional del diccionario terminológico de sinónimos (ej. OmniClass, UN/CEFACT).
    #[serde(default, alias = "synonym_terminology_uri")]
    pub synonym_dictionary_uri: Option<String>,
    /// Factor de ponderación de relevancia del campo en el cálculo de ranking BM25F.
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

/// Perfil de análisis de supervivencia, procesos de renovación y riesgo estocástico a nivel de entidad.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct SurvivalAnalysisProfile {
    /// Indica si el análisis de supervivencia y confiabilidad está activo para la entidad.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Nombre del atributo de referencia al sujeto o activo bajo estudio (ej. `"asset_id"`).
    #[serde(default, alias = "subject_ref_attr")]
    pub subject_ref_field: String,
    /// Valores de estado o categoría que representan un evento terminal (falla observada).
    #[serde(default)]
    pub terminal_event_values: Vec<String>,
    /// Valores de estado que representan observaciones censuradas por la derecha (preventivos, cierres sin falla).
    #[serde(default)]
    pub censored_event_values: Vec<String>,
    /// Atributo epoch que registra el instante de ocurrencia o inicio del evento.
    #[serde(default, alias = "timestamp_attr")]
    pub event_epoch_field: String,
    /// Atributo epoch opcional que registra la fecha/hora de restablecimiento del servicio.
    #[serde(default, alias = "completed_at_attr")]
    pub recovery_epoch_field: Option<String>,
    /// Atributo numérico que registra la duración de la restauración (MTTR / tiempo de parada).
    #[serde(default, alias = "duration_attr")]
    pub restoration_duration_field: String,
    /// Indica si se ajusta automáticamente un modelo de Weibull de 3 parámetros ($\beta, \eta, \gamma$).
    #[serde(default = "default_true")]
    pub fit_weibull_3p: bool,
    /// Habilita el estimador no paramétrico de Kaplan-Meier con soporte de datos censurados.
    #[serde(default = "default_true")]
    pub enable_kaplan_meier_censoring: bool,
    /// Filtro declarativo por atributos para restringir los registros que alimentan el modelo de confiabilidad.
    #[serde(default)]
    pub event_filter: HashMap<String, Vec<String>>,
    /// Familias de distribución probabilística evaluadas en el ajuste de supervivencia.
    #[serde(default)]
    pub distributions: Vec<DistributionFamily>,
}

/// Perfil de matriz de co-momentos multivariados en Zona 6B (`MbfCrossCovarianceMatrix`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct CrossCovarianceProfile {
    /// Indica si el mantenimiento incremental de la matriz de covarianza cruzada está habilitado.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Lista ordenada de atributos numéricos de medida cuyos co-momentos pareados se rastrean.
    #[serde(default, alias = "measure_attributes")]
    pub measure_fields: Vec<String>,
}

/// Perfil de inteligencia geoespacial Uber H3 + Morton Z-Order + R-Tree MBR en Zona 5B.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct SpatialProfile {
    /// Indica si la indexación geoespacial automática está activa para la entidad.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Nombre del atributo que contiene la latitud geográfica en grados decimales WGS84.
    #[serde(default, alias = "latitude_attr")]
    pub lat_field: String,
    /// Nombre del atributo que contiene la longitud geográfica en grados decimales WGS84.
    #[serde(default, alias = "longitude_attr")]
    pub lon_field: String,
    /// Resoluciones jerárquicas de celdas hexagonales Uber H3 precalculadas al indexar.
    #[serde(default)]
    pub h3_resolutions: Vec<u8>,
    /// Habilita la codificación de curva de llenado de espacio Morton Z-Order de 64 bits y MBR.
    #[serde(default = "default_true", alias = "rtree_bounding_box")]
    pub morton_z_order: bool,
}
