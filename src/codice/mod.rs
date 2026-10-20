//! Códice — registro de única fuente de verdad (SSOT) de los modelos JSON en `config/models/`.
//!
//! Carga, valida y compila los 60 modelos declarativos en el arranque (fail-fast, ADR-005)
//! y provee motores de coerción de tipos, validación estructural/semántica y generación
//! de identificadores (secuencias atómicas ACID y códigos estocásticos Base36).
//!
//! # Garantías
//!
//! - El registro es inmutable tras el arranque (`OnceLock`), con resolución $O(1)$.
//! - Cualquier modelo inválido aborta el arranque: el motor nunca opera con esquemas parciales.
//!
//! # Submódulos
//!
//! - `registry` — `CodeRegistry`, descriptores de atributos, restricciones y canales de ejecución.
//! - `profiles` — perfiles matemáticos, espectrales, geoespaciales, de supervivencia y difusos.
//! - `coercion` — conversiones seguras de tipos escalares en payloads JSON.
//! - `validator` — validación estructural y semántica hacia `DatomValue`.
//! - `generator` — inyector de atributos autogenerados (secuencias y Base36).
//! - `sequence` — contadores secuenciales ACID con resolución jerárquica de ámbito.
//! - `base36` — generador de códigos alfanuméricos aleatorios no predecibles (CSPRNG).

/// Generador estocástico de códigos alfanuméricos Base36 mediante CSPRNG.
pub mod base36;
/// Motor centralizado de coerción segura de tipos escalares para payloads JSON.
pub mod coercion;
/// Inyector de atributos autogenerados (secuencias ACID y códigos Base36).
pub mod generator;
/// Perfiles declarativos matemáticos, espectrales, de supervivencia, geoespaciales y difusos.
pub mod profiles;
/// Registro en memoria (SSOT) de modelos de entidad, atributos y restricciones.
pub mod registry;
/// Generador de códigos secuenciales atómicos con herencia de ámbito.
pub mod sequence;

pub use registry::{global, init_global};
pub use registry::{
    AffineTransformSpec, AttrType, AttributeDescriptor, CodeRegistry, CrossCovarianceProfile,
    DistributionFamily, EngineChannel, EntityModel, FuzzySearchProfile, MathProfile,
    PhoneticAlgorithm, SpatialProfile, SpectralProfile, SurvivalAnalysisProfile,
};
/// Validador estructural y semántico de payloads JSON frente a modelos del Códice.
pub mod validator;
