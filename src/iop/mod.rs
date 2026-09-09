//! iop — the step-based ingest pipeline: Cedar, then Quota, then Janus.
//!
//! [`pipeline`] es el motor Railway (cortocircuito al primer error) y
//! [`core`] el orquestador raíz; cada paso ([`cedar_step`], [`quota_step`],
//! [`janus_step`]) tiene una única responsabilidad. [`error_response`]
//! traduce errores de dominio a respuestas gRPC forenses y [`sherlog`]
//! reporta fallas al canal EDA.
pub mod cedar_step; // [STUB FASE 4] IopStep wrapper para CedarAuthorizer
pub mod core;
pub mod error_response;
pub mod janus_step;
pub mod pipeline;
pub mod quota_step; // [STUB FASE 4] IopStep wrapper para QuotaGuard
pub mod sherlog; // — Fault Bus / Módulo V // — Paso 3 del pipeline
