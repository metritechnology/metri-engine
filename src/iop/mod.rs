pub mod pipeline;
pub mod error_response;
pub mod core;
pub mod sherlog;        // [PORTED_FROM: metri.iop.sherlog] — Fault Bus / Módulo V
pub mod cedar_step;    // [STUB FASE 4] IopStep wrapper para CedarAuthorizer
pub mod quota_step;    // [STUB FASE 4] IopStep wrapper para QuotaGuard
pub mod janus_step;    // [PORTED_FROM: ig/init-key :iop/janus-router] — Paso 3 del pipeline
