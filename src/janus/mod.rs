//! Subsistema `janus` — Read Path analítico y transaccional de `metri-engine`.
//!
//! Orquesta la normalización, validación, enriquecimiento batch/cross-filter,
//! compilación de AST IR (JSON y FlatBuffers), selección de plan EAV, caché
//! read-through y ensamblaje de resultados (`OutputCast` y fusión multiserie).

pub mod cache;
pub mod fbs;
pub mod normalizer;
pub mod router;
pub mod validator;

// Compiladores y agregadores del Read Path
pub mod abac_clauses;
pub mod aggregator;
pub mod ast_compiler;
pub mod batch_enricher;
pub mod filter_compiler;
pub mod multi_series;
pub mod plan_selector;

#[cfg(test)]
mod testing;
