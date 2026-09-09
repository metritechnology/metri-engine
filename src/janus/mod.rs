//! janus — the read path of the metri engine.
//!
//! janus/ — Read Path del Metri Engine.
//! Write Path → ver src/janus_router/

pub mod fbs;
pub mod normalizer;
pub mod router;
pub mod validator; // Read Path: CedarCtx, QueryChunk, run_query_pipeline

// FASE 3 — Compiladores del Read Path
pub mod abac_clauses;
pub mod aggregator;
pub mod ast_compiler;
pub mod batch_enricher;
pub mod filter_compiler;
pub mod multi_series;
pub mod plan_selector;

#[cfg(test)]
mod testing;
