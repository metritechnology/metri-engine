// janus/ — Read Path del Metri Engine.
// [PORTED_FROM: src/metri/janus/]
// Write Path → ver src/janus_router/

pub mod normalizer;
pub mod validator;
pub mod fbs;
pub mod router;         // Read Path: CedarCtx, QueryChunk, run_query_pipeline

// FASE 3 — Compiladores del Read Path
pub mod abac_clauses;
pub mod filter_compiler;
pub mod ast_compiler;
pub mod batch_enricher;
pub mod multi_series;
pub mod plan_selector;
pub mod aggregator;

#[cfg(test)]
pub mod contract_tests;
