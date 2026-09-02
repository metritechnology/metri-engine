pub mod aggregation; // Agregación completa: 14 funciones + FilterNode + derive_columns
pub(crate) mod caster;
pub mod channel;
pub mod comparison; // 5 tipos AnalyticalComparison
pub mod compiler;
pub mod executor;
pub(crate) mod filter;
pub mod fuzzy; // Levenshtein + omnisearch
pub mod hierarchy; // inject_has_children (Modo 2 EAV)
pub(crate) mod hydrator;
