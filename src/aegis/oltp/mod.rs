pub mod compiler;
pub mod executor;
pub mod channel;
pub mod aggregation;  // Agregación completa: 14 funciones + FilterNode + derive_columns
pub mod fuzzy;        // [PORTED_FROM: datalog/fuzzy.clj] Levenshtein + omnisearch
pub mod hierarchy;    // [PORTED_FROM: datalog/hierarchy.clj] inject_has_children (Modo 2 EAV)
pub mod comparison;   // [PORTED_FROM: datalog/comparison.clj] 5 tipos AnalyticalComparison
pub(crate) mod filter;
pub(crate) mod hydrator;
pub(crate) mod caster;
