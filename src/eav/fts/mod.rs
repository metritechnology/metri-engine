//! Subsistema de búsqueda de texto completo (FTS) — índice de trigramas, fonética romance y búsqueda híbrida en Zona 5B (`.mbf`).
/// Motor de búsqueda difusa, espacial y fonética sobre el corpus FTS y la Zona 5B.
pub mod searcher;
/// Generador de trigramas normalizados y claves fonéticas para indexación FTS.
pub mod trigram;
