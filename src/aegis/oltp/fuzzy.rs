//! Coincidencia aproximada (`fuzzy matching`) para búsquedas Omnisearch y operador `MATCHES`.
//!
//! Pipeline de evaluación en cortocircuito:
//! 1. Subcadena exacta insensible a mayúsculas y tildes (vía rápida).
//! 2. Canonicalización de códigos técnicos alfanuméricos (preservando dígitos exactos).
//! 3. Puntuación de intersección de trigramas ($\ge 0,75$).
//! 4. Distancia de edición de Damerau-Levenshtein y clave fonética romance por token
//!    con umbral adaptativo según longitud (`<=2` -> `0`, `3..=8` -> `1`, `>=9` -> `2`).

use crate::eav::fts::searcher::{canonicalize_alphanumeric_code, damerau_levenshtein};
use crate::eav::fts::trigram::generate_trigrams;
use crate::mbf::zones::hybrid_search::MbfHybridSearchIndex;
use std::collections::HashSet;

/// Threshold adaptativo por longitud del término.
pub fn fuzzy_threshold(term: &str) -> usize {
    match term.chars().count() {
        0..=2 => 0,
        3..=8 => 1,
        _ => 2,
    }
}

/// Remueve los acentos diacríticos del español para búsquedas insensibles a acentos.
pub fn remove_accents(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'á' | 'Á' => 'a',
            'é' | 'É' => 'e',
            'í' | 'Í' => 'i',
            'ó' | 'Ó' => 'o',
            'ú' | 'Ú' => 'u',
            'ü' | 'Ü' => 'u',
            'ñ' | 'Ñ' => 'n',
            other => other,
        })
        .collect()
}

/// Tokeniza un valor en palabras (lowercase y sin acentos) separadas por espacios, guiones, puntos, guiones bajos.
/// "Chiller A-01" → ["chiller", "a", "01"]
fn tokenize(s: &str) -> Vec<String> {
    remove_accents(&s.to_lowercase())
        .split(|c: char| c.is_whitespace() || c == '-' || c == '_' || c == '.' || c == '#')
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

fn fuzzy_match_single_term(
    value: &str,
    v_lower: &str,
    val_tokens: &[String],
    t_lower: &str,
) -> bool {
    if t_lower.is_empty() {
        return false;
    }
    if v_lower.contains(t_lower) {
        return true;
    }

    let has_digit = t_lower.chars().any(|c| c.is_ascii_digit());
    if t_lower.chars().count() >= 2 {
        let q_code = canonicalize_alphanumeric_code(t_lower);
        if q_code.len() >= 2 {
            let v_code = canonicalize_alphanumeric_code(value);
            if v_code.contains(&q_code) {
                return true;
            }
        }
    }
    // Si el término contiene dígitos (ej. "001", "SN-2026-001", "A-TU77MXU"),
    // sólo debe coincidir por substring o código alfanumérico canónico (homóglifos O->0, I/L->1),
    // nunca sustituyendo dígitos distintos ("001" != "002" ni "011").
    if has_digit {
        return false;
    }

    let term_trigrams = generate_trigrams(t_lower);
    let val_trigrams = generate_trigrams(v_lower);

    if !term_trigrams.is_empty() && !val_trigrams.is_empty() {
        let val_set: HashSet<_> = val_trigrams.iter().collect();
        let matched = term_trigrams.iter().filter(|t| val_set.contains(t)).count();
        let score = matched as f32 / term_trigrams.len() as f32;
        if score >= 0.75 {
            return true;
        }
    }

    let thresh = fuzzy_threshold(t_lower);
    if thresh == 0 {
        return false;
    }

    let q_phon = if t_lower.chars().count() >= 4 {
        MbfHybridSearchIndex::encode_romance_phonetic(t_lower)
    } else {
        String::new()
    };

    val_tokens.iter().any(|token| {
        let dl = damerau_levenshtein(token, t_lower);
        if dl <= thresh {
            return true;
        }
        if !q_phon.is_empty()
            && dl <= 2
            && MbfHybridSearchIndex::encode_romance_phonetic(token) == q_phon
        {
            return true;
        }
        false
    })
}

/// True si `value` contiene `term` de forma aproximada, aprovechando capacidades técnicas EAV.
///
/// Pipeline (short-circuit):
///   1. Guard: vacío → false
///   2. Fast path: substring case-insensitive y sin acentos
///   3. Alphanumeric code normalizer (Spec 09 §5.3.B)
///   4. Fast Fuzzy path: Trigram intersection score (>= 0.75 de coincidencia)
///   5. Deep Fuzzy & Phonetic path: Damerau-Levenshtein + Romance Phonetic por token y multi-palabra
pub fn fuzzy_match(value: &str, term: &str) -> bool {
    let term_trimmed = term.trim();
    if value.is_empty() || term_trimmed.is_empty() {
        return false;
    }

    let v_lower = remove_accents(&value.to_lowercase());
    let t_lower = remove_accents(&term_trimmed.to_lowercase());

    // Fast path
    if v_lower.contains(&t_lower) {
        return true;
    }

    let val_tokens = tokenize(value);
    if fuzzy_match_single_term(value, &v_lower, &val_tokens, &t_lower) {
        return true;
    }

    let query_words: Vec<&str> = t_lower
        .split_whitespace()
        .filter(|w| !w.is_empty())
        .collect();
    if query_words.len() > 1 {
        return query_words
            .iter()
            .all(|qw| fuzzy_match_single_term(value, &v_lower, &val_tokens, qw));
    }

    false
}
