// aegis/oltp/fuzzy.rs — Fuzzy matching para Omnisearch (operador MATCHES).
//
// Algoritmos (aprovechando EAV FTS):
//   1. Substring case-insensitive (fast path)
//   2. Trigram Intersection Score (fast fuzzy path)
//   3. Damerau-Levenshtein distance O(m·n) (deep fuzzy path)
//   4. Word-level tokenization
//
// Threshold adaptativo:
//   ≤ 2 chars → 0 (solo exacto)
//   3-8 chars → 1 (1 error tipográfico)
//   ≥ 9 chars → 2 (2 errores)

use crate::eav::fts::searcher::damerau_levenshtein;
use crate::eav::fts::trigram::generate_trigrams;
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
        .split(|c: char| c.is_whitespace() || c == '-' || c == '_' || c == '.')
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

/// True si `value` contiene `term` de forma aproximada, aprovechando capacidades técnicas EAV.
///
/// Pipeline (short-circuit):
///   1. Guard: vacío → false
///   2. Fast path: substring case-insensitive y sin acentos
///   3. Fast Fuzzy path: Trigram intersection score (>= 0.75 de coincidencia)
///   4. Deep Fuzzy path: Damerau-Levenshtein distance <= threshold por token
pub fn fuzzy_match(value: &str, term: &str) -> bool {
    if value.is_empty() || term.is_empty() {
        return false;
    }

    let v_lower = remove_accents(&value.to_lowercase());
    let t_lower = remove_accents(&term.to_lowercase());

    // Fast path
    if v_lower.contains(&t_lower) {
        return true;
    }

    // Fast Fuzzy Path via Trigrams (EAV FTS Capability)
    let term_trigrams = generate_trigrams(&t_lower);
    let val_trigrams = generate_trigrams(&v_lower);

    if !term_trigrams.is_empty() && !val_trigrams.is_empty() {
        let val_set: HashSet<_> = val_trigrams.iter().collect();
        let matched = term_trigrams.iter().filter(|t| val_set.contains(t)).count();
        let score = matched as f32 / term_trigrams.len() as f32;
        if score >= 0.75 {
            return true;
        }
    }

    // Deep Fuzzy path via Damerau-Levenshtein (EAV FTS Capability)
    let thresh = fuzzy_threshold(&t_lower);
    if thresh == 0 {
        return false;
    }

    tokenize(value)
        .iter()
        .any(|token| damerau_levenshtein(token, &t_lower) <= thresh)
}
