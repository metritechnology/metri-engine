// [PORTED_FROM: src/metri/aegis/datalog/fuzzy.clj]
// aegis/oltp/fuzzy.rs — Fuzzy matching para Omnisearch (operador MATCHES).
//
// Algoritmos (puros, sin dependencias externas):
//   1. Levenshtein-Wagner-Fischer O(m·n) rolling-row O(n)
//   2. Substring case-insensitive (fast path)
//   3. Word-level tokenization
//
// Threshold adaptativo:
//   ≤ 2 chars → 0 (solo exacto)
//   3-8 chars → 1 (1 error tipográfico)
//   ≥ 9 chars → 2 (2 errores)

/// Distancia de edición mínima Levenshtein entre `a` y `b`.
/// Usa rolling-row para O(n) espacio.
///
/// [PORTED_FROM: (levenshtein-distance a b)]
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let la = a_chars.len();
    let lb = b_chars.len();

    if la == 0 { return lb; }
    if lb == 0 { return la; }
    if a == b  { return 0; }

    let mut prev: Vec<usize> = (0..=lb).collect();

    for i in 0..la {
        let mut curr = vec![0usize; lb + 1];
        curr[0] = i + 1;
        for j in 0..lb {
            let cost = if a_chars[i] == b_chars[j] { 0 } else { 1 };
            curr[j + 1] = (curr[j] + 1)
                .min(prev[j + 1] + 1)
                .min(prev[j] + cost);
        }
        prev = curr;
    }
    prev[lb]
}

/// Threshold adaptativo por longitud del término.
///
/// [PORTED_FROM: (fuzzy-threshold term)]
pub fn fuzzy_threshold(term: &str) -> usize {
    match term.chars().count() {
        0..=2 => 0,
        3..=8 => 1,
        _     => 2,
    }
}

/// Tokeniza un valor en palabras (lowercase) separadas por espacios, guiones, puntos, guiones bajos.
/// "Chiller A-01" → ["chiller", "a", "01"]
///
/// [PORTED_FROM: (tokenize s)]
fn tokenize(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| c.is_whitespace() || c == '-' || c == '_' || c == '.')
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

/// True si `value` contiene `term` de forma aproximada.
///
/// Pipeline (short-circuit):
///   1. Guard: vacío → false
///   2. Fast path: substring case-insensitive
///   3. Fuzzy path: algún token ≤ levenshtein threshold
///
/// [PORTED_FROM: (fuzzy-match? value term)]
pub fn fuzzy_match(value: &str, term: &str) -> bool {
    if value.is_empty() || term.is_empty() { return false; }

    let v_lower = value.to_lowercase();
    let t_lower = term.to_lowercase();

    // Fast path
    if v_lower.contains(&t_lower) { return true; }

    // Fuzzy path
    let thresh = fuzzy_threshold(&t_lower);
    if thresh == 0 { return false; }

    tokenize(value)
        .iter()
        .any(|token| levenshtein(token, &t_lower) <= thresh)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_match() {
        assert!(fuzzy_match("Chiller A-01", "Chiller"));
        assert!(fuzzy_match("Chiller A-01", "chiller"));
    }

    #[test]
    fn fuzzy_one_error() {
        assert!(fuzzy_match("Chiller A-01", "Chiler")); // lev=1
        assert!(fuzzy_match("Rack Server", "Rak"));     // lev=1 token 'rack'
    }

    #[test]
    fn no_match_short_term() {
        // threshold=0 para ≤2 chars, and it should NOT match fast-path.
        assert!(!fuzzy_match("UPS B-12", "xy"));
    }

    #[test]
    fn levenshtein_basic() {
        assert_eq!(levenshtein("rack", "rak"), 1);
        assert_eq!(levenshtein("chiller", "chiler"), 1);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("", "abc"), 3);
    }
}
