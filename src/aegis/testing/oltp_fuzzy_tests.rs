use crate::aegis::oltp::fuzzy::*;

#[test]
fn substring_match() {
    assert!(fuzzy_match("Chiller A-01", "Chiller"));
    assert!(fuzzy_match("Chiller A-01", "chiller"));
}

#[test]
fn fuzzy_one_error() {
    assert!(fuzzy_match("Chiller A-01", "Chiler")); // dl=1
    assert!(fuzzy_match("Rack Server", "Rak")); // dl=1 token 'rack'
}

#[test]
fn fuzzy_transposition() {
    // Damerau-Levenshtein soporta transposiciones (dl=1)
    assert!(fuzzy_match("Bomba", "Bobma"));
}

#[test]
fn no_match_short_term() {
    // threshold=0 para ≤2 chars, and it should NOT match fast-path.
    assert!(!fuzzy_match("UPS B-12", "xy"));
}
