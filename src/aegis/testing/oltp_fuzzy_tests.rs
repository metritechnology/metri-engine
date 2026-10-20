//! Pruebas unitarias de coincidencia difusa, fonética y códigos alfanuméricos (`aegis::oltp::fuzzy`).
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

#[test]
fn fuzzy_accent_phonetic_code_and_multitoken() {
    assert!(fuzzy_match(
        "Bomba Centrífuga Multietapa #001",
        "Centrifuga"
    ));
    assert!(fuzzy_match("Bomba Centrífuga Multietapa #001", "Bomba 001"));
    assert!(!fuzzy_match(
        "Bomba Centrífuga Multietapa #011",
        "Bomba 001"
    ));
    assert!(fuzzy_match("Válvula de Control Neumática #004", "balbula"));
    assert!(fuzzy_match("SN-2026-001", "SN2026001"));
    assert!(fuzzy_match("SN-2026-001", "SN-2026-O01"));
    assert!(!fuzzy_match("SN-2026-002", "SN-2026-001"));
    assert!(fuzzy_match("A-TU77MXU", "ATU77MXU"));
    assert!(fuzzy_match(
        "Variador de Frecuencia Danfoss #100",
        "Variador Danfoss"
    ));
}
