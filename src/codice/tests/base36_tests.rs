use super::*;
use std::collections::HashSet;

#[test]
fn generate_has_correct_length() {
    let code = generate("A-", 7);
    assert!(code.starts_with("A-"));
    assert_eq!(code.len(), 9); // 2 prefix + 7 chars
}

#[test]
fn generate_is_unique() {
    // Generar 1000 códigos y verificar que no hay colisiones
    let codes: HashSet<String> = (0..1000).map(|_| generate("T-", 7)).collect();
    assert_eq!(codes.len(), 1000, "Se esperaban 1000 códigos únicos");
}

#[test]
fn generate_uses_only_base36_chars() {
    let code = generate("", 20);
    for ch in code.chars() {
        assert!(
            ch.is_ascii_alphanumeric() && (ch.is_numeric() || ch.is_uppercase()),
            "Char inválido: {ch}"
        );
    }
}
