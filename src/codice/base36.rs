// [PORTED_FROM: src/metri/codice/base36.clj]
// codice/base36.rs — Generador estocástico Base36.
// En Clojure: SecureRandom singleton + repeated lazy-seq.
// En Rust:    rand::thread_rng() (CSPRNG equivalente a SecureRandom).
//
// Zero-Drop Policy: mismas garantías de unicidad y no-predictibilidad.
// P(colisión con length=7) < 1/36^7 ≈ 1 en 78.000 millones.

use rand::Rng;

/// Alfabeto canónico Base36 — 10 dígitos + 26 letras mayúsculas.
/// [PORTED_FROM: (def ALPHABET "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")]
const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Genera un código alfanumérico Base36 de longitud `length` con prefijo `prefix`.
///
/// Ejemplo: generate("A-", 7) → "A-X7K2M9P"
///
/// Garantías:
///   ✔ Función pura (excepto CSPRNG — efectuado localmente, sin I/O externo)
///   ✔ Thread-safe — thread_rng() usa un RNG por thread (más rápido que Mutex<SecureRandom>)
///   ✔ Unicidad probabilística: P(colisión length=7) < 1/36^7
///   ✔ No predecible — ChaCha20 CSPRNG
///
/// [PORTED_FROM: (generate [prefix length])]
pub fn generate(prefix: &str, length: usize) -> String {
    let mut rng = rand::thread_rng();
    let mut result = String::with_capacity(prefix.len() + length);
    result.push_str(prefix);
    for _ in 0..length {
        let idx = rng.gen_range(0..ALPHABET.len());
        result.push(ALPHABET[idx] as char);
    }
    result
}

#[cfg(test)]
mod tests {
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
}
