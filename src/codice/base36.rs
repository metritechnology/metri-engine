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
#[path = "tests/base36_tests.rs"]
mod tests;
