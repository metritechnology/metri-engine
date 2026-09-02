// [PORTED_FROM: src/metri/janus_router/ulid.clj]
// janus/ulid.rs — Generador ULID monotónico.
// Spec: https://github.com/ulid/spec
//
// Formato: ttttttttttrrrrrrrrrrrrrrrrr (26 chars Crockford Base32)
//   ├─ 48 bits timestamp (ms epoch) — lexicográficamente ordenable
//   └─ 80 bits random — unicidad global
//
// Usamos Ulid::new() con un Mutex global para garantía de monotonía:
// si el nuevo ULID tiene el mismo ms que el anterior, incrementamos
// manualmente — equivalente a UlidCreator/getMonotonicUlid() de JVM.

use std::sync::Mutex;

use once_cell::sync::Lazy;
use tracing::warn;
use ulid::Ulid;

// Último ULID generado — para garantía de orden monotónico.
// [PORTED_FROM: UlidCreator/getMonotonicUlid() — JVM thread-safe via internal counter]
static LAST_ULID: Lazy<Mutex<Option<Ulid>>> = Lazy::new(|| Mutex::new(None));

// Alfabeto Crockford Base32 para validación.
// [PORTED_FROM: (def ^:private ^String CROCKFORD "0123456789ABCDEFGHJKMNPQRSTVWXYZ")]
const CROCKFORD: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Genera un ULID monotónico de 26 caracteres Crockford Base32.
/// Thread-safe. Nunca lanza.
///
/// [PORTED_FROM: (defn generate ^String [] (-> (UlidCreator/getMonotonicUlid) (.toString)))]
pub fn generate() -> String {
    let mut last = LAST_ULID.lock().unwrap_or_else(|e| {
        warn!("[ULID] Mutex poisoned — reiniciando estado monotónico");
        e.into_inner()
    });

    let candidate = Ulid::new();

    // Garantía monotónica: si el candidato es <= al último, incrementamos.
    // [PORTED_FROM: monotonía del UlidCreator JVM dentro del mismo ms]
    let next = match *last {
        Some(prev) if candidate <= prev => {
            // Mismo ms o colisión — incrementar bits random del previo.
            prev.increment().unwrap_or_else(|| {
                warn!("[ULID] Overflow monotónico — emitiendo ULID fresco");
                Ulid::new()
            })
        }
        _ => candidate,
    };

    *last = Some(next);
    next.to_string()
}

/// Retorna true si `s` es un ULID válido (26 chars Crockford Base32).
///
/// [PORTED_FROM: (defn ulid? [s] (and (string? s) (= 26 (count s)) (every? ...)))]
pub fn is_ulid(s: &str) -> bool {
    s.len() == 26
        && s.chars()
            .all(|c| CROCKFORD.contains(c.to_ascii_uppercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_26_chars() {
        let id = generate();
        assert_eq!(id.len(), 26, "ULID debe tener 26 caracteres: {id}");
    }

    #[test]
    fn validates_valid_ulid() {
        let id = generate();
        assert!(is_ulid(&id), "ULID generado debe ser válido: {id}");
    }

    #[test]
    fn rejects_invalid() {
        assert!(!is_ulid("short"));
        assert!(!is_ulid("01HQZK7GXFM5AZBVDNWEJTR4!!"));
    }

    #[test]
    fn monotonic_ordering() {
        let ids: Vec<String> = (0..100).map(|_| generate()).collect();
        for w in ids.windows(2) {
            assert!(w[0] <= w[1], "ULID monotónico violado: {} > {}", w[0], w[1]);
        }
    }
}
