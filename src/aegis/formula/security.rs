// aegis/formula/security.rs — Validación de seguridad para fórmulas.
// SRP: Solo valida. No parsea, no evalúa, no compila.

use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use regex::Regex;
use once_cell::sync::Lazy;

/// Palabras clave SQL prohibidas en fórmulas (prevención de inyección).
/// Se validan con word-boundary regex (`\bKEYWORD\b`) para evitar falsos positivos.
const SQL_KEYWORD_BLACKLIST: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "CREATE", "EXEC", "EXECUTE", "UNION", "FROM", "WHERE",
    "JOIN", "INTO", "GRANT", "REVOKE", "TRUNCATE",
];

/// Patrones literales (NO word-boundary) — siempre peligrosos.
const SQL_LITERAL_BLACKLIST: &[&str] = &[
    "--", "/*", "*/", ";",
];

/// Regex pre-compilada para word-boundary matching de SQL keywords.
static SQL_KEYWORD_RE: Lazy<Regex> = Lazy::new(|| {
    let pattern = SQL_KEYWORD_BLACKLIST
        .iter()
        .map(|kw| format!(r"\b{}\b", kw))
        .collect::<Vec<_>>()
        .join("|");
    Regex::new(&pattern).unwrap()
});

/// Valida que la fórmula no contenga fragmentos SQL peligrosos.
///
/// Usa **word-boundary matching** (`\bSELECT\b`) para keywords SQL,
/// evitando falsos positivos en atributos como `selection_count` o `updated_from`.
/// Los patrones literales (`--`, `/*`, `*/`, `;`) se buscan por substring directo.
pub fn validate_formula(formula: &str) -> Result<(), FormulaError> {
    let upper = formula.to_uppercase();

    // 1. Word-boundary matching para SQL keywords
    if let Some(m) = SQL_KEYWORD_RE.find(&upper) {
        return Err(FormulaError::SqlInjectionDetected {
            fragment: m.as_str().to_string(),
        });
    }

    // 2. Substring matching para patrones literales (siempre peligrosos)
    for pattern in SQL_LITERAL_BLACKLIST {
        if upper.contains(pattern) {
            return Err(FormulaError::SqlInjectionDetected {
                fragment: pattern.to_string(),
            });
        }
    }

    Ok(())
}

/// Valida que todas las funciones invocadas estén registradas.
pub fn validate_functions(formula: &str, registry: &FunctionRegistry) -> Result<(), FormulaError> {
    let re = Regex::new(r"([A-Z_]\w*)\s*\(").unwrap();
    for caps in re.captures_iter(&formula.to_uppercase()) {
        let fn_name = &caps[1];
        if !registry.is_registered(fn_name) {
            return Err(FormulaError::UnknownFunction {
                name: fn_name.to_string(),
            });
        }
    }
    Ok(())
}
