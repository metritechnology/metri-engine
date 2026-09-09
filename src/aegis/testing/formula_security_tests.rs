//! Tests for `aegis::formula::security`.
use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::security::{validate_formula, validate_functions};

#[test]
fn test_security_blacklist_keywords() {
    // Exact word boundaries should fail
    assert!(matches!(
        validate_formula("SELECT * FROM users").unwrap_err(),
        FormulaError::SqlInjectionDetected { .. }
    ));

    assert!(matches!(
        validate_formula("a + b; DROP TABLE users").unwrap_err(),
        FormulaError::SqlInjectionDetected { .. }
    ));

    assert!(matches!(
        validate_formula("a UNION SELECT b").unwrap_err(),
        FormulaError::SqlInjectionDetected { .. }
    ));
}

#[test]
fn test_security_blacklist_literals() {
    // Dangerous literals should fail
    assert!(matches!(
        validate_formula("a -- comment").unwrap_err(),
        FormulaError::SqlInjectionDetected { .. }
    ));

    assert!(matches!(
        validate_formula("a /* comment */ b").unwrap_err(),
        FormulaError::SqlInjectionDetected { .. }
    ));
}

#[test]
fn test_security_false_positives_allowed() {
    // Words containing keywords as substrings should succeed
    assert!(validate_formula("selection_count + 1").is_ok());
    assert!(validate_formula("updated_from_timestamp").is_ok());
    assert!(validate_formula("inner_join_score / 100").is_ok());
}

#[test]
fn test_security_validate_functions() {
    let reg = FunctionRegistry::standard();

    // Valid functions should succeed
    assert!(validate_functions("ROUND(ABS(x), 2)", &reg).is_ok());

    // Invalid functions should fail
    assert!(matches!(
        validate_functions("NOT_A_REAL_FUNCTION(x)", &reg).unwrap_err(),
        FormulaError::UnknownFunction { .. }
    ));
}
