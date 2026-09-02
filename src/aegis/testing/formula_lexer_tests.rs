use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::lexer::tokenize;
use crate::aegis::formula::token::{Operator, Token};

#[test]
fn test_tokenize_basic_arithmetic() {
    let tokens = tokenize("1.5 + 2 * 3 - 4 / 5").unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Literal(1.5),
            Token::Operator(Operator::Add),
            Token::Literal(2.0),
            Token::Operator(Operator::Mul),
            Token::Literal(3.0),
            Token::Operator(Operator::Sub),
            Token::Literal(4.0),
            Token::Operator(Operator::Div),
            Token::Literal(5.0),
        ]
    );
}

#[test]
fn test_tokenize_unary_minus() {
    let tokens = tokenize("-1 + (-2)").unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Operator(Operator::Neg),
            Token::Literal(1.0),
            Token::Operator(Operator::Add),
            Token::ParenOpen,
            Token::Operator(Operator::Neg),
            Token::Literal(2.0),
            Token::ParenClose,
        ]
    );
}

#[test]
fn test_tokenize_variables_and_namespaces() {
    let tokens = tokenize("asset/revenue - work_order/total-cost").unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Variable("asset/revenue".to_string()),
            Token::Operator(Operator::Sub),
            Token::Variable("work_order/total-cost".to_string()),
        ]
    );
}

#[test]
fn test_tokenize_function_calls() {
    let tokens = tokenize("ROUND(ABS(asset/profit), 2)").unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Function("ROUND".to_string(), 0),
            Token::ParenOpen,
            Token::Function("ABS".to_string(), 0),
            Token::ParenOpen,
            Token::Variable("asset/profit".to_string()),
            Token::ParenClose,
            Token::Comma,
            Token::Literal(2.0),
            Token::ParenClose,
        ]
    );
}

#[test]
fn test_tokenize_empty_and_spaces() {
    assert_eq!(tokenize("  ").unwrap_err(), FormulaError::EmptyFormula);
    assert_eq!(tokenize("").unwrap_err(), FormulaError::EmptyFormula);
}

#[test]
fn test_tokenize_unexpected_token() {
    let err = tokenize("a @ b").unwrap_err();
    assert!(matches!(
        err,
        FormulaError::UnexpectedToken { position: 2, .. }
    ));
}

#[test]
fn test_tokenize_formula_too_long() {
    let long_formula = "a".repeat(4097);
    assert!(matches!(
        tokenize(&long_formula).unwrap_err(),
        FormulaError::FormulaTooLong { .. }
    ));
}

#[test]
fn test_tokenize_too_many_tokens() {
    let tokens_formula = "1 + ".repeat(257); // 514 tokens
    assert!(matches!(
        tokenize(&tokens_formula).unwrap_err(),
        FormulaError::TooManyTokens { .. }
    ));
}
