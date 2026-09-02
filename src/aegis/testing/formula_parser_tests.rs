use crate::aegis::formula::lexer::tokenize;
use crate::aegis::formula::parser::to_rpn;
use crate::aegis::formula::token::{Token, Operator};
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::errors::FormulaError;

#[test]
fn test_parser_shunting_yard_basic() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("a + b * c").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    assert_eq!(rpn, vec![
        Token::Variable("a".to_string()),
        Token::Variable("b".to_string()),
        Token::Variable("c".to_string()),
        Token::Operator(Operator::Mul),
        Token::Operator(Operator::Add),
    ]);
}

#[test]
fn test_parser_unaries() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("-a * -b").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    assert_eq!(rpn, vec![
        Token::Variable("a".to_string()),
        Token::Operator(Operator::Neg),
        Token::Variable("b".to_string()),
        Token::Operator(Operator::Neg),
        Token::Operator(Operator::Mul),
    ]);
}

#[test]
fn test_parser_function_arity() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("ROUND(ABS(a), 2)").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    assert_eq!(rpn, vec![
        Token::Variable("a".to_string()),
        Token::Function("ABS".to_string(), 1),
        Token::Literal(2.0),
        Token::Function("ROUND".to_string(), 2),
    ]);
}

#[test]
fn test_parser_unbalanced_parentheses() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("(a + b").unwrap();
    assert_eq!(to_rpn(tokens, &reg).unwrap_err(), FormulaError::UnbalancedParentheses { position: 0 });

    let tokens2 = tokenize("a + b)").unwrap();
    assert_eq!(to_rpn(tokens2, &reg).unwrap_err(), FormulaError::UnbalancedParentheses { position: 0 });
}

#[test]
fn test_parser_unknown_function() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("NOT_A_REAL_FUNCTION(a)").unwrap();
    assert!(matches!(
        to_rpn(tokens, &reg).unwrap_err(),
        FormulaError::UnknownFunction { .. }
    ));
}

#[test]
fn test_parser_nesting_too_deep() {
    let reg = FunctionRegistry::standard();
    // 33 parenthesis opens
    let open_parens = "(".repeat(33);
    let close_parens = ")".repeat(33);
    let formula = format!("{}1{}", open_parens, close_parens);
    let tokens = tokenize(&formula).unwrap();
    assert!(matches!(
        to_rpn(tokens, &reg).unwrap_err(),
        FormulaError::NestingTooDeep { .. }
    ));
}

#[test]
fn test_parser_func_nesting_too_deep() {
    let reg = FunctionRegistry::standard();
    // 17 nested function calls: ABS(ABS(...ABS(1)...))
    let open_calls = "ABS(".repeat(17);
    let close_parens = ")".repeat(17);
    let formula = format!("{}1{}", open_calls, close_parens);
    let tokens = tokenize(&formula).unwrap();
    assert!(matches!(
        to_rpn(tokens, &reg).unwrap_err(),
        FormulaError::FunctionNestingTooDeep { .. }
    ));
}
