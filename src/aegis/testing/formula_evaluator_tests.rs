use crate::aegis::formula::lexer::tokenize;
use crate::aegis::formula::parser::to_rpn;
use crate::aegis::formula::evaluator::FormulaEvaluator;
use crate::aegis::formula::resolver::VariableResolver;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::errors::FormulaError;
use std::collections::HashMap;

struct MockResolver {
    vars: HashMap<String, f64>,
}

impl VariableResolver for MockResolver {
    fn resolve(&self, var_name: &str) -> Option<f64> {
        self.vars.get(var_name).copied()
    }
}

#[test]
fn test_evaluate_basic_arithmetic() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("2 * (3 + 4) - 5").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);

    let resolver = MockResolver { vars: HashMap::new() };
    let result = evaluator.evaluate(&resolver, &reg).unwrap();
    assert_eq!(result, 9.0);
}

#[test]
fn test_evaluate_variables() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("asset/revenue - asset/cost").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);

    let mut vars = HashMap::new();
    vars.insert("asset/revenue".to_string(), 1000.0);
    vars.insert("asset/cost".to_string(), 450.0);
    let resolver = MockResolver { vars };

    let result = evaluator.evaluate(&resolver, &reg).unwrap();
    assert_eq!(result, 550.0);
}

#[test]
fn test_evaluate_unresolved_variable() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("asset/revenue - asset/cost").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);

    let mut vars = HashMap::new();
    vars.insert("asset/revenue".to_string(), 1000.0);
    // Cost is missing
    let resolver = MockResolver { vars };

    let result = evaluator.evaluate(&resolver, &reg).unwrap();
    assert!(result.is_nan());
}

#[test]
fn test_evaluate_division_by_zero() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("100 / (5 - 5)").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);

    let resolver = MockResolver { vars: HashMap::new() };
    let result = evaluator.evaluate(&resolver, &reg).unwrap();
    assert!(result.is_nan());
}

#[test]
fn test_evaluate_nested_functions() {
    let reg = FunctionRegistry::standard();
    let tokens = tokenize("ROUND(SQRT(25) * 1.25, 1)").unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);

    let resolver = MockResolver { vars: HashMap::new() };
    let result = evaluator.evaluate(&resolver, &reg).unwrap();
    assert_eq!(result, 6.3); // SQRT(25) = 5. 5 * 1.25 = 6.25. ROUND(6.25, 1) = 6.3
}

#[test]
fn test_debug_failing_cases() {
    let reg = FunctionRegistry::standard();
    
    let mut vars = HashMap::new();
    vars.insert("health_score".to_string(), 12.0);
    vars.insert("current_meter_reading".to_string(), 100.0);
    let resolver = MockResolver { vars };

    let formula = "health_score ^ 3.0";
    let tokens = tokenize(formula).unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);
    let result = evaluator.evaluate(&resolver, &reg);
    println!("DEBUG EVAL: '{}' -> {:?}", formula, result);
    
    let formula2 = "ROUND(12.4)";
    let tokens2 = tokenize(formula2).unwrap();
    let rpn2 = to_rpn(tokens2, &reg).unwrap();
    let evaluator2 = FormulaEvaluator::new(rpn2);
    let result2 = evaluator2.evaluate(&resolver, &reg);
    println!("DEBUG EVAL: '{}' -> {:?}", formula2, result2);

    let formula3 = "COALESCE(telemetry_config, health_score)";
    let tokens3 = tokenize(formula3).unwrap();
    let rpn3 = to_rpn(tokens3, &reg).unwrap();
    let evaluator3 = FormulaEvaluator::new(rpn3);
    let result3 = evaluator3.evaluate(&resolver, &reg);
    println!("DEBUG EVAL: '{}' -> {:?}", formula3, result3);
}
