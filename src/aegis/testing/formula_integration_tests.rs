use crate::aegis::formula::compiler_olap::OlapFormulaCompiler;
use crate::aegis::formula::evaluator::FormulaEvaluator;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::lexer::tokenize;
use crate::aegis::formula::parser::to_rpn;
use crate::aegis::formula::resolver::OltpVariableResolver;
use serde_json::json;

#[test]
fn test_integration_olap_compilation() {
    let reg = FunctionRegistry::standard();

    // 1. Basic namespace resolution and space formatting
    let formula1 = "((asset/revenue - invoice/total-cost) / NULLIF(asset/revenue, 0)) * 100";
    let sql1 = OlapFormulaCompiler::compile(formula1, &reg).unwrap();
    assert_eq!(sql1, "((revenue - total_cost) / NULLIF(revenue, 0)) * 100");

    // 2. Unary minus spacing check
    let formula2 = "-asset/cost + asset/revenue";
    let sql2 = OlapFormulaCompiler::compile(formula2, &reg).unwrap();
    assert_eq!(sql2, "-cost + revenue");
}

#[test]
fn test_integration_oltp_evaluation() {
    let reg = FunctionRegistry::standard();
    let formula = "((asset/revenue - invoice/total-cost) / NULLIF(asset/revenue, 0)) * 100";

    // 1. Tokens and RPN
    let tokens = tokenize(formula).unwrap();
    let rpn = to_rpn(tokens, &reg).unwrap();
    let evaluator = FormulaEvaluator::new(rpn);

    // 2. Evaluation with namespace keys
    let row1 = json!({
        "asset/revenue": 1000.0,
        "invoice/total-cost": 400.0
    });
    let resolver1 = OltpVariableResolver::new(row1.as_object().unwrap());
    let res1 = evaluator.evaluate(&resolver1, &reg).unwrap();
    assert_eq!(res1, 60.0);

    // 3. Evaluation with bare/split keys fallback
    let row2_with_hyphen = json!({
        "revenue": 500.0,
        "total-cost": 250.0
    });
    let resolver2 = OltpVariableResolver::new(row2_with_hyphen.as_object().unwrap());
    let res2 = evaluator.evaluate(&resolver2, &reg).unwrap();
    assert_eq!(res2, 50.0);
}
