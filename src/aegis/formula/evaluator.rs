//! Decoupled RPN formula evaluator.
//!
//! Evaluador RPN desacoplado.
//! ISP: Solo expone evaluate(). No conoce parseo ni compilación SQL.
//! DIP: Depende de traits (VariableResolver, FormulaFunction), NO de implementaciones concretas.

use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::resolver::VariableResolver;
use crate::aegis::formula::token::{Operator, Token};

pub struct FormulaEvaluator {
    rpn_tokens: Vec<Token>,
}

impl FormulaEvaluator {
    pub fn new(rpn_tokens: Vec<Token>) -> Self {
        Self { rpn_tokens }
    }

    /// Evalúa la fórmula contra un resolver de variables y un registro de funciones.
    /// DIP: recibe traits, no implementaciones concretas.
    pub fn evaluate(
        &self,
        resolver: &dyn VariableResolver,
        functions: &FunctionRegistry,
    ) -> Result<f64, FormulaError> {
        let mut stack: Vec<f64> = Vec::new();

        for token in &self.rpn_tokens {
            match token {
                Token::Literal(val) => stack.push(*val),

                Token::Variable(name) => {
                    let val = resolver.resolve(name).unwrap_or(f64::NAN);
                    stack.push(val);
                }

                Token::Operator(op) => {
                    if matches!(op, Operator::Neg) {
                        let operand = stack.pop().ok_or(FormulaError::EmptyFormula)?;
                        stack.push(-operand);
                    } else {
                        let right = stack.pop().ok_or(FormulaError::EmptyFormula)?;
                        let left = stack.pop().ok_or(FormulaError::EmptyFormula)?;
                        let result = match op {
                            Operator::Add => left + right,
                            Operator::Sub => left - right,
                            Operator::Mul => left * right,
                            Operator::Div => {
                                if right == 0.0 {
                                    f64::NAN
                                } else {
                                    left / right
                                }
                            }
                            Operator::Mod => {
                                if right == 0.0 {
                                    f64::NAN
                                } else {
                                    left % right
                                }
                            }
                            Operator::Power => left.powf(right),
                            // Neg es unario y se evaluó arriba; este brazo es
                            // defensivo ante una violación del contrato
                            // parser→evaluador: error, nunca pánico (R3).
                            Operator::Neg => {
                                return Err(FormulaError::MathDomainError {
                                    function: "neg".to_string(),
                                    detail: "operador unario en evaluación binaria".to_string(),
                                })
                            }
                        };
                        stack.push(result);
                    }
                }

                Token::Function(name, arity) => {
                    let func = functions
                        .get(name)
                        .ok_or_else(|| FormulaError::UnknownFunction { name: name.clone() })?;

                    // Validar aridad si la función no es variádica
                    if let Some(expected) = func.arity() {
                        if *arity != expected {
                            return Err(FormulaError::ArityMismatch {
                                function: name.clone(),
                                expected,
                                got: *arity,
                            });
                        }
                    }

                    let mut args = Vec::with_capacity(*arity);
                    for _ in 0..*arity {
                        args.push(stack.pop().ok_or(FormulaError::EmptyFormula)?);
                    }
                    args.reverse(); // RPN: args están en orden inverso

                    let result = func.evaluate(&args).unwrap_or(f64::NAN);
                    stack.push(result);
                }

                _ => {} // ParenOpen/Close/Comma ya fueron consumidos por el parser
            }
        }

        stack.pop().ok_or(FormulaError::EmptyFormula)
    }
}
