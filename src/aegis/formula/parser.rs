// aegis/formula/parser.rs — Convierte tokens infix a cola RPN (Reverse Polish Notation).
// SRP: Solo reordena tokens. No tokeniza, no evalúa, no compila a SQL.

use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::token::{Operator, Token};

/// Convierte una secuencia de tokens infix a Reverse Polish Notation (RPN)
/// usando el algoritmo Shunting-Yard.
///
/// - Resuelve la aridad de funciones contando argumentos separados por Comma.
/// - Valida paréntesis balanceados.
/// - Valida que las funciones existan en el `FunctionRegistry`.
/// - Valida límites de profundidad (paréntesis max 32, funciones max 16).
///
/// # Errors
/// - `FormulaError::UnbalancedParentheses` si los paréntesis no cierran.
/// - `FormulaError::UnknownFunction` si una función no está registrada.
/// - `FormulaError::NestingTooDeep` si la profundidad de paréntesis excede 32.
/// - `FormulaError::FunctionNestingTooDeep` si la profundidad de funciones excede 16.
pub fn to_rpn(tokens: Vec<Token>, registry: &FunctionRegistry) -> Result<Vec<Token>, FormulaError> {
    let mut output: Vec<Token> = Vec::new();
    let mut op_stack: Vec<Token> = Vec::new();
    let mut arity_stack: Vec<usize> = Vec::new(); // Track function arity
    let mut paren_depth = 0;
    let mut func_depth = 0;

    for token in tokens {
        match &token {
            Token::Literal(_) | Token::Variable(_) => {
                output.push(token);
                // Si estamos dentro de una función y es el primer argumento
                if let Some(arity) = arity_stack.last_mut() {
                    if *arity == 0 {
                        *arity = 1;
                    }
                }
            }
            Token::Function(name, _) => {
                if !registry.is_registered(name) {
                    return Err(FormulaError::UnknownFunction { name: name.clone() });
                }
                if let Some(arity) = arity_stack.last_mut() {
                    if *arity == 0 {
                        *arity = 1;
                    }
                }
                func_depth += 1;
                if func_depth > 16 {
                    return Err(FormulaError::FunctionNestingTooDeep {
                        depth: func_depth,
                        max: 16,
                    });
                }
                op_stack.push(token.clone());
                arity_stack.push(0);
            }
            Token::Comma => {
                // Flush operadores hasta el paréntesis de la función o coma
                while let Some(top) = op_stack.last() {
                    if matches!(top, Token::ParenOpen) {
                        break;
                    }
                    output.push(op_stack.pop().unwrap());
                }
                if let Some(arity) = arity_stack.last_mut() {
                    *arity += 1;
                }
            }
            Token::Operator(op) => {
                if matches!(op, Operator::Neg) {
                    if let Some(arity) = arity_stack.last_mut() {
                        if *arity == 0 {
                            *arity = 1;
                        }
                    }
                }
                while let Some(Token::Operator(top_op)) = op_stack.last() {
                    if (op.is_left_associative() && op.precedence() <= top_op.precedence())
                        || (!op.is_left_associative() && op.precedence() < top_op.precedence())
                    {
                        output.push(op_stack.pop().unwrap());
                    } else {
                        break;
                    }
                }
                op_stack.push(token);
            }
            Token::ParenOpen => {
                if let Some(arity) = arity_stack.last_mut() {
                    if *arity == 0 {
                        *arity = 1;
                    }
                }
                paren_depth += 1;
                if paren_depth > 32 {
                    return Err(FormulaError::NestingTooDeep {
                        depth: paren_depth,
                        max: 32,
                    });
                }
                op_stack.push(token);
            }
            Token::ParenClose => {
                if paren_depth == 0 {
                    return Err(FormulaError::UnbalancedParentheses { position: 0 });
                }
                paren_depth -= 1;
                let mut found_open = false;
                while let Some(top) = op_stack.pop() {
                    if matches!(top, Token::ParenOpen) {
                        found_open = true;
                        break;
                    }
                    output.push(top);
                }
                if !found_open {
                    return Err(FormulaError::UnbalancedParentheses { position: 0 });
                }
                // Si el token anterior al paréntesis era una función, emitirla
                if let Some(Token::Function(name, _)) = op_stack.last() {
                    let arity = arity_stack.pop().unwrap_or(0);
                    let fn_name = name.clone();
                    op_stack.pop();
                    output.push(Token::Function(fn_name, arity));
                    func_depth -= 1;
                }
            }
        }
    }

    // Flush operadores restantes
    while let Some(top) = op_stack.pop() {
        if matches!(top, Token::ParenOpen) {
            return Err(FormulaError::UnbalancedParentheses { position: 0 });
        }
        output.push(top);
    }

    Ok(output)
}
