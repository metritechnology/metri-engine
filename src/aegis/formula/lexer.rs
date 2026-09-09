//! Formula lexer — formula string to token stream.
//!
//! Convierte un string de fórmula en una secuencia de tokens.
//! SRP: Solo tokeniza. No reordena, no evalúa, no valida semántica.

use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::token::{Operator, Token};
use std::iter::Peekable;
use std::str::CharIndices;

/// Tokeniza un string de fórmula en una secuencia de tokens atómicos.
///
/// Reconoce: literales numéricos, variables (con namespace `dominio/atributo`),
/// operadores (+, -, *, /, %), paréntesis, comas, y nombres de función
/// (cualquier IDENTIFIER seguido de `(`).
///
/// # Errors
/// - `FormulaError::UnexpectedToken` si encuentra un carácter no reconocido.
/// - `FormulaError::EmptyFormula` si el input está vacío o es solo whitespace.
/// - `FormulaError::FormulaTooLong` si el string excede 4096 caracteres.
/// - `FormulaError::TooManyTokens` si la cantidad de tokens excede 512.
pub fn tokenize(formula: &str) -> Result<Vec<Token>, FormulaError> {
    if formula.trim().is_empty() {
        return Err(FormulaError::EmptyFormula);
    }
    if formula.len() > 4096 {
        return Err(FormulaError::FormulaTooLong {
            length: formula.len(),
            max: 4096,
        });
    }

    let mut tokens = Vec::new();
    let mut chars = formula.char_indices().peekable();

    while let Some(&(pos, ch)) = chars.peek() {
        if tokens.len() >= 512 {
            return Err(FormulaError::TooManyTokens {
                count: tokens.len() + 1,
                max: 512,
            });
        }

        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '+' => {
                tokens.push(Token::Operator(Operator::Add));
                chars.next();
            }
            '-' => {
                // Distinguir negación unaria vs resta binaria:
                // Es unario si es el primer token, o si el token anterior es
                // un operador, ParenOpen, o Comma.
                let is_unary = tokens.is_empty()
                    || matches!(
                        tokens.last(),
                        Some(Token::Operator(_) | Token::ParenOpen | Token::Comma)
                    );
                if is_unary {
                    tokens.push(Token::Operator(Operator::Neg));
                } else {
                    tokens.push(Token::Operator(Operator::Sub));
                }
                chars.next();
            }
            '*' => {
                tokens.push(Token::Operator(Operator::Mul));
                chars.next();
            }
            '/' => {
                tokens.push(Token::Operator(Operator::Div));
                chars.next();
            }
            '%' => {
                tokens.push(Token::Operator(Operator::Mod));
                chars.next();
            }
            '^' => {
                tokens.push(Token::Operator(Operator::Power));
                chars.next();
            }
            '(' => {
                tokens.push(Token::ParenOpen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::ParenClose);
                chars.next();
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
            }
            '0'..='9' | '.' => {
                // Scan numérico: consume dígitos y punto decimal
                let num_str = scan_number(&mut chars);
                let val = num_str
                    .parse::<f64>()
                    .map_err(|_| FormulaError::UnexpectedToken {
                        position: pos,
                        found: num_str.clone(),
                    })?;
                tokens.push(Token::Literal(val));
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                // Scan identificador: consume alfanuméricos, _, - y /
                let ident = scan_identifier(&mut chars);
                // Si el siguiente carácter no-whitespace es `(`, es una función
                skip_whitespace(&mut chars);
                if chars.peek().map(|&(_, c)| c) == Some('(') {
                    tokens.push(Token::Function(ident.to_uppercase(), 0)); // arity se resuelve en parser
                } else {
                    tokens.push(Token::Variable(ident));
                }
            }
            _ => {
                return Err(FormulaError::UnexpectedToken {
                    position: pos,
                    found: ch.to_string(),
                });
            }
        }
    }
    Ok(tokens)
}

fn scan_number(chars: &mut Peekable<CharIndices<'_>>) -> String {
    let mut num_str = String::new();
    let mut has_dot = false;
    while let Some(&(_, c)) = chars.peek() {
        if c.is_ascii_digit() {
            num_str.push(c);
            chars.next();
        } else if c == '.' && !has_dot {
            num_str.push(c);
            has_dot = true;
            chars.next();
        } else {
            break;
        }
    }
    num_str
}

fn scan_identifier(chars: &mut Peekable<CharIndices<'_>>) -> String {
    let mut ident = String::new();
    while let Some(&(_, c)) = chars.peek() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/' {
            if c == '/' {
                let mut temp_chars = chars.clone();
                temp_chars.next(); // consume '/'
                if let Some((_, next_c)) = temp_chars.next() {
                    if next_c.is_ascii_alphabetic() || next_c == '_' {
                        ident.push(c);
                        chars.next(); // consume '/'
                        continue;
                    }
                }
                break; // it's division, not namespace separator
            }
            ident.push(c);
            chars.next();
        } else {
            break;
        }
    }
    ident
}

fn skip_whitespace(chars: &mut Peekable<CharIndices<'_>>) {
    while let Some(&(_, c)) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}
