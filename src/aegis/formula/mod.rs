//! Motor de evaluación de fórmulas matemáticas sandboxeado de Aegis.
//!
//! Implementa un pipeline determinista (`lexer` -> `parser` -> `resolver` -> `evaluator`)
//! alineado con los principios SOLID:
//! - `[S]` Responsabilidad única en `token`, `lexer`, `parser`, `evaluator`, `resolver`, `security` y `errors`.
//! - `[O]` Extensibilidad abierta/cerrada mediante el trait `FormulaFunction` y `FunctionRegistry`.
//! - `[L]` Sustitución de Liskov en implementaciones del trait `VariableResolver`.
//! - `[I]` Segregación de interfaces entre validación, parseo y evaluación RPN.
//! - `[D]` Inversión de dependencias en `FormulaEvaluator` hacia `VariableResolver` y `FunctionRegistry`.

pub mod errors;
pub mod evaluator;
pub mod functions;
pub mod functions_registry;
pub mod lexer;
pub mod parser;
pub mod resolver;
pub mod security;
pub mod token;

// Re-exports para conveniencia
pub use errors::FormulaError;
pub use evaluator::FormulaEvaluator;
pub use functions_registry::FunctionRegistry;
pub use resolver::{OlapVariableResolver, OltpVariableResolver, VariableResolver};
pub use token::{Operator, Token};
