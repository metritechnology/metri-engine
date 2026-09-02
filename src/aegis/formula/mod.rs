// aegis/formula/mod.rs — Motor de Fórmulas (§05.05)
//
// Arquitectura SOLID:
//   [S] token, lexer, parser, evaluator, resolver, security, errors — SRP
//   [O] FormulaFunction trait + FunctionRegistry — OCP
//   [L] VariableResolver trait: Oltp/Olap intercambiables — LSP
//   [I] FormulaEvaluator ≠ FormulaCompiler ≠ FormulaValidator — ISP
//   [D] evaluator → VariableResolver trait, FormulaFunction trait — DIP

pub mod compiler_olap;
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
