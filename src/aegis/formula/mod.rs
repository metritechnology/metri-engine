// aegis/formula/mod.rs — Motor de Fórmulas (§05.05)
//
// Arquitectura SOLID:
//   [S] token, lexer, parser, evaluator, resolver, security, errors — SRP
//   [O] FormulaFunction trait + FunctionRegistry — OCP
//   [L] VariableResolver trait: Oltp/Olap intercambiables — LSP
//   [I] FormulaEvaluator ≠ FormulaCompiler ≠ FormulaValidator — ISP
//   [D] evaluator → VariableResolver trait, FormulaFunction trait — DIP

pub mod token;
pub mod lexer;
pub mod parser;
pub mod evaluator;
pub mod resolver;
pub mod functions;
pub mod functions_registry;
pub mod compiler_olap;
pub mod security;
pub mod errors;

// Re-exports para conveniencia
pub use token::{Token, Operator};
pub use evaluator::FormulaEvaluator;
pub use resolver::{VariableResolver, OltpVariableResolver, OlapVariableResolver};
pub use functions_registry::FunctionRegistry;
pub use errors::FormulaError;
