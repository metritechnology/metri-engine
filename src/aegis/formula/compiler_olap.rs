// aegis/formula/compiler_olap.rs — Compila fórmulas a SQL projection strings.
//
// Flujo: formula_str → validate → tokenize → to_rpn (solo validar) → tokens_to_sql
//        El RPN se genera SOLO para validar sintaxis y aridad.
//        El SQL se reconstruye desde los tokens INFIX (pre-RPN).

use crate::aegis::formula::errors::FormulaError;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::formula::resolver::OlapVariableResolver;
use crate::aegis::formula::token::{Operator, Token};
use crate::aegis::formula::{lexer, parser, security};

pub struct OlapFormulaCompiler;

impl OlapFormulaCompiler {
    /// Compila una fórmula a un string SQL projection.
    ///
    /// Pipeline:
    ///   1. Validación de seguridad (word-boundary blacklist)
    ///   2. Tokenización (lexer) — resuelve ambigüedad `/`
    ///   3. Parsing (Shunting-Yard) — valida sintaxis + aridad (RPN descartado)
    ///   4. Reconstrucción SQL desde tokens infix con namespaces resueltos
    ///
    /// # Ejemplo
    /// ```ignore
    /// let sql = OlapFormulaCompiler::compile(
    ///     "(asset/revenue - invoice/cost) / NULLIF(asset/revenue, 0)",
    ///     &registry,
    /// )?;
    /// // → "(revenue - cost) / NULLIF(revenue, 0)"
    /// ```
    pub fn compile(formula_str: &str, registry: &FunctionRegistry) -> Result<String, FormulaError> {
        // 1. Validación de seguridad anti-inyección (word-boundary)
        security::validate_formula(formula_str)?;

        // 2. Tokenización — resuelve la ambigüedad `/` (namespace vs división)
        let tokens = lexer::tokenize(formula_str)?;

        // 3. Parsing completo — valida sintaxis, paréntesis, aridad de funciones
        //    El resultado RPN se DESCARTA; solo se usa para validación.
        let _rpn = parser::to_rpn(tokens.clone(), registry)?;

        // 4. Reconstrucción SQL desde tokens INFIX (no RPN)
        let sql = Self::tokens_to_sql(&tokens);

        Ok(sql)
    }

    /// Reconstruye un string SQL desde tokens infix, resolviendo namespaces.
    ///
    /// Transforma:
    ///   Variable("asset/revenue") → "revenue"
    ///   Variable("asset/health-score") → "health_score"
    ///   Operator(Div) → "/"
    ///   Function("NULLIF", _) → "NULLIF"
    ///
    /// Preserva el orden infix original, que es directamente compatible con SQL.
    fn tokens_to_sql(tokens: &[Token]) -> String {
        let mut parts: Vec<String> = Vec::with_capacity(tokens.len());

        for token in tokens {
            let part = match token {
                Token::Literal(v) => {
                    if *v == (*v as i64) as f64 && !v.is_nan() {
                        format!("{}", *v as i64) // 100.0 → "100" (entero limpio)
                    } else {
                        format!("{}", v) // 3.14 → "3.14"
                    }
                }
                Token::Variable(name) => OlapVariableResolver::resolve_to_sql_column(name),
                Token::Operator(op) => match op {
                    Operator::Add => "+".to_string(),
                    Operator::Sub => "-".to_string(),
                    Operator::Mul => "*".to_string(),
                    Operator::Div => "/".to_string(),
                    Operator::Mod => "%".to_string(),
                    Operator::Power => "^".to_string(),
                    Operator::Neg => "-".to_string(), // unario
                },
                Token::Function(name, _) => name.clone(),
                Token::ParenOpen => "(".to_string(),
                Token::ParenClose => ")".to_string(),
                Token::Comma => ",".to_string(),
            };
            parts.push(part);
        }

        // Join con espacios inteligentes: no poner espacio después de `(` ni antes de `)`/`,`
        let mut sql = String::with_capacity(parts.iter().map(|p| p.len() + 1).sum());
        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                let prev = &parts[i - 1];
                let is_after_open = prev.ends_with('('); // "(" → no espacio después
                let is_before_close = part.starts_with(')'); // ")" → no espacio antes
                let is_before_comma = part.starts_with(','); // "," → no espacio antes
                let is_unary_neg = prev == "-"
                    && matches!(
                        parts.get(i.wrapping_sub(2)).map(|s| s.as_str()),
                        None | Some("(")
                            | Some("+")
                            | Some("-")
                            | Some("*")
                            | Some("/")
                            | Some("%")
                            | Some("^")
                            | Some(",")
                    );
                let is_func_call =
                    part == "(" && matches!(tokens.get(i - 1), Some(Token::Function(_, _)));

                let needs_space = !is_after_open
                    && !is_before_close
                    && !is_before_comma
                    && !is_unary_neg
                    && !is_func_call;

                if needs_space {
                    sql.push(' ');
                }
            }
            sql.push_str(part);
        }
        sql
    }
}
