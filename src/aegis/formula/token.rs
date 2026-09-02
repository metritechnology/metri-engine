// aegis/formula/token.rs — Solo definición de tipos del AST.
// SRP: No contiene lógica de parseo ni evaluación.

/// Operadores aritméticos binarios y unario.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Add,   // +    precedencia 1
    Sub,   // -    precedencia 1
    Mul,   // *    precedencia 2
    Div,   // /    precedencia 2
    Mod,   // %    precedencia 2
    Power, // ^    precedencia 3
    Neg,   // -    precedencia 4 (unario)
}

impl Operator {
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Add | Operator::Sub => 1,
            Operator::Mul | Operator::Div | Operator::Mod => 2,
            Operator::Power => 3,
            Operator::Neg => 4,
        }
    }

    pub fn is_left_associative(&self) -> bool {
        !matches!(self, Operator::Neg | Operator::Power)
    }
}

/// Token atómico de una fórmula parseada.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Literal(f64),            // 100, 3.14
    Variable(String),        // "asset/revenue", "cost"
    Operator(Operator),      // +, -, *, /, %
    Function(String, usize), // ("NULLIF", 2)
    ParenOpen,               // (
    ParenClose,              // )
    Comma,                   // ,
}
