//! Definición de tokens léxicos y operadores aritméticos del motor de fórmulas.
//!
//! Contiene únicamente los tipos de datos del vocabulario léxico (`Operator` y `Token`),
//! junto con sus reglas de precedencia y asociatividad.

/// Operadores aritméticos binarios y unarios soportados por el motor de fórmulas.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    /// Adición binaria (`+`), precedencia 1.
    Add, // +    precedencia 1
    /// Sustracción binaria (`-`), precedencia 1.
    Sub, // -    precedencia 1
    /// Multiplicación binaria (`*`), precedencia 2.
    Mul, // *    precedencia 2
    /// División binaria (`/`), precedencia 2.
    Div, // /    precedencia 2
    /// Módulo o residuo binario (`%`), precedencia 2.
    Mod, // %    precedencia 2
    /// Potenciación binaria (`^`), precedencia 3 (asociativa por la derecha).
    Power, // ^    precedencia 3
    /// Negación aritmética unaria (`-`), precedencia 4 (asociativa por la derecha).
    Neg, // -    precedencia 4 (unario)
}

impl Operator {
    /// Retorna el nivel de precedencia numérica del operador (`1` menor, `4` mayor).
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Add | Operator::Sub => 1,
            Operator::Mul | Operator::Div | Operator::Mod => 2,
            Operator::Power => 3,
            Operator::Neg => 4,
        }
    }

    /// Indica si el operador se agrupa de izquierda a derecha (`true` para todos salvo `Power` y `Neg`).
    pub fn is_left_associative(&self) -> bool {
        !matches!(self, Operator::Neg | Operator::Power)
    }
}

/// Unidad léxica atómica producida por el analizador léxico y consumida por el parser/evaluador.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Constante numérica en punto flotante de doble precisión (ej. `100.0`, `3.14`).
    Literal(f64), // 100, 3.14
    /// Identificador de atributo o métrica (ej. `"asset/revenue"`, `"cost"`).
    Variable(String), // "asset/revenue", "cost"
    /// Operador aritmético binario o unario.
    Operator(Operator), // +, -, *, /, %
    /// Invocación a función con su nombre canónico en mayúsculas y su aridad resuelta.
    Function(String, usize), // ("NULLIF", 2)
    /// Paréntesis de apertura `(`.
    ParenOpen, // (
    /// Paréntesis de cierre `)`.
    ParenClose, // )
    /// Separador de argumentos de función `,`.
    Comma, // ,
}
