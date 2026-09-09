//! Dynamic function registry — extend without touching the evaluator.
//!
//! Registro dinámico de funciones.
//! OCP: Nuevas funciones se REGISTRAN sin modificar el evaluador ni el parser.

use crate::aegis::formula::functions::FormulaFunction;
use std::collections::HashMap;

pub struct FunctionRegistry {
    functions: HashMap<String, Box<dyn FormulaFunction>>,
}

impl FunctionRegistry {
    /// Crea el registro con todas las funciones estándar del catálogo.
    pub fn standard() -> Self {
        use crate::aegis::formula::functions::*;
        let mut reg = Self {
            functions: HashMap::new(),
        };
        reg.register(Box::new(AbsFunction));
        reg.register(Box::new(RoundFunction));
        reg.register(Box::new(CeilFunction));
        reg.register(Box::new(FloorFunction));
        reg.register(Box::new(PowerFunction));
        reg.register(Box::new(SqrtFunction));
        reg.register(Box::new(LogFunction));
        reg.register(Box::new(Log10Function));
        reg.register(Box::new(ModFunction));
        reg.register(Box::new(SignFunction));
        reg.register(Box::new(NullifFunction));
        reg.register(Box::new(CoalesceFunction));
        reg.register(Box::new(IfFunction));
        reg.register(Box::new(GreatestFunction));
        reg.register(Box::new(LeastFunction));
        reg.register(Box::new(ClampFunction));
        reg
    }

    /// Registra una función nueva (OCP: extensión sin modificación).
    pub fn register(&mut self, func: Box<dyn FormulaFunction>) {
        self.functions.insert(func.name().to_uppercase(), func);
    }

    /// Busca una función por nombre (case-insensitive).
    pub fn get(&self, name: &str) -> Option<&dyn FormulaFunction> {
        self.functions.get(&name.to_uppercase()).map(|f| f.as_ref())
    }

    /// Valida si un nombre de función es conocido.
    pub fn is_registered(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_uppercase())
    }
}
