//! Extensible trait for formula functions.
//!
//! Trait extensible para funciones de fórmula.
//! OCP: Para agregar una nueva función, solo se crea un nuevo struct
//! que implemente FormulaFunction y se registra.

/// Contrato que toda función de fórmula debe implementar.
pub trait FormulaFunction: Send + Sync {
    /// Nombre canónico de la función (uppercase).
    fn name(&self) -> &'static str;

    /// Número de argumentos esperados. None = variádica.
    fn arity(&self) -> Option<usize>;

    /// Evalúa la función con los argumentos dados (orden: arg1, arg2, ...).
    /// Retorna None si la evaluación es indefinida (ej: NULLIF match, div/0).
    fn evaluate(&self, args: &[f64]) -> Option<f64>;
}

// ── Implementaciones concretas (cada una en su propio bloque OCP) ───────────

pub struct AbsFunction;
impl FormulaFunction for AbsFunction {
    fn name(&self) -> &'static str {
        "ABS"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        args.first().map(|x| x.abs())
    }
}

pub struct RoundFunction;
impl FormulaFunction for RoundFunction {
    fn name(&self) -> &'static str {
        "ROUND"
    }
    fn arity(&self) -> Option<usize> {
        None
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        let n = *args.get(1).unwrap_or(&0.0) as i32;
        let factor = 10_f64.powi(n);
        Some((x * factor).round() / factor)
    }
}

pub struct CeilFunction;
impl FormulaFunction for CeilFunction {
    fn name(&self) -> &'static str {
        "CEIL"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        args.first().map(|x| x.ceil())
    }
}

pub struct FloorFunction;
impl FormulaFunction for FloorFunction {
    fn name(&self) -> &'static str {
        "FLOOR"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        args.first().map(|x| x.floor())
    }
}

pub struct PowerFunction;
impl FormulaFunction for PowerFunction {
    fn name(&self) -> &'static str {
        "POWER"
    }
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let base = *args.first()?;
        let exponent = *args.get(1)?;
        Some(base.powf(exponent))
    }
}

pub struct SqrtFunction;
impl FormulaFunction for SqrtFunction {
    fn name(&self) -> &'static str {
        "SQRT"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        if *x < 0.0 {
            None
        } else {
            Some(x.sqrt())
        }
    }
}

pub struct LogFunction;
impl FormulaFunction for LogFunction {
    fn name(&self) -> &'static str {
        "LOG"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        if *x <= 0.0 {
            None
        } else {
            Some(x.ln())
        }
    }
}

pub struct Log10Function;
impl FormulaFunction for Log10Function {
    fn name(&self) -> &'static str {
        "LOG10"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        if *x <= 0.0 {
            None
        } else {
            Some(x.log10())
        }
    }
}

pub struct ModFunction;
impl FormulaFunction for ModFunction {
    fn name(&self) -> &'static str {
        "MOD"
    }
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        let y = args.get(1)?;
        if *y == 0.0 {
            None
        } else {
            Some(x % y)
        }
    }
}

pub struct SignFunction;
impl FormulaFunction for SignFunction {
    fn name(&self) -> &'static str {
        "SIGN"
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        if x.is_nan() {
            Some(f64::NAN)
        } else {
            Some(if *x > 0.0 {
                1.0
            } else if *x < 0.0 {
                -1.0
            } else {
                0.0
            })
        }
    }
}

pub struct NullifFunction;
impl FormulaFunction for NullifFunction {
    fn name(&self) -> &'static str {
        "NULLIF"
    }
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        let y = args.get(1)?;
        if x == y {
            None
        } else {
            Some(*x)
        }
    }
}

pub struct CoalesceFunction;
impl FormulaFunction for CoalesceFunction {
    fn name(&self) -> &'static str {
        "COALESCE"
    }
    fn arity(&self) -> Option<usize> {
        None
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        args.iter().copied().find(|x| !x.is_nan())
    }
}

pub struct IfFunction;
impl FormulaFunction for IfFunction {
    fn name(&self) -> &'static str {
        "IF"
    }
    fn arity(&self) -> Option<usize> {
        Some(3)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let cond = args.first()?;
        let then_val = args.get(1)?;
        let else_val = args.get(2)?;
        Some(if *cond != 0.0 { *then_val } else { *else_val })
    }
}

pub struct GreatestFunction;
impl FormulaFunction for GreatestFunction {
    fn name(&self) -> &'static str {
        "GREATEST"
    }
    fn arity(&self) -> Option<usize> {
        None
    } // Variádica
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        if args.is_empty() {
            return None;
        }
        args.iter().copied().reduce(f64::max)
    }
}

pub struct LeastFunction;
impl FormulaFunction for LeastFunction {
    fn name(&self) -> &'static str {
        "LEAST"
    }
    fn arity(&self) -> Option<usize> {
        None
    } // Variádica
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        if args.is_empty() {
            return None;
        }
        args.iter().copied().reduce(f64::min)
    }
}

pub struct ClampFunction;
impl FormulaFunction for ClampFunction {
    fn name(&self) -> &'static str {
        "CLAMP"
    }
    fn arity(&self) -> Option<usize> {
        Some(3)
    }
    fn evaluate(&self, args: &[f64]) -> Option<f64> {
        let x = args.first()?;
        let lo = args.get(1)?;
        let hi = args.get(2)?;
        Some(x.clamp(*lo, *hi))
    }
}
