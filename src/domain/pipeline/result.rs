// Equivalencia: Railway-Pattern helpers (ok?, error?, unwrap)
// En el stack anterior: (ok? result), (unwrap result)
// En Rust: Result<T, DomainError> es nativo — estos helpers son
//          funciones de conveniencia para compatibilidad semántica.

use crate::domain::errors::DomainError;

/// Tipo Railway — equivale a [:ok T] | [:error DomainError] de el stack anterior.
pub type Railway<T> = Result<T, DomainError>;

/// ok? — retorna true si el resultado es Ok(…)
#[inline]
pub fn is_ok<T>(result: &Railway<T>) -> bool {
    result.is_ok()
}

/// error? — retorna true si el resultado es Err(…)
#[inline]
pub fn is_error<T>(result: &Railway<T>) -> bool {
    result.is_err()
}

/// unwrap — extrae el body de Ok(T). Panics si es Err.
///
/// Zero-Drop: el el stack anterior lanzaba ExceptionInfo con {:result result}.
/// En Rust usamos expect con el mismo mensaje descriptivo.
#[inline]
pub fn unwrap_railway<T>(result: Railway<T>, context: &str) -> T {
    result.unwrap_or_else(|err| {
        panic!("Cannot unwrap an error result — context: {context} — error: {err:?}")
    })
}

/// Macro de conveniencia: construye un Ok Railway
#[macro_export]
macro_rules! ok {
    ($val:expr) => {
        Ok::<_, $crate::domain::errors::DomainError>($val)
    };
}

/// Macro de conveniencia: construye un Err Railway
#[macro_export]
macro_rules! fail {
    ($code:expr, $detail:expr) => {
        Err($crate::domain::errors::DomainError::new($code, $detail))
    };
}

#[cfg(test)]
#[path = "../tests/result_tests.rs"]
mod tests;
