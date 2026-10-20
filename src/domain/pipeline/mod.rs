//! Utilidades de composición Railway-Oriented Programming sobre `Result<T, DomainError>`.

/// Tipo alias `Railway<T>`, predicados `is_ok`/`is_error` y macros `ok!` y `fail!`.
pub mod result;
pub use result::{is_error, is_ok, Railway};
