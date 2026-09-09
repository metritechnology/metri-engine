//! Railway pipeline helpers over `Result<T, DomainError>`.
pub mod result;
pub use result::{is_error, is_ok, Railway};
