//! Audit contracts — action typing and interceptor port for the OLAP audit log.
//!
//! [`action_type`] deriva la acción (puro) y [`protocol`] define el
//! interceptor fire-and-forget que nunca bloquea ni altera el resultado.
pub mod action_type;
pub mod protocol;
