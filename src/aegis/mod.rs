//! Aegis — the compilers and executors for the unified Metri Vortex (.mbf) engine.
//!
//! Módulos principales:
//!
//! - [`mbf`] — ejecutor vectorizado sobre Metri Binary Format (`.mbf`).
//! - [`oltp`] — planes físicos transaccionales y analíticos sobre `.mbf`.
//! - [`formula`] — lenguaje de fórmulas sandboxeado (lexer → parser →
//!   resolver → evaluator).
//!
//! Más utilidades transversales: [`ast_ir`] (IR tipado), [`pagination`]
//! (cursores), [`label_template`] (interpolación) y [`temporal_bridge`]
//! (tipos de tiempo proto/FBS → `temporal`).
//!
//! # Guarantees
//!
//! - Ejecución unificada `.mbf` con aislamiento estricto por tenant.
//! - Sin I/O en compiladores: los compiladores son puros; el I/O vive en los ejecutores.
pub mod ast_ir;
pub mod formula;
pub mod label_template;
pub mod mbf; // Ejecutor vectorizado sobre Metri Binary Format (.mbf)
pub mod oltp;
pub mod pagination; // Paginación cursor Base64(offset:limit)
pub mod temporal_bridge; // Bridge proto/FBS → temporal canónico

#[cfg(test)]
mod testing;
