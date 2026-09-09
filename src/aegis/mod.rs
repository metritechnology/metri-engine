//! Aegis — the compilers: OLAP SQL, OLTP executor and the formula engine.
//!
//! Tres compiladores bajo un mismo paraguas:
//!
//! - [`sql`] — AST IR → SQL (Athena/Postgres/MySQL vía `sea-query`).
//! - [`oltp`] — planes físicos sobre el motor EAV en memoria.
//! - [`formula`] — lenguaje de fórmulas sandboxeado (lexer → parser →
//!   resolver → evaluator), con compilación a SQL para OLAP.
//!
//! Más utilidades transversales: [`ast_ir`] (IR tipado), [`pagination`]
//! (cursores), [`label_template`] (interpolación) y [`temporal_bridge`]
//! (tipos de tiempo proto/FBS → `temporal`).
//!
//! # Guarantees
//!
//! - SQL por construcción parametrizada: identificadores solo vía
//!   `sea-query`; el gate de `sql::security` verifica aislamiento por tenant.
//! - Sin I/O: los compiladores son puros; el I/O vive en los ejecutores.
pub mod ast_ir;
pub mod formula;
pub mod label_template;
pub mod oltp;
pub mod pagination; // Paginación cursor Base64(offset:limit)
pub mod sql;
pub mod temporal_bridge; // Bridge proto/FBS → temporal canónico

#[cfg(test)]
mod testing;
