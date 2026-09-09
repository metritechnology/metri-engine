//! OLAP SQL compiler — AST IR to Athena/Postgres/MySQL via `sea-query`.
//!
//! # Guarantees
//!
//! - Inyección imposible por construcción: no hay concatenación de SQL;
//!   valores como parámetros e identificadores como alias de `sea-query`.
//! - `security` verifica el aislamiento por tenant antes de renderizar.
//!
//! # Submodules
//!
//! - `compiler` orquesta; `select_compiler`, `metric_compiler`,
//!   `where_compiler` y `cte_compiler` compilan cada parte.
//! - `dialect` abstrae la sintaxis por motor.
//! - `registry` + `virtual_table` soportan entidades híbridas EAV/rollup.
//! - `fuzzy` expande términos Damerau-Levenshtein-1 a regex exactas.
pub mod compiler;
pub(crate) mod cte_compiler;
pub(crate) mod dialect;
pub mod fuzzy;
pub(crate) mod metric_compiler;
pub mod registry;
pub(crate) mod security;
pub(crate) mod select_compiler;
pub(crate) mod virtual_table;
pub(crate) mod where_compiler;
