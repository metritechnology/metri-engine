// Include del código generado por flatc para el AST IR.
// El schema está en src/janus/janus_ir_ast.fbs y se compila
// vía build.rs automáticamente.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(clippy::all)]
// Los lints de restricción (unwrap_used…) NO vienen en clippy::all: el código
// generado por flatc usa unwrap() por diseño y queda exento (PLAN_PATRON_RESULT.md §4.4).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

include!(concat!(env!("OUT_DIR"), "/janus_ir_ast_generated.rs"));

// Re-exportamos los tipos principales para facilitar su uso
pub use metres::eav::*;
