// Include del código generado por flatc para el AST IR.
// El schema está en src/janus/janus_ir_ast.fbs y se compila
// vía build.rs automáticamente.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/janus_ir_ast_generated.rs"));

// Re-exportamos los tipos principales para facilitar su uso
pub use metres::eav::*;
