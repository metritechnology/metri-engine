//! Envoltorio del código Rust autogenerado por `flatc` a partir de `src/janus/janus_ir_ast.fbs`.
//!
//! Expone las estructuras zero-copy de FlatBuffers utilizadas como contrato IR
//! entre el compilador de consultas (`janus`) y los ejecutores (`aegis`).

#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(clippy::all)]
// Los lints de restricción (unwrap_used…) NO vienen en clippy::all: el código
// generado por flatc usa unwrap() y unsafe por diseño y queda exento.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, unsafe_code)]

include!(concat!(env!("OUT_DIR"), "/janus_ir_ast_generated.rs"));

// Re-exportamos los tipos principales para facilitar su uso
pub use metres::eav::*;
