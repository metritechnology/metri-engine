//! Compiladores y ejecutores analíticos y transaccionales sobre el motor unificado `.mbf` (Aegis).
//!
//! Submódulos principales:
//! - `mbf`: Ejecutor analítico vectorizado de latencia cero sobre el formato binario `.mbf` (`Metri Binary Format`).
//! - `oltp`: Planes físicos transaccionales y analíticos en memoria sobre datoms EAV / `.mbf`.
//! - `formula`: Motor de evaluación de fórmulas matemáticas sandboxeado (`lexer` -> `parser` -> `resolver` -> `evaluator`).
//! - `ast_ir`: Representación intermedia tipada (`AstIr`) desacoplada de JSON crudo.
//! - `pagination`: Codificación y decodificación de cursores opacos `Base64(offset:limit)`.
//! - `label_template`: Interpolación pura de plantillas estilo Mustache (`{{campo}}`).
//! - `temporal_bridge`: Puente entre los tipos temporales Protobuf/FlatBuffers y el módulo `temporal` canónico.
//!
//! # Garantías Arquitectónicas
//! - Ejecución unificada sobre `.mbf` con aislamiento criptográfico y estructural por `tenant_id`.
//! - Separación estricta entre compilación pura (sin I/O) y ejecución física.
pub mod ast_ir;
pub mod formula;
pub mod label_template;
pub mod mbf; // Ejecutor vectorizado sobre Metri Binary Format (.mbf)
pub mod oltp;
pub mod pagination; // Paginación cursor Base64(offset:limit)
pub mod temporal_bridge; // Bridge proto/FBS → temporal canónico

#[cfg(test)]
mod testing;
