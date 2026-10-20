//! Pipeline de Orquestación de Ingesta (`iop`) — cadena secuencial Cedar → Quota → Janus.
//!
//! `pipeline` implementa el motor Railway (cortocircuito al primer error) y
//! `core` contiene el orquestador raíz (`IopOrchestrator`) con compensación inversa;
//! cada paso (`cedar_step`, `quota_step`, `janus_step`) encapsula una única
//! responsabilidad. `error_response` traduce errores de dominio a respuestas gRPC
//! forenses con redacción de campos sensibles y `sherlog` reporta fallas al canal EDA.
pub mod cedar_step;
pub mod core;
pub mod error_response;
pub mod janus_step;
pub mod pipeline;
pub mod quota_step;
pub mod sherlog;
