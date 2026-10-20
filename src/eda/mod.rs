//! Arquitectura orientada a eventos (EDA): drenaje transaccional del Outbox y despacho asíncrono.
//!
//! El patrón Transactional Outbox completa su ciclo en este módulo: la entidad `outbox_event`
//! nace en `crate::eav::writer::outbox` dentro de la misma transacción ACID `.mbf` que la mutación
//! de negocio, y el submódulo `moira` la drena con semántica de reclamo atómico (`claim`),
//! reintento con *backoff* exponencial y *watchdog* de huérfanos hacia SQS FIFO / EventBridge.
pub mod moira;
