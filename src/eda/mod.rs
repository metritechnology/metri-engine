//! eda — event-driven architecture: outbox drainage and fault reporting.
//!
//! El patrón outbox completa su ciclo aquí: la fila `outbox_event` nace en
//! [`crate::eav::writer::outbox`] dentro de la transacción EAV y [`moira`]
//! la drena con su semántica de reintento hacia EventBridge/SQS.
//! Por qué outbox y no publicación directa: el evento no puede perderse —
//! nace en la misma transacción que la mutación que lo causa.
pub mod moira;
