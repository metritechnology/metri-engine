//! Contrato de eventos de dominio — lado productor de `metri-contracts` v1.0.
//!
//! `metri-schedulers` (Go) parsea estos sobres bajo un contrato estricto: un campo
//! ausente envía el evento a DLQ o impide el ordenamiento del ledger.
//! Los tipos de este módulo reflejan `metri-contracts/schema/envelope.schema.json`,
//! verificados por los fixtures dorados en `golden_test.rs`.
//!
//! Siguiendo DIP, este módulo es independiente de `DatomValue` y del motor de
//! almacenamiento subyacente: opera exclusivamente con `serde_json::Value`.

/// Cálculo de diferencias (`Delta`, `DeltaEntry`) y clasificación de cambios de bitácora.
pub mod delta;
/// Estructura del sobre de evento `DomainEnvelope` y validación fail-closed.
pub mod envelope;

// El candado golden solo compila donde existe el repo hermano metri-contracts
// (cfg lo emite build.rs); en CI y clones sin el hermano se excluye.
#[cfg(all(test, has_metri_contracts))]
#[path = "golden_test.rs"]
mod golden_test;

pub use delta::{compute_delta, is_bookkeeping_only, Delta, DeltaEntry};
pub use envelope::{DomainEnvelope, ScheduledJobEventInput, ScheduledJobOp, SCHEMA_VERSION};
