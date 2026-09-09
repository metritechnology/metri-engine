//! Domain event contract — producer side of metri-contracts.
//!
//! domain/events — el contrato de eventos de dominio, del lado del productor.
//!
//! metri-schedulers (Go) parsea estos sobres con un contrato estricto: un campo
//! ausente no es ruido, es un evento que cae a DLQ o un ledger que no puede
//! ordenar. Los tipos de este módulo son el espejo Rust de
//! `metri-contracts/schema/envelope.schema.json`; los fixtures dorados de
//! `metri-contracts/golden/` son el candado que impide que los dos lados
//! diverjan (golden_test.rs).
//!
//! DIP: este módulo no conoce DatomValue ni DynamoDB — trabaja con
//! serde_json::Value. La conversión desde el almacén EAV vive en la
//! infraestructura (infrastructure/domain_event_bus.rs).

pub mod delta;
pub mod envelope;

// El candado golden solo compila donde existe el repo hermano metri-contracts
// (cfg lo emite build.rs); en CI y clones sin el hermano se excluye.
#[cfg(all(test, has_metri_contracts))]
#[path = "golden_test.rs"]
mod golden_test;

pub use delta::{compute_delta, is_bookkeeping_only, Delta, DeltaEntry};
pub use envelope::{DomainEnvelope, ScheduledJobEventInput, ScheduledJobOp, SCHEMA_VERSION};
