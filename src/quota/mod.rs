//! Subsistema de control de cuotas (`quota`) — gobierno atómico de límites de recursos por tenant.
//!
//! Reparto de responsabilidades:
//!
//! · `resolver`     — qué fila `domain_quota` gobierna esta operación y con
//! qué techo. Es una LECTURA: sirve para localizar la cuota, nunca para
//! decidir.
//! · `ledger`       — el contador con autoridad. Techo y débito en la misma
//! escritura condicional sobre `MbfStorageClient`, sin ventana de carrera.
//! · `reservations` — el ticket de una reserva de IA, que sobrevive al proceso
//! que lo abrió para que cualquier réplica pueda cerrarlo.
//! · `projection`   — `current_usage` superpuesto en lectura desde el contador,
//! para que nadie tenga que escribirlo en el log de datoms.
//! · `sweeper`      — recolector periódico de reservas expiradas no conciliadas.
//!
//! La distinción importa: leer `current_usage` y compararlo con `max_limit` en
//! memoria es la carrera que este módulo existe para no volver a tener.

#[cfg(test)]
#[path = "tests/fakes.rs"]
pub mod test_fakes;

pub mod ledger;
pub mod projection;
pub mod reservations;
pub mod resolver;
pub mod sweeper;

pub use ledger::{DebitOutcome, QuotaCounter, QuotaLedger, SettleOutcome};
pub use projection::QuotaUsageOverlay;
pub use reservations::{
    settlement_key, split_wire_id, wire_id, ClaimResult, CloseReason, MemoryReservationStore,
    Reservation, ReservationStore,
};
pub use resolver::{OltpQueryRunner, QuotaResolver, QuotaSpec};
