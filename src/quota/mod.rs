// quota/ — Todo lo que decide si un tenant puede consumir, en un solo sitio.
//
// Antes esto vivía repartido: el contador atómico en `eav/writer/counter.rs`,
// y la resolución de «qué cuota aplica hoy» duplicada en `iop/quota_step.rs` y
// en `grpc/quota_service.rs`. Las dos copias ya habían divergido —una leía
// `period_key` y la otra `domain_quota/period_key`, y solo una de ellas
// contemplaba las dos formas—, que es exactamente lo que pasa con la lógica
// duplicada cuando nadie la mira.
//
// Reparto de responsabilidades:
//
//   · `resolver`     — qué fila `domain_quota` gobierna esta operación y con
//     qué techo. Es una LECTURA: sirve para localizar la cuota, nunca para
//     decidir.
//   · `ledger`       — el contador con autoridad. Techo y débito en la misma
//     escritura condicional, así que decidir con él no tiene ventana de carrera.
//   · `reservations` — el ticket de una reserva de IA, que sobrevive al proceso
//     que lo abrió para que cualquier réplica pueda cerrarlo.
//   · `dynamo_store` — dónde viven esos tickets en producción.
//   · `projection`   — `current_usage` superpuesto en lectura desde el contador,
//     para que nadie tenga que escribirlo.
//   · `sweeper`      — quien recoge los que nadie concilió.
//
// La distinción importa: leer `current_usage` y compararlo con `max_limit` en
// memoria es la carrera que este módulo existe para no volver a tener.

#[cfg(test)]
#[path = "tests/fakes.rs"]
pub mod test_fakes;

pub mod dynamo_store;
pub mod ledger;
pub mod projection;
pub mod reservations;
pub mod resolver;
pub mod sweeper;

pub use dynamo_store::DynamoReservationStore;
pub use ledger::{DebitOutcome, QuotaCounter, QuotaLedger, SettleOutcome};
pub use projection::QuotaUsageOverlay;
pub use reservations::{
    settlement_key, split_wire_id, wire_id, ClaimResult, CloseReason, MemoryReservationStore,
    Reservation, ReservationStore,
};
pub use resolver::{OltpQueryRunner, QuotaResolver, QuotaSpec};
