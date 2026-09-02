// quota/reservations.rs — El ticket de una reserva de IA, y quién lo guarda.
//
// POR QUÉ NO PUEDE VIVIR EN MEMORIA
// ─────────────────────────────────
// Hasta aquí las reservas estaban en un `HashMap` dentro de `QuotaServiceImpl`,
// con un recolector propio por proceso. Con más de una réplica eso se rompe de
// tres formas, y las tres cuestan cuota de verdad:
//
//   · La conciliación aterriza en otra réplica, que no encuentra el ticket,
//     asume que ya expiró y CARGA el consumo real — mientras la réplica que sí
//     lo tiene reintegra la estimación entera al vencer. Se devuelve de más.
//   · Un reinicio se lleva los tickets en vuelo: su débito queda aplicado para
//     siempre, hasta que rote el periodo. Cada despliegue filtra cuota.
//   · `Instant` es monotónico por proceso: ni se comparte ni sobrevive.
//
// Con el ticket en un store compartido, cualquier réplica puede cerrarlo, y el
// `claim` condicional decide cuál lo hace.
//
// ESTADOS
// ───────
// Solo hay dos, `Open` y `Closed`. «Conciliándose» no es un estado: es una
// reserva abierta con un lease vivo. Un estado intermedio explícito obligaría a
// devolverlo a `Open` cuando el lease vence, y ese paso extra es otra escritura
// que puede fallar; con el lease, que expire es simplemente que la condición
// del `claim` vuelva a ser cierta.
//
// EXACTAMENTE UNA VEZ
// ───────────────────
// El `claim` reparte el trabajo, no lo garantiza: entre ganar el lease y
// escribir en el contador cabe una muerte, y el siguiente que reclame no sabría
// si el apunte llegó a aplicarse. Por eso todo apunte ligado a una reserva pasa
// por `QuotaCounter::settle_once` con la clave `{id}#final`. El `claim` evita el
// trabajo repetido; la clave evita el apunte repetido.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::domain::errors::DomainError;

/// Particiones del índice de barrido.
///
/// Sin repartir, todas las réplicas consultarían la misma partición del GSI
/// para encontrar lo vencido, y el barrido se convertiría en el punto caliente
/// que viene a evitar.
pub const SWEEP_SHARDS: u8 = 16;

/// Estado de una reserva. «Conciliándose» es `Open` con `lease_until` vivo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationStatus {
    Open,
    Closed,
}

/// Por qué se cerró una reserva. Es traza, no lógica: nada la vuelve a leer
/// para decidir, pero sin ella no hay forma de distinguir en los logs una
/// conciliación normal de un reintegro por vencimiento.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// El cliente concilió a tiempo.
    Settled,
    /// Venció y el barrido devolvió los tokens.
    Expired,
    /// Venció sin que el débito llegara a aplicarse: no había nada que devolver.
    Abandoned,
    /// El techo rechazó el débito, así que la reserva nunca llegó a existir.
    Rejected,
}

impl CloseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseReason::Settled => "SETTLED",
            CloseReason::Expired => "EXPIRED",
            CloseReason::Abandoned => "ABANDONED",
            CloseReason::Rejected => "REJECTED",
        }
    }

    /// Una razón que no se reconoce se trata como `Settled`, que es la
    /// interpretación prudente: significa «no vuelvas a apuntar nada».
    pub fn parse(raw: &str) -> CloseReason {
        match raw {
            "EXPIRED" => CloseReason::Expired,
            "ABANDONED" => CloseReason::Abandoned,
            "REJECTED" => CloseReason::Rejected,
            _ => CloseReason::Settled,
        }
    }
}

/// Una reserva de tokens en vuelo.
#[derive(Debug, Clone, PartialEq)]
pub struct Reservation {
    /// ULID. El identificador que viaja al cliente lleva además el tenant
    /// delante; ver [`wire_id`].
    pub id: String,
    pub tenant_id: String,
    pub quota_id: String,
    /// Lo que se apuntó por adelantado y habrá que ajustar al conciliar.
    pub estimated: i64,
    /// Si el débito llegó a aplicarse en el contador.
    ///
    /// Es la diferencia entre devolver tokens y no devolver nada: una reserva
    /// abierta cuyo débito nunca se aplicó no tiene nada que reintegrar, y
    /// reintegrarla igualmente regalaría `estimated` tokens.
    pub debited: bool,
    /// Epoch en segundos a partir del cual el barrido puede recogerla.
    pub expires_at: i64,
}

/// Qué pasó al intentar hacerse cargo de una reserva.
///
/// Las tres primeras ramas llevan la reserva porque quien no pudo reclamarla
/// TAMBIÉN necesita saber de qué se trataba: una conciliación que llega tarde
/// tiene que poder distinguir «ya la concilió alguien» de «venció y se
/// devolvió», y decidir en consecuencia. Sin ese dato la única salida era
/// suponer, que es de donde salía la doble devolución.
#[derive(Debug, Clone, PartialEq)]
pub enum ClaimResult {
    /// Es tuya durante el lease.
    Claimed(Reservation),
    /// Otro la tiene ahora mismo. Reintentar dentro de un momento.
    Leased(Reservation),
    /// Ya se cerró, y por esta razón.
    Closed(Reservation, CloseReason),
    /// No existe: nunca se abrió, o el TTL ya la borró.
    NotFound,
}

/// Dónde viven las reservas en vuelo.
///
/// La implementación de producción es `dynamo_store::DynamoReservationStore`;
/// `MemoryReservationStore` sirve para tests y para un despliegue de una sola
/// réplica, donde compartir el ticket no aporta nada.
#[async_trait::async_trait]
pub trait ReservationStore: Send + Sync {
    /// Registra la reserva ANTES del débito.
    ///
    /// Este orden es la mitad del arreglo: si el proceso muere entre abrir y
    /// debitar, queda un ticket con `debited = false` que el barrido cierra sin
    /// tocar el contador. Al revés —debitar y luego registrar— la muerte deja
    /// un débito sin ticket que lo reclame, que es la fuga que había.
    async fn open(&self, reservation: &Reservation) -> Result<(), DomainError>;

    /// Apunta que el débito se aplicó.
    async fn mark_debited(&self, tenant_id: &str, id: &str) -> Result<(), DomainError>;

    /// Se hace cargo de la reserva durante `lease`, si nadie más la tiene.
    async fn claim(
        &self,
        tenant_id: &str,
        id: &str,
        lease: Duration,
    ) -> Result<ClaimResult, DomainError>;

    /// Cierra la reserva y la saca del índice de barrido.
    async fn close(
        &self,
        tenant_id: &str,
        id: &str,
        reason: CloseReason,
    ) -> Result<(), DomainError>;

    /// Reservas vencidas de una partición, para el barrido.
    async fn sweep(
        &self,
        shard: u8,
        now: i64,
        limit: i32,
    ) -> Result<Vec<Reservation>, DomainError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// El identificador que ve el cliente
// ─────────────────────────────────────────────────────────────────────────────

/// `{tenant}:{ulid}`.
///
/// El tenant va delante para poder localizar el item sin consultar nada —la
/// clave de DynamoDB está partida por tenant—, no como dato de confianza. Lo
/// que cuenta es el tenant guardado en el item; el del ticket solo dice dónde
/// buscar, y apuntar a otro sitio no encuentra nada.
///
/// El formato anterior era `{quota}:{estimated}:{tenant}:{uuid}`: llevaba
/// dentro las cifras con las que se conciliaba, sin firma, de modo que
/// cualquiera podía fabricar uno. Ahora el ticket no dice nada que se use para
/// calcular.
pub fn wire_id(tenant_id: &str, id: &str) -> String {
    format!("{tenant_id}:{id}")
}

/// Parte un identificador de reserva en (tenant, ulid).
///
/// Devuelve `None` para cualquier otra forma, incluida la de cuatro partes que
/// emitían las versiones anteriores: quien llama decide qué hacer con ella.
pub fn split_wire_id(wire: &str) -> Option<(&str, &str)> {
    let (tenant, id) = wire.split_once(':')?;
    if tenant.is_empty() || id.is_empty() || id.contains(':') {
        return None;
    }
    Some((tenant, id))
}

/// Clave de idempotencia del apunte con el que se cierra una reserva.
///
/// La comparten la conciliación y el barrido a propósito: son dos caminos al
/// MISMO apunte, y compartir clave es lo que impide que se aplique dos veces
/// cuando los dos creen que les toca. Cambiar una de las dos sin la otra
/// reabriría justo el agujero que esto cierra.
pub fn settlement_key(id: &str) -> String {
    format!("{id}#final")
}

/// Clave de idempotencia del cargo que llega DESPUÉS de cerrarse la reserva.
///
/// Es un apunte distinto del de cierre, y por eso lleva clave distinta: el de
/// cierre ya se aplicó —devolviendo la estimación al vencer—, y este apunta lo
/// que el modelo consumió de verdad. Compartir clave con `settlement_key` haría
/// que el segundo se descartara en silencio y el consumo no se cobrara nunca.
pub fn late_charge_key(id: &str) -> String {
    format!("{id}#late")
}

/// La partición de barrido de una reserva. Determinista y sin estado: el mismo
/// id cae siempre en la misma, la escriba quien la escriba.
pub fn shard_of(id: &str) -> u8 {
    // FNV-1a de 64 bits. No hace falta que sea criptográfico: solo tiene que
    // repartir, y el id ya es un ULID.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % SWEEP_SHARDS as u64) as u8
}

// ─────────────────────────────────────────────────────────────────────────────
// Store en memoria
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Stored {
    reservation: Reservation,
    status: ReservationStatus,
    lease_until: i64,
    close_reason: CloseReason,
}

/// Reservas en el proceso. Para tests y para despliegues de una sola réplica.
///
/// Cumple el mismo contrato que el store de DynamoDB, incluido el lease, así
/// que lo que se prueba contra este vale para el otro salvo en lo que solo
/// DynamoDB puede fallar: la contención real y el índice de barrido.
#[derive(Default)]
pub struct MemoryReservationStore {
    items: Mutex<HashMap<String, Stored>>,
}

impl MemoryReservationStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(tenant_id: &str, id: &str) -> String {
        format!("{tenant_id}#{id}")
    }

    /// Cuántas reservas hay abiertas. Solo para tests y para la métrica.
    pub fn open_count(&self) -> usize {
        self.items
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.status == ReservationStatus::Open)
            .count()
    }
}

#[async_trait::async_trait]
impl ReservationStore for MemoryReservationStore {
    async fn open(&self, reservation: &Reservation) -> Result<(), DomainError> {
        let mut items = self.items.lock().unwrap();
        items.insert(
            Self::key(&reservation.tenant_id, &reservation.id),
            Stored {
                reservation: reservation.clone(),
                status: ReservationStatus::Open,
                lease_until: 0,
                close_reason: CloseReason::Settled,
            },
        );
        Ok(())
    }

    async fn mark_debited(&self, tenant_id: &str, id: &str) -> Result<(), DomainError> {
        let mut items = self.items.lock().unwrap();
        if let Some(stored) = items.get_mut(&Self::key(tenant_id, id)) {
            stored.reservation.debited = true;
        }
        Ok(())
    }

    async fn claim(
        &self,
        tenant_id: &str,
        id: &str,
        lease: Duration,
    ) -> Result<ClaimResult, DomainError> {
        let now = chrono::Utc::now().timestamp();
        let mut items = self.items.lock().unwrap();

        let Some(stored) = items.get_mut(&Self::key(tenant_id, id)) else {
            return Ok(ClaimResult::NotFound);
        };
        if stored.status == ReservationStatus::Closed {
            return Ok(ClaimResult::Closed(stored.reservation.clone(), stored.close_reason));
        }
        if stored.lease_until > now {
            return Ok(ClaimResult::Leased(stored.reservation.clone()));
        }

        stored.lease_until = now + lease.as_secs() as i64;
        Ok(ClaimResult::Claimed(stored.reservation.clone()))
    }

    async fn close(
        &self,
        tenant_id: &str,
        id: &str,
        reason: CloseReason,
    ) -> Result<(), DomainError> {
        let mut items = self.items.lock().unwrap();
        if let Some(stored) = items.get_mut(&Self::key(tenant_id, id)) {
            stored.status = ReservationStatus::Closed;
            stored.close_reason = reason;
            stored.lease_until = 0;
        }
        Ok(())
    }

    async fn sweep(&self, shard: u8, now: i64, limit: i32) -> Result<Vec<Reservation>, DomainError> {
        let items = self.items.lock().unwrap();
        Ok(items
            .values()
            .filter(|s| s.status == ReservationStatus::Open)
            .filter(|s| s.reservation.expires_at < now)
            .filter(|s| shard_of(&s.reservation.id) == shard)
            .take(limit.max(0) as usize)
            .map(|s| s.reservation.clone())
            .collect())
    }
}

#[cfg(test)]
#[path = "tests/reservations_tests.rs"]
mod tests;
