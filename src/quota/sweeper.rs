// quota/sweeper.rs — Quien recoge las reservas que nadie concilió.
//
// Sustituye al recolector que vivía dentro de `QuotaServiceImpl` recorriendo su
// propio `HashMap`. La diferencia no es de forma: aquel solo veía las reservas
// de su proceso, así que las de una réplica caída no las recogía nadie y su
// débito se quedaba aplicado hasta el cambio de periodo.
//
// Ahora las reservas vencidas están en un índice que todas las réplicas ven, y
// el trabajo se reparte solo: quien gana el `claim` la cierra, y los demás
// siguen. No hace falta elegir un líder ni coordinar nada.
//
// QUÉ HACE CON CADA UNA
// ─────────────────────
//   · con débito aplicado → devuelve `estimated` y la cierra como EXPIRED.
//   · sin débito aplicado → la cierra como ABANDONED, sin tocar el contador.
//     Es el caso de la muerte entre abrir la reserva y debitarla: no hay nada
//     que devolver, y devolverlo igualmente regalaría esos tokens.
//
// Si el apunte falla, la reserva NO se cierra: al vencer el lease vuelve a
// estar disponible y otro lo reintenta. Reintentarlo es seguro porque el apunte
// va con la clave de `settlement_key`, así que aunque el fallo fuera solo de
// respuesta y el apunte hubiera entrado, no se aplica dos veces.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};

use crate::quota::ledger::QuotaCounter;
use crate::quota::reservations::{
    settlement_key, ClaimResult, CloseReason, ReservationStore, SWEEP_SHARDS,
};

/// Cuánto tiempo se reserva el barredor una reserva para cerrarla.
///
/// Tiene que cubrir de sobra un apunte y un cierre. Si el proceso muere en
/// medio —o si el apunte falla—, esto es lo que tarda en poder recogerla otro.
const SWEEP_LEASE: Duration = Duration::from_secs(30);

/// Reservas vencidas que se recogen por partición y vuelta.
const SWEEP_BATCH: i32 = 25;

/// Qué hizo una vuelta del barrido. Se registra en el log de cada vuelta con
/// trabajo, y es lo que alimenta las métricas de la fase 5.
#[derive(Debug, Default, PartialEq)]
pub struct SweepReport {
    /// Vencidas con débito aplicado: tokens devueltos.
    pub refunded: usize,
    /// Vencidas sin débito: cerradas sin tocar el contador.
    pub abandoned: usize,
    /// De otro: ya cerradas, o con el lease en manos ajenas.
    pub skipped: usize,
    /// No se pudieron cerrar. Vuelven en la siguiente vuelta.
    pub failed: usize,
}

impl SweepReport {
    pub fn touched(&self) -> usize {
        self.refunded + self.abandoned + self.skipped + self.failed
    }
}

/// Una vuelta completa sobre las particiones indicadas.
///
/// Recibe `now` y `lease` en vez de leer el reloj y usar la constante para que
/// los tests puedan situarse después del vencimiento sin esperar — y comprobar
/// que una reserva que falló vuelve a estar disponible cuando el lease vence.
pub async fn sweep_once(
    store: &dyn ReservationStore,
    counter: &dyn QuotaCounter,
    shards: impl IntoIterator<Item = u8>,
    now: i64,
    lease: Duration,
) -> SweepReport {
    let mut report = SweepReport::default();

    for shard in shards {
        let vencidas = match store.sweep(shard, now, SWEEP_BATCH).await {
            Ok(v) => v,
            Err(e) => {
                warn!(shard, error = ?e, "[QuotaSweeper] No se pudo leer la partición");
                report.failed += 1;
                continue;
            }
        };

        for r in vencidas {
            // El `claim` es lo que reparte: si lo gana otro, esta réplica ni
            // siquiera intenta el apunte.
            match store.claim(&r.tenant_id, &r.id, lease).await {
                Ok(ClaimResult::Claimed(_)) => {}
                Ok(_) => {
                    report.skipped += 1;
                    continue;
                }
                Err(e) => {
                    warn!(reserva = %r.id, error = ?e, "[QuotaSweeper] No se pudo reclamar");
                    report.failed += 1;
                    continue;
                }
            }

            if r.debited {
                if let Err(e) = counter
                    .settle_once(
                        &r.tenant_id,
                        &r.quota_id,
                        -r.estimated,
                        &settlement_key(&r.id),
                    )
                    .await
                {
                    // Sin cerrar: al vencer el lease vuelve a estar disponible.
                    // La clave de idempotencia hace que reintentarlo sea seguro.
                    error!(
                        reserva = %r.id, cuota = %r.quota_id, error = ?e,
                        "[QuotaSweeper] No se pudo devolver la reserva vencida — se reintenta"
                    );
                    report.failed += 1;
                    continue;
                }
            }

            let reason = if r.debited {
                CloseReason::Expired
            } else {
                CloseReason::Abandoned
            };
            if let Err(e) = store.close(&r.tenant_id, &r.id, reason).await {
                // El apunte ya se aplicó y quedó marcado. Que el cierre falle
                // solo significa que la reserva volverá a aparecer vencida; el
                // siguiente intento encontrará la marca y no repetirá nada.
                warn!(
                    reserva = %r.id, error = ?e,
                    "[QuotaSweeper] Devuelta pero no cerrada — reaparecerá y no se repetirá el apunte"
                );
                report.failed += 1;
                continue;
            }

            if r.debited {
                info!(
                    reserva = %r.id, cuota = %r.quota_id, devuelto = r.estimated,
                    "[QuotaSweeper] Reserva vencida devuelta"
                );
                report.refunded += 1;
            } else {
                info!(
                    reserva = %r.id,
                    "[QuotaSweeper] Reserva vencida sin débito — cerrada sin tocar el contador"
                );
                report.abandoned += 1;
            }
        }
    }

    report
}

/// Todas las particiones, empezando por una al azar.
///
/// El arranque aleatorio evita que las réplicas recorran el índice en el mismo
/// orden y se encuentren siempre en la misma partición: chocarían en el
/// `claim`, que no da resultados incorrectos pero sí trabajo desperdiciado.
fn shard_rotation() -> impl Iterator<Item = u8> {
    let start = {
        use rand::Rng;
        rand::thread_rng().gen_range(0..SWEEP_SHARDS)
    };
    (0..SWEEP_SHARDS).map(move |i| (start + i) % SWEEP_SHARDS)
}

/// Período de barrido en producción. Los tests que necesiten otro ritmo
/// usan `spawn_with_period`; nada consulta el entorno en caliente.
pub const SWEEP_PERIOD: Duration = Duration::from_secs(15);

/// Lanza el barrido periódico. Una tarea por réplica; todas hacen lo mismo y
/// ninguna necesita saber de las otras.
pub fn spawn(store: Arc<dyn ReservationStore>, counter: Arc<dyn QuotaCounter>) {
    spawn_with_period(store, counter, SWEEP_PERIOD);
}

pub fn spawn_with_period(
    store: Arc<dyn ReservationStore>,
    counter: Arc<dyn QuotaCounter>,
    period: Duration,
) {
    tokio::spawn(async move {
        info!(
            cada_ms = period.as_millis(),
            particiones = SWEEP_SHARDS,
            "[QuotaSweeper] Barrido de reservas vencidas en marcha"
        );
        let mut interval = tokio::time::interval(period);
        loop {
            interval.tick().await;
            let now = chrono::Utc::now().timestamp();
            let report = sweep_once(
                store.as_ref(),
                counter.as_ref(),
                shard_rotation(),
                now,
                SWEEP_LEASE,
            )
            .await;
            if report.touched() > 0 {
                info!(?report, "[QuotaSweeper] Vuelta completada");
            }
        }
    });
}

#[cfg(test)]
#[path = "tests/sweeper_tests.rs"]
mod tests;
