use super::*;

use crate::quota::reservations::{MemoryReservationStore, Reservation};
use crate::quota::test_fakes::FakeCounter;

fn ahora() -> i64 {
    chrono::Utc::now().timestamp()
}

fn vencida(id: &str, estimated: i64, debited: bool) -> Reservation {
    Reservation {
        id: id.to_string(),
        tenant_id: "tnt_01".to_string(),
        quota_id: "q_01".to_string(),
        estimated,
        debited,
        expires_at: ahora() - 1,
    }
}

/// Todas las particiones, que es lo que hace una vuelta real.
fn todas() -> impl Iterator<Item = u8> {
    0..SWEEP_SHARDS
}

/// El caso que justifica el barrido: alguien reservó, se murió sin conciliar, y
/// esos tokens tienen que volver. Antes solo los devolvía la réplica que los
/// había reservado; si era ella la que moría, no los devolvía nadie.
#[tokio::test]
async fn una_reserva_vencida_con_debito_se_devuelve() {
    let store = MemoryReservationStore::new();
    let counter = FakeCounter::at("q_01", 100);

    let r = vencida("A", 40, true);
    store.open(&r).await.unwrap();
    store.mark_debited("tnt_01", "A").await.unwrap();

    let report = sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)).await;

    assert_eq!(report.refunded, 1);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(60),
        "devolvió los 40 apuntados"
    );
    assert_eq!(store.open_count(), 0, "y la cerró");
}

/// La otra mitad del arreglo: una reserva que venció ANTES de que el débito se
/// aplicara no tiene nada que devolver. Devolverle la estimación igualmente
/// —que es lo que haría un barrido que no mirase esto— regalaría esos tokens.
#[tokio::test]
async fn una_reserva_vencida_sin_debito_no_toca_el_contador() {
    let store = MemoryReservationStore::new();
    let counter = FakeCounter::at("q_01", 100);

    store.open(&vencida("A", 40, false)).await.unwrap();

    let report = sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)).await;

    assert_eq!(report.abandoned, 1);
    assert_eq!(report.refunded, 0);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(100),
        "el contador no se movió"
    );
    assert_eq!(store.open_count(), 0);
}

/// Dos réplicas barriendo a la vez es lo normal, no la excepción: todas
/// ejecutan el mismo bucle. El `claim` reparte el trabajo y la clave de
/// idempotencia respalda el reparto.
#[tokio::test]
async fn dos_barridos_simultaneos_devuelven_una_sola_vez() {
    let store = MemoryReservationStore::new();
    let counter = FakeCounter::at("q_01", 100);

    store.open(&vencida("A", 40, true)).await.unwrap();
    store.mark_debited("tnt_01", "A").await.unwrap();

    let (a, b) = tokio::join!(
        sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)),
        sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)),
    );

    assert_eq!(a.refunded + b.refunded, 1, "solo uno la devuelve");
    assert_eq!(counter.usage_of("q_01"), Some(60));
}

/// Y una segunda vuelta después de cerrarla tampoco la vuelve a tocar.
#[tokio::test]
async fn barrer_dos_veces_no_devuelve_dos_veces() {
    let store = MemoryReservationStore::new();
    let counter = FakeCounter::at("q_01", 100);

    store.open(&vencida("A", 40, true)).await.unwrap();
    store.mark_debited("tnt_01", "A").await.unwrap();

    sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)).await;
    let segunda = sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)).await;

    assert_eq!(segunda.touched(), 0);
    assert_eq!(counter.usage_of("q_01"), Some(60));
}

/// Si el apunte falla, la reserva NO se cierra: tiene que volver a aparecer.
/// Cerrarla sería dar por devuelto algo que no se devolvió.
#[tokio::test]
async fn si_el_apunte_falla_la_reserva_sigue_pendiente() {
    let store = MemoryReservationStore::new();
    let counter = FakeCounter::at("q_01", 100);
    counter.break_it("contador caído");

    store.open(&vencida("A", 40, true)).await.unwrap();
    store.mark_debited("tnt_01", "A").await.unwrap();

    // Lease de cero para no tener que esperar a que venza el de la vuelta que
    // falla: en producción son 30 s, y ese es justo el retardo del reintento.
    let report = sweep_once(&store, &counter, todas(), ahora(), Duration::ZERO).await;
    assert_eq!(report.failed, 1);
    assert_eq!(report.refunded, 0);
    assert_eq!(
        store.open_count(),
        1,
        "sigue abierta para el siguiente intento"
    );

    // Cuando el contador vuelve, la recoge la siguiente vuelta.
    counter.fix_it();
    let segunda = sweep_once(&store, &counter, todas(), ahora(), Duration::ZERO).await;
    assert_eq!(segunda.refunded, 1);
    assert_eq!(counter.usage_of("q_01"), Some(60));
}

#[tokio::test]
async fn lo_que_no_ha_vencido_no_se_toca() {
    let store = MemoryReservationStore::new();
    let counter = FakeCounter::at("q_01", 100);

    let mut viva = vencida("A", 40, true);
    viva.expires_at = ahora() + 300;
    store.open(&viva).await.unwrap();
    store.mark_debited("tnt_01", "A").await.unwrap();

    let report = sweep_once(&store, &counter, todas(), ahora(), Duration::from_secs(30)).await;

    assert_eq!(report.touched(), 0);
    assert_eq!(counter.usage_of("q_01"), Some(100));
    assert_eq!(store.open_count(), 1);
}
