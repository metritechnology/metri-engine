//! Tests for `quota::reservations`.
use super::*;

fn reserva(id: &str, estimated: i64, expires_at: i64) -> Reservation {
    Reservation {
        id: id.to_string(),
        tenant_id: "tnt_01".to_string(),
        quota_id: "q_01".to_string(),
        estimated,
        debited: false,
        expires_at,
    }
}

fn ahora() -> i64 {
    chrono::Utc::now().timestamp()
}

// ─────────────────────────────────────────────────────────────────────────────
// El identificador que ve el cliente
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn el_ticket_se_compone_y_se_parte() {
    let wire = wire_id("tnt_01", "01J9ABC");
    assert_eq!(wire, "tnt_01:01J9ABC");
    assert_eq!(split_wire_id(&wire), Some(("tnt_01", "01J9ABC")));
}

/// El formato anterior —`{cuota}:{estimado}:{tenant}:{uuid}`— tiene que quedar
/// fuera para que la conciliación pueda distinguirlo y tratarlo aparte durante
/// el despliegue. Que se colara como ticket nuevo haría buscar una reserva con
/// un id que no existe.
#[test]
fn el_formato_anterior_no_se_confunde_con_el_nuevo() {
    assert_eq!(split_wire_id("q_01:4000:tnt_01:uuid-xyz"), None);
    assert_eq!(split_wire_id("sin-separador"), None);
    assert_eq!(split_wire_id(":01J9"), None);
    assert_eq!(split_wire_id("tnt_01:"), None);
}

/// Las dos claves tienen que ser distintas: una cierra la reserva y la otra
/// apunta lo consumido cuando ya se cerró. Compartirlas haría que el segundo
/// apunte se descartara en silencio.
#[test]
fn las_claves_de_cierre_y_de_cargo_tardio_no_coinciden() {
    assert_ne!(settlement_key("01J9"), late_charge_key("01J9"));
}

#[test]
fn el_reparto_en_particiones_es_estable_y_cabe_en_el_rango() {
    for i in 0..500 {
        let id = format!("01J9ABCDEF{i}");
        let s = shard_of(&id);
        assert!(s < SWEEP_SHARDS);
        assert_eq!(s, shard_of(&id), "el mismo id cae siempre en la misma");
    }

    // Y reparte: con 500 ids no puede caer todo en una.
    let distintas: std::collections::HashSet<u8> = (0..500)
        .map(|i| shard_of(&format!("01J9ABCDEF{i}")))
        .collect();
    assert!(
        distintas.len() > 8,
        "reparto pobre: {} particiones",
        distintas.len()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Contrato del store
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn una_reserva_abierta_se_puede_reclamar_una_vez() {
    let store = MemoryReservationStore::new();
    store.open(&reserva("A", 100, ahora() + 90)).await.unwrap();

    let primera = store
        .claim("tnt_01", "A", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(matches!(primera, ClaimResult::Claimed(_)));

    // El segundo llega mientras el lease está vivo.
    let segunda = store
        .claim("tnt_01", "A", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(
        matches!(segunda, ClaimResult::Leased(_)),
        "pero fue {segunda:?}"
    );
}

/// Que el lease venza es, por sí solo, que la reserva vuelva a estar libre. No
/// hay ninguna escritura que la devuelva a su sitio, y por tanto ninguna que
/// pueda fallar dejándola bloqueada para siempre.
#[tokio::test]
async fn un_lease_vencido_la_libera_sin_intervencion() {
    let store = MemoryReservationStore::new();
    store.open(&reserva("A", 100, ahora() + 90)).await.unwrap();

    store
        .claim("tnt_01", "A", Duration::from_secs(0))
        .await
        .unwrap();
    let otra = store
        .claim("tnt_01", "A", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(matches!(otra, ClaimResult::Claimed(_)), "pero fue {otra:?}");
}

/// Quien llega tarde necesita saber POR QUÉ se cerró: devolver lo mismo para
/// «ya se concilió» y «venció y se devolvió» es lo que hacía que el consumo se
/// apuntara dos veces, o ninguna.
#[tokio::test]
async fn una_reserva_cerrada_dice_por_que() {
    let store = MemoryReservationStore::new();
    store.open(&reserva("A", 100, ahora() + 90)).await.unwrap();
    store
        .close("tnt_01", "A", CloseReason::Expired)
        .await
        .unwrap();

    match store
        .claim("tnt_01", "A", Duration::from_secs(30))
        .await
        .unwrap()
    {
        ClaimResult::Closed(r, razon) => {
            assert_eq!(razon, CloseReason::Expired);
            assert_eq!(r.estimated, 100, "y con qué se abrió");
        }
        otro => panic!("se esperaba cerrada, fue {otro:?}"),
    }
}

#[tokio::test]
async fn una_reserva_que_no_existe_se_distingue_de_una_cerrada() {
    let store = MemoryReservationStore::new();
    let r = store
        .claim("tnt_01", "fantasma", Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(r, ClaimResult::NotFound);
}

#[tokio::test]
async fn el_debito_se_marca_y_se_ve_al_reclamar() {
    let store = MemoryReservationStore::new();
    store.open(&reserva("A", 100, ahora() + 90)).await.unwrap();
    store.mark_debited("tnt_01", "A").await.unwrap();

    match store
        .claim("tnt_01", "A", Duration::from_secs(30))
        .await
        .unwrap()
    {
        ClaimResult::Claimed(r) => assert!(r.debited),
        otro => panic!("se esperaba reclamada, fue {otro:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Barrido
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn el_barrido_solo_ve_lo_vencido_y_abierto() {
    let store = MemoryReservationStore::new();
    let ahora = ahora();

    let vencida = reserva("vencida", 10, ahora - 1);
    let viva = reserva("viva", 10, ahora + 300);
    let cerrada = reserva("cerrada", 10, ahora - 1);
    store.open(&vencida).await.unwrap();
    store.open(&viva).await.unwrap();
    store.open(&cerrada).await.unwrap();
    store
        .close("tnt_01", "cerrada", CloseReason::Settled)
        .await
        .unwrap();

    let mut encontradas = Vec::new();
    for shard in 0..SWEEP_SHARDS {
        encontradas.extend(store.sweep(shard, ahora, 25).await.unwrap());
    }

    let ids: Vec<&str> = encontradas.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["vencida"]);
}

/// Cada reserva aparece en su partición y solo en la suya: si el barrido de una
/// réplica no cubre todas, ninguna reserva puede quedar en tierra de nadie.
#[tokio::test]
async fn cada_reserva_vive_en_una_sola_particion() {
    let store = MemoryReservationStore::new();
    let ahora = ahora();
    for i in 0..40 {
        store
            .open(&reserva(&format!("r{i}"), 10, ahora - 1))
            .await
            .unwrap();
    }

    let mut total = 0;
    for shard in 0..SWEEP_SHARDS {
        total += store.sweep(shard, ahora, 100).await.unwrap().len();
    }
    assert_eq!(total, 40);
}
