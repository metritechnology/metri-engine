// Contra DynamoDB Local. `make infra` levanta el contenedor y crea la tabla con
// su GSI-SWEEP; se ejecutan con `make test-integration`.
//
// Lo que se prueba aquí y no se puede probar con el store en memoria es
// justamente lo que hace que este sirva: que la condición del `claim` la evalúe
// DynamoDB —y no un `Mutex` de este proceso—, y que el índice de barrido
// contenga solo lo que falta por cerrar.

use super::*;

use crate::quota::reservations::{
    ClaimResult, CloseReason, Reservation, ReservationStore, SWEEP_SHARDS,
};

const TABLA: &str = "metri-quota-local";

async fn store() -> DynamoReservationStore {
    std::env::set_var("DYNAMODB_ENDPOINT", "http://localhost:8000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    DynamoReservationStore::new(Arc::new(DynamoClient::new(TABLA).await), TABLA)
}

fn ahora() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Cada test estrena reserva: son repetibles sin limpiar la tabla.
fn nueva(tenant: &str, estimated: i64, expires_at: i64) -> Reservation {
    Reservation {
        id: ulid::Ulid::new().to_string(),
        tenant_id: tenant.to_string(),
        quota_id: "q_test".to_string(),
        estimated,
        debited: false,
        expires_at,
    }
}

/// Busca una reserva concreta recorriendo todas las particiones.
async fn buscar(s: &DynamoReservationStore, id: &str, now: i64) -> bool {
    for shard in 0..SWEEP_SHARDS {
        if s.sweep(shard, now, 100)
            .await
            .unwrap()
            .iter()
            .any(|r| r.id == id)
        {
            return true;
        }
    }
    false
}

#[tokio::test]
#[ignore]
async fn el_ciclo_completo_de_una_reserva() {
    let s = store().await;
    let r = nueva("tnt_test", 400, ahora() + 90);

    s.open(&r).await.unwrap();
    s.mark_debited(&r.tenant_id, &r.id).await.unwrap();

    match s
        .claim(&r.tenant_id, &r.id, Duration::from_secs(30))
        .await
        .unwrap()
    {
        ClaimResult::Claimed(leida) => {
            assert_eq!(leida.estimated, 400);
            assert_eq!(leida.quota_id, "q_test");
            assert!(leida.debited, "la marca del débito viaja con la reserva");
        }
        otro => panic!("se esperaba reclamada, fue {otro:?}"),
    }

    s.close(&r.tenant_id, &r.id, CloseReason::Settled)
        .await
        .unwrap();

    match s
        .claim(&r.tenant_id, &r.id, Duration::from_secs(30))
        .await
        .unwrap()
    {
        ClaimResult::Closed(_, razon) => assert_eq!(razon, CloseReason::Settled),
        otro => panic!("se esperaba cerrada, fue {otro:?}"),
    }
}

/// La condición del `claim` la evalúa DynamoDB dentro de la escritura, así que
/// dos réplicas que la pidan a la vez no pueden ganarla las dos. Es lo que
/// reparte el trabajo del barrido sin coordinar nada.
#[tokio::test]
#[ignore]
async fn dos_pretendientes_simultaneos_y_solo_uno_gana() {
    let s = store().await;
    // Tenant propio del test: los claim son por partición (tenant, quota) y
    // otros tests en paralelo no deben compartir el lease.
    let r = nueva(
        &format!("tnt_claim_{}", ulid::Ulid::new()),
        400,
        ahora() + 90,
    );
    s.open(&r).await.unwrap();

    let (a, b) = tokio::join!(
        s.claim(&r.tenant_id, &r.id, Duration::from_secs(30)),
        s.claim(&r.tenant_id, &r.id, Duration::from_secs(30)),
    );

    let ganadores = [a.unwrap(), b.unwrap()]
        .iter()
        .filter(|c| matches!(c, ClaimResult::Claimed(_)))
        .count();
    assert_eq!(ganadores, 1, "solo uno se la lleva");
}

/// Que el lease venza basta para liberarla: no hay ninguna escritura de vuelta
/// que pueda fallar y dejarla bloqueada para siempre.
#[tokio::test]
#[ignore]
async fn un_lease_de_cero_la_deja_libre_al_instante() {
    let s = store().await;
    // Tenant propio: un lease de otro test en paralelo no debe bloquearlo.
    let r = nueva(
        &format!("tnt_lease_{}", ulid::Ulid::new()),
        400,
        ahora() + 90,
    );
    s.open(&r).await.unwrap();

    s.claim(&r.tenant_id, &r.id, Duration::from_secs(0))
        .await
        .unwrap();
    // Los leases vencen con granularidad de segundos: esperar a cruzar la
    // frontera del segundo para que el lease de duración 0 esté vencido de
    // verdad (sin esto el test era una moneda al aire).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let segunda = s
        .claim(&r.tenant_id, &r.id, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(
        matches!(segunda, ClaimResult::Claimed(_)),
        "fue {segunda:?}"
    );
}

/// El índice solo contiene lo que falta por cerrar: al cerrar se borran sus
/// claves. Si no, crecería con todo el histórico y el barrido se volvería más
/// caro cada día.
#[tokio::test]
#[ignore]
async fn cerrar_una_reserva_la_saca_del_indice_de_barrido() {
    let s = store().await;
    let vencida = nueva("tnt_test", 400, ahora() - 1);
    s.open(&vencida).await.unwrap();

    assert!(
        buscar(&s, &vencida.id, ahora()).await,
        "vencida y abierta: aparece"
    );

    s.close(&vencida.tenant_id, &vencida.id, CloseReason::Expired)
        .await
        .unwrap();
    assert!(!buscar(&s, &vencida.id, ahora()).await, "cerrada: ya no");
}

#[tokio::test]
#[ignore]
async fn lo_que_no_ha_vencido_no_sale_en_el_indice() {
    let s = store().await;
    let viva = nueva("tnt_test", 400, ahora() + 3600);
    s.open(&viva).await.unwrap();

    assert!(!buscar(&s, &viva.id, ahora()).await);
    assert!(
        buscar(&s, &viva.id, ahora() + 7200).await,
        "cuando pase su hora, sí"
    );
}

#[tokio::test]
#[ignore]
async fn una_reserva_inexistente_se_distingue_de_una_cerrada() {
    let s = store().await;
    let inventada = ulid::Ulid::new().to_string();
    let r = s
        .claim("tnt_test", &inventada, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(r, ClaimResult::NotFound);
}

/// El tenant del ticket solo dice dónde buscar. Apuntar a otro no encuentra
/// nada, ni siquiera con un id válido.
#[tokio::test]
#[ignore]
async fn la_reserva_de_un_tenant_no_aparece_bajo_otro() {
    let s = store().await;
    let r = nueva("tnt_dueño", 400, ahora() + 90);
    s.open(&r).await.unwrap();

    let ajena = s
        .claim("tnt_otro", &r.id, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(ajena, ClaimResult::NotFound);
}
