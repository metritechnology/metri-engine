//! Tests for `grpc::quota_service`.
use super::*;

use serde_json::Value;

use crate::quota::sweeper::sweep_once;
use crate::quota::test_fakes::FakeCounter;
use crate::quota::MemoryReservationStore;

// ─────────────────────────────────────────────────────────────────────────────
// Andamiaje
// ─────────────────────────────────────────────────────────────────────────────

struct MockOltp {
    filas: Vec<Value>,
}

#[async_trait::async_trait]
impl OltpQueryRunner for MockOltp {
    async fn run_oltp_query(
        &self,
        _tenant_id: &str,
        _ast_ir: &Value,
    ) -> Result<Value, crate::domain::errors::DomainError> {
        Ok(Value::Array(self.filas.clone()))
    }
}

type Servicio = QuotaServiceImpl<MockOltp>;

fn cuota(max_limit: i64, current_usage: i64) -> Value {
    json!({
        "id": "q_01",
        "max_limit": max_limit,
        "current_usage": current_usage,
        "period_key": "LIFETIME"
    })
}

/// Una réplica del motor: comparte store y contador con las demás, que es
/// exactamente lo que no pasaba antes.
fn replica(
    store: Arc<dyn ReservationStore>,
    counter: Arc<FakeCounter>,
    filas: Vec<Value>,
) -> Servicio {
    QuotaServiceImpl::with_parts(MockOltp { filas }, counter, store, 90)
}

fn con_sesion<T>(body: T, tenant: &str, user: &str) -> Request<T> {
    let mut req = Request::new(body);
    req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant.to_string(),
        user_id: user.to_string(),
        jti: "jti_test".to_string(),
    });
    req
}

fn reserva_de(tenant: &str, estimated: i64) -> Request<ReserveTokensRequest> {
    con_sesion(
        ReserveTokensRequest {
            tenant_id: tenant.to_string(),
            model_urn: "llm:aws:nova-pro".to_string(),
            estimated_tokens: estimated,
        },
        tenant,
        "usr_01",
    )
}

fn conciliacion_de(
    tenant: &str,
    reservation_id: &str,
    input: i64,
    output: i64,
) -> Request<ReconcileTokensRequest> {
    con_sesion(
        ReconcileTokensRequest {
            reservation_id: reservation_id.to_string(),
            actual_input_tokens: input,
            actual_output_tokens: output,
        },
        tenant,
        "usr_01",
    )
}

async fn reservar(servicio: &Servicio, tenant: &str, estimated: i64) -> ReserveTokensResponse {
    servicio
        .reserve_tokens(reserva_de(tenant, estimated))
        .await
        .expect("la reserva no debería fallar")
        .into_inner()
}

// ─────────────────────────────────────────────────────────────────────────────
// El fallo que motivó la fase 2
// ─────────────────────────────────────────────────────────────────────────────

/// Reserva en una réplica, conciliación en otra.
///
/// Con las reservas en memoria, la réplica B no encontraba el ticket, asumía
/// que había expirado y CARGABA el consumo real — mientras la réplica A
/// reintegraba además la estimación entera al vencer. El contador acababa por
/// debajo de la realidad y el tenant recuperaba cuota que había gastado.
#[tokio::test]
async fn reservar_en_una_replica_y_conciliar_en_otra_cuadra_la_cuenta() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());

    let replica_a = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );
    let replica_b = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let reserva = reservar(&replica_a, "tnt_01", 400).await;
    assert!(reserva.status.unwrap().success);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(400),
        "la estimación queda apuntada"
    );

    let respuesta = replica_b
        .reconcile_tokens(conciliacion_de("tnt_01", &reserva.reservation_id, 100, 50))
        .await
        .expect("la otra réplica también puede conciliar")
        .into_inner();

    assert!(respuesta.status.unwrap().success);
    assert_eq!(respuesta.tokens_consumed, 150);
    assert_eq!(respuesta.tokens_returned, 250);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(150),
        "solo lo consumido de verdad"
    );
}

/// Y el barrido posterior no vuelve a tocarla: está cerrada.
#[tokio::test]
async fn tras_conciliar_el_barrido_no_devuelve_nada() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let reserva = reservar(&servicio, "tnt_01", 400).await;
    servicio
        .reconcile_tokens(conciliacion_de("tnt_01", &reserva.reservation_id, 150, 0))
        .await
        .unwrap();

    let futuro = chrono::Utc::now().timestamp() + 10_000;
    let report = sweep_once(
        store.as_ref(),
        counter.as_ref(),
        0..crate::quota::reservations::SWEEP_SHARDS,
        futuro,
        Duration::from_secs(30),
    )
    .await;

    assert_eq!(report.touched(), 0);
    assert_eq!(counter.usage_of("q_01"), Some(150));
}

/// Un reintento del cliente no puede contar dos veces.
#[tokio::test]
async fn conciliar_dos_veces_no_cambia_la_cuenta() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let reserva = reservar(&servicio, "tnt_01", 400).await;
    servicio
        .reconcile_tokens(conciliacion_de("tnt_01", &reserva.reservation_id, 150, 0))
        .await
        .unwrap();
    let segunda = servicio
        .reconcile_tokens(conciliacion_de("tnt_01", &reserva.reservation_id, 150, 0))
        .await
        .expect("un reintento no es un error")
        .into_inner();

    assert!(segunda.status.unwrap().success);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(150),
        "el contador no se movió"
    );
}

/// La conciliación que llega después de que el barrido devolviera la reserva.
///
/// Aquí está la razón de que el store guarde POR QUÉ se cerró: la estimación ya
/// se devolvió entera, así que lo único que falta por apuntar es lo que el
/// modelo gastó de verdad. Sin distinguir el motivo, este caso o no cobraba
/// nada o cobraba dos veces.
#[tokio::test]
async fn una_conciliacion_tardia_apunta_lo_consumido_tras_el_reintegro() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let reserva = reservar(&servicio, "tnt_01", 400).await;
    assert_eq!(counter.usage_of("q_01"), Some(400));

    // Pasa el tiempo y el barrido la recoge.
    let futuro = chrono::Utc::now().timestamp() + 10_000;
    let report = sweep_once(
        store.as_ref(),
        counter.as_ref(),
        0..crate::quota::reservations::SWEEP_SHARDS,
        futuro,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(report.refunded, 1);
    assert_eq!(counter.usage_of("q_01"), Some(0), "se devolvió entera");

    // Y ahora llega el cliente, tarde.
    let respuesta = servicio
        .reconcile_tokens(conciliacion_de("tnt_01", &reserva.reservation_id, 100, 50))
        .await
        .unwrap()
        .into_inner();

    assert!(respuesta.status.unwrap().success);
    assert_eq!(respuesta.tokens_consumed, 150);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(150),
        "se apunta lo gastado, ni más ni menos"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Reservas que nunca llegaron a debitar
// ─────────────────────────────────────────────────────────────────────────────

/// Si el contador no responde, la reserva queda abierta sin débito. El barrido
/// la cierra SIN devolver nada: no hay nada que devolver, y devolverlo sería
/// regalar `estimated` tokens.
#[tokio::test]
async fn una_reserva_cuyo_debito_fallo_se_cierra_sin_devolver() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::at("q_01", 500));
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    counter.break_it("contador caído");
    let fallo = servicio.reserve_tokens(reserva_de("tnt_01", 400)).await;
    assert!(fallo.is_err(), "la reserva falla y el cliente lo sabe");
    counter.fix_it();

    let futuro = chrono::Utc::now().timestamp() + 10_000;
    let report = sweep_once(
        store.as_ref(),
        counter.as_ref(),
        0..crate::quota::reservations::SWEEP_SHARDS,
        futuro,
        Duration::from_secs(30),
    )
    .await;

    assert_eq!(report.abandoned, 1);
    assert_eq!(report.refunded, 0);
    assert_eq!(counter.usage_of("q_01"), Some(500), "intacto");
}

/// Una cuota agotada no puede dejar un ticket vivo: si lo dejara, el barrido
/// intentaría devolver un débito que nunca se aplicó.
#[tokio::test]
async fn la_cuota_agotada_no_deja_reserva_pendiente() {
    let store = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::at("q_01", 1000));
    let servicio = replica(
        Arc::clone(&store) as Arc<dyn ReservationStore>,
        Arc::clone(&counter),
        vec![cuota(1000, 1000)],
    );

    let respuesta = reservar(&servicio, "tnt_01", 400).await;
    let status = respuesta.status.unwrap();

    assert!(!status.success);
    assert_eq!(status.error_code, "QUOTA_EXHAUSTED");
    assert_eq!(
        store.open_count(),
        0,
        "el ticket rechazado no queda abierto"
    );
    assert_eq!(counter.usage_of("q_01"), Some(1000));
}

/// Conciliar una reserva que el techo rechazó no puede apuntar consumo: sería
/// cobrar por una llamada que el sistema negó.
#[tokio::test]
async fn no_se_concilia_una_reserva_que_nunca_debito() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::at("q_01", 500));
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    counter.break_it("contador caído");
    let _ = servicio.reserve_tokens(reserva_de("tnt_01", 400)).await;
    counter.fix_it();

    // El barrido la marca como abandonada.
    let futuro = chrono::Utc::now().timestamp() + 10_000;
    sweep_once(
        store.as_ref(),
        counter.as_ref(),
        0..crate::quota::reservations::SWEEP_SHARDS,
        futuro,
        Duration::from_secs(30),
    )
    .await;

    // Y no hay ticket que conciliar. El id se reconstruye desde el store porque
    // la reserva nunca llegó a devolverse al cliente.
    let abierta = store.sweep(0, futuro, 1).await.unwrap();
    assert!(abierta.is_empty(), "ya está cerrada");
    assert_eq!(counter.usage_of("q_01"), Some(500));
}

// ─────────────────────────────────────────────────────────────────────────────
// Quién puede tocar qué
// ─────────────────────────────────────────────────────────────────────────────

/// El `tenant_id` viene en el cuerpo de la petición. Sin contrastarlo con la
/// sesión, cualquier cliente autenticado podía gastar la cuota de otro.
#[tokio::test]
async fn no_se_reserva_a_nombre_de_otro_tenant() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let peticion = con_sesion(
        ReserveTokensRequest {
            tenant_id: "tnt_victima".to_string(),
            model_urn: "llm:aws:nova-pro".to_string(),
            estimated_tokens: 400,
        },
        "tnt_atacante",
        "usr_01",
    );

    let err = servicio.reserve_tokens(peticion).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(counter.usage_of("q_01"), None, "no se tocó nada");
}

/// Lo mismo por el otro lado: el ticket ya no lleva dentro el tenant como dato
/// de confianza, y aunque se adivinara, el de la sesión tiene que coincidir.
#[tokio::test]
async fn no_se_concilia_la_reserva_de_otro_tenant() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let reserva = reservar(&servicio, "tnt_01", 400).await;

    let err = servicio
        .reconcile_tokens(conciliacion_de(
            "tnt_atacante",
            &reserva.reservation_id,
            10,
            10,
        ))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        counter.usage_of("q_01"),
        Some(400),
        "la reserva ajena sigue intacta"
    );
}

/// El BFF y el tenant maestro sí operan en nombre de otros: es como llaman hoy.
#[tokio::test]
async fn una_cuenta_de_sistema_si_puede_reservar_por_otro() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let peticion = con_sesion(
        ReserveTokensRequest {
            tenant_id: "tnt_cliente".to_string(),
            model_urn: "llm:aws:nova-pro".to_string(),
            estimated_tokens: 400,
        },
        "tnt_otro",
        "usr_system_bff",
    );

    let respuesta = servicio
        .reserve_tokens(peticion)
        .await
        .unwrap()
        .into_inner();
    assert!(respuesta.status.unwrap().success);
}

#[tokio::test]
async fn sin_sesion_no_se_sirve() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let err = servicio
        .reserve_tokens(Request::new(ReserveTokensRequest {
            tenant_id: "tnt_01".to_string(),
            model_urn: "llm:aws:nova-pro".to_string(),
            estimated_tokens: 400,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

// ─────────────────────────────────────────────────────────────────────────────
// Cifras que pone el cliente
// ─────────────────────────────────────────────────────────────────────────────

/// Una estimación negativa debitaría en negativo: regalaría cuota.
#[tokio::test]
async fn una_estimacion_no_positiva_se_rechaza() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    for estimado in [0, -500] {
        let err = servicio
            .reserve_tokens(reserva_de("tnt_01", estimado))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "estimado = {estimado}"
        );
    }
    assert_eq!(counter.usage_of("q_01"), None);
}

/// Y un consumo negativo al conciliar devolvería más de lo reservado.
#[tokio::test]
async fn un_consumo_negativo_se_rechaza() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let reserva = reservar(&servicio, "tnt_01", 400).await;
    let err = servicio
        .reconcile_tokens(conciliacion_de("tnt_01", &reserva.reservation_id, -1000, 0))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(counter.usage_of("q_01"), Some(400));
}

// ─────────────────────────────────────────────────────────────────────────────
// Convivencia durante el despliegue
// ─────────────────────────────────────────────────────────────────────────────

/// Un ticket emitido por la versión anterior. Su dueño —si sigue vivo— devolverá
/// la estimación al vencer, así que aquí solo se apunta lo consumido.
#[tokio::test]
async fn un_ticket_del_formato_anterior_solo_apunta_lo_consumido() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::at("q_01", 400));
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let respuesta = servicio
        .reconcile_tokens(conciliacion_de(
            "tnt_01",
            "q_01:400:tnt_01:uuid-xyz",
            100,
            50,
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(respuesta.status.unwrap().success);
    assert_eq!(respuesta.tokens_consumed, 150);
    assert_eq!(counter.usage_of("q_01"), Some(550), "carga el consumo real");
}

#[tokio::test]
async fn un_ticket_ilegible_se_rechaza() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let err = servicio
        .reconcile_tokens(conciliacion_de("tnt_01", "no-es-un-ticket", 10, 10))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Un ticket con formato correcto pero que no existe no puede apuntar nada.
#[tokio::test]
async fn un_ticket_inventado_no_encuentra_reserva() {
    let store: Arc<dyn ReservationStore> = Arc::new(MemoryReservationStore::new());
    let counter = Arc::new(FakeCounter::new());
    let servicio = replica(
        Arc::clone(&store),
        Arc::clone(&counter),
        vec![cuota(1000, 0)],
    );

    let err = servicio
        .reconcile_tokens(conciliacion_de("tnt_01", "tnt_01:01JINVENTADO", 10, 10))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    assert_eq!(counter.usage_of("q_01"), None);
}
