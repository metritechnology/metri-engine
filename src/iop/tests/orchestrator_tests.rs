//! Tests for `iop::orchestrator`.
// tests/orchestrator_tests.rs — la mitad «deshacer» del pipeline.
//
// El paso de cuota debita en el paso 2 y Janus escribe en el 3. Entre ambos
// cabe un fallo. Antes de esto, el `?` de `run_steps` cortaba y el débito se
// quedaba: un tenant con errores intermitentes de escritura agotaba su plan sin
// haber creado nada, y el contador solo bajaba al rotar el periodo.

use super::*;
use crate::domain::errors::ErrorCode;
use std::sync::Mutex;

/// Paso configurable que anota cuándo lo compensan.
struct PasoDeMentira {
    nombre: &'static str,
    falla: bool,
    deja_reserva: bool,
    compensados: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait::async_trait]
impl IopStep for PasoDeMentira {
    async fn execute(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        if self.falla {
            return Err(DomainError::new(
                ErrorCode::Eav001,
                format!("{} falló", self.nombre),
            ));
        }
        if self.deja_reserva {
            ctx.quota_reservation =
                Some(serde_json::json!({ "id": "quota_01", "status": "pending" }));
        }
        Ok(ctx)
    }

    async fn compensate(&self, _ctx: &IopContext) {
        self.compensados.lock().unwrap().push(self.nombre);
    }
}

struct NotifierMudo;
#[async_trait::async_trait]
impl crate::iop::sherlog::IFaultNotifier for NotifierMudo {
    async fn notify(
        &self,
        _dto: &Value,
        _sev: &crate::iop::sherlog::FaultSeverity,
    ) -> Result<(), DomainError> {
        Ok(())
    }
}

struct CanalMudo;
#[async_trait::async_trait]
impl crate::janus_router::router::IWriteChannel for CanalMudo {
    async fn route(&self, _ctx: IopContext) -> Result<Value, DomainError> {
        Ok(Value::Null)
    }
}

fn orquestador(steps: Vec<Arc<dyn IopStep>>) -> IopOrchestrator {
    IopOrchestrator {
        steps,
        moira_emitter: None,
        audit_interceptor: None,
        fault_notifier: Arc::new(NotifierMudo),
        olap_channel: Arc::new(CanalMudo),
    }
}

fn contexto() -> IopContext {
    IopContext::new(
        "tnt_01",
        "usr_01",
        "asset",
        "CREATE",
        serde_json::Map::new(),
    )
}

fn paso(
    nombre: &'static str,
    falla: bool,
    deja_reserva: bool,
    compensados: &Arc<Mutex<Vec<&'static str>>>,
) -> Arc<dyn IopStep> {
    Arc::new(PasoDeMentira {
        nombre,
        falla,
        deja_reserva,
        compensados: Arc::clone(compensados),
    })
}

/// El caso que motivó todo esto: la cuota ya debitó y Janus falla.
#[tokio::test]
async fn un_fallo_posterior_deshace_lo_ya_hecho() {
    let compensados = Arc::new(Mutex::new(Vec::new()));
    let orq = orquestador(vec![
        paso("cedar", false, false, &compensados),
        paso("quota", false, true, &compensados), // deja reserva
        paso("janus", true, false, &compensados), // y aquí se rompe
    ]);

    let resultado = orq.run_steps(contexto()).await;

    assert!(
        resultado.is_err(),
        "el error original debe llegar al cliente"
    );
    // Orden inverso: se deshace lo último primero, como en cualquier saga.
    assert_eq!(*compensados.lock().unwrap(), vec!["quota", "cedar"]);
}

/// Si es la propia cuota la que rechaza, no hubo débito y no hay nada que
/// devolver. Compensar aquí restaría una unidad que nadie sumó.
#[tokio::test]
async fn si_falla_el_paso_que_reserva_no_se_compensa_nada() {
    let compensados = Arc::new(Mutex::new(Vec::new()));
    let orq = orquestador(vec![
        paso("cedar", false, false, &compensados),
        paso("quota", true, false, &compensados),
    ]);

    assert!(orq.run_steps(contexto()).await.is_err());
    assert!(compensados.lock().unwrap().is_empty());
}

/// El camino feliz no compensa. Obvio, y por eso conviene fijarlo: una
/// compensación de más devuelve cuota que sí se consumió.
#[tokio::test]
async fn el_camino_feliz_no_deshace_nada() {
    let compensados = Arc::new(Mutex::new(Vec::new()));
    let orq = orquestador(vec![
        paso("cedar", false, false, &compensados),
        paso("quota", false, true, &compensados),
        paso("janus", false, false, &compensados),
    ]);

    let ctx = orq
        .run_steps(contexto())
        .await
        .expect("los tres pasos van bien");

    assert!(compensados.lock().unwrap().is_empty());
    assert!(ctx.quota_reservation.is_some());
}

/// Sin efectos externos no se clona el contexto ni se compensa: es el camino de
/// la mayoría de las peticiones —UPDATE, DELETE— y no debe pagar nada.
#[tokio::test]
async fn sin_reserva_un_fallo_no_dispara_compensacion() {
    let compensados = Arc::new(Mutex::new(Vec::new()));
    let orq = orquestador(vec![
        paso("cedar", false, false, &compensados),
        paso("janus", true, false, &compensados),
    ]);

    assert!(orq.run_steps(contexto()).await.is_err());
    assert!(compensados.lock().unwrap().is_empty());
    assert!(!contexto().has_reversible_effects());
}
