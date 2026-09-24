//! Tests for `janus_router::saga`.
// Pruebas del SagaBuilder — proyección declarativa hacia scheduled_job.
//
// Cubren los casos límite que separan una proyección correcta de una que parece
// correcta: derivación del trigger, fan-out por pre-notificación, traversal del
// mapping y las omisiones deliberadas.

use super::*;
use serde_json::json;

/// Inicializa el registry global con los modelos reales.
///
/// Dos cuidados, ambos aprendidos a golpes:
///   1. `Once` — los tests corren en paralelo e `init_global` entra en pánico si se
///      llama dos veces.
///   2. `catch_unwind` DENTRO del `Once` — otros módulos de test (p. ej. janus_step)
///      inicializan el registry por su cuenta. Si ganan la carrera, `init_global`
///      entra en pánico, **envenena el Once** y todos los tests posteriores fallan
///      con un error que no tiene nada que ver con lo que prueban.
static REGISTRY_INIT: std::sync::Once = std::sync::Once::new();

fn init_registry() {
    REGISTRY_INIT.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // silencia el ruido del intento fallido
        let _ = std::panic::catch_unwind(|| {
            let dir = std::path::Path::new("config/models");
            let (registry, _rules) =
                crate::codice::CodeRegistry::build(dir).expect("config/models debe compilar");
            crate::codice::init_global(registry);
        });
        std::panic::set_hook(prev);
    });
    // Sea quien sea el que ganó la carrera, el registry debe estar disponible.
    crate::codice::global();
}

fn model(name: &str) -> crate::codice::registry::EntityModel {
    init_registry();
    crate::codice::global()
        .get_model(name)
        .expect("modelo ausente")
        .clone()
}

/// Reader real contra DynamoDB stub. Sólo lo consultan los mappings con traversal
/// (`[campo_ref, attr]`); los tests de aquí usan mappings directos o toleran el fallo
/// de lectura, que `resolve_source` degrada a "campo ausente".
async fn reader() -> EavReader {
    let ddb = std::sync::Arc::new(
        crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-test").await,
    );
    EavReader::new(ddb, "metri-eav-test")
}

#[tokio::test]
async fn reminder_proyecta_exact_time_con_fanout() {
    let m = model("reminder");
    let payload = json!({
        "title": "Limpieza de válvulas",
        "message": "Revisar presión",
        "target_user_id": "01USER",
        "reminder_datetime": "2026-06-01T08:00:00Z",
        "prenotify_minutes_array": [30, 1440],
        "iana_timezone": "America/Bogota"
    });

    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01PARENT", "01ME")
        .await
        .expect("la proyección debe validar");

    // dos avisos previos + el disparo principal
    assert_eq!(jobs.len(), 3, "esperados 3 jobs, obtenidos {}", jobs.len());

    let base = 1_780_300_800i64; // 2026-06-01T08:00:00Z
    let mut exprs: Vec<i64> = jobs
        .iter()
        .map(|j| match j.attrs.get("trigger_expression").unwrap() {
            DatomValue::Str(s) => s.parse::<i64>().unwrap(),
            other => panic!("trigger_expression inesperado: {other:?}"),
        })
        .collect();
    exprs.sort_unstable();

    assert_eq!(exprs, vec![base - 1440 * 60, base - 30 * 60, base]);

    for j in &jobs {
        assert!(
            matches!(j.attrs.get("trigger_type"), Some(DatomValue::Str(s)) if s == "EXACT_TIME")
        );
        assert!(matches!(j.attrs.get("created_by"), Some(DatomValue::Str(s)) if s == "01ME"));
        assert!(
            matches!(j.attrs.get("parent_entity_ref"), Some(DatomValue::Uuid(s)) if s == "01PARENT")
        );
    }
}

#[tokio::test]
async fn los_hashes_de_idempotencia_no_colisionan_entre_offsets() {
    let m = model("reminder");
    let payload = json!({
        "title": "t", "message": "m", "target_user_id": "01U",
        "reminder_datetime": "2026-06-01T08:00:00Z",
        "prenotify_minutes_array": [30, 1440]
    });
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .unwrap();

    let mut hashes: Vec<String> = jobs
        .iter()
        .map(|j| match j.attrs.get("idempotency_hash").unwrap() {
            DatomValue::Str(s) => s.clone(),
            o => panic!("{o:?}"),
        })
        .collect();
    hashes.sort();
    let total = hashes.len();
    hashes.dedup();
    assert_eq!(
        hashes.len(),
        total,
        "dos avisos del mismo padre comparten hash: se deduplicarían entre sí"
    );
}

#[tokio::test]
async fn mapping_proyecta_rutas_anidadas_en_action_payload() {
    let m = model("reminder");
    let payload = json!({
        "title": "t",
        "message": "Texto exacto para el operador",
        "target_user_id": "01USER",
        "reminder_datetime": "2026-06-01T08:00:00Z"
    });
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);

    // action_payload es json: se almacena serializado
    let ap = match jobs[0].attrs.get("action_payload").unwrap() {
        DatomValue::Str(s) => serde_json::from_str::<serde_json::Value>(s).unwrap(),
        o => panic!("{o:?}"),
    };
    assert_eq!(
        ap["content"]["message_override"],
        json!("Texto exacto para el operador"),
        "el mapping debe descender dentro de action_payload: {ap}"
    );
    assert_eq!(ap["source_entity"], json!("reminder"));
}

#[tokio::test]
async fn madre_sin_fuente_de_trigger_falla_explicitamente() {
    let m = model("reminder");
    let payload = json!({ "title": "t", "message": "m", "target_user_id": "01U" });
    let err = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME").await;
    assert!(
        err.is_err(),
        "sin reminder_datetime la proyección no puede inventar cuándo disparar"
    );
}

#[tokio::test]
async fn entidad_sin_mapping_no_proyecta_nada() {
    let m = model("work_order");
    let payload = json!({ "title": "x" });
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .unwrap();
    assert!(
        jobs.is_empty(),
        "work_order no declara shadow_sagas_mapping"
    );
}

#[test]
fn work_order_de_trazabilidad_valida_contra_codice() {
    let m = model("work_order");
    let payload = json!({
        "work_order_number": "WO-0042",
        "title": "Cambio de aceite trimestral",
        "category": "PREVENTIVE",
        "preventive_maintenance_id": "01HZZZZZZZZZZZZZZZZZZZZZ04",
        "scheduled_job_id": "01HZZZZZZZZZZZZZZZZZZZZZ01"
    });

    let attrs = crate::codice::validator::validate_payload(&m, &payload, "tnt_1", true)
        .expect("la OT generada con trazabilidad debe validar contra el Códice");
    assert!(attrs.contains_key("preventive_maintenance_id"));
    assert!(attrs.contains_key("scheduled_job_id"));
}

/// El constraint composite (unique por tenant sobre scheduled_job_id+asset_id)
/// debe haber compilado al registry: es la capa definitiva de idempotencia del
/// loop — aunque Valkey falle y el bus duplique el fired, la segunda OT no
/// existe. El par y no el job solo, porque una ruta ENUMERATED materializa N
/// OTs del mismo job (una por parada).
#[test]
fn constraint_unico_de_trazabilidad_compila() {
    let m = model("work_order");
    let unique: Vec<_> = m
        .constraints
        .iter()
        .filter(|c| c.attributes == ["scheduled_job_id", "asset_id"])
        .collect();
    assert_eq!(
        unique.len(),
        1,
        "work_order debe declarar unique(tenant, [scheduled_job_id]): {:?}",
        m.constraints
    );
}

// ── P0c: el diff de convergencia de la reproyección ─────────────────────────

fn proj(hash: &str) -> SagaProjection {
    let mut attrs = HashMap::new();
    attrs.insert(
        "idempotency_hash".to_string(),
        DatomValue::Str(hash.to_string()),
    );
    SagaProjection {
        entity_type: "scheduled_job".to_string(),
        entity_id: format!("01J_{hash}"),
        attrs,
    }
}

fn live(id: &str, hash: Option<&str>, status: &str) -> LiveSagaJob {
    LiveSagaJob {
        job_id: id.to_string(),
        idempotency_hash: hash.map(str::to_string),
        status: status.to_string(),
    }
}

fn hashes_of(projs: &[SagaProjection]) -> Vec<String> {
    projs
        .iter()
        .map(|p| match p.attrs.get("idempotency_hash") {
            Some(DatomValue::Str(h)) => h.clone(),
            _ => String::new(),
        })
        .collect()
}

/// Editar el cron de la madre: el job viejo queda huérfano (muere) y el nuevo
/// nace — nunca coexisten dos verdades del calendario.
#[test]
fn diff_cron_editado_mata_el_viejo_y_crea_el_nuevo() {
    let recon = diff_sagas(
        vec![proj("HASH_NUEVO")],
        vec![live("01JOB_A", Some("HASH_VIEJO"), "ACTIVE")],
        "ACTIVE",
    );
    assert_eq!(recon.to_delete, vec!["01JOB_A".to_string()]);
    assert_eq!(hashes_of(&recon.to_create), vec!["HASH_NUEVO".to_string()]);
    assert!(recon.to_restatus.is_empty());
}

/// Pausar la madre: el hash sigue vivo — el job NO muere, converge a SUSPENDED
/// (Chronos lo mapea a DISABLED sin destruirlo: reactivar es barato).
#[test]
fn diff_pausa_convierte_el_estado_sin_destruir() {
    let recon = diff_sagas(
        vec![proj("HASH_A")],
        vec![live("01JOB_A", Some("HASH_A"), "ACTIVE")],
        "SUSPENDED",
    );
    assert!(recon.to_delete.is_empty());
    assert!(recon.to_create.is_empty());
    assert_eq!(
        recon.to_restatus,
        vec![("01JOB_A".to_string(), "SUSPENDED".to_string())]
    );
}

/// Un update que no toca el calendario ni la salud: nada que hacer.
#[test]
fn diff_sin_cambios_es_noop() {
    let recon = diff_sagas(
        vec![proj("HASH_A")],
        vec![live("01JOB_A", Some("HASH_A"), "ACTIVE")],
        "ACTIVE",
    );
    assert!(recon.to_create.is_empty());
    assert!(recon.to_delete.is_empty());
    assert!(recon.to_restatus.is_empty());
}

/// COMPLETED cerró su ciclo: ni se resucita ni se suspende — la bitácora del
/// pasado no se reescribe.
#[test]
fn diff_no_toca_jobs_con_ciclo_cerrado() {
    let recon = diff_sagas(
        vec![proj("HASH_A")],
        vec![live("01JOB_A", Some("HASH_A"), "COMPLETED")],
        "SUSPENDED",
    );
    assert!(recon.to_restatus.is_empty());
    assert!(recon.to_delete.is_empty());
}

/// Un job sin hash no tiene identidad comparable: se conserva (conservador) y
/// no cuenta como huérfano.
#[test]
fn diff_conserva_el_job_sin_hash() {
    let recon = diff_sagas(
        vec![proj("HASH_A")],
        vec![live("01JOB_LEGACY", None, "ACTIVE")],
        "ACTIVE",
    );
    assert!(recon.to_delete.is_empty());
    assert!(recon.to_restatus.is_empty());
}

/// Un aviso previo nuevo (prenotify) sólo añade su job: el principal sigue
/// vivo con su hash intacto.
#[test]
fn diff_prenotify_nuevo_solo_crea_el_suyo() {
    let recon = diff_sagas(
        vec![proj("HASH_MAIN"), proj("HASH_PRE_1440")],
        vec![live("01JOB_MAIN", Some("HASH_MAIN"), "ACTIVE")],
        "ACTIVE",
    );
    assert!(recon.to_delete.is_empty());
    assert_eq!(
        hashes_of(&recon.to_create),
        vec!["HASH_PRE_1440".to_string()]
    );
}

/// La salud de la madre dicta el estado de sus trampas.
#[test]
fn desired_status_sigue_la_salud_de_la_madre() {
    assert_eq!(desired_job_status(None), "ACTIVE");
    assert_eq!(desired_job_status(Some("ACTIVE")), "ACTIVE");
    assert_eq!(desired_job_status(Some("PAUSED")), "SUSPENDED");
    assert_eq!(desired_job_status(Some("RETIRED")), "SUSPENDED");
}

// NOTA (PLAN_DESCACOPLE_PM_ENGINE.md D2): los tests que ejercitaban la
// proyección de `preventive_maintenance` (trigger cron/telemetría, molde,
// anticipación) se retiraron con el mapping — la pauta ya no proyecta. La
// cobertura del SagaBuilder genérico vive en los tests de `reminder` y en los
// del pm-orchestrator (metri-cmms-plugin/internal/usecases).
