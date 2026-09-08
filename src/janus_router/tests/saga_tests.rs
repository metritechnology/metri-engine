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

#[tokio::test]
async fn preventive_maintenance_deriva_cron_del_atributo_declarado() {
    let m = model("preventive_maintenance");
    let payload = json!({
        "asset_id": "01ASSET",
        "cron_expression": "0 8 1 */6 *",
        "iana_timezone": "America/Bogota",
        "next_due_date": 1_780_300_800i64
    });
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "sin prenotify sólo existe el disparo principal"
    );
    assert!(matches!(jobs[0].attrs.get("trigger_type"), Some(DatomValue::Str(s)) if s == "CRON"));
    assert!(
        matches!(jobs[0].attrs.get("trigger_expression"), Some(DatomValue::Str(s)) if s == "0 8 1 */6 *")
    );
}

#[tokio::test]
async fn la_condicion_fisica_gana_al_calendario() {
    let m = model("preventive_maintenance");
    let payload = json!({
        "asset_id": "01ASSET",
        "cron_expression": "0 8 1 */6 *",
        "meter_based_trigger": { "metric_code": "VIBRATION", "operator": "GT", "threshold": 5000.0 }
    });
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .unwrap();
    assert!(
        matches!(jobs[0].attrs.get("trigger_type"), Some(DatomValue::Str(s)) if s == "TELEMETRY")
    );
    assert!(
        matches!(jobs[0].attrs.get("trigger_expression"), Some(DatomValue::Str(s)) if s == "VIBRATION > 5000"),
        "debe emitir la gramática que exige Schedulers: {:?}",
        jobs[0].attrs.get("trigger_expression")
    );
}

#[tokio::test]
async fn telemetry_omite_las_prenotificaciones() {
    let m = model("preventive_maintenance");
    let payload = json!({
        "asset_id": "01ASSET",
        "meter_based_trigger": { "metric_code": "VIBRATION", "operator": "GT", "threshold": 5000.0 },
        "prenotify_before_minutes": [1440, 30]
    });
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "una condición física no tiene 'antes' que programar"
    );
}

#[tokio::test]
async fn test_preventive_maintenance_saga_projection_extensions() {
    let m = model("preventive_maintenance");
    let payload = json!({
        "asset_id": "01ASSET",
        "template_id": "01TEMPLATE",
        "cron_expression": "0 8 1 */6 *",
        "iana_timezone": "America/Bogota",
        "next_due_date": 1_780_300_800i64
    });
    let jobs = build_saga_projections(
        &reader().await,
        "tnt_1",
        &m,
        &payload,
        "01PM_PARENT",
        "01USER",
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 1);

    // RUT-H2: action_type es RPC_CALL
    assert_eq!(
        jobs[0].attrs.get("action_type").unwrap(),
        &DatomValue::Str("RPC_CALL".to_string())
    );

    // RUT-H3, RUT-D5 y extensiones __const y __self dentro de action_payload
    let ap = match jobs[0].attrs.get("action_payload").unwrap() {
        DatomValue::Str(s) => serde_json::from_str::<serde_json::Value>(s).unwrap(),
        o => panic!("{o:?}"),
    };
    assert_eq!(ap["entity_type"], json!("work_order"));
    assert_eq!(ap["content"]["asset_id"], json!("01ASSET"));
    assert_eq!(ap["payload"]["work_order_template_id"], json!("01TEMPLATE"));
    assert_eq!(
        ap["payload"]["preventive_maintenance_id"],
        json!("01PM_PARENT")
    );
}

/// Regresión de producción (2026-09-03): el CREATE de una pauta desde el panel
/// llegaba al engine con el cron en `null` y el motor rechazaba con JNS_001;
/// corregido el cron por el lado del panel, la validación de la entidad pasaba
/// a quejarse de que `template_id` «está ausente» con el atributo presente en
/// el payload. Este test congela el payload EXACTO que envía metri-app —con la
/// referencia escalar, el `advance_notice_meter_value` nulo de la sección
/// oculta y sin `id` de cliente (ADR-006)— y exige que valide y proyecte.
#[tokio::test]
async fn el_payload_de_create_del_panel_valida_y_proyecta() {
    let m = model("preventive_maintenance");
    let payload = json!({
        "asset_id": "01M1F9PB4RW8X73SP4F55Q7TED",
        "template_id": "01M1EYF5MPFCH65NRRAGJJ8KY6",
        "status": "ACTIVE",
        "cron_expression": "0 9 * * *",
        "iana_timezone": "UTC",
        "advance_notice_meter_value": null,
        "advance_notice_days": 7.0,
        "tenant_id": "01M12AGKPCR3YDYW9HG9ZQYXS6"
    });

    // 1. La validación de la entidad acepta el payload completo y la
    //    referencia escalar sobrevive como Str (el síntoma en producción era
    //    un CDX_001 que no la veía).
    let attrs = crate::codice::validator::validate_payload(&m, &payload, "tnt_1", true)
        .expect("el payload de Create PM Plan debe validar contra el Códice");
    assert!(
        matches!(attrs.get("template_id"), Some(DatomValue::Str(s)) if s == "01M1EYF5MPFCH65NRRAGJJ8KY6"),
        "template_id debe sobrevivir a la validación: {:?}",
        attrs.get("template_id")
    );

    // 2. Y el SagaBuilder deriva el disparo sin quejarse de fuentes de trigger.
    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .expect("el saga debe proyectar con el cron presente");
    assert_eq!(jobs.len(), 1);
    assert!(matches!(jobs[0].attrs.get("trigger_type"), Some(DatomValue::Str(s)) if s == "CRON"));
    assert!(matches!(
        jobs[0].attrs.get("trigger_expression"),
        Some(DatomValue::Str(s)) if s == "0 9 * * *"
    ));
}

// ── Contrato del loop PM cerrado (Tramo 2 de la integración metri-schedulers) ─

/// El payload que la saga proyecta para cada scheduled_job debe validar
/// completo contra el Códice. Es la costura exacta donde el fired del ejecutor
/// toca al motor: si un atributo proyectado no existe en el modelo, la OT
/// jamás se creará — y el fallo se descubriría meses después, el día que
/// tocaba el mantenimiento.
#[tokio::test]
async fn saga_payload_valida_contra_el_codice() {
    let m = model("preventive_maintenance");
    let payload = json!({
        "template_id": "01M1EYF5MPFCH65NRRAGJJ8KY6",
        "asset_id": "01ASSET",
        "cron_expression": "0 9 * * *",
        "iana_timezone": "UTC",
        "next_due_date": 1767225600000i64,
        "recurrence_basis": "FIXED_CALENDAR"
    });

    let jobs = build_saga_projections(&reader().await, "tnt_1", &m, &payload, "01P", "01ME")
        .await
        .expect("la proyección debe validar");
    assert!(
        !jobs.is_empty(),
        "una pauta cron debe proyectar al menos el job principal"
    );

    let job_model = model("scheduled_job");
    for job in &jobs {
        let attrs_json = crate::eav::writer::outbox::datom_map_to_json(&job.attrs);
        crate::codice::validator::validate_payload(&job_model, &attrs_json, "tnt_1", true)
            .unwrap_or_else(|e| {
                panic!(
                    "el payload proyectado del job debe validar contra el Códice: {e:?}\n{attrs_json}"
                )
            });
    }
}

/// La trazabilidad del loop: una OT generada por un disparo lleva la pauta y
/// el job que la engendraron. Ambas referencias deben existir en el modelo —
/// antes del Tramo 2 el validador las rechazaría y el fired del Hub habría
/// muerto con un CDX_001 el día del mantenimiento.
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
