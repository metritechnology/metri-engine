//! Tests for `domain::events::golden`.
// golden_test — el candado del contrato (Tramo 0 del plan de integración).
//
// Lee los fixtures dorados de metri-contracts/golden/ — los MISMOS archivos que
// parsea `internal/event/envelope_test.go` del Hub — y verifica que el
// productor Rust:
//   1. deserializa cada fixture (los nombres de campo SON el contrato),
//   2. re-serializa a exactamente el mismo JSON,
//   3. clasifica el delta de bitácora igual que el guard Go (§10.4),
//   4. el builder produce, desde atributos crudos del canal, el sobre exacto
//      que el Hub espera.
//
// Si este test se pone rojo junto con un cambio aquí, el contrato cambió:
// actualiza metri-contracts primero, nunca al revés.

use super::delta::{compute_delta, is_bookkeeping_only, Delta};
use super::envelope::{DomainEnvelope, ScheduledJobEventInput, ScheduledJobOp};
use serde_json::{json, Value};

const GOLDEN_CRON: &str =
    include_str!("../../../../metri-contracts/golden/scheduled_job_created_cron.json");
const GOLDEN_TELEMETRY: &str =
    include_str!("../../../../metri-contracts/golden/scheduled_job_created_telemetry.json");
const GOLDEN_BOOKKEEPING: &str =
    include_str!("../../../../metri-contracts/golden/scheduled_job_updated_bookkeeping.json");
const GOLDEN_STATUS: &str =
    include_str!("../../../../metri-contracts/golden/scheduled_job_updated_status.json");
const GOLDEN_DELETED: &str =
    include_str!("../../../../metri-contracts/golden/scheduled_job_deleted.json");

fn parse_golden(raw: &str) -> (Value, DomainEnvelope) {
    let detail: Value = serde_json::from_str(raw).expect("fixture dorado debe ser JSON válido");
    let envelope: DomainEnvelope =
        serde_json::from_value(detail.clone()).expect("fixture debe deserializar como sobre v1.0");
    (detail, envelope)
}

#[test]
fn fixtures_dorados_deserializan_como_sobre_v1() {
    for (nombre, raw) in [
        ("cron", GOLDEN_CRON),
        ("telemetry", GOLDEN_TELEMETRY),
        ("bookkeeping", GOLDEN_BOOKKEEPING),
        ("status", GOLDEN_STATUS),
        ("deleted", GOLDEN_DELETED),
    ] {
        let (detail, env) = parse_golden(raw);
        assert_eq!(env.schema_version, "1.0", "{nombre}");
        assert_eq!(detail["schema_version"], json!("1.0"), "{nombre}");
        assert!(!env.job_id.is_empty(), "{nombre}: job_id obligatorio");
        assert_eq!(
            env.ulid.len(),
            26,
            "{nombre}: ulid debe ser ULID de 26 chars"
        );
    }
}

#[test]
fn re_serializacion_es_fiel_al_fixture() {
    for raw in [
        GOLDEN_CRON,
        GOLDEN_TELEMETRY,
        GOLDEN_BOOKKEEPING,
        GOLDEN_STATUS,
        GOLDEN_DELETED,
    ] {
        let (detail, env) = parse_golden(raw);
        let roundtrip = env.to_detail();
        assert_eq!(
            roundtrip, detail,
            "el sobre Rust debe ser bit-a-bit el contrato (campos extra o faltantes rompen al Hub)"
        );
    }
}

#[test]
fn guard_de_bitacora_clasifica_igual_que_el_hub() {
    let (bookkeeping, _) = parse_golden(GOLDEN_BOOKKEEPING);
    let delta_book: Delta = serde_json::from_value(bookkeeping["delta"].clone()).unwrap();
    assert!(
        is_bookkeeping_only(&delta_book),
        "run_count/last_run_at/last_error puros → el Hub debe DESCARTAR el evento"
    );

    let (status, _) = parse_golden(GOLDEN_STATUS);
    let delta_status: Delta = serde_json::from_value(status["delta"].clone()).unwrap();
    assert!(
        !is_bookkeeping_only(&delta_status),
        "un cambio real de status → el Hub debe REPROVISIONAR la trampa"
    );
}

#[test]
fn builder_produce_el_fixture_cron_exacto() {
    let expected: Value = serde_json::from_str(GOLDEN_CRON).unwrap();

    // Los atributos crudos tal como llegan del canal OLTP (payload aplanado).
    let attributes = json!({
        "trigger_type": "CRON",
        "trigger_expression": "0 8 * * 1-5",
        "iana_timezone": "America/Bogota",
        "action_type": "RPC_CALL",
        "action_payload": expected["action_payload"].clone(),
        "idempotency_hash": expected["idempotency_hash"],
        "created_by": expected["created_by"],
        "target_webhook_id": "",
        "status": "PENDING",
        "correlation_id": expected["correlation_id"]
    });

    let env = DomainEnvelope::from_attributes(ScheduledJobEventInput {
        op: ScheduledJobOp::Created,
        entity_id: expected["job_id"].as_str().unwrap(),
        tenant_id: expected["tenant_id"].as_str().unwrap(),
        mutation_ulid: expected["ulid"].as_str().unwrap(),
        attributes: attributes.as_object().unwrap(),
        delta: None,
        causation_id: None,
    })
    .expect("el sobre del fixture debe construirse desde atributos crudos");

    assert_eq!(
        env.to_detail(),
        expected,
        "lo que el motor publica debe ser exactamente lo que el Hub parsea"
    );
}

#[test]
fn delta_del_canal_entrante_coincide_con_el_contrato() {
    // El delta que transact calcula (Value maps crudos) debe serializar al
    // formato {before, after} que el guard del Hub consume.
    let before = json!({"status": "ACTIVE"});
    let after = json!({"status": "SUSPENDED"});
    let delta = compute_delta(before.as_object().unwrap(), after.as_object().unwrap());

    let as_json = serde_json::to_value(&delta).unwrap();
    assert_eq!(
        as_json,
        json!({"status": {"before": "ACTIVE", "after": "SUSPENDED"}})
    );
}
