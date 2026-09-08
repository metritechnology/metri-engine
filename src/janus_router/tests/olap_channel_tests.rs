// janus_router/tests/olap_channel_tests.rs — Fase 1 de PLAN_COSTO_OLAP.md (Puerta 1)
//
// Red primero (Regla 02): el canal debe emitir ⌈N/50⌉ llamadas PutRecordBatch
// en vez de N llamadas PutRecord, y cada record individual debe viajar con un
// payload byte a byte idéntico al camino anterior (JSON decorado + newline).
//
// Nota de alcance: NO se empaquetan varios records lógicos en un record de
// Firehose. El destino real de los streams es Iceberg (AppendOnly: false,
// UniqueKeys: [id]) y AWS exige un único JSON válido por record — el
// empaquetado produce ICEBERG_BAD_DATA y la pérdida de los records. Ver
// docs/architecture/MEDICION_COSTO_OLAP.md §4.

use serde_json::{json, Map, Value};
use std::sync::Arc;

use crate::infrastructure::kinesis::SpyStreamWriter;
use crate::iop::core::IopContext;
use crate::janus_router::olap_channel::OlapChannel;
use crate::janus_router::router::IWriteChannel;

static INIT: std::sync::Once = std::sync::Once::new();

fn init_codice() {
    INIT.call_once(|| {
        if crate::codice::registry::global_opt().is_none() {
            let models_dir = std::path::Path::new("config/models");
            if let Ok((registry, _)) = crate::codice::CodeRegistry::build(models_dir) {
                crate::codice::init_global(registry);
            }
        }
    });
}

fn bulk_ctx(entity: &str, tenant: &str, records: Vec<Value>) -> IopContext {
    let mut req = Map::new();
    req.insert("data".to_string(), Value::Array(records));
    IopContext::new(tenant, "usr_test", entity, "bulk_ingest", req)
}

/// Records de ~700 B serializados, el perfil de un audit_log real
/// (interceptor.rs) que motiva la Puerta 1.
fn sample_records(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "action_type": "TEST",
                "seq": i,
                "payload": format!("cuerpo-de-{}-{}", i, "x".repeat(600)),
            })
        })
        .collect()
}

#[tokio::test]
async fn n_registros_producen_una_llamada_por_chunk_y_no_una_por_registro() {
    init_codice();
    let spy = Arc::new(SpyStreamWriter::new());
    let channel = OlapChannel::new("metri-olap-stream", spy.clone());

    let n = 120usize; // ⌈120/50⌉ = 3 llamadas, no 120
    let out = channel
        .route(bulk_ctx("audit_log", "tnt_test", sample_records(n)))
        .await
        .expect("la ingesta OLAP debe salir");

    assert_eq!(out["ingested_count"], json!(n));
    assert_eq!(
        spy.batch_calls(),
        (n + 49) / 50,
        "N records deben agruparse en ⌈N/50⌉ llamadas PutRecordBatch"
    );
}

#[tokio::test]
async fn cada_record_viaja_byte_a_byte_identico_al_camino_anterior() {
    init_codice();
    let spy = Arc::new(SpyStreamWriter::new());
    let channel = OlapChannel::new("metri-olap-stream", spy.clone());

    let n = 7usize;
    channel
        .route(bulk_ctx("audit_log", "tnt_test", sample_records(n)))
        .await
        .expect("la ingesta OLAP debe salir");

    let captured = spy.drain();
    assert_eq!(captured.len(), n, "cada record lógico debe sobrevivir");

    for rec in &captured {
        assert_eq!(
            rec.stream_name, "metri-olap-stream-audit-log",
            "underscore → hyphen en el nombre del stream"
        );
        assert_eq!(
            rec.data.last(),
            Some(&b'\n'),
            "mismo contrato newline-delimited que put_record"
        );
        let obj: Value = serde_json::from_slice(&rec.data[..rec.data.len() - 1])
            .expect("payload debe ser un único JSON válido (destino Iceberg)");
        assert!(obj["id"].is_string(), "decorado con ULID");
        assert_eq!(obj["_tenant"], json!("tnt_test"));
        assert!(obj["created_at"].is_i64(), "created_at epoch-ms");
    }
}

#[tokio::test]
async fn rpc_transact_sigue_bloqueado_en_el_canal_batcheado() {
    init_codice();
    let spy = Arc::new(SpyStreamWriter::new());
    let channel = OlapChannel::new("metri-olap-stream", spy.clone());

    // rpc Transact = request SIN "data" → JNS_OLAP_001, cero llamadas al stream.
    let ctx = IopContext::new("tnt_test", "usr_test", "audit_log", "transact", Map::new());
    let err = channel
        .route(ctx)
        .await
        .err()
        .expect("rpc Transact debe ser bloqueado");
    assert!(format!("{err:?}").contains("JnsOlap001"));
    assert_eq!(spy.batch_calls(), 0);
}
