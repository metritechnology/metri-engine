//! Conformidad de contratos — reemplazo Rust de los validadores Clojure.
//!
//! Lo que `docs/architecture/validate_contracts.clj` y `validate_traceability.clj`
//! hacían con Clojure, lo hace ahora el propio engine en su lenguaje nativo:
//! verifica que el contrato gRPC expone la superficie que el motor implementa,
//! que el Códice y el catálogo de errores están completos, y que la fuente
//! única del contrato es una sola.

use std::path::Path;

const PROTO: &str = include_str!("../proto/metri.proto");

fn manifest(p: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(p).to_string_lossy().into()
}

#[test]
fn el_servicio_principal_expone_todos_los_rpcs() {
    for rpc in [
        "rpc Discovery",
        "rpc Explore",
        "rpc Query",
        "rpc ListEntities",
        "rpc Transact",
        "rpc BulkIngest",
        "rpc MatchRoutingRulesBatch",
    ] {
        assert!(PROTO.contains(rpc), "Falta en el contrato: {rpc}");
    }
}

#[test]
fn los_tres_servicios_estan_en_el_contrato() {
    for svc in ["service MetriService", "service QuotaService", "service AgentConfigService"] {
        assert!(PROTO.contains(svc), "Falta el servicio: {svc}");
    }
}

#[test]
fn los_seis_viz_types_del_contrato_estan_en_output_cast() {
    // OutputCastType: 0=UNSPECIFIED, 1..=6 el resto
    for viz in ["KPI = 1", "TIMESERIES = 2", "TABLE = 3", "PIE = 4", "BUBBLE = 5", "CSV_EXPORT = 6"] {
        assert!(PROTO.contains(viz), "OutputCastType incompleto: falta {viz}");
    }
}

#[test]
fn los_mensajes_clave_del_pipeline_estan_definidos() {
    for msg in [
        "message QueryRequest",
        "message AnalyticsRequest",
        "message MetricDefinition",
        "message ListEntitiesRequest",
        "message ListEntitiesResponse",
        "message TimeFrameContext",
        "enum AggregationFunction",
    ] {
        assert!(PROTO.contains(msg), "Falta el mensaje: {msg}");
    }
}

#[test]
fn el_contrato_vive_en_una_sola_copia() {
    // La lección de la Fase 1/Parte VI: tres copias del proto matan «el contrato».
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut copies = vec![];
    for entry in walk(root, 0) {
        let name = entry.file_name().unwrap().to_string_lossy();
        if name == "metri.proto" {
            copies.push(entry);
        }
    }
    assert_eq!(copies.len(), 1, "metri.proto debe existir UNA sola vez: {:?}", copies);
    assert!(copies[0].ends_with("proto/metri.proto"));
    assert!(!root.join("src/grpc/gen").exists(), "El código generado no se commitea: include_proto! usa OUT_DIR");
}

fn walk(dir: &Path, depth: usize) -> Vec<std::path::PathBuf> {
    if depth > 4 {
        return vec![];
    }
    let mut out = vec![];
    let Ok(rd) = std::fs::read_dir(dir) else { return out };
    for e in rd.flatten() {
        let p = e.path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        if p.is_dir() {
            if matches!(name.as_str(), "target" | ".git" | ".venv" | "node_modules" | ".aws-sam") {
                continue;
            }
            out.extend(walk(&p, depth + 1));
        } else {
            out.push(p);
        }
    }
    out
}

#[test]
fn el_codice_mantiene_el_ssot_de_modelos() {
    let models_path = manifest("config/models");
    let models = Path::new(&models_path);
    let count = std::fs::read_dir(models).unwrap().flatten().filter(|e| {
        e.path().extension().map(|x| x == "json").unwrap_or(false)
    }).count();
    assert!(count >= 55, "El Códice perdió modelos: {count}");
}

#[test]
fn el_catalogo_de_errores_cubre_las_familias_del_motor() {
    let catalog = std::fs::read_to_string(manifest("config/errors/error_catalog.toml"))
        .expect("error_catalog.toml es el catálogo canónico");
    for family in ["jns", "aeg", "eav", "cod"] {
        assert!(catalog.contains(family), "El catálogo no cubre la familia {family}");
    }
}
