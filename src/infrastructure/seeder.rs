//! OLAP infrastructure seeder — pure logic: naming, DDL, drift.
//!
//! Fase 4 de PLAN_CORRECCIONES_PENDIENTES.md
//!
//! Lógica pura del seeder de infraestructura OLAP: naming, mapeo de tipos del
//! Códice a Athena, generación del DDL Iceberg, configuración deseada de los
//! delivery streams y detección de drift. Sin I/O — el binario
//! `src/bin/firehose-seeder.rs` es la CLI fina que ejecuta contra AWS.
//!
//! Contexto: el seeder original (`sync-firehose`, era Clojure) desapareció del
//! repo y la configuración viva de Firehose quedó innombrada — la causa de raíz
//! del incidente de entrega (WarehouseLocation inexistente, ver
//! MEDICION_COSTO_OLAP.md §5a). Este módulo devuelve esa gestión al repo.

use crate::codice::registry::{AttrType, AttributeDescriptor};

// ── Naming ───────────────────────────────────────────────────────────────────

/// Nombre del delivery stream de una entidad — misma regla que `OlapChannel`:
/// underscore → hyphen. `audit_log` → `metri-olap-stream-audit-log`.
pub fn stream_name(prefix: &str, entity: &str) -> String {
    format!("{}-{}", prefix, entity.replace('_', "-"))
}

/// Nombre de la tabla Iceberg/Glue de una entidad: el nombre de la entidad tal
/// cual (los streams vivos usan `audit_log`, con underscore).
pub fn table_name(entity: &str) -> String {
    entity.to_string()
}

/// LogGroup de CloudWatch de la entrega de un stream (Fase 2: logging ON).
pub fn log_group(stream: &str) -> String {
    format!("/aws/kinesisfirehose/{stream}")
}

// ── Mapeo de tipos Códice → Athena ──────────────────────────────────────────

/// Mapeo único y explícito para el DDL Iceberg. Epoch → bigint (ms), los
/// numéricos → double, todo lo demás → string (los arrays y JSON viajan
/// serializados; el esquema fino de columnas es trabajo del lector).
pub fn athena_type(attr_type: &AttrType) -> &'static str {
    match attr_type {
        AttrType::Number | AttrType::Decimal => "double",
        AttrType::Epoch => "bigint",
        AttrType::Boolean => "boolean",
        AttrType::String
        | AttrType::Array
        | AttrType::Reference
        | AttrType::Uuid
        | AttrType::Bytes
        | AttrType::Enum
        | AttrType::Json
        | AttrType::Unknown(_) => "string",
    }
}

/// DDL Iceberg idempotente por entidad. Las tres columnas de sistema primero —
/// el mismo orden que dejaron los streams vivos (`id`, `_tenant`, `created_at`).
pub fn build_ddl(
    entity: &str,
    attributes: &[AttributeDescriptor],
    database: &str,
    lake_bucket: &str,
) -> String {
    let table = table_name(entity);
    let mut cols: Vec<String> = vec![
        "  id string".to_string(),
        "  _tenant string".to_string(),
        "  created_at bigint".to_string(),
    ];
    for attr in attributes {
        cols.push(format!("  {} {}", attr.name, athena_type(&attr.attr_type)));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {database}.{table} (\n{},\n)\
         LOCATION 's3://{lake_bucket}/iceberg-data/{table}/'\n\
         TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='snappy')",
        cols.join(",\n")
    )
}

// ── Drift del delivery stream ───────────────────────────────────────────────

/// Campos del stream que el seeder considera deseados. El logging va SIEMPRE
/// ON (Fase 2) con LogGroup y LogStream explícitos — el API los exige.
pub struct DesiredDestination {
    /// `s3://<lake>/iceberg-data/` — el parámetro cuya ausencia causó el
    /// incidente. Siempre explícito.
    pub warehouse_location: String,
    pub buffering_size_mbs: i32,
    pub buffering_interval_s: i32,
    pub retry_seconds: i32,
}

impl DesiredDestination {
    pub fn for_lake(lake_bucket: &str) -> Self {
        Self {
            warehouse_location: format!("s3://{lake_bucket}/iceberg-data/"),
            buffering_size_mbs: 64,
            buffering_interval_s: 300,
            retry_seconds: 300,
        }
    }
}

/// Retrato de lo que describe el stream vivo — solo los campos que el seeder
/// evalúa. El binario lo construye desde el `describe-delivery-stream` real.
#[derive(Debug, Clone, PartialEq)]
pub struct DestinationSnapshot {
    pub warehouse_location: Option<String>,
    pub logging_enabled: bool,
    pub buffering_size_mbs: i32,
    pub buffering_interval_s: i32,
    pub retry_seconds: i32,
}

/// Veredicto de drift. La distinción importa: el API de Firehose permite
/// actualizar logging/buffering/retry, pero `CatalogConfiguration` es
/// inmutable — sin `WarehouseLocation` solo cabe recrear el stream.
#[derive(Debug, PartialEq)]
pub enum StreamDrift {
    /// Nada que hacer — correr de nuevo no cambia nada (idempotencia).
    Ok,
    /// Campos actualizables en caliente vía `update-destination`.
    NeedsUpdate {
        logging: bool,
        buffering: bool,
        retry: bool,
    },
    /// Drift inmutable: falta `WarehouseLocation`. Recrear el stream es la
    /// única vía; el binario solo lo hace con flag explícito.
    RecreateRequired,
}

pub fn evaluate_drift(current: &DestinationSnapshot, desired: &DesiredDestination) -> StreamDrift {
    if current.warehouse_location.is_none() {
        return StreamDrift::RecreateRequired;
    }
    let logging = !current.logging_enabled;
    let buffering = current.buffering_size_mbs != desired.buffering_size_mbs
        || current.buffering_interval_s != desired.buffering_interval_s;
    let retry = current.retry_seconds != desired.retry_seconds;
    if logging || buffering || retry {
        StreamDrift::NeedsUpdate {
            logging,
            buffering,
            retry,
        }
    } else {
        StreamDrift::Ok
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(name: &str, t: AttrType) -> AttributeDescriptor {
        AttributeDescriptor {
            name: name.to_string(),
            attr_type: t,
            label: None,
            required: false,
            unique: None,
            indexed: false,
            fts: false,
            is_dimension: false,
            is_metric: false,
            entity_ref: None,
            options: Vec::new(),
            is_sequence_scope: false,
            is_sequence_scope_via: false,
            sensitive: false,
            auto_generate: None,
            validation_regex: None,
            default_value: None,
        }
    }

    #[test]
    fn el_naming_coincide_con_olap_channel() {
        assert_eq!(
            stream_name("metri-olap-stream", "audit_log"),
            "metri-olap-stream-audit-log"
        );
        assert_eq!(table_name("audit_log"), "audit_log");
        assert_eq!(
            log_group("metri-olap-stream-domain-fault"),
            "/aws/kinesisfirehose/metri-olap-stream-domain-fault"
        );
    }

    #[test]
    fn mapeo_de_tipos_codice_a_athena() {
        assert_eq!(athena_type(&AttrType::Epoch), "bigint");
        assert_eq!(athena_type(&AttrType::Decimal), "double");
        assert_eq!(athena_type(&AttrType::Number), "double");
        assert_eq!(athena_type(&AttrType::Boolean), "boolean");
        assert_eq!(athena_type(&AttrType::Json), "string");
        assert_eq!(athena_type(&AttrType::Reference), "string");
    }

    #[test]
    fn ddl_con_columnas_de_sistema_primero_y_warehouse_explcito() {
        let attrs = vec![
            attr("tenant_id", AttrType::Reference),
            attr("retryable", AttrType::Boolean),
            attr("occurred_at", AttrType::Epoch),
            attr("reading_value", AttrType::Decimal),
        ];
        let ddl = build_ddl("domain_fault", &attrs, "metri_olap", "mi-lake");
        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS metri_olap.domain_fault"));
        assert!(ddl.contains("  id string,\n  _tenant string,\n  created_at bigint,"));
        assert!(ddl.contains("  tenant_id string,\n  retryable boolean,\n  occurred_at bigint,\n  reading_value double"));
        assert!(ddl.contains("LOCATION 's3://mi-lake/iceberg-data/domain_fault/'"));
        assert!(ddl.contains("'table_type'='ICEBERG'"));
    }

    #[test]
    fn warehouse_ausente_exige_recreacion_y_el_resto_es_actualizable() {
        let desired = DesiredDestination::for_lake("lake");
        // Sin warehouse: recreación, aunque todo lo demás esté perfecto.
        let sin_warehouse = DestinationSnapshot {
            warehouse_location: None,
            logging_enabled: true,
            buffering_size_mbs: 64,
            buffering_interval_s: 300,
            retry_seconds: 300,
        };
        assert_eq!(
            evaluate_drift(&sin_warehouse, &desired),
            StreamDrift::RecreateRequired
        );
        // Logging OFF: actualizable en caliente.
        let logging_off = DestinationSnapshot {
            warehouse_location: Some("s3://lake/iceberg-data/".into()),
            logging_enabled: false,
            buffering_size_mbs: 64,
            buffering_interval_s: 300,
            retry_seconds: 300,
        };
        assert_eq!(
            evaluate_drift(&logging_off, &desired),
            StreamDrift::NeedsUpdate {
                logging: true,
                buffering: false,
                retry: false
            }
        );
        // Idempotencia: segunda corrida sin cambios.
        let ok = DestinationSnapshot {
            warehouse_location: Some("s3://lake/iceberg-data/".into()),
            logging_enabled: true,
            buffering_size_mbs: 64,
            buffering_interval_s: 300,
            retry_seconds: 300,
        };
        assert_eq!(evaluate_drift(&ok, &desired), StreamDrift::Ok);
    }
}
