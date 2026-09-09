//! firehose-seeder — idempotent OLAP infrastructure seeder for Codice entities.
//!
//! Gestor idempotente de la infraestructura OLAP del Códice: para cada entidad
//! `engine: "olap"` asegura (a) la tabla Iceberg en Glue/Athena y (b) el
//! delivery stream de Firehose con su configuración canónica — incluido el
//! `WarehouseLocation` explícito cuya ausencia causó la pérdida de datos del
//! 30-jul (MEDICION_COSTO_OLAP.md §5a).
//!
//! Dry-run por defecto. `--apply` escribe. `make sync-firehose` y
//! `make sync-iceberg` son los puntos de entrada canónicos.
//!
//! Preflight (Regla 05): nada se escribe si el prefijo `iceberg-data/<tabla>/`
//! no existe en el lake — el fallo es accionable, no un stack trace.

// Cerradura final del patrón Result (PLAN_PATRON_RESULT.md §4.4): prohibido
// unwrap/expect/panic en código no-test. Los únicos sitios permitidos son las
// invariantes documentadas en scripts/dev/result_pattern_allowlist.json, cada
// una con su #[allow] y comentario. Bajo cfg(test) se desactiva: los tests
// usan unwrap/expect libremente.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented
    )
)]
use std::path::PathBuf;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_firehose::types::{
    BufferingHints, CatalogConfiguration, CloudWatchLoggingOptions, CompressionFormat,
    DeliveryStreamType, DestinationTableConfiguration, EncryptionConfiguration,
    IcebergS3BackupMode, NoEncryptionConfig, RetryOptions, S3DestinationConfiguration,
};
use metri_engine::codice::registry::{CodeRegistry, EngineChannel};
use metri_engine::infrastructure::seeder::{
    build_ddl, evaluate_drift, log_group, stream_name, table_name, DesiredDestination,
    DestinationSnapshot, StreamDrift,
};

struct Args {
    models_dir: PathBuf,
    lake_bucket: String,
    stream_prefix: String,
    database: String,
    apply: bool,
    only_firehose: bool,
    only_iceberg: bool,
    entity: Option<String>,
    recreate_on_catalog_drift: bool,
    delivery_role_arn: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        models_dir: PathBuf::from("config/models"),
        lake_bucket: std::env::var("AWS_S3_LAKE_BUCKET")
            .unwrap_or_else(|_| "metri-lake-982592308819-us-east-1".to_string()),
        stream_prefix: "metri-olap-stream".to_string(),
        database: "metri_olap".to_string(),
        apply: false,
        only_firehose: false,
        only_iceberg: false,
        entity: None,
        recreate_on_catalog_drift: false,
        delivery_role_arn: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--apply" => args.apply = true,
            "--only-firehose" => args.only_firehose = true,
            "--only-iceberg" => args.only_iceberg = true,
            "--recreate-on-catalog-drift" => args.recreate_on_catalog_drift = true,
            "--models-dir" => args.models_dir = PathBuf::from(next_arg(&mut it, "--models-dir")),
            "--lake-bucket" => args.lake_bucket = next_arg(&mut it, "--lake-bucket"),
            "--stream-prefix" => args.stream_prefix = next_arg(&mut it, "--stream-prefix"),
            "--database" => args.database = next_arg(&mut it, "--database"),
            "--entity" => args.entity = Some(next_arg(&mut it, "--entity")),
            "--delivery-role-arn" => {
                args.delivery_role_arn = Some(next_arg(&mut it, "--delivery-role-arn"))
            }
            other => {
                eprintln!("argumento desconocido: {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = parse_args();
    let cfg = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let glue = aws_sdk_glue::Client::new(&cfg);
    let firehose = aws_sdk_firehose::Client::new(&cfg);
    let athena = aws_sdk_athena::Client::new(&cfg);

    // ── Códice: entidades OLAP ──────────────────────────────────────────────
    let (registry, _) = CodeRegistry::build(&args.models_dir).unwrap_or_else(|e| {
        eprintln!("✗ Códice no cargable desde {:?}: {e}", args.models_dir);
        std::process::exit(2);
    });
    let entities: Vec<String> = registry
        .entity_names()
        .filter(|e| matches!(registry.get_engine(e), Some(EngineChannel::Olap)))
        .map(|s| s.to_string())
        .filter(|e| args.entity.as_ref().map(|f| f == e).unwrap_or(true))
        .collect();
    if entities.is_empty() {
        eprintln!(
            "✗ ninguna entidad engine:olap en el Códice ({:?})",
            args.models_dir
        );
        return std::process::ExitCode::from(2);
    }
    println!(
        "Entidades OLAP ({}): {}",
        entities.len(),
        entities.join(", ")
    );
    println!(
        "Modo: {} | lake: s3://{} | database: {}",
        if args.apply { "APPLY" } else { "dry-run" },
        args.lake_bucket,
        args.database
    );

    let desired = DesiredDestination::for_lake(&args.lake_bucket);
    let mut failures = 0usize;

    // ── Preflight global: el warehouse debe existir en S3 (Regla 05) ────────
    if !prefix_exists(&s3, &args.lake_bucket, "iceberg-data/").await {
        eprintln!(
            "✗ PREFLIGHT FALLIDO: s3://{}/iceberg-data/ no existe.\n  \
             Acción: aws s3api put-object --bucket {} --key iceberg-data/\n  \
             (el error 'Warehouse location does not exist' de Firehose nace aquí)",
            args.lake_bucket, args.lake_bucket
        );
        return std::process::ExitCode::from(1);
    }
    println!("preflight ✓ s3://{}/iceberg-data/", args.lake_bucket);

    // ── Por entidad ─────────────────────────────────────────────────────────
    for entity in &entities {
        let table = table_name(entity);
        let stream = stream_name(&args.stream_prefix, entity);
        println!("\n── {entity} → tabla {table}, stream {stream}");

        // (a) Tabla Iceberg ──
        if !args.only_firehose {
            let prefix = format!("iceberg-data/{table}/");
            if !prefix_exists(&s3, &args.lake_bucket, &prefix).await {
                failures += 1;
                eprintln!(
                    "   ✗ preflight: s3://{}/{prefix} no existe.\n     \
                     Acción: aws s3api put-object --bucket {} --key {prefix}",
                    args.lake_bucket, args.lake_bucket
                );
                continue;
            }
            match glue
                .get_table()
                .database_name(&args.database)
                .name(&table)
                .send()
                .await
            {
                Ok(out) => {
                    let meta = out
                        .table()
                        .and_then(|t| t.parameters())
                        .and_then(|p| p.get("metadata_location"))
                        .cloned();
                    let Some(meta) = meta else {
                        failures += 1;
                        eprintln!("   ✗ tabla {table} sin metadata_location — no es Iceberg. Bórrala del catálogo y vuelve a correr el seeder.");
                        continue;
                    };
                    let key = meta
                        .strip_prefix(&format!("s3://{}/", args.lake_bucket))
                        .unwrap_or(&meta)
                        .to_string();
                    let meta_ok = s3
                        .head_object()
                        .bucket(&args.lake_bucket)
                        .key(&key)
                        .send()
                        .await
                        .is_ok();
                    if meta_ok {
                        println!("   ✓ tabla Iceberg válida");
                    } else {
                        println!("   ⚠ tabla HUECA (metadata borrada del lake: {key}) — recreando");
                        if args.apply {
                            if let Err(e) = glue
                                .delete_table()
                                .database_name(&args.database)
                                .name(&table)
                                .send()
                                .await
                            {
                                failures += 1;
                                eprintln!("   ✗ no pude borrar la tabla hueca: {e}");
                                continue;
                            }
                        }
                        if !ensure_ddl(&athena, entity, &registry, &args).await {
                            failures += 1;
                        }
                    }
                }
                Err(e) => {
                    let svc = e.into_service_error();
                    if !svc.is_entity_not_found_exception() {
                        failures += 1;
                        eprintln!("   ✗ glue get_table: {svc}");
                        continue;
                    }
                    println!("   · tabla inexistente — creando por DDL");
                    if !ensure_ddl(&athena, entity, &registry, &args).await {
                        failures += 1;
                    }
                }
            }
        }

        // (b) Delivery stream ──
        if !args.only_iceberg {
            match describe_stream(&firehose, &stream).await {
                None => {
                    println!("   · stream inexistente — creando con configuración canónica");
                    let Some(role) = args
                        .delivery_role_arn
                        .clone()
                        .or_else(|| std::env::var("FIREHOSE_DELIVERY_ROLE_ARN").ok())
                    else {
                        failures += 1;
                        eprintln!("   ✗ para CREAR un stream hace falta --delivery-role-arn (o env FIREHOSE_DELIVERY_ROLE_ARN): el rol que asume Firehose para escribir S3/Glue/KMS.");
                        continue;
                    };
                    if args.apply {
                        match create_stream(&firehose, &stream, &args, &role, &desired).await {
                            Ok(()) => println!("   ✓ stream creado"),
                            Err(e) => {
                                failures += 1;
                                eprintln!("   ✗ create-delivery-stream: {e}");
                            }
                        }
                    }
                }
                Some(desc) => {
                    let snapshot = DestinationSnapshot {
                        warehouse_location: desc.catalog_warehouse.clone(),
                        logging_enabled: desc.logging_enabled,
                        buffering_size_mbs: desc.buffering_size_mbs,
                        buffering_interval_s: desc.buffering_interval_s,
                        retry_seconds: desc.retry_seconds,
                    };
                    match evaluate_drift(&snapshot, &desired) {
                        StreamDrift::Ok => println!("   ✓ stream conforme"),
                        StreamDrift::RecreateRequired => {
                            if args.recreate_on_catalog_drift && args.apply {
                                println!("   ⚠ drift inmutable (sin WarehouseLocation) — recreando por flag explícito");
                                if let Err(e) = recreate_stream(
                                    &firehose,
                                    &stream,
                                    &args,
                                    &desc.role_arn,
                                    &desired,
                                )
                                .await
                                {
                                    failures += 1;
                                    eprintln!("   ✗ recreación: {e}");
                                } else {
                                    println!("   ✓ stream recreado con WarehouseLocation");
                                }
                            } else {
                                println!(
                                    "   ⚠ RECREACIÓN REQUERIDA: el stream no declara WarehouseLocation (drift inmutable).\n     \
                                     Con la tabla ya materializada el tráfico fluye; para normalizar la config: \
                                     --recreate-on-catalog-drift --apply"
                                );
                            }
                        }
                        StreamDrift::NeedsUpdate {
                            logging,
                            buffering,
                            retry,
                        } => {
                            println!("   ⚠ drift actualizable (logging={logging} buffering={buffering} retry={retry})");
                            if args.apply {
                                if let Err(e) =
                                    update_stream(&firehose, &stream, &desc, &desired).await
                                {
                                    failures += 1;
                                    eprintln!("   ✗ update-destination: {e}");
                                } else {
                                    println!("   ✓ stream actualizado");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!(
        "\nResumen: {} entidades, {} fallos{}",
        entities.len(),
        failures,
        if args.apply {
            ""
        } else {
            " — dry-run, nada escrito"
        }
    );
    if failures > 0 {
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Un prefijo "existe" si hay al menos un objeto debajo — S3 no tiene
/// carpetas reales: la clave-marcador `iceberg-data/` puede no existir aunque
/// el prefijo sí tenga contenido.
async fn prefix_exists(s3: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> bool {
    s3.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1)
        .send()
        .await
        .map(|o| o.key_count().unwrap_or(0) > 0)
        .unwrap_or(false)
}

// ── Tablas: DDL vía Athena ──────────────────────────────────────────────────

async fn ensure_ddl(
    athena: &aws_sdk_athena::Client,
    entity: &str,
    registry: &CodeRegistry,
    args: &Args,
) -> bool {
    let attrs = registry
        .get_attributes(entity)
        .map(|a| a.to_vec())
        .unwrap_or_default();
    let sql = build_ddl(entity, &attrs, &args.database, &args.lake_bucket);
    if !args.apply {
        println!("   · DDL (dry-run): {} …", sql.lines().next().unwrap_or(""));
        return true;
    }
    let out = match athena
        .start_query_execution()
        .query_string(sql)
        .work_group("metri-analytics")
        .result_configuration(
            aws_sdk_athena::types::ResultConfiguration::builder()
                .output_location(format!("s3://{}/athena-results/", args.lake_bucket))
                .build(),
        )
        .send()
        .await
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("   ✗ athena start: {e}");
            return false;
        }
    };
    let Some(qid) = out.query_execution_id().map(|s| s.to_string()) else {
        eprintln!("   ✗ athena sin query id");
        return false;
    };
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Ok(q) = athena
            .get_query_execution()
            .query_execution_id(&qid)
            .send()
            .await
        {
            let Some(exec) = q.query_execution() else {
                continue;
            };
            let state = exec.status().map(|s| s.state()).unwrap_or_default();
            let running = matches!(
                state,
                Some(aws_sdk_athena::types::QueryExecutionState::Running)
                    | Some(aws_sdk_athena::types::QueryExecutionState::Queued)
            );
            if !running {
                let ok = matches!(
                    state,
                    Some(aws_sdk_athena::types::QueryExecutionState::Succeeded)
                );
                if ok {
                    println!("   ✓ tabla creada por DDL");
                } else {
                    let reason = exec
                        .status()
                        .and_then(|s| s.state_change_reason())
                        .unwrap_or_default();
                    eprintln!("   ✗ DDL falló: {reason}");
                }
                return ok;
            }
        }
    }
    eprintln!("   ✗ DDL sin terminar tras 60 s");
    false
}

// ── Streams: describe / update / create / recreate ──────────────────────────

/// None = el stream no existe.
async fn describe_stream(
    firehose: &aws_sdk_firehose::Client,
    stream: &str,
) -> Option<StreamDescription> {
    let out = match firehose
        .describe_delivery_stream()
        .delivery_stream_name(stream)
        .send()
        .await
    {
        Ok(o) => o,
        Err(e) => {
            let svc = e.into_service_error();
            if svc.is_resource_not_found_exception() {
                return None;
            }
            eprintln!("   ✗ describe: {svc}");
            return None;
        }
    };
    let d = out.delivery_stream_description()?;
    let dest = d.destinations().first()?;
    let ice = dest.iceberg_destination_description()?;
    Some(StreamDescription {
        role_arn: ice.role_arn().unwrap_or_default().to_string(),
        destination_id: dest.destination_id().to_string(),
        version_id: d.version_id().to_string(),
        catalog_warehouse: ice
            .catalog_configuration()
            .and_then(|c| c.warehouse_location())
            .map(|s| s.to_string()),
        logging_enabled: ice
            .cloud_watch_logging_options()
            .and_then(|l| l.enabled())
            .unwrap_or(false),
        buffering_size_mbs: ice
            .buffering_hints()
            .and_then(|b| b.size_in_mbs())
            .unwrap_or(0),
        buffering_interval_s: ice
            .buffering_hints()
            .and_then(|b| b.interval_in_seconds())
            .unwrap_or(0),
        retry_seconds: ice
            .retry_options()
            .and_then(|r| r.duration_in_seconds())
            .unwrap_or(0),
        ice: ice.clone(),
    })
}

struct StreamDescription {
    role_arn: String,
    destination_id: String,
    version_id: String,
    catalog_warehouse: Option<String>,
    logging_enabled: bool,
    buffering_size_mbs: i32,
    buffering_interval_s: i32,
    retry_seconds: i32,
    ice: aws_sdk_firehose::types::IcebergDestinationDescription,
}

async fn update_stream(
    firehose: &aws_sdk_firehose::Client,
    stream: &str,
    desc: &StreamDescription,
    desired: &DesiredDestination,
) -> Result<(), metri_engine::domain::errors::DomainError> {
    let ice = &desc.ice;
    let mut upd = IcebergDestinationUpdate::builder()
        .role_arn(desc.role_arn.clone())
        .s3_backup_mode(
            ice.s3_backup_mode()
                .cloned()
                .unwrap_or(IcebergS3BackupMode::FailedDataOnly),
        )
        .buffering_hints(
            BufferingHints::builder()
                .size_in_mbs(desired.buffering_size_mbs)
                .interval_in_seconds(desired.buffering_interval_s)
                .build(),
        )
        .retry_options(
            RetryOptions::builder()
                .duration_in_seconds(desired.retry_seconds)
                .build(),
        )
        .cloud_watch_logging_options(
            CloudWatchLoggingOptions::builder()
                .enabled(true)
                .log_group_name(log_group(stream))
                .log_stream_name("iceberg-delivery")
                .build(),
        );
    if let Some(catalog) = ice.catalog_configuration() {
        // El catálogo es inmutable en update: se pasa idéntico al actual.
        upd = upd.catalog_configuration(catalog.clone());
    }
    for t in ice.destination_table_configuration_list() {
        upd = upd.destination_table_configuration_list(t.clone());
    }
    if let Some(s3d) = ice.s3_destination_description() {
        let mut s3u = S3DestinationConfiguration::builder()
            .role_arn(s3d.role_arn().to_string())
            .bucket_arn(s3d.bucket_arn().to_string())
            .prefix(s3d.prefix().unwrap_or_default().to_string())
            .error_output_prefix(s3d.error_output_prefix().unwrap_or_default())
            .compression_format(::std::clone::Clone::clone(s3d.compression_format()))
            .cloud_watch_logging_options(
                CloudWatchLoggingOptions::builder()
                    .enabled(true)
                    .log_group_name(log_group(stream))
                    .log_stream_name("s3-backup")
                    .build(),
            );
        if let Some(b) = s3d.buffering_hints() {
            s3u = s3u.buffering_hints(
                BufferingHints::builder()
                    .size_in_mbs(b.size_in_mbs().unwrap_or(5))
                    .interval_in_seconds(b.interval_in_seconds().unwrap_or(300))
                    .build(),
            );
        }
        if let Some(enc) = s3d.encryption_configuration() {
            let cloned = EncryptionConfiguration::builder()
                .set_no_encryption_config(enc.no_encryption_config().cloned())
                .set_kms_encryption_config(enc.kms_encryption_config().cloned())
                .build();
            s3u = s3u.encryption_configuration(cloned);
        }
        upd = upd.s3_configuration(
            s3u.build()
                .map_err(|e| fe("S3DestinationUpdate incompleto", &e))?,
        );
    }
    let update = upd.build();
    firehose
        .update_destination()
        .delivery_stream_name(stream)
        .destination_id(&desc.destination_id)
        .current_delivery_stream_version_id(&desc.version_id)
        .iceberg_destination_update(update)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| fe("aws-sdk", &e))
}

async fn create_stream(
    firehose: &aws_sdk_firehose::Client,
    stream: &str,
    args: &Args,
    role_arn: &str,
    desired: &DesiredDestination,
) -> Result<(), metri_engine::domain::errors::DomainError> {
    let slug = stream
        .strip_prefix(&format!("{}-", args.stream_prefix))
        .unwrap_or(stream);
    let table = table_name(&slug.replace('-', "_"));
    let table_cfg = DestinationTableConfiguration::builder()
        .destination_table_name(table)
        .destination_database_name(args.database.clone())
        .unique_keys("id")
        .s3_error_output_prefix(format!(
            "errors/firehose/{slug}/!{{firehose:error-output-type}}/"
        ))
        .build()
        .map_err(|e| fe("aws-sdk", &e))?;
    let config = IcebergDestinationConfiguration::builder()
        .destination_table_configuration_list(table_cfg)
        .buffering_hints(
            BufferingHints::builder()
                .size_in_mbs(desired.buffering_size_mbs)
                .interval_in_seconds(desired.buffering_interval_s)
                .build(),
        )
        .cloud_watch_logging_options(
            CloudWatchLoggingOptions::builder()
                .enabled(true)
                .log_group_name(log_group(stream))
                .log_stream_name("iceberg-delivery")
                .build(),
        )
        .role_arn(role_arn)
        .s3_backup_mode(IcebergS3BackupMode::FailedDataOnly)
        .catalog_configuration(
            CatalogConfiguration::builder()
                .catalog_arn(catalog_arn_for(args))
                .warehouse_location(&desired.warehouse_location)
                .build(),
        )
        .retry_options(
            RetryOptions::builder()
                .duration_in_seconds(desired.retry_seconds)
                .build(),
        )
        .s3_configuration(
            S3DestinationConfiguration::builder()
                .role_arn(role_arn)
                .bucket_arn(format!("arn:aws:s3:::{}", args.lake_bucket))
                .prefix(format!(
                    "firehose-backup/{slug}/year=!{{timestamp:yyyy}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/"
                ))
                .error_output_prefix("errors/firehose/!{firehose:error-output-type}/")
                .buffering_hints(
                    BufferingHints::builder()
                        .size_in_mbs(5)
                        .interval_in_seconds(300)
                        .build(),
                )
                .compression_format(CompressionFormat::Uncompressed)
                .encryption_configuration(
                    EncryptionConfiguration::builder()
                        .no_encryption_config(NoEncryptionConfig::NoEncryption)
                        .build(),
                )
                .cloud_watch_logging_options(
                    CloudWatchLoggingOptions::builder()
                        .enabled(true)
                        .log_group_name(log_group(stream))
                        .log_stream_name("s3-backup")
                        .build(),
                )
                .build()
                .map_err(|e| fe("aws-sdk", &e))?,
        )
        .build()
        .map_err(|e| fe("IcebergDestinationConfiguration incompleta", &e))?;
    firehose
        .create_delivery_stream()
        .delivery_stream_name(stream)
        .delivery_stream_type(DeliveryStreamType::DirectPut)
        .iceberg_destination_configuration(config)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| fe("aws-sdk", &e))
}

async fn recreate_stream(
    firehose: &aws_sdk_firehose::Client,
    stream: &str,
    args: &Args,
    role_arn: &str,
    desired: &DesiredDestination,
) -> Result<(), metri_engine::domain::errors::DomainError> {
    firehose
        .delete_delivery_stream()
        .delivery_stream_name(stream)
        .send()
        .await
        .map_err(|e| fe("delete", &e))?;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(5)).await;
        if describe_stream(firehose, stream).await.is_none() {
            return create_stream(firehose, stream, args, role_arn, desired).await;
        }
    }
    Err(fe(
        "wait-deleting",
        "el stream siguió DELETING más de 150 s",
    ))
}

fn catalog_arn_for(args: &Args) -> String {
    // La región viene del perfil/config del SDK; para el ARN del catálogo
    // Glue basta la default de la cuenta de trabajo.
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let account = std::env::var("AWS_ACCOUNT_ID").unwrap_or_else(|_| "982592308819".to_string());
    let _ = args;
    format!("arn:aws:glue:{region}:{account}:catalog")
}

use aws_sdk_firehose::types::{IcebergDestinationConfiguration, IcebergDestinationUpdate};

/// Valor del flag CLI; sin valor → error de uso y exit(2), nunca pánico.
fn next_arg(it: &mut impl Iterator<Item = String>, flag: &str) -> String {
    match it.next() {
        Some(v) => v,
        None => {
            eprintln!("flag '{flag}' requiere un valor");
            std::process::exit(2);
        }
    }
}

/// Error de operación Firehose/S3 del seeder — INFRA_FIREHOSE_001.
fn fe(context: &str, e: impl std::fmt::Display) -> metri_engine::domain::errors::DomainError {
    metri_engine::domain::errors::DomainError::infra(
        metri_engine::domain::errors::ErrorCode::InfraFirehose001,
        format!("{context}: {e}"),
    )
}
