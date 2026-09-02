// grpc/server.rs — Inicializador del Servidor gRPC Tonic
// SRP: Configura el servidor HTTP/2 y los interceptores para AWS Lambda.

use http::Method;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use super::pb::agent_config_service_server::AgentConfigServiceServer;
use super::pb::metri_service_server::MetriServiceServer;
use super::pb::quota_service_server::QuotaServiceServer;
use super::service::MetriGrpcService;

pub async fn start_lambda_grpc_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = std::env::var("GRPC_PORT").unwrap_or_else(|_| "9090".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    info!("Iniciando gRPC Server (Tonic-Web) en {}...", addr);

    // Lee la tabla EAV y el prefijo OLAP desde env vars (alineado con template.yaml)
    let eav_table =
        std::env::var("EAV_TABLE_NAME").unwrap_or_else(|_| "metri-eav-local".to_string());
    let olap_prefix =
        std::env::var("KINESIS_STREAM_PREFIX").unwrap_or_else(|_| "metri-olap-stream".to_string());

    let ddb_client = Arc::new(crate::infrastructure::dynamodb::DynamoClient::new(&eav_table).await);
    let query_exec =
        crate::eav::reader::query::EavQueryExecutor::new(Arc::clone(&ddb_client), &eav_table);
    let pull_read = crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), &eav_table);

    // El consumo de cuota vive en un contador atómico, fuera del log de datoms.
    // Superponerlo al leer `domain_quota` es lo que permite que el camino de
    // escritura no tenga que mantener una copia al día: ver quota/projection.rs.
    let quota_counter: Arc<dyn crate::quota::QuotaCounter> = Arc::new(
        crate::quota::QuotaLedger::new(Arc::clone(&ddb_client), &eav_table),
    );
    let oltp_exec = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read.clone())
        .with_overlay(Arc::new(crate::quota::QuotaUsageOverlay::new(Arc::clone(
            &quota_counter,
        ))));

    // ── Write Path — channel registry ────────────────────────────────────────
    // OLTP: EavWriter → TransactWriteItems DynamoDB (tabla: EAV_TABLE_NAME)
    let eav_writer = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), &eav_table);
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()),
    );

    // OLAP: Real or Stub Firehose client injection
    let stream_writer: Arc<dyn crate::domain::protocols::IStreamWriter> =
        if std::env::var("KINESIS_MODE").unwrap_or_default() == "stub" {
            info!("[Server] KinesisFirehoseWriter inicializado en MODO STUB (StubStreamWriter)");
            Arc::new(crate::infrastructure::kinesis::StubStreamWriter::new())
        } else {
            Arc::new(crate::infrastructure::kinesis::KinesisFirehoseWriter::new().await)
        };
    let olap_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::olap_channel::OlapChannel::new(&olap_prefix, stream_writer),
    );

    let mut channel_registry: std::collections::HashMap<
        crate::codice::registry::EngineChannel,
        Arc<dyn crate::janus_router::router::IWriteChannel>,
    > = std::collections::HashMap::new();
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Oltp,
        Arc::clone(&oltp_channel),
    );
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Olap,
        Arc::clone(&olap_channel),
    );

    let janus_router = Arc::new(crate::janus_router::router::JanusRouter::new(
        channel_registry,
    ));

    // ── Interceptors y Pipeline ──────────────────────────────────────────────
    let audit_interceptor = Arc::new(
        crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(
            &olap_channel,
        )),
    );

    let athena_engine: Option<Arc<dyn crate::domain::protocols::IQueryEngine>> = if std::env::var(
        "ATHENA_MODE",
    )
    .unwrap_or_default()
        == "stub"
    {
        let lake_bucket =
            std::env::var("AWS_S3_LAKE_BUCKET").unwrap_or_else(|_| "metri-lake-local".to_string());
        info!(
            "[Server] IQueryEngine inicializado en MODO LOCAL (LocalS3QueryEngine) | bucket: {}",
            lake_bucket
        );
        let engine =
            crate::infrastructure::local_s3_query_engine::LocalS3QueryEngine::new(lake_bucket)
                .await;
        Some(Arc::new(engine) as Arc<dyn crate::domain::protocols::IQueryEngine>)
    } else {
        let workgroup =
            std::env::var("ATHENA_WORKGROUP").unwrap_or_else(|_| "metri-analytics".to_string());
        let s3_bucket = std::env::var("AWS_S3_LAKE_BUCKET")
            .unwrap_or_else(|_| "metri-lake-982592308819-us-east-1".to_string());
        let output_location = format!("s3://{s3_bucket}/athena-results/");
        let database =
            std::env::var("GLUE_DATABASE_NAME").unwrap_or_else(|_| "metri_olap".to_string());
        info!("[Server] Inicializando AthenaQueryEngine con workgroup: {}, database: {}, output_location: {}", workgroup, database, output_location);
        let engine = crate::infrastructure::athena::AthenaQueryEngine::new(
            workgroup,
            output_location,
            database,
        )
        .await;
        Some(Arc::new(engine) as Arc<dyn crate::domain::protocols::IQueryEngine>)
    };

    // ── SQS FIFO y Moira Emitter ─────────────────────────────────────────────
    let sqs_mode = std::env::var("SQS_MODE").unwrap_or_default();
    let sqs_bus: Arc<dyn crate::domain::protocols::ISqsBus> = if sqs_mode == "stub" {
        info!("[Server] SqsFifoBus inicializado en MODO STUB (StubSqsBus)");
        Arc::new(crate::infrastructure::sqs::StubSqsBus::new())
    } else {
        let queue_url = std::env::var("OUTBOX_QUEUE_URL")
            .unwrap_or_else(|_| "http://localhost:9324/000000000000/metri-outbox.fifo".to_string());
        info!("[Server] Inicializando SqsFifoBus con queue: {}", queue_url);
        Arc::new(crate::infrastructure::sqs::SqsFifoBus::new(queue_url).await)
    };

    let pull_read_arc = Arc::new(pull_read.clone());
    let moira_emitter = Arc::new(crate::eda::moira::MoiraEmitterImpl::new(
        pull_read_arc,
        Arc::new(eav_writer.clone()),
        sqs_bus,
        oltp_exec.clone(),
    ));

    // Zero-Trust Session Store and Principal Cache initialization
    // Guard fail-closed (Fase 4): solo los entornos no productivos CONOCIDOS
    // usan el secreto por defecto; un ENVIRONMENT ausente, desconocido o
    // productivo exige un secreto fuerte. La decisión vive como función pura
    // en domain::config y está testeada.
    let hmac_secret_str =
        match crate::domain::config::resolve_hmac_secret(
            std::env::var("ENVIRONMENT").ok().as_deref(),
            std::env::var("HMAC_SECRET").ok().as_deref(),
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("FATAL SECURITY ERROR: {e} — Aborting server startup.");
                std::process::exit(1);
            }
        };

    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        hmac_secret_str.into_bytes(),
        Arc::clone(&ddb_client),
        eav_table.clone(),
    ));
    let principal_cache = Arc::new(crate::cedar::authorizer::InMemoryPrincipalCache::new());

    // ── Sherlog / EventBridge Notifier ───────────────────────────────────────
    let eb_mode = std::env::var("EVENTBRIDGE_MODE").unwrap_or_default();
    let fault_bus_name =
        std::env::var("FAULT_BUS_NAME").unwrap_or_else(|_| "metri-faults".to_string());

    let fault_notifier: Arc<dyn crate::iop::sherlog::IFaultNotifier> = if eb_mode == "stub" {
        info!("[Server] EventBridgeNotifier inicializado en MODO STUB (NoopFaultNotifier)");
        Arc::new(crate::iop::sherlog::NoopFaultNotifier)
    } else {
        info!(
            "[Server] Inicializando real EventBridgeNotifier para bus: {}",
            fault_bus_name
        );
        Arc::new(crate::iop::sherlog::EventBridgeNotifier::new(fault_bus_name).await)
    };

    let s3_mode = std::env::var("S3_MODE").unwrap_or_default();
    let export_bucket = std::env::var("AWS_S3_EXPORT_BUCKET")
        .unwrap_or_else(|_| "metri-csv-exports-982592308819-us-east-1".to_string());

    let export_storage: Option<Arc<dyn crate::domain::protocols::IExportStorage>> = if s3_mode
        == "stub"
    {
        info!("[Server] ExportStorage inicializado en MODO STUB (StubExportStorage)");
        Some(Arc::new(
            crate::infrastructure::s3_export::StubExportStorage::new(),
        ))
    } else {
        info!(
            "[Server] Inicializando real S3ExportStorage para bucket: {}",
            export_bucket
        );
        let storage = crate::infrastructure::s3_export::S3ExportStorage::new(export_bucket).await;
        Some(Arc::new(storage) as Arc<dyn crate::domain::protocols::IExportStorage>)
    };

    let grpc_service = MetriGrpcService::new(
        oltp_exec.clone(),
        eav_writer.clone(),
        janus_router,
        audit_interceptor,
        athena_engine,
        Some(Arc::clone(&moira_emitter) as Arc<dyn crate::iop::core::MoiraEmitter>),
        valkey_store,
        principal_cache,
        fault_notifier,
        Arc::clone(&olap_channel),
        export_storage,
    );
    let auth_interceptor = super::interceptors::auth_waf_interceptor;
    let service = MetriServiceServer::with_interceptor(grpc_service, auth_interceptor);
    let web_service = tonic_web::enable(service);

    // ── MoiraRoutingService (Microservicio de Ruteo EDA) ──────────────────────
    let eda_grpc_impl = super::eda_service::EdaGrpcService::new(
        Arc::clone(&moira_emitter) as Arc<dyn crate::iop::core::MoiraEmitter>,
        oltp_exec.clone(),
    );
    let eda_service_raw = super::pb::eda::v1::moira_routing_service_server::MoiraRoutingServiceServer::with_interceptor(eda_grpc_impl, auth_interceptor);
    let eda_web_service = tonic_web::enable(eda_service_raw);

    // ── QuotaService (IA Token Accounting) ───────────────────────────────────
    //
    // Las reservas en vuelo viven en su propia tabla, compartidas entre
    // réplicas: cualquiera puede conciliar o devolver lo que otra reservó.
    // `memory` sirve para un despliegue de una sola réplica o para desarrollo
    // sin la tabla creada; con más de una réplica es incorrecto, porque cada
    // proceso solo vería sus propios tickets.
    let quota_table =
        std::env::var("QUOTA_TABLE").unwrap_or_else(|_| "metri-quota-local".to_string());
    let reservation_store: Arc<dyn crate::quota::ReservationStore> =
        if std::env::var("QUOTA_RESERVATION_STORE").unwrap_or_default() == "memory" {
            tracing::warn!(
                "[Server] Reservas de cuota EN MEMORIA — no válido con más de una réplica"
            );
            Arc::new(crate::quota::MemoryReservationStore::new())
        } else {
            Arc::new(crate::quota::DynamoReservationStore::new(
                Arc::clone(&ddb_client),
                &quota_table,
            ))
        };

    let quota_grpc_impl = super::quota_service::QuotaServiceImpl::new(
        oltp_exec.clone(),
        eav_writer.clone(),
        reservation_store,
    );
    let quota_service_raw = QuotaServiceServer::with_interceptor(quota_grpc_impl, auth_interceptor);
    let quota_web_service = tonic_web::enable(quota_service_raw);

    // ── AgentConfigService (Prompt Dependency Injection) ─────────────────────
    let config_grpc_impl = super::agent_config_service::AgentConfigServiceImpl::new();
    let config_service_raw =
        AgentConfigServiceServer::with_interceptor(config_grpc_impl, auth_interceptor);
    let config_web_service = tonic_web::enable(config_service_raw);

    // FASE 10: Habilitar gRPC Server Reflection para testing (grpcurl)
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(super::pb::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    // Ejecutar semilla de seguridad Master User si corresponde
    if let Err(e) = super::bootstrap::check_and_bootstrap_master(&oltp_exec, &oltp_channel).await {
        tracing::error!("Fallo crítico al inicializar el bootstrap máster: {:?}", e);
    }

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .expose_headers(Any);

    Server::builder()
        .accept_http1(true)
        .layer(cors)
        .add_service(reflection_service)
        .add_service(web_service)
        .add_service(eda_web_service)
        .add_service(quota_web_service)
        .add_service(config_web_service)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    info!("gRPC Server detenido de forma limpia (graceful shutdown completado)");
    Ok(())
}

/// Helper para capturar señales asíncronas de parada (SIGINT o SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("falló al instalar manejador de Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("falló al instalar manejador de señal terminate")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Señal SIGINT (Ctrl+C) capturada. Iniciando graceful shutdown...");
        },
        _ = terminate => {
            info!("Señal SIGTERM (Terminación) capturada. Iniciando graceful shutdown...");
        },
    }
}
